#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KafkaListener end-to-end test suite.

Scenarios:
  correctness  — INSERT + UPDATE + DELETE, verify completeness / no-dup / ordering / payload
  stress       — sustained concurrent writes, measure listener throughput & P99

Usage:
  python test_kafka_listener.py correctness [--vertices N] [--edges-per-vertex N]
  python test_kafka_listener.py stress      [--vertices N] [--writers N] [--duration SEC]
  python test_kafka_listener.py all         (run both)

Environment:
  - NebulaGraph cluster (graphd:9669, storaged, listener)
  - Kafka broker (127.0.0.1:9092)
  - Space: basketballplayer (spaceId=2, 10 partitions, FIXED_STRING(32))
"""

import argparse
import json
import math
import os
import statistics
import sys
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from kafka import KafkaConsumer, TopicPartition
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
GRAPH_HOST = os.getenv("NEBULA_GRAPH_HOST", "127.0.0.1")
GRAPH_PORT = int(os.getenv("NEBULA_GRAPH_PORT", "9669"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")
SPACE_NAME = os.getenv("NEBULA_SPACE", "basketballplayer")
SPACE_ID = int(os.getenv("NEBULA_SPACE_ID", "2"))
TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "nebula")
PARTITION_COUNT = int(os.getenv("NEBULA_PARTITION_COUNT", "10"))

TAG_NAME = "player"
EDGE_NAME = "serve"
NGQL_BATCH = 500  # rows per INSERT statement (nGQL limit is ~4MB per statement)


# ---------------------------------------------------------------------------
# Nebula helpers
# ---------------------------------------------------------------------------
class NebulaPool:
    """Thin wrapper around ConnectionPool for multi-session use."""

    def __init__(self, host=GRAPH_HOST, port=GRAPH_PORT, max_conns=32):
        cfg = Config()
        cfg.timeout = 60000
        cfg.max_connection_pool_size = max_conns
        self._pool = ConnectionPool()
        self._pool.init([(host, port)], cfg)

    def session(self):
        s = self._pool.get_session("root", "nebula")
        s.execute(f"USE {SPACE_NAME}")
        return s

    def close(self):
        self._pool.close()


def _execute(session, ngql):
    r = session.execute(ngql)
    if not r.is_succeeded():
        raise RuntimeError(f"nGQL failed: {r.error_msg()}\n  statement (truncated): {ngql[:200]}")
    return r


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------
def _topics():
    return [f"{TOPIC_PREFIX}_{SPACE_ID}_{i}" for i in range(1, PARTITION_COUNT + 1)]


def snapshot_offsets(broker=KAFKA_BROKER):
    consumer = KafkaConsumer(bootstrap_servers=broker)
    offsets = {}
    for topic in _topics():
        parts = consumer.partitions_for_topic(topic)
        if parts:
            tps = [TopicPartition(topic, p) for p in parts]
            end = consumer.end_offsets(tps)
            offsets[topic] = {tp.partition: off for tp, off in end.items()}
    consumer.close()
    return offsets


def consume_after(start_offsets, timeout_sec, broker=KAFKA_BROKER):
    """Consume all messages produced after *start_offsets*.

    Stops when no new messages arrive for 4 consecutive seconds or *timeout_sec*
    is exceeded.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        auto_offset_reset="latest",
        value_deserializer=lambda m: m.decode("utf-8", errors="replace"),
        key_deserializer=lambda m: m.decode("utf-8", errors="replace") if m else None,
    )
    all_tps = []
    for topic, part_offs in start_offsets.items():
        for part, off in part_offs.items():
            all_tps.append((TopicPartition(topic, part), off))

    consumer.assign([tp for tp, _ in all_tps])
    for tp, off in all_tps:
        consumer.seek(tp, off)

    messages = []
    deadline = time.time() + timeout_sec
    empty_rounds = 0
    while time.time() < deadline:
        batch = consumer.poll(timeout_ms=2000)
        if batch:
            empty_rounds = 0
            for tp, records in batch.items():
                for rec in records:
                    messages.append({
                        "topic": rec.topic,
                        "partition": rec.partition,
                        "offset": rec.offset,
                        "key": rec.key,
                        "value": rec.value,
                        "kafka_ts": rec.timestamp,
                    })
        else:
            empty_rounds += 1
            if empty_rounds >= 2:
                break
    consumer.close()
    return messages


def parse_messages(raw_messages, test_id):
    """Parse JSON and filter to this test run only."""
    parsed, errors = [], 0
    for msg in raw_messages:
        try:
            d = json.loads(msg["value"])
        except (json.JSONDecodeError, TypeError):
            errors += 1
            continue
        d["_topic"] = msg["topic"]
        d["_part"] = msg["partition"]
        d["_offset"] = msg["offset"]
        d["_kts"] = msg["kafka_ts"]
        parsed.append(d)

    mine = []
    for m in parsed:
        t = m.get("type", "")
        if t == "vertex" and test_id in m.get("vertexId", ""):
            mine.append(m)
        elif t == "edge" and test_id in m.get("srcId", ""):
            mine.append(m)
        elif t == "signal":
            pass  # not relevant to test data
    return mine, parsed, errors


# ---------------------------------------------------------------------------
# Percentile helper
# ---------------------------------------------------------------------------
def percentiles(data, *ps):
    if not data:
        return [0.0] * len(ps)
    s = sorted(data)
    n = len(s)
    return [s[min(int(n * p), n - 1)] for p in ps]


# ---------------------------------------------------------------------------
# Verification helpers
# ---------------------------------------------------------------------------
class Verdict:
    def __init__(self):
        self._rows = []

    def check(self, name, passed, detail=""):
        self._rows.append((name, passed, detail))

    @property
    def all_pass(self):
        return all(p for _, p, _ in self._rows)

    def print_summary(self):
        for name, passed, detail in self._rows:
            tag = "PASS" if passed else "FAIL"
            print(f"  [{tag}] {name}  {detail}")


def verify_completeness(test_msgs, expected_vids, expected_edges):
    rx_v, rx_e = set(), set()
    for m in test_msgs:
        if m["type"] == "vertex":
            rx_v.add(m.get("vertexId", ""))
        elif m["type"] == "edge":
            rx_e.add(f'{m["srcId"]}->{m["dstId"]}@{m.get("ranking", 0)}')
    miss_v = expected_vids - rx_v
    miss_e = expected_edges - rx_e
    return miss_v, miss_e, rx_v, rx_e


def verify_no_dup(test_msgs):
    counts = defaultdict(int)
    for m in test_msgs:
        k = (m.get("_topic", ""), m.get("logId", ""), m.get("seq", 0))
        counts[k] += 1
    return {k: v for k, v in counts.items() if v > 1}


def verify_ordering(test_msgs):
    by_topic = defaultdict(list)
    for m in test_msgs:
        by_topic[m.get("_topic", "")].append(m)

    logid_violations, seq_violations = 0, 0
    for topic, msgs in by_topic.items():
        msgs.sort(key=lambda x: x.get("_offset", 0))
        prev = -1
        for m in msgs:
            lid = m.get("logId", 0)
            if lid < prev:
                logid_violations += 1
            prev = lid

        by_logid = defaultdict(list)
        for m in msgs:
            by_logid[m.get("logId", 0)].append(m)
        for lid, lms in by_logid.items():
            lms.sort(key=lambda x: x.get("_offset", 0))
            ps = -1
            for m in lms:
                sq = m.get("seq", 0)
                if sq < ps:
                    seq_violations += 1
                ps = sq

    return logid_violations, seq_violations, by_topic


def verify_payload(test_msgs):
    field_err, meta_err, checked = 0, 0, 0
    for m in test_msgs:
        checked += 1
        for f in ("logId", "timestamp", "seq", "spaceId", "partId"):
            if f not in m:
                meta_err += 1
                break
        if m.get("type") == "vertex" and m.get("graphOperation") in ("UPSERT_VERTEX",):
            props = m.get("properties")
            if not props or "name" not in props or "age" not in props:
                field_err += 1
            elif not isinstance(props["age"], (int, float)):
                field_err += 1
        elif m.get("type") == "edge" and m.get("graphOperation") in ("UPSERT_EDGE",):
            props = m.get("properties")
            if not props or "start_year" not in props:
                field_err += 1
    return field_err, meta_err, checked


def verify_deletes(test_msgs, expected_del_vids, expected_del_edges):
    """Check that DELETE_TAG / DELETE_EDGE messages arrived for every deleted entity."""
    rx_del_v, rx_del_e = set(), set()
    for m in test_msgs:
        op = m.get("graphOperation", "")
        if op == "DELETE_TAG":
            rx_del_v.add(m.get("vertexId", ""))
        elif op == "DELETE_EDGE":
            rx_del_e.add(f'{m["srcId"]}->{m["dstId"]}@{m.get("ranking", 0)}')
    miss_dv = expected_del_vids - rx_del_v
    miss_de = expected_del_edges - rx_del_e
    return miss_dv, miss_de


def verify_updates(test_msgs, updated_vids_expected_age):
    """For UPSERT_VERTEX of updated vids, the *last* message should carry the new age."""
    last_by_vid = {}
    for m in test_msgs:
        if m.get("type") == "vertex" and m.get("graphOperation") == "UPSERT_VERTEX":
            vid = m.get("vertexId", "")
            if vid in updated_vids_expected_age:
                last_by_vid[vid] = m

    wrong = 0
    for vid, expected_age in updated_vids_expected_age.items():
        m = last_by_vid.get(vid)
        if m is None:
            wrong += 1
            continue
        actual = m.get("properties", {}).get("age")
        if actual != expected_age:
            wrong += 1
    return wrong, len(updated_vids_expected_age)


# ---------------------------------------------------------------------------
# Scenario: correctness
# ---------------------------------------------------------------------------
def run_correctness(args):
    num_v = args.vertices
    epv = args.edges_per_vertex
    test_id = uuid.uuid4().hex[:8]

    print("=" * 72)
    print(f"Correctness test   id={test_id}   V={num_v}  E={num_v * epv}")
    print("=" * 72)

    pool = NebulaPool()
    session = pool.session()

    off0 = snapshot_offsets()
    write_start = time.time()

    # ---- Phase 1: INSERT vertices + edges ----
    expected_vids = set()
    expected_edges = set()

    buf = []
    for i in range(num_v):
        vid = f"t{test_id}_v{i}"
        expected_vids.add(vid)
        buf.append(f'"{vid}":("Player_{test_id}_{i}", {20 + i % 40})')
        if len(buf) >= NGQL_BATCH or i == num_v - 1:
            _execute(session, f"INSERT VERTEX {TAG_NAME}(name, age) VALUES " + ",".join(buf))
            buf.clear()

    buf = []
    for i in range(num_v):
        src = f"t{test_id}_v{i}"
        for j in range(epv):
            dst = f"t{test_id}_v{(i + j + 1) % num_v}"
            expected_edges.add(f"{src}->{dst}@{j}")
            buf.append(f'"{src}"->"{dst}"@{j}:({2000 + i % 20}, {2010 + i % 20})')
            if len(buf) >= NGQL_BATCH:
                _execute(session, f"INSERT EDGE {EDGE_NAME}(start_year, end_year) VALUES " + ",".join(buf))
                buf.clear()
    if buf:
        _execute(session, f"INSERT EDGE {EDGE_NAME}(start_year, end_year) VALUES " + ",".join(buf))
        buf.clear()

    insert_done_ts = time.time()
    print(f"[INSERT] {num_v} V + {num_v * epv} E  in {insert_done_ts - write_start:.2f}s")

    # ---- Phase 2: UPDATE (upsert) a subset ----
    update_count = max(1, num_v // 5)
    updated_ages = {}
    buf = []
    for i in range(update_count):
        vid = f"t{test_id}_v{i}"
        new_age = 99
        updated_ages[vid] = new_age
        buf.append(f'"{vid}":("Player_{test_id}_{i}_v2", {new_age})')
        if len(buf) >= NGQL_BATCH or i == update_count - 1:
            _execute(session, f"INSERT VERTEX {TAG_NAME}(name, age) VALUES " + ",".join(buf))
            buf.clear()

    print(f"[UPDATE] {update_count} vertices updated (age -> 99)")

    # ---- Phase 3: DELETE a subset ----
    delete_v_count = max(1, num_v // 10)
    delete_v_start = num_v - delete_v_count
    del_vids = set()
    del_edges = set()

    buf_v = []
    for i in range(delete_v_start, num_v):
        vid = f"t{test_id}_v{i}"
        del_vids.add(vid)
        buf_v.append(f'"{vid}"')
        if len(buf_v) >= NGQL_BATCH or i == num_v - 1:
            _execute(session, f"DELETE TAG {TAG_NAME} FROM " + ",".join(buf_v))
            buf_v.clear()

    buf_e = []
    for i in range(delete_v_start, num_v):
        src = f"t{test_id}_v{i}"
        for j in range(epv):
            dst = f"t{test_id}_v{(i + j + 1) % num_v}"
            del_edges.add(f"{src}->{dst}@{j}")
            buf_e.append(f'"{src}"->"{dst}"@{j}')
            if len(buf_e) >= NGQL_BATCH:
                _execute(session, f"DELETE EDGE {EDGE_NAME} " + ",".join(buf_e))
                buf_e.clear()
    if buf_e:
        _execute(session, f"DELETE EDGE {EDGE_NAME} " + ",".join(buf_e))
        buf_e.clear()

    write_end = time.time()
    print(f"[DELETE] {len(del_vids)} V + {len(del_edges)} E  "
          f"total write phase {write_end - write_start:.2f}s")

    session.release()
    pool.close()

    # ---- Phase 4: consume Kafka ----
    wait_sec = max(30, int((write_end - write_start) * 3))
    print(f"\n[KAFKA] Waiting up to {wait_sec}s for messages ...")
    time.sleep(3)  # let listener flush last batch
    raw = consume_after(off0, wait_sec)
    print(f"[KAFKA] Received {len(raw)} raw messages")

    mine, all_parsed, json_err = parse_messages(raw, test_id)
    print(f"[KAFKA] {len(mine)} matched test_id / {len(all_parsed)} total parsed / {json_err} json errors")

    # ---- Phase 5: verify ----
    v = Verdict()

    miss_v, miss_e, rx_v, rx_e = verify_completeness(mine, expected_vids, expected_edges)
    v.check("Completeness (vertex)",
            len(miss_v) == 0,
            f"{len(rx_v)}/{len(expected_vids)}" + (f"  missing: {list(miss_v)[:5]}" if miss_v else ""))
    v.check("Completeness (edge)",
            len(miss_e) == 0,
            f"{len(rx_e)}/{len(expected_edges)}" + (f"  missing: {list(miss_e)[:5]}" if miss_e else ""))

    dups = verify_no_dup(mine)
    v.check("No duplicates (logId+seq)",
            len(dups) == 0,
            f"dups={len(dups)}")

    lid_v, seq_v, by_topic = verify_ordering(mine)
    v.check("Ordering (logId monotonic)",
            lid_v == 0, f"violations={lid_v}")
    v.check("Ordering (seq within logId)",
            seq_v == 0, f"violations={seq_v}")

    f_err, m_err, checked = verify_payload(mine)
    v.check("Payload correctness",
            f_err == 0 and m_err == 0,
            f"field_err={f_err} meta_err={m_err} checked={checked}")

    v.check("JSON parse",
            json_err == 0,
            f"errors={json_err}")

    upd_wrong, upd_total = verify_updates(mine, updated_ages)
    v.check("Update (last msg has new age)",
            upd_wrong == 0,
            f"wrong={upd_wrong}/{upd_total}")

    miss_dv, miss_de = verify_deletes(mine, del_vids, del_edges)
    v.check("Delete vertex msgs arrived",
            len(miss_dv) == 0,
            f"missing={len(miss_dv)}/{len(del_vids)}")
    v.check("Delete edge msgs arrived",
            len(miss_de) == 0,
            f"missing={len(miss_de)}/{len(del_edges)}")

    # ---- Phase 6: per-partition stats ----
    print(f"\n{'Part':>6} {'Total':>8} {'Vtx':>8} {'Edge':>8} {'Del':>8} {'logId range':>18}")
    for topic in sorted(by_topic.keys()):
        msgs = by_topic[topic]
        nv = sum(1 for m in msgs if m.get("type") == "vertex" and m.get("graphOperation") == "UPSERT_VERTEX")
        ne = sum(1 for m in msgs if m.get("type") == "edge" and m.get("graphOperation") == "UPSERT_EDGE")
        nd = sum(1 for m in msgs if m.get("graphOperation", "").startswith("DELETE"))
        lids = [m.get("logId", 0) for m in msgs]
        lr = f"{min(lids)}-{max(lids)}" if lids else "N/A"
        part = topic.rsplit("_", 1)[-1]
        print(f"  {part:>4} {len(msgs):>8} {nv:>8} {ne:>8} {nd:>8} {lr:>18}")

    print("\n" + "=" * 72)
    v.print_summary()
    print("=" * 72)
    return v.all_pass


# ---------------------------------------------------------------------------
# Scenario: stress
# ---------------------------------------------------------------------------
def run_stress(args):
    num_v = args.vertices
    writers = args.writers
    duration = args.duration
    test_id = uuid.uuid4().hex[:8]

    print("=" * 72)
    print(f"Stress test   id={test_id}   target_V={num_v}  writers={writers}  duration={duration}s")
    print("=" * 72)

    pool = NebulaPool(max_conns=writers + 4)
    off0 = snapshot_offsets()

    counter_lock = Lock()
    total_written = [0]
    write_latencies = []  # per-statement latency in ms
    errors = [0]
    stop_flag = [False]

    def writer_fn(worker_id, chunk_start, chunk_end):
        s = pool.session()
        local_lat = []
        try:
            buf = []
            for i in range(chunk_start, chunk_end):
                if stop_flag[0]:
                    break
                vid = f"s{test_id}_w{worker_id}_v{i}"
                buf.append(f'"{vid}":("Stress_{i}", {i % 60})')
                if len(buf) >= NGQL_BATCH:
                    ngql = f"INSERT VERTEX {TAG_NAME}(name, age) VALUES " + ",".join(buf)
                    t0 = time.monotonic()
                    try:
                        _execute(s, ngql)
                    except RuntimeError as e:
                        with counter_lock:
                            errors[0] += 1
                        buf.clear()
                        continue
                    elapsed_ms = (time.monotonic() - t0) * 1000
                    local_lat.append(elapsed_ms)
                    with counter_lock:
                        total_written[0] += len(buf)
                    buf.clear()

            if buf and not stop_flag[0]:
                ngql = f"INSERT VERTEX {TAG_NAME}(name, age) VALUES " + ",".join(buf)
                t0 = time.monotonic()
                try:
                    _execute(s, ngql)
                except RuntimeError:
                    with counter_lock:
                        errors[0] += 1
                else:
                    elapsed_ms = (time.monotonic() - t0) * 1000
                    local_lat.append(elapsed_ms)
                    with counter_lock:
                        total_written[0] += len(buf)
        finally:
            s.release()
        return local_lat

    chunk = math.ceil(num_v / writers)
    wall_start = time.monotonic()

    with ThreadPoolExecutor(max_workers=writers) as executor:
        futures = []
        for w in range(writers):
            cs = w * chunk
            ce = min(cs + chunk, num_v)
            if cs >= ce:
                break
            futures.append(executor.submit(writer_fn, w, cs, ce))

        # Soft time-limit: signal stop after *duration* seconds
        deadline = time.monotonic() + duration
        for f in as_completed(futures, timeout=max(duration * 3, 120)):
            lats = f.result()
            write_latencies.extend(lats)
            if time.monotonic() > deadline:
                stop_flag[0] = True

    wall_elapsed = time.monotonic() - wall_start
    pool.close()

    print(f"\n[WRITE] {total_written[0]} vertices in {wall_elapsed:.2f}s  "
          f"= {total_written[0] / wall_elapsed:.0f} v/s  errors={errors[0]}")

    if write_latencies:
        p50, p90, p99 = percentiles(write_latencies, 0.5, 0.9, 0.99)
        print(f"[WRITE latency per {NGQL_BATCH}-row batch]  "
              f"P50={p50:.1f}ms  P90={p90:.1f}ms  P99={p99:.1f}ms  MAX={max(write_latencies):.1f}ms")

    # ---- Consume & measure listener throughput ----
    wait_sec = max(30, int(wall_elapsed * 2))
    print(f"\n[KAFKA] Waiting up to {wait_sec}s ...")
    time.sleep(3)
    raw = consume_after(off0, wait_sec)
    mine, _, _ = parse_messages(raw, test_id)
    print(f"[KAFKA] {len(mine)} messages for this test run  (total raw: {len(raw)})")

    if len(mine) < 2:
        print("[WARN] Too few messages to compute throughput")
        return True

    # Listener throughput: messages span in Kafka timestamps
    kts = sorted(m.get("_kts", 0) for m in mine if m.get("_kts", 0) > 0)
    if len(kts) >= 2 and (kts[-1] - kts[0]) > 0:
        span_s = (kts[-1] - kts[0]) / 1000.0
        listener_qps = len(mine) / span_s
        print(f"[LISTENER throughput]  {listener_qps:.0f} msg/s  span={span_s:.2f}s  msgs={len(mine)}")
    else:
        print("[WARN] Kafka timestamps too close to measure throughput")

    # E2E latency: wallclock approach
    # We recorded write_start wallclock; Kafka messages carry broker timestamp.
    # Since they're on the same machine in test, the diff is meaningful.
    write_wall_start_ms = wall_start * 1000  # monotonic, not wallclock — skip cross-clock diff
    # Instead, measure "drain time": how long after writes finished until last msg appeared
    if kts:
        last_kafka_ms = kts[-1]
        write_end_wall_ms = time.time() * 1000  # approximate; real end was earlier
        # More useful: total pipeline time = last kafka ts - first kafka ts (already shown above)
        # and "completeness lag" = time from write-finish to last-kafka-message
        print(f"[LISTENER lag]  last Kafka msg was at offset {kts[-1]}  "
              f"({len(mine)} msgs over {(kts[-1] - kts[0]) / 1000:.1f}s)")

    # Per-batch latency inside listener (from message timestamp field in payload)
    batch_latencies = []
    for m in mine:
        lt = m.get("timestamp", 0)
        kt = m.get("_kts", 0)
        if lt > 0 and kt > 0:
            lt_ms = lt * 1000 if lt < 1e12 else lt
            d = kt - lt_ms
            if 0 <= d < 300_000:  # discard nonsense values
                batch_latencies.append(d)

    if batch_latencies:
        p50, p90, p99 = percentiles(batch_latencies, 0.5, 0.9, 0.99)
        print(f"[LISTENER->Kafka latency]  "
              f"P50={p50:.0f}ms  P90={p90:.0f}ms  P99={p99:.0f}ms  "
              f"MAX={max(batch_latencies):.0f}ms  samples={len(batch_latencies)}")

    # Delivery ratio
    ratio = len(mine) / total_written[0] * 100 if total_written[0] else 0
    print(f"[DELIVERY] {len(mine)}/{total_written[0]} = {ratio:.1f}%")
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="KafkaListener test suite")
    sub = ap.add_subparsers(dest="scenario")

    c = sub.add_parser("correctness", help="Functional correctness test")
    c.add_argument("--vertices", type=int, default=2000)
    c.add_argument("--edges-per-vertex", type=int, default=2)

    s = sub.add_parser("stress", help="Sustained throughput stress test")
    s.add_argument("--vertices", type=int, default=50000)
    s.add_argument("--writers", type=int, default=8)
    s.add_argument("--duration", type=int, default=60, help="soft time-limit in seconds")

    a = sub.add_parser("all", help="Run correctness then stress")
    a.add_argument("--vertices", type=int, default=2000)
    a.add_argument("--edges-per-vertex", type=int, default=2)
    a.add_argument("--stress-vertices", type=int, default=50000)
    a.add_argument("--writers", type=int, default=8)
    a.add_argument("--duration", type=int, default=60)

    args = ap.parse_args()
    if not args.scenario:
        ap.print_help()
        return 1

    ok = True
    if args.scenario in ("correctness", "all"):
        ok = run_correctness(args) and ok

    if args.scenario == "stress":
        run_stress(args)
    elif args.scenario == "all":
        # build a namespace the stress function expects
        ns = argparse.Namespace(
            vertices=args.stress_vertices,
            writers=args.writers,
            duration=args.duration,
        )
        run_stress(ns)

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
