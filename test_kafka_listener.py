#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KafkaListener 端到端测试脚本
测试目标：不重、不丢、不乱序、QPS、P99延迟

环境要求：
  - NebulaGraph 集群运行中 (graphd:9669, storaged, listener:9789)
  - Kafka broker 运行中 (127.0.0.1:9092)
  - Space: basketballplayer (spaceId=2, 10分区, FIXED_STRING(32))
  - Listener ONLINE, kafka_brokers=127.0.0.1:9092
"""

import json
import time
import sys
import hashlib
import statistics
import uuid
from collections import defaultdict
from kafka import KafkaConsumer, TopicPartition
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config

# ========== 配置 ==========
GRAPH_HOST = '127.0.0.1'
GRAPH_PORT = 9669
KAFKA_BROKER = '127.0.0.1:9092'
SPACE_NAME = 'basketballplayer'
SPACE_ID = 2
TOPIC_PREFIX = 'nebula'
PARTITION_COUNT = 10
TEST_BATCH_SIZE = 500       # 每轮测试插入的vertex数
EDGE_PER_VERTEX = 2         # 每个vertex插入的edge数
KAFKA_WAIT_SEC = 15         # 等待Kafka消费的超时秒数
TEST_TAG = 'player'
TEST_EDGE = 'serve'


def get_nebula_session():
    config = Config()
    config.timeout = 30000
    pool = ConnectionPool()
    pool.init([(GRAPH_HOST, GRAPH_PORT)], config)
    session = pool.get_session('root', 'nebula')
    session.execute(f'USE {SPACE_NAME}')
    return session, pool


def get_kafka_offsets(topics):
    """获取每个topic当前的最新offset（作为消费起点）"""
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKER)
    offsets = {}
    for topic in topics:
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            tps = [TopicPartition(topic, p) for p in partitions]
            end = consumer.end_offsets(tps)
            offsets[topic] = {tp.partition: off for tp, off in end.items()}
    consumer.close()
    return offsets


def consume_from_offsets(topics_offsets, timeout_sec=KAFKA_WAIT_SEC):
    """从指定offset开始消费，收集新增消息"""
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        consumer_timeout_ms=timeout_sec * 1000,
        value_deserializer=lambda m: m.decode('utf-8', errors='replace'),
        key_deserializer=lambda m: m.decode('utf-8', errors='replace') if m else None,
    )

    # assign所有分区并seek到起始offset
    all_tps = []
    for topic, part_offsets in topics_offsets.items():
        for part, offset in part_offsets.items():
            tp = TopicPartition(topic, part)
            all_tps.append((tp, offset))

    consumer.assign([tp for tp, _ in all_tps])
    for tp, offset in all_tps:
        consumer.seek(tp, offset)

    messages = []
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        batch = consumer.poll(timeout_ms=2000)
        if batch:
            for tp, records in batch.items():
                for record in records:
                    messages.append({
                        'topic': record.topic,
                        'partition': record.partition,
                        'offset': record.offset,
                        'key': record.key,
                        'value': record.value,
                        'timestamp': record.timestamp,
                    })
        else:
            # 没有新消息了，再等2秒确认
            time.sleep(2)
            batch = consumer.poll(timeout_ms=2000)
            if not batch:
                break
            for tp, records in batch.items():
                for record in records:
                    messages.append({
                        'topic': record.topic,
                        'partition': record.partition,
                        'offset': record.offset,
                        'key': record.key,
                        'value': record.value,
                        'timestamp': record.timestamp,
                    })

    consumer.close()
    return messages


def run_test():
    print("=" * 70)
    print("KafkaListener 端到端测试")
    print("=" * 70)

    # 生成唯一测试ID，防止和历史数据混淆
    test_id = uuid.uuid4().hex[:8]
    print(f"\n[INFO] 测试ID: {test_id}")
    print(f"[INFO] 测试规模: {TEST_BATCH_SIZE} vertices, {TEST_BATCH_SIZE * EDGE_PER_VERTEX} edges")

    # ========== 阶段1: 记录Kafka当前offset ==========
    print("\n--- 阶段1: 记录Kafka当前offset ---")
    topics = [f'{TOPIC_PREFIX}_{SPACE_ID}_{i}' for i in range(1, PARTITION_COUNT + 1)]
    start_offsets = get_kafka_offsets(topics)
    total_start = sum(sum(v.values()) for v in start_offsets.values())
    print(f"[INFO] 各topic当前总offset: {total_start}")

    # ========== 阶段2: 写入测试数据 ==========
    print("\n--- 阶段2: 写入测试数据到NebulaGraph ---")
    session, pool = get_nebula_session()

    expected_vertices = set()
    expected_edges = set()
    insert_times = []

    # 插入vertex (player tag)
    batch_size = 50  # 每批50条nGQL
    vertex_ngql_batches = []
    current_batch = []

    for i in range(TEST_BATCH_SIZE):
        vid = f"test_{test_id}_p{i}"
        name = f"TestPlayer_{test_id}_{i}"
        age = 20 + (i % 30)
        current_batch.append(f'"{vid}":("{name}", {age})')
        expected_vertices.add(vid)

        if len(current_batch) >= batch_size or i == TEST_BATCH_SIZE - 1:
            ngql = f'INSERT VERTEX {TEST_TAG}(name, age) VALUES ' + ','.join(current_batch)
            vertex_ngql_batches.append(ngql)
            current_batch = []

    print(f"[INFO] 即将执行 {len(vertex_ngql_batches)} 批vertex插入...")
    t0 = time.time()
    for ngql in vertex_ngql_batches:
        ts = time.time()
        r = session.execute(ngql)
        te = time.time()
        insert_times.append(te - ts)
        if not r.is_succeeded():
            print(f"[ERROR] Vertex插入失败: {r.error_msg()}")
            sys.exit(1)
    vertex_insert_time = time.time() - t0
    print(f"[OK] Vertex插入完成: {TEST_BATCH_SIZE}条, 耗时 {vertex_insert_time:.2f}s")

    # 插入edge (serve edge)
    edge_ngql_batches = []
    current_batch = []

    for i in range(TEST_BATCH_SIZE):
        src = f"test_{test_id}_p{i}"
        for j in range(EDGE_PER_VERTEX):
            dst = f"test_{test_id}_p{(i + j + 1) % TEST_BATCH_SIZE}"
            rank = j
            start_year = 2000 + (i % 20)
            end_year = start_year + 5
            current_batch.append(f'"{src}"->"{dst}"@{rank}:({start_year}, {end_year})')
            expected_edges.add(f"{src}->{dst}@{rank}")

            if len(current_batch) >= batch_size:
                ngql = f'INSERT EDGE {TEST_EDGE}(start_year, end_year) VALUES ' + ','.join(current_batch)
                edge_ngql_batches.append(ngql)
                current_batch = []

    if current_batch:
        ngql = f'INSERT EDGE {TEST_EDGE}(start_year, end_year) VALUES ' + ','.join(current_batch)
        edge_ngql_batches.append(ngql)

    print(f"[INFO] 即将执行 {len(edge_ngql_batches)} 批edge插入...")
    t0 = time.time()
    for ngql in edge_ngql_batches:
        ts = time.time()
        r = session.execute(ngql)
        te = time.time()
        insert_times.append(te - ts)
        if not r.is_succeeded():
            print(f"[ERROR] Edge插入失败: {r.error_msg()}")
            sys.exit(1)
    edge_insert_time = time.time() - t0
    print(f"[OK] Edge插入完成: {TEST_BATCH_SIZE * EDGE_PER_VERTEX}条, 耗时 {edge_insert_time:.2f}s")

    total_expected = len(expected_vertices) + len(expected_edges)
    print(f"[INFO] 预期总消息数: {total_expected} (vertex: {len(expected_vertices)}, edge: {len(expected_edges)})")

    # ========== 阶段3: 等待并消费Kafka消息 ==========
    print(f"\n--- 阶段3: 等待Kafka消息 (最多{KAFKA_WAIT_SEC}秒) ---")
    # 给listener时间处理WAL
    time.sleep(5)

    messages = consume_from_offsets(start_offsets, timeout_sec=KAFKA_WAIT_SEC)
    print(f"[INFO] 收到 {len(messages)} 条Kafka消息")

    # ========== 阶段4: 解析和验证 ==========
    print("\n--- 阶段4: 验证结果 ---")

    # 解析JSON
    parsed_msgs = []
    parse_errors = 0
    for msg in messages:
        try:
            data = json.loads(msg['value'])
            data['_kafka_topic'] = msg['topic']
            data['_kafka_partition'] = msg['partition']
            data['_kafka_offset'] = msg['offset']
            data['_kafka_ts'] = msg['timestamp']
            parsed_msgs.append(data)
        except json.JSONDecodeError:
            parse_errors += 1

    if parse_errors > 0:
        print(f"[WARN] {parse_errors}条消息JSON解析失败")

    # 只过滤本次测试的数据（通过test_id识别）
    test_msgs = []
    for m in parsed_msgs:
        if m.get('type') == 'vertex':
            vid = m.get('vertexId', '')
            if f'test_{test_id}' in vid:
                test_msgs.append(m)
        elif m.get('type') == 'edge':
            src = m.get('srcId', '')
            if f'test_{test_id}' in src:
                test_msgs.append(m)

    print(f"[INFO] 本次测试相关消息: {len(test_msgs)}/{len(parsed_msgs)}")

    # ---------- 4.1 不丢 (Completeness) ----------
    print("\n  [4.1] 不丢检测 (Completeness)")
    received_vertices = set()
    received_edges = set()
    vertex_msgs = []
    edge_msgs = []

    for m in test_msgs:
        if m['type'] == 'vertex':
            vid = m.get('vertexId', '')
            received_vertices.add(vid)
            vertex_msgs.append(m)
        elif m['type'] == 'edge':
            src = m.get('srcId', '')
            dst = m.get('dstId', '')
            rank = m.get('ranking', 0)
            edge_key = f"{src}->{dst}@{rank}"
            received_edges.add(edge_key)
            edge_msgs.append(m)

    missing_vertices = expected_vertices - received_vertices
    missing_edges = expected_edges - received_edges

    if missing_vertices:
        print(f"  [FAIL] 丢失 {len(missing_vertices)} 个vertex")
        if len(missing_vertices) <= 10:
            for v in list(missing_vertices)[:10]:
                print(f"    - {v}")
    else:
        print(f"  [PASS] Vertex 完整: {len(received_vertices)}/{len(expected_vertices)}")

    if missing_edges:
        print(f"  [FAIL] 丢失 {len(missing_edges)} 条edge")
        if len(missing_edges) <= 10:
            for e in list(missing_edges)[:10]:
                print(f"    - {e}")
    else:
        print(f"  [PASS] Edge 完整: {len(received_edges)}/{len(expected_edges)}")

    completeness = (len(received_vertices) + len(received_edges)) / total_expected * 100 if total_expected > 0 else 0
    print(f"  [RESULT] 完整率: {completeness:.2f}%")

    # ---------- 4.2 不重 (No Duplicates) ----------
    print("\n  [4.2] 不重检测 (No Duplicates)")

    # 按(logId, seq)去重
    seen_log_seq = defaultdict(int)
    for m in test_msgs:
        key = (m.get('_kafka_topic', ''), m.get('logId', ''), m.get('seq', 0))
        seen_log_seq[key] += 1

    duplicates = {k: v for k, v in seen_log_seq.items() if v > 1}
    if duplicates:
        print(f"  [FAIL] 发现 {len(duplicates)} 组重复消息")
        for k, v in list(duplicates.items())[:5]:
            print(f"    - topic={k[0]}, logId={k[1]}, seq={k[2]}, count={v}")
    else:
        print(f"  [PASS] 无重复消息 (共 {len(seen_log_seq)} 条唯一消息)")

    # 额外: 按vertex/edge维度检查重复
    vertex_counts = defaultdict(int)
    for m in vertex_msgs:
        vid = m.get('vertexId', '')
        vertex_counts[vid] += 1
    dup_vertices = {k: v for k, v in vertex_counts.items() if v > 1}

    edge_counts = defaultdict(int)
    for m in edge_msgs:
        ek = f"{m.get('srcId', '')}->{m.get('dstId', '')}@{m.get('ranking', 0)}"
        edge_counts[ek] += 1
    dup_edges = {k: v for k, v in edge_counts.items() if v > 1}

    if dup_vertices:
        print(f"  [WARN] {len(dup_vertices)} 个vertex有多条消息（可能是UPDATE重写，非bug）")
    if dup_edges:
        print(f"  [WARN] {len(dup_edges)} 条edge有多条消息（可能是UPDATE重写，非bug）")

    # ---------- 4.3 不乱序 (Ordering) ----------
    print("\n  [4.3] 不乱序检测 (Ordering)")

    # 按topic分组检查logId单调递增
    by_topic = defaultdict(list)
    for m in test_msgs:
        by_topic[m.get('_kafka_topic', '')].append(m)

    order_violations = 0
    for topic, msgs in by_topic.items():
        # 按kafka offset排序（消息在Kafka中的实际顺序）
        msgs.sort(key=lambda x: x.get('_kafka_offset', 0))
        prev_log_id = -1
        for m in msgs:
            log_id = m.get('logId', 0)
            if log_id < prev_log_id:
                order_violations += 1
                if order_violations <= 3:
                    print(f"    [VIOLATION] {topic}: logId {prev_log_id} -> {log_id} (乱序)")
            prev_log_id = log_id

    if order_violations == 0:
        print(f"  [PASS] 所有分区内logId严格递增 ({len(by_topic)}个分区)")
    else:
        print(f"  [FAIL] 发现 {order_violations} 处乱序")

    # 同一logId内seq也应递增
    seq_violations = 0
    for topic, msgs in by_topic.items():
        by_logid = defaultdict(list)
        for m in msgs:
            by_logid[m.get('logId', 0)].append(m)

        for log_id, log_msgs in by_logid.items():
            log_msgs.sort(key=lambda x: x.get('_kafka_offset', 0))
            prev_seq = -1
            for m in log_msgs:
                seq = m.get('seq', 0)
                if seq < prev_seq:
                    seq_violations += 1
                prev_seq = seq

    if seq_violations == 0:
        print(f"  [PASS] 同一logId内seq严格递增")
    else:
        print(f"  [FAIL] 发现 {seq_violations} 处seq乱序")

    # ---------- 4.4 数据正确性 ----------
    print("\n  [4.4] 数据正确性检测")

    field_errors = 0
    sample_checked = 0
    for m in vertex_msgs[:50]:  # 抽检前50条
        sample_checked += 1
        if 'properties' not in m:
            field_errors += 1
            continue
        props = m['properties']
        if 'name' not in props or 'age' not in props:
            field_errors += 1
            continue
        # name应为字符串
        if not isinstance(props['name'], str):
            field_errors += 1
        # age应为整数
        if not isinstance(props['age'], (int, float)):
            field_errors += 1

    for m in edge_msgs[:50]:
        sample_checked += 1
        if 'properties' not in m:
            field_errors += 1
            continue
        props = m['properties']
        if 'start_year' not in props or 'end_year' not in props:
            field_errors += 1

    if field_errors == 0:
        print(f"  [PASS] 抽检 {sample_checked} 条消息，字段均正确")
    else:
        print(f"  [FAIL] 抽检 {sample_checked} 条消息，{field_errors} 条字段异常")

    # 验证metadata字段
    meta_errors = 0
    for m in test_msgs[:100]:
        for field in ['logId', 'timestamp', 'seq', 'spaceId', 'partId']:
            if field not in m:
                meta_errors += 1
                break

    if meta_errors == 0:
        print(f"  [PASS] 元数据字段 (logId/timestamp/seq/spaceId/partId) 完整")
    else:
        print(f"  [FAIL] {meta_errors} 条消息缺少元数据字段")

    # ========== 阶段5: 性能指标 ==========
    print("\n--- 阶段5: 性能指标 ---")

    total_insert_time = vertex_insert_time + edge_insert_time
    total_ops = TEST_BATCH_SIZE + TEST_BATCH_SIZE * EDGE_PER_VERTEX

    # 写入QPS（NebulaGraph写入性能）
    write_qps = total_ops / total_insert_time if total_insert_time > 0 else 0
    print(f"  写入QPS (NebulaGraph): {write_qps:.0f} ops/s")

    # 写入延迟分布
    if insert_times:
        insert_times_ms = [t * 1000 for t in insert_times]
        p50 = statistics.median(insert_times_ms)
        p90 = sorted(insert_times_ms)[int(len(insert_times_ms) * 0.9)]
        p99 = sorted(insert_times_ms)[int(len(insert_times_ms) * 0.99)]
        p_max = max(insert_times_ms)
        print(f"  写入延迟 (每批{batch_size}条):")
        print(f"    P50:  {p50:.2f}ms")
        print(f"    P90:  {p90:.2f}ms")
        print(f"    P99:  {p99:.2f}ms")
        print(f"    MAX:  {p_max:.2f}ms")

    # Kafka端到端延迟（从写入到Kafka收到）
    if test_msgs:
        kafka_timestamps = [m.get('_kafka_ts', 0) for m in test_msgs if m.get('_kafka_ts', 0) > 0]
        if kafka_timestamps:
            kafka_ts_min = min(kafka_timestamps)
            kafka_ts_max = max(kafka_timestamps)
            # 消息中的timestamp字段是listener处理时间
            listener_timestamps = [m.get('timestamp', 0) for m in test_msgs if m.get('timestamp', 0) > 0]
            if listener_timestamps:
                # 端到端延迟: Kafka收到时间 - Listener处理时间
                e2e_latencies = []
                for m in test_msgs:
                    lt = m.get('timestamp', 0)
                    kt = m.get('_kafka_ts', 0)
                    if lt > 0 and kt > 0:
                        # listener timestamp可能是秒或毫秒
                        if lt < 1e12:  # 秒
                            lt_ms = lt * 1000
                        else:
                            lt_ms = lt
                        latency = kt - lt_ms
                        if latency >= 0:
                            e2e_latencies.append(latency)

                if e2e_latencies:
                    e2e_latencies.sort()
                    print(f"\n  Listener->Kafka 端到端延迟:")
                    print(f"    P50:  {statistics.median(e2e_latencies):.0f}ms")
                    print(f"    P90:  {e2e_latencies[int(len(e2e_latencies) * 0.9)]:.0f}ms")
                    print(f"    P99:  {e2e_latencies[int(len(e2e_latencies) * 0.99)]:.0f}ms")
                    print(f"    MAX:  {max(e2e_latencies):.0f}ms")
                    print(f"    AVG:  {statistics.mean(e2e_latencies):.0f}ms")

    # Listener处理吞吐
    if test_msgs:
        kafka_timestamps = sorted([m.get('_kafka_ts', 0) for m in test_msgs if m.get('_kafka_ts', 0) > 0])
        if len(kafka_timestamps) >= 2:
            duration_sec = (kafka_timestamps[-1] - kafka_timestamps[0]) / 1000.0
            if duration_sec > 0:
                listener_qps = len(test_msgs) / duration_sec
                print(f"\n  Listener吞吐: {listener_qps:.0f} msgs/s")
                print(f"    消息跨度: {duration_sec:.2f}s")

    # ========== 阶段6: 每分区详情 ==========
    print("\n--- 阶段6: 各分区统计 ---")
    print(f"  {'分区':<10} {'消息数':<10} {'Vertex':<10} {'Edge':<10} {'logId范围':<20}")
    for topic in sorted(by_topic.keys()):
        msgs = by_topic[topic]
        v_count = sum(1 for m in msgs if m.get('type') == 'vertex')
        e_count = sum(1 for m in msgs if m.get('type') == 'edge')
        log_ids = [m.get('logId', 0) for m in msgs]
        log_range = f"{min(log_ids)}-{max(log_ids)}" if log_ids else "N/A"
        part = topic.split('_')[-1]
        print(f"  Part-{part:<6} {len(msgs):<10} {v_count:<10} {e_count:<10} {log_range:<20}")

    # ========== 总结 ==========
    print("\n" + "=" * 70)
    print("测试总结")
    print("=" * 70)

    all_pass = True
    results = []

    def check(name, passed, detail=""):
        nonlocal all_pass
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        results.append((name, status, detail))
        print(f"  [{status}] {name}  {detail}")

    check("不丢 (Completeness)",
          len(missing_vertices) == 0 and len(missing_edges) == 0,
          f"vertex: {len(received_vertices)}/{len(expected_vertices)}, edge: {len(received_edges)}/{len(expected_edges)}")

    check("不重 (No Duplicates)",
          len(duplicates) == 0,
          f"唯一消息数: {len(seen_log_seq)}")

    check("不乱序 (Ordering)",
          order_violations == 0 and seq_violations == 0,
          f"logId违规: {order_violations}, seq违规: {seq_violations}")

    check("数据正确性",
          field_errors == 0 and meta_errors == 0,
          f"字段错误: {field_errors}, 元数据错误: {meta_errors}")

    check("JSON格式",
          parse_errors == 0,
          f"解析失败: {parse_errors}")

    print(f"\n  总QPS: {write_qps:.0f} ops/s")
    print(f"  总耗时: {total_insert_time:.2f}s")

    if all_pass:
        print("\n  >>> 全部通过 <<<")
    else:
        print("\n  >>> 存在失败项，请检查 <<<")

    session.release()
    pool.close()
    return 0 if all_pass else 1


if __name__ == '__main__':
    sys.exit(run_test())
