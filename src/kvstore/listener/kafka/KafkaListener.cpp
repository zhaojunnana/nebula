/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/listener/kafka/KafkaListener.h"

#include <fcntl.h>
#include <unistd.h>

#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/listener/kafka/KafkaAdapter.h"

DEFINE_int32(kafka_listener_batch_size,
             10000,
             "Max number of entries per batch when kafka listener commits");
DEFINE_string(kafka_topic_prefix, "nebula", "Kafka topic name prefix");
DEFINE_string(kafka_brokers, "", "Kafka broker addresses, e.g. 127.0.0.1:9092,127.0.0.1:9093");
DEFINE_string(kafka_username, "", "Kafka SASL username");
DEFINE_string(kafka_password, "", "Kafka SASL password");
DEFINE_string(kafka_security_protocol,
              "PLAINTEXT",
              "Kafka security protocol, e.g. PLAINTEXT, SASL_PLAINTEXT, SASL_SSL");
DEFINE_string(kafka_sasl_mechanism,
              "",
              "Kafka SASL mechanism, e.g. PLAIN, SCRAM-SHA-256, SCRAM-SHA-512");
DEFINE_int32(kafka_batch_size, 1048576, "Kafka producer batch size in bytes (default 1MB)");
DEFINE_int32(kafka_linger_ms, 50, "Kafka producer linger time in ms for batching");

namespace nebula {
namespace kvstore {

namespace {

/**
 * @brief Convert a nebula Value to folly::dynamic with proper typing.
 * Avoids Value::toString() which wraps strings in extra quotes.
 */
folly::dynamic valueToDynamic(const Value& val) {
  switch (val.type()) {
    case Value::Type::INT:
      return val.getInt();
    case Value::Type::FLOAT:
      return val.getFloat();
    case Value::Type::BOOL:
      return val.getBool();
    case Value::Type::STRING:
      return val.getStr();
    case Value::Type::NULLVALUE:
    case Value::Type::__EMPTY__:
      return nullptr;
    case Value::Type::DATE:
      return val.getDate().toString();
    case Value::Type::TIME:
      return val.getTime().toString();
    case Value::Type::DATETIME:
      return val.getDateTime().toString();
    default:
      return val.toString();
  }
}

}  // anonymous namespace

void KafkaListener::init() {
  auto vRet = schemaMan_->getSpaceVidLen(spaceId_);
  if (!vRet.ok()) {
    LOG(FATAL) << "vid length error";
  }
  vIdLen_ = vRet.value();

  auto vidTypeRet = schemaMan_->getSpaceVidType(spaceId_);
  if (!vidTypeRet.ok()) {
    LOG(FATAL) << "vid type error:" << vidTypeRet.status().message();
  }
  isIntVid_ = vidTypeRet.value() == nebula::cpp2::PropertyType::INT64;

  auto sRet = schemaMan_->toGraphSpaceName(spaceId_);
  if (!sRet.ok()) {
    LOG(FATAL) << "space name error";
  }
  topicName_ = folly::stringPrintf("%s_%s", FLAGS_kafka_topic_prefix.c_str(), sRet.value().c_str());
}

bool KafkaListener::apply(BatchHolder& batch, LogID logId, int64_t timestamp) {
  std::vector<KafkaMessage> messages;
  messages.reserve(batch.getBatch().size());

  int32_t seqInBatch = 0;
  for (const auto& log : batch.getBatch()) {
    auto type = std::get<0>(log);
    const auto& key = std::get<1>(log);
    const auto& value = std::get<2>(log);

    bool isTag = NebulaKeyUtils::isTag(vIdLen_, key);
    bool isEdge = NebulaKeyUtils::isEdge(vIdLen_, key);
    if (!(isTag || isEdge)) {
      continue;
    }

    KafkaMessage msg;
    folly::dynamic payload = folly::dynamic::object;
    payload["spaceId"] = spaceId_;
    payload["partId"] = partId_;
    payload["logId"] = logId;
    payload["timestamp"] = timestamp;
    payload["seq"] = seqInBatch++;

    if (isTag) {
      auto tagId = NebulaKeyUtils::getTagId(vIdLen_, key);

      // Sentinel key: this is a rebuild-index signal, not real vertex data
      if (tagId == 0x7FFFFFFF /*sentinel: rebuild-index signal*/) {
        payload["type"] = "signal";
        payload["graphOperation"] = "REBUILD_INDEX";
        msg.key = "REBUILD_INDEX";
        msg.value = folly::toJson(payload);
        messages.emplace_back(std::move(msg));
        continue;
      }

      auto vertexId = normalizeVid(NebulaKeyUtils::getVertexId(vIdLen_, key).toString());

      payload["type"] = "vertex";
      payload["vertexId"] = vertexId;
      payload["tagId"] = tagId;
      auto tagName = schemaMan_->toTagName(spaceId_, tagId);
      if (tagName.ok()) {
        payload["tagName"] = tagName.value();
      }

      msg.key = vertexId;

      if (type == BatchLogType::OP_BATCH_PUT) {
        payload["operation"] = "PUT";
        // Graph semantic: PUT on a vertex tag = INSERT VERTEX or UPDATE VERTEX
        // (NebulaGraph uses upsert semantics — INSERT and UPDATE both produce PUT)
        payload["graphOperation"] = "UPSERT_VERTEX";
        auto reader = RowReaderWrapper::getTagPropReader(schemaMan_, spaceId_, tagId, value);
        if (reader != nullptr) {
          folly::dynamic props = folly::dynamic::object;
          for (size_t i = 0; i < reader->numFields(); ++i) {
            auto fieldName = reader->getSchema()->getFieldName(i);
            props[fieldName] = valueToDynamic(reader->getValueByIndex(i));
          }
          payload["properties"] = std::move(props);
        } else {
          LOG(ERROR) << "get tag reader failed, tagID " << tagId;
        }
      } else if (type == BatchLogType::OP_BATCH_REMOVE) {
        payload["operation"] = "REMOVE";
        payload["graphOperation"] = "DELETE_VERTEX";
      }
    } else {
      auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, key);
      if (edgeType < 0) {
        // skip reverse edges
        continue;
      }
      auto srcId = normalizeVid(NebulaKeyUtils::getSrcId(vIdLen_, key).toString());
      auto dstId = normalizeVid(NebulaKeyUtils::getDstId(vIdLen_, key).toString());
      auto rank = NebulaKeyUtils::getRank(vIdLen_, key);

      payload["type"] = "edge";
      payload["srcId"] = srcId;
      payload["dstId"] = dstId;
      payload["edgeType"] = edgeType;
      payload["ranking"] = rank;
      auto edgeName = schemaMan_->toEdgeName(spaceId_, edgeType);
      if (edgeName.ok()) {
        payload["edgeName"] = edgeName.value();
      }

      msg.key = srcId;

      if (type == BatchLogType::OP_BATCH_PUT) {
        payload["operation"] = "PUT";
        payload["graphOperation"] = "UPSERT_EDGE";
        auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_, spaceId_, edgeType, value);
        if (reader != nullptr) {
          folly::dynamic props = folly::dynamic::object;
          for (size_t i = 0; i < reader->numFields(); ++i) {
            auto fieldName = reader->getSchema()->getFieldName(i);
            props[fieldName] = valueToDynamic(reader->getValueByIndex(i));
          }
          payload["properties"] = std::move(props);
        } else {
          LOG(ERROR) << "get edge reader failed, edgeType " << edgeType;
        }
      } else if (type == BatchLogType::OP_BATCH_REMOVE) {
        payload["operation"] = "REMOVE";
        payload["graphOperation"] = "DELETE_EDGE";
      }
    }

    msg.value = folly::toJson(payload);
    messages.emplace_back(std::move(msg));
  }

  if (messages.empty()) {
    return true;
  }

  auto kafkaAdapterRes = getKafkaAdapter();
  if (!kafkaAdapterRes.ok()) {
    LOG(ERROR) << kafkaAdapterRes.status();
    return false;
  }
  auto* kafkaAdapter = kafkaAdapterRes.value();
  auto status = kafkaAdapter->sendBatch(messages);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to send messages to Kafka: " << status;
    // Do NOT reset kafkaAdapter_ here: destroying the producer loses its PID,
    // which breaks idempotent-producer deduplication on retry.
    return false;
  }
  return true;
}

bool KafkaListener::persist(LogID lastId, TermID lastTerm, LogID lastApplyLogId) {
  if (!writeAppliedId(lastId, lastTerm, lastApplyLogId)) {
    LOG(FATAL) << "last apply ids write failed";
  }
  return true;
}

std::pair<LogID, TermID> KafkaListener::lastCommittedLogId() {
  if (access(lastApplyLogFile_->c_str(), F_OK) != 0) {
    VLOG(3) << "Invalid or nonexistent file : " << *lastApplyLogFile_;
    return {0, 0};
  }
  int32_t fd = open(lastApplyLogFile_->c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(FATAL) << "Failed to open the file \"" << lastApplyLogFile_->c_str() << "\" (" << errno
               << "): " << strerror(errno);
  }
  // read last logId from listener wal file.
  LogID logId;
  CHECK_EQ(pread(fd, reinterpret_cast<char*>(&logId), sizeof(LogID), 0),
           static_cast<ssize_t>(sizeof(LogID)));

  // read last termId from listener wal file.
  TermID termId;
  CHECK_EQ(pread(fd, reinterpret_cast<char*>(&termId), sizeof(TermID), sizeof(LogID)),
           static_cast<ssize_t>(sizeof(TermID)));
  close(fd);
  return {logId, termId};
}

LogID KafkaListener::lastApplyLogId() {
  if (access(lastApplyLogFile_->c_str(), 0) != 0) {
    VLOG(3) << "Invalid or nonexistent file : " << *lastApplyLogFile_;
    return 0;
  }
  int32_t fd = open(lastApplyLogFile_->c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(FATAL) << "Failed to open the file \"" << lastApplyLogFile_->c_str() << "\" (" << errno
               << "): " << strerror(errno);
  }
  // read last applied logId from listener wal file.
  LogID logId;
  auto offset = sizeof(LogID) + sizeof(TermID);
  CHECK_EQ(pread(fd, reinterpret_cast<char*>(&logId), sizeof(LogID), offset),
           static_cast<ssize_t>(sizeof(LogID)));
  close(fd);
  return logId;
}

bool KafkaListener::writeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId) {
  int32_t fd = open(lastApplyLogFile_->c_str(), O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    VLOG(3) << "Failed to open file \"" << lastApplyLogFile_->c_str() << "\" (errno: " << errno
            << "): " << strerror(errno);
    return false;
  }
  auto raw = encodeAppliedId(lastId, lastTerm, lastApplyLogId);
  ssize_t written = write(fd, raw.c_str(), raw.size());
  if (written != (ssize_t)raw.size()) {
    VLOG(4) << idStr_ << "bytesWritten:" << written << ", expected:" << raw.size()
            << ", error:" << strerror(errno);
    close(fd);
    return false;
  }
  if (fsync(fd) != 0) {
    LOG(ERROR) << idStr_ << "fsync failed: " << strerror(errno);
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

std::string KafkaListener::encodeAppliedId(LogID lastId,
                                           TermID lastTerm,
                                           LogID lastApplyLogId) const {
  std::string val;
  val.reserve(sizeof(LogID) * 2 + sizeof(TermID));
  val.append(reinterpret_cast<const char*>(&lastId), sizeof(LogID))
      .append(reinterpret_cast<const char*>(&lastTerm), sizeof(TermID))
      .append(reinterpret_cast<const char*>(&lastApplyLogId), sizeof(LogID));
  return val;
}

void KafkaListener::processLogs() {
  // Outer loop: keep processing batches while there is a backlog.
  // This avoids returning to doApply() (and potentially sleeping) between batches.
  while (true) {
    std::unique_ptr<LogIterator> iter;
    {
      std::lock_guard<std::mutex> guard(raftLock_);
      if (lastApplyLogId_ >= committedLogId_) {
        return;
      }
      iter = wal_->iterator(lastApplyLogId_ + 1, committedLogId_);
    }

    LogID lastApplyId = -1;
    int64_t lastTimestamp = 0;
    BatchHolder batch;
    while (iter->valid()) {
      lastApplyId = iter->logId();
      auto log = iter->logMsg();
      if (log.empty()) {
        // skip the heartbeat
        ++(*iter);
        continue;
      }

      DCHECK_GE(log.size(), sizeof(int64_t) + 1 + sizeof(uint32_t));
      lastTimestamp = getTimestamp(log);
      switch (log[sizeof(int64_t)]) {
        case OP_PUT: {
          auto pieces = decodeMultiValues(log);
          DCHECK_EQ(2, pieces.size());
          batch.put(pieces[0].toString(), pieces[1].toString());
          break;
        }
        case OP_MULTI_PUT: {
          auto kvs = decodeMultiValues(log);
          DCHECK_EQ(0, kvs.size() % 2);
          for (size_t i = 0; i < kvs.size(); i += 2) {
            batch.put(kvs[i].toString(), kvs[i + 1].toString());
          }
          break;
        }
        case OP_REMOVE: {
          auto key = decodeSingleValue(log);
          batch.remove(key.toString());
          break;
        }
        case OP_REMOVE_RANGE: {
          // Rebuild-index emits OP_REMOVE_RANGE. We inject a sentinel tag key
          // so that apply() produces a REBUILD_INDEX signal for downstream consumers.
          std::string sentinelVid(vIdLen_, '\xFF');
          auto signalKey =
              NebulaKeyUtils::tagKey(vIdLen_, partId_, sentinelVid, 0x7FFFFFFF /*sentinel tagId*/);
          batch.remove(std::move(signalKey));
          break;
        }
        case OP_MULTI_REMOVE: {
          auto keys = decodeMultiValues(log);
          for (auto key : keys) {
            batch.remove(key.toString());
          }
          break;
        }
        case OP_BATCH_WRITE: {
          auto batchData = decodeBatchValue(log);
          for (auto& op : batchData) {
            switch (op.first) {
              case BatchLogType::OP_BATCH_PUT: {
                batch.put(op.second.first.toString(), op.second.second.toString());
                break;
              }
              case BatchLogType::OP_BATCH_REMOVE: {
                batch.remove(op.second.first.toString());
                break;
              }
              case BatchLogType::OP_BATCH_REMOVE_RANGE: {
                LOG(WARNING) << "KafkaListener don't deal with OP_BATCH_REMOVE_RANGE";
                break;
              }
            }
          }
          break;
        }
        case OP_TRANS_LEADER:
        case OP_ADD_LEARNER:
        case OP_ADD_PEER:
        case OP_REMOVE_PEER: {
          break;
        }
        default: {
          LOG(WARNING) << idStr_
                       << "Unknown operation: " << static_cast<int32_t>(log[sizeof(int64_t)]);
        }
      }

      if (static_cast<int32_t>(batch.getBatch().size()) >= FLAGS_kafka_listener_batch_size) {
        break;
      }
      ++(*iter);
    }

    // apply to state machine
    if (lastApplyId != -1 && apply(batch, lastApplyId, lastTimestamp)) {
      std::lock_guard<std::mutex> guard(raftLock_);
      lastApplyLogId_ = lastApplyId;
      persist(committedLogId_, term_, lastApplyLogId_);
      VLOG(2) << idStr_ << "Listener succeeded apply log to " << lastApplyLogId_;
    } else {
      // apply failed or no logs, break out to let doApply() handle sleep/retry
      break;
    }
  }
}

std::tuple<nebula::cpp2::ErrorCode, int64_t, int64_t> KafkaListener::commitSnapshot(
    const std::vector<std::string>& rows,
    LogID committedLogId,
    TermID committedLogTerm,
    bool finished) {
  VLOG(2) << idStr_ << "Listener is committing snapshot.";
  int64_t count = 0;
  int64_t size = 0;
  BatchHolder batch;
  for (const auto& row : rows) {
    count++;
    size += row.size();
    auto kv = decodeKV(row);
    batch.put(kv.first.toString(), kv.second.toString());
  }
  if (!apply(batch, committedLogId, 0)) {
    LOG(INFO) << idStr_ << "Failed to apply data while committing snapshot.";
    return {
        nebula::cpp2::ErrorCode::E_RAFT_PERSIST_SNAPSHOT_FAILED, kNoSnapshotCount, kNoSnapshotSize};
  }
  if (finished) {
    CHECK(!raftLock_.try_lock());
    leaderCommitId_ = committedLogId;
    lastApplyLogId_ = committedLogId;
    persist(committedLogId, committedLogTerm, lastApplyLogId_);
    LOG(INFO) << folly::sformat(
        "Commit snapshot to : committedLogId={},"
        "committedLogTerm={}, lastApplyLogId_={}",
        committedLogId,
        committedLogTerm,
        lastApplyLogId_);
  }
  return {nebula::cpp2::ErrorCode::SUCCEEDED, count, size};
}

std::string KafkaListener::normalizeVid(const std::string& vid) const {
  if (!isIntVid_) {
    return folly::rtrim(folly::StringPiece(vid), [](char c) { return c == '\0'; }).toString();
  } else {
    return std::to_string(*reinterpret_cast<const int64_t*>(vid.data()));
  }
}

StatusOr<KafkaAdapter*> KafkaListener::getKafkaAdapter() {
  if (kafkaAdapter_ && kafkaAdapter_->isValid()) {
    return kafkaAdapter_.get();
  }

  if (FLAGS_kafka_brokers.empty()) {
    LOG(ERROR) << "kafka_brokers is not set";
    return ::nebula::Status::Error("kafka_brokers is not set");
  }

  KafkaClientConfig config;
  config.brokers = FLAGS_kafka_brokers;
  if (!FLAGS_kafka_username.empty()) {
    config.username = FLAGS_kafka_username;
  }
  if (!FLAGS_kafka_password.empty()) {
    config.password = FLAGS_kafka_password;
  }
  config.saslMechanism = FLAGS_kafka_sasl_mechanism;
  config.securityProtocol = FLAGS_kafka_security_protocol;
  config.batchSize = FLAGS_kafka_batch_size;
  config.lingerMs = FLAGS_kafka_linger_ms;
  kafkaAdapter_ = std::make_unique<KafkaAdapter>(std::move(config), topicName_, partId_);
  return kafkaAdapter_.get();
}

}  // namespace kvstore
}  // namespace nebula
