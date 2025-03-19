/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/listener/nebula/NebulaListener.h"

#include <folly/executors/IOThreadPoolExecutor.h>

#include "common/utils/NebulaKeyUtils.h"

DEFINE_string(write_meta_server_addrs, "", "listener write to meta server address");
using nebula::storage::StorageClient;

namespace nebula {
namespace kvstore {
void NebulaListener::init() {
  auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_write_meta_server_addrs);
  if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
    LOG(ERROR) << "Can't get listener metaServer address, status:" << metaAddrsRet.status()
               << ", FLAGS_write_meta_server_addrs:" << FLAGS_write_meta_server_addrs;
    return;
  }
  std::vector<HostAddr> hosts = metaAddrsRet.value();

  auto threadFactory = std::make_shared<folly::NamedThreadFactory>("nebula-listener");
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(16, std::move(threadFactory));
  // Meta client
  meta::MetaClientOptions options;
  options.skipConfig_ = true;
  metaClient_ = std::make_unique<meta::MetaClient>(std::move(ioThreadPool), std::move(hosts), options);
  // Load data try 3 time
  bool loadDataOk = metaClient_->waitForMetadReady(3);
  if (loadDataOk) {
    storage_ = std::make_unique<storage::StorageClient>(std::move(ioThreadPool), metaClient_.get());
  }
  ESListener::init();
  updateWriteSpace();
  executor_ = std::make_unique<folly::IOThreadPoolExecutor>(16);
}

bool NebulaListener::applyBatch(const BatchHolder& batch) {
  // TagID + props
  std::unordered_map<std::string,
                     std::pair<std::unordered_map<TagID, std::vector<std::string>>,
                               std::vector<storage::cpp2::NewVertex>>>
      vertexMap;
  // EdgeType + props
  std::unordered_map<std::string,
                     std::pair<std::vector<std::string>, std::vector<storage::cpp2::NewEdge>>>
      edgeMap;

  std::vector<Value> deleteVertices;
  std::vector<storage::cpp2::EdgeKey> deleteEdges;

  updateWriteSpace();
  for (const auto& log : batch.getBatch()) {
    BatchLogType type = std::get<0>(log);
    const std::string& key = std::get<1>(log);
    const std::string& value = std::get<2>(log);

    bool isTag = nebula::NebulaKeyUtils::isTag(vIdLen_, key);
    bool isEdge = nebula::NebulaKeyUtils::isEdge(vIdLen_, key);
    if (!(isTag || isEdge)) {
      continue;
    }
    nebula::RowReaderWrapper reader;
    if (isTag) {
      auto tagId = NebulaKeyUtils::getTagId(vIdLen_, key);
      auto tagName = schemaMan_->toTagName(spaceId_, tagId);
      if (!tagName.ok()) {
        LOG(ERROR) << "get tag schema failed, tagID " << tagId;
        continue;
      }
      auto writeTagIdRet = metaClient_->getTagIDByNameFromCache(writeSpaceId_, tagName.value());
      if (!writeTagIdRet.ok()) {
        LOG(ERROR) << "get writeTagId failed, tagName " << tagName.value();
        continue;
      }
      auto writeTagId = writeTagIdRet.value();
      if (type == BatchLogType::OP_BATCH_PUT) {
        reader = RowReaderWrapper::getTagPropReader(schemaMan_, spaceId_, tagId, value);
        if (reader == nullptr) {
          LOG(ERROR) << "get tag reader failed, tagID " << tagId;
          continue;
        }
        std::string vkey = std::to_string(writeTagId);
        for (size_t i = 0; i < reader->numFields(); ++i) {
          vkey += reader->getSchema()->getFieldName(i);
        }
        auto& vertexPairRef = vertexMap[vkey];
        auto& tagPropNamesRef = vertexPairRef.first;
        auto& verticesRef = vertexPairRef.second;
        if (tagPropNamesRef.empty()) {
          for (size_t i = 0; i < reader->numFields(); ++i) {
            tagPropNamesRef[writeTagId].emplace_back(reader->getSchema()->getFieldName(i));
          }
        }
        std::string vid = NebulaKeyUtils::getVertexId(vIdLen_, key).toString();
        vid = normalizeVid(vid);
        std::vector<Value> props;
        for (size_t i = 0; i < reader->numFields(); ++i) {
          props.emplace_back(reader->getValueByIndex(i));
        }
        nebula::storage::cpp2::NewVertex newVertex;
        std::vector<nebula::storage::cpp2::NewTag> newTags;
        nebula::storage::cpp2::NewTag newTag;
        newTag.tag_id_ref() = writeTagId;
        newTag.props_ref() = std::move(props);
        newTags.push_back(std::move(newTag));
        newVertex.id_ref() = vid;
        newVertex.tags_ref() = std::move(newTags);
        verticesRef.emplace_back(newVertex);
      } else if (type == BatchLogType::OP_BATCH_REMOVE) {
        std::string vid = NebulaKeyUtils::getVertexId(vIdLen_, key).toString();
        vid = normalizeVid(vid);
        deleteVertices.emplace_back(Value(std::move(vid)));
      }
    } else {
      auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, key);
      if (edgeType < 0) {
        continue;
      }
      auto edgeName = schemaMan_->toEdgeName(spaceId_, edgeType);
      if (!edgeName.ok()) {
        LOG(ERROR) << "get edge schema failed, edgeID " << edgeType;
        continue;
      }
      auto writeEdgeTypeRet =
          metaClient_->getEdgeTypeByNameFromCache(writeSpaceId_, edgeName.value());
      if (!writeEdgeTypeRet.ok()) {
        LOG(ERROR) << "get writeEdgeType failed, edgeName " << edgeName.value();
        continue;
      }
      auto writeEdgeType = writeEdgeTypeRet.value();
      if (type == BatchLogType::OP_BATCH_PUT) {
        reader = RowReaderWrapper::getEdgePropReader(schemaMan_, spaceId_, edgeType, value);
        if (reader == nullptr) {
          LOG(ERROR) << "get edge reader failed, schema ID " << edgeType;
          continue;
        }
        std::string ekey = std::to_string(writeEdgeType);
        for (size_t i = 0; i < reader->numFields(); ++i) {
          ekey += reader->getSchema()->getFieldName(i);
        }
        auto& edgePairRef = edgeMap[ekey];
        auto& edgePropNameRef = edgePairRef.first;
        auto& edgesRef = edgePairRef.second;
        if (edgePropNameRef.empty()) {
          for (size_t i = 0; i < reader->numFields(); ++i) {
            edgePropNameRef.emplace_back(reader->getSchema()->getFieldName(i));
          }
        }
        std::string src = NebulaKeyUtils::getSrcId(vIdLen_, key).toString();
        std::string dst = NebulaKeyUtils::getDstId(vIdLen_, key).toString();
        int rank = 0;
        rank = NebulaKeyUtils::getRank(vIdLen_, key);
        std::vector<Value> props;
        for (size_t i = 0; i < reader->numFields(); ++i) {
          props.emplace_back(reader->getValueByIndex(i));
        }
        nebula::storage::cpp2::NewEdge newEdge;
        nebula::storage::cpp2::EdgeKey edgeKey;
        edgeKey.src_ref() = normalizeVid(src);
        edgeKey.edge_type_ref() = writeEdgeType;
        edgeKey.ranking_ref() = rank;
        edgeKey.dst_ref() = normalizeVid(dst);
        newEdge.key_ref() = std::move(edgeKey);
        newEdge.props_ref() = std::move(props);
        edgesRef.emplace_back(newEdge);
      } else if (type == BatchLogType::OP_BATCH_REMOVE) {
        std::string src = NebulaKeyUtils::getSrcId(vIdLen_, key).toString();
        std::string dst = NebulaKeyUtils::getDstId(vIdLen_, key).toString();
        src = normalizeVid(src);
        dst = normalizeVid(dst);
        int rank = 0;
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.src_ref() = src;
        edgeKey.dst_ref() = dst;
        edgeKey.ranking_ref() = rank;
        edgeKey.edge_type_ref() = writeEdgeType;
        deleteEdges.emplace_back(edgeKey);

        edgeKey.src_ref() = std::move(dst);
        edgeKey.dst_ref() = std::move(src);
        edgeKey.edge_type_ref() = -writeEdgeType;
        deleteEdges.emplace_back(std::move(edgeKey));
      }
    }
  }
  StorageClient::CommonRequestParam param(writeSpaceId_, 1, 1, false);
  if (!vertexMap.empty()) {
    for (const auto& [key, value] : vertexMap) {
      storage_->addVertices(param, value.second, value.first, false, false)
          .via(executor_.get())
          .thenValue([](storage::StorageRpcResponse<storage::cpp2::ExecResponse> rpcResp) {
            auto completeness = rpcResp.completeness();
            if (completeness != 100) {
              const auto& failedCodes = rpcResp.failedParts();
              for (auto failedCode : failedCodes) {
                LOG(ERROR) << "add vertex failed, error "
                           << apache::thrift::util::enumNameSafe(failedCode.second) << ", part "
                           << failedCode.first;
              }
            }
          });
    }
  }
  if (!deleteVertices.empty()) {
    storage_->deleteVertices(param, std::move(deleteVertices))
        .via(executor_.get())
        .thenValue([](storage::StorageRpcResponse<storage::cpp2::ExecResponse> rpcResp) {
          auto completeness = rpcResp.completeness();
          if (completeness != 100) {
            const auto& failedCodes = rpcResp.failedParts();
            for (auto failedCode : failedCodes) {
              LOG(ERROR) << "delete vertex failed, error "
                         << apache::thrift::util::enumNameSafe(failedCode.second) << ", part "
                         << failedCode.first;
            }
          }
        });
  }
  if (!edgeMap.empty()) {
    for (const auto& [key, value] : edgeMap) {
      storage_->addEdges(param, value.second, value.first, false, false)
          .via(executor_.get())
          .thenValue([](storage::StorageRpcResponse<storage::cpp2::ExecResponse> rpcResp) {
            auto completeness = rpcResp.completeness();
            if (completeness != 100) {
              const auto& failedCodes = rpcResp.failedParts();
              for (auto failedCode : failedCodes) {
                LOG(ERROR) << "add edge failed, error "
                           << apache::thrift::util::enumNameSafe(failedCode.second) << ", part "
                           << failedCode.first;
              }
            }
          });
    }
  }
  if (!deleteEdges.empty()) {
    storage_->deleteEdges(param, std::move(deleteEdges))
        .via(executor_.get())
        .thenValue([](storage::StorageRpcResponse<storage::cpp2::ExecResponse> rpcResp) {
          auto completeness = rpcResp.completeness();
          if (completeness != 100) {
            const auto& failedCodes = rpcResp.failedParts();
            for (auto failedCode : failedCodes) {
              LOG(ERROR) << "delete edge failed, error "
                         << apache::thrift::util::enumNameSafe(failedCode.second) << ", part "
                         << failedCode.first;
            }
          }
        });
  }
  return true;
}

void NebulaListener::updateWriteSpace() {
  StatusOr<meta::cpp2::SpaceItem> resp = metaClient_->getSpace(spaceName_->c_str()).get();
  if (!resp.ok()) {
    LOG(ERROR) << "Get space properties failed for space " << spaceName_->c_str();
  }
  writeSpaceId_ = resp.value().get_space_id();
}

void NebulaListener::resetListener() {}

bool NebulaListener::pursueLeaderDone() {
  return true;
}

void NebulaListener::processLogs() {
  std::unique_ptr<LogIterator> iter;
  {
    std::lock_guard<std::mutex> guard(raftLock_);
    if (lastApplyLogId_ >= committedLogId_) {
      return;
    }
    iter = wal_->iterator(lastApplyLogId_ + 1, committedLogId_);
  }

  LogID lastApplyId = -1;
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
        LOG(WARNING) << "ESListener don't deal with OP_REMOVE_RANGE";
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
          // OP_BATCH_REMOVE and OP_BATCH_REMOVE_RANGE is ignored
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
              batch.rangeRemove(op.second.first.toString(), op.second.second.toString());
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
        VLOG(2) << idStr_ << "Unknown operation: " << static_cast<int32_t>(log[0]);
      }
    }

    if (static_cast<int32_t>(batch.size()) > 1000) {
      break;
    }
    ++(*iter);
  }

  // apply to state machine
  if (lastApplyId != -1 && applyBatch(batch)) {
    std::lock_guard<std::mutex> guard(raftLock_);
    lastApplyLogId_ = lastApplyId;
    persist(committedLogId_, term_, lastApplyLogId_);
    VLOG(2) << idStr_ << "Listener succeeded apply log to " << lastApplyLogId_;
  }
}

std::tuple<nebula::cpp2::ErrorCode, int64_t, int64_t> NebulaListener::commitSnapshot(
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
  if (!applyBatch(batch)) {
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

}  // namespace kvstore
}  // namespace nebula