//
// Created by admin on 2023/9/7.
//

#include "graph/executor/stream/MockGetNeighborsStreamExecutor.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/Utils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

std::shared_ptr<RoundResult> MockGetNeighborsStreamExecutor::executeOneRound(
  std::shared_ptr<DataSet> input, std::unordered_map<Value, nebula::storage::cpp2::ScanCursor> offset) {
  std::vector<Value> vids;
  vids = input.get()->colValues(nebula::kVid);
  if (vids.empty()) {
    return std::make_shared<RoundResult>();
  }

  time::Duration getNbrTime;
  StorageClient* storageClient = qctx_->getStorageClient();
  QueryExpressionContext qec(qctx()->ectx());
  StorageClient::CommonRequestParam param(expand_->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  std::shared_ptr<RoundResult> roundResult;
  roundResult = storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(vids),
                     {},
                     storage::cpp2::EdgeDirection::OUT_EDGE,
                     nullptr,
                     nullptr,
                     expand_->edgeProps(),
                     nullptr,
                     false,
                     false,
                     std::vector<storage::cpp2::OrderBy>(),
                     1,
                     nullptr,
                     nullptr,
                     offset)
      .via(runner())
      .ensure([this, getNbrTime]() {
        SCOPED_TIMER(&execTime_);
        addState("total_rpc_time", getNbrTime);
      })
      .thenValue([this](nebula::storage::StorageRpcResponse<nebula::storage::cpp2::GetNeighborsResponse>&& resp) {
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        auto& hostLatency = resp.hostLatency();
        for (size_t i = 0; i < hostLatency.size(); ++i) {
          size_t size = 0u;
          auto& result = resp.responses()[i];
          if (result.vertices_ref().has_value()) {
            size = (*result.vertices_ref()).size();
          }
          auto info = util::collectRespProfileData(result.result, hostLatency[i], size);
          addState(folly::sformat("resp[{}]", i), info);
        }

        auto ds = std::make_shared<nebula::DataSet>();
        std::unordered_map<Value, nebula::storage::cpp2::ScanCursor> retCursors;
        bool hasNext  = false;
        for (size_t i = 0; i < hostLatency.size(); ++i) {
            auto& result = resp.responses()[i];
            auto dataset = result.get_vertices();
            if (dataset == nullptr) {
              continue;
            }
            ds->colNames = dataset->colNames;
            for (auto row : dataset->rows) {
              ds->rows.emplace_back(std::move(row));
            }
            
            auto cur = result.get_cursors();
            for(auto it = cur.begin(); it != cur.end(); it++){
              if (it->second.next_cursor_ref().has_value()) {
                hasNext = true;
              }
            }

            retCursors.insert(result.get_cursors().begin(), result.get_cursors().end());
        }
        return std::make_shared<RoundResult>(ds, hasNext, retCursors);
        //return roundResult;
        // return handleResponse(resp);
      }).get();
      return roundResult;
      // return std::make_shared<RoundResult>();
}

std::shared_ptr<RoundResult> MockGetNeighborsStreamExecutor::handleResponse(RpcResponse& resps) {
  // auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  // NG_RETURN_IF_ERROR(result);
  // ResultBuilder builder;
  // builder.state(result.value());

  auto& responses = resps.responses();
  // List list;

  auto ds = std::make_shared<nebula::DataSet>();
  std::unordered_map<Value, nebula::storage::cpp2::ScanCursor> retCursors;
  bool hasNext  = false;
  for (auto& resp : responses) {
    auto dataset = resp.get_vertices();
    if (dataset == nullptr) {
      continue;
    }
    ds->colNames = dataset->colNames;
    for (auto row : dataset->rows) {
      ds->rows.emplace_back(std::move(row));
    }
    
    auto cur = resp.get_cursors();
    for(auto it = cur.begin(); it != cur.end(); it++){
      if (it->second.next_cursor_ref().has_value()) {
        hasNext = true;
      }
    }

    retCursors.insert(resp.get_cursors().begin(), resp.get_cursors().end());

  }
  return std::make_shared<RoundResult>(ds, hasNext, retCursors);
}

}  // namespace graph
}  // namespace nebula