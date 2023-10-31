// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
//
// Created by admin on 2023/9/7.
//

#ifndef GRAPH_EXECUTOR_STREAM_GETNEIGHBORSSTREAMEXECUTOR_H_
#define GRAPH_EXECUTOR_STREAM_GETNEIGHBORSSTREAMEXECUTOR_H_

#include <atomic>
#include <cstdint>
#include "graph/executor/StreamExecutor.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

class MockGetNeighborsStreamExecutor final : public StreamExecutor {
 public:
  MockGetNeighborsStreamExecutor(const PlanNode *node, QueryContext *qctx)
      : StreamExecutor("MockGetNeighborsStreamExecutor", node, qctx) {
    expand_ = asNode<Traverse>(node);
  }

  std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input,
    std::unordered_map<Value, nebula::storage::cpp2::ScanCursor> offset) override;

 private:
  using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;
  std::shared_ptr<RoundResult> handleResponse(RpcResponse& resps);

 private:
  const Traverse* expand_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_STREAM_MOCKGETNEIGHBORSSTREAMEXECUTOR_H_
