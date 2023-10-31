// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_STREAM_INDEXSCANSTREAMEXECUTOR_H_
#define GRAPH_EXECUTOR_STREAM_INDEXSCANSTREAMEXECUTOR_H_

#include "graph/executor/StorageAccessStreamExecutor.h"
#include "graph/planner/plan/Query.h"
// used in lookup and match scenarios.
// fetch data from storage layer, according to the index selected by the optimizer.
namespace nebula {
namespace graph {
using ClusterCursors = std::unordered_map<HostAddr, Offset>;

class IndexScanStreamExecutor final : public StorageAccessStreamExecutor {
 public:
  IndexScanStreamExecutor(const PlanNode *node, QueryContext *qctx)
      : StorageAccessStreamExecutor("IndexScanStreamExecutor", node, qctx) {
    gn_ = asNode<IndexScan>(node);
  }

  std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input, Offset offset) override;

 private:
  folly::Future<StatusOr<std::shared_ptr<RoundResult>>> indexScan(Offset& offset);

  ClusterCursors transToClusterCursors(Offset& offset);

  template <typename Resp>
  StatusOr<std::shared_ptr<RoundResult>> handleResp(storage::StorageRpcResponse<Resp> &&rpcResp);

 private:
  const IndexScan *gn_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_STREAM_INDEXSCANSTREAMEXECUTOR_H_
