// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_STREAM_MOCKTRANSPORTSTREAMEXECUTOR_H_
#define GRAPH_EXECUTOR_STREAM_MOCKTRANSPORTSTREAMEXECUTOR_H_

#include "graph/executor/StreamExecutor.h"
#include "graph/planner/plan/Query.h"
// used in lookup and match scenarios.
// fetch data from storage layer, according to the index selected by the optimizer.
namespace nebula {
namespace graph {

class MockTransportStreamExecutor final : public StreamExecutor {
 public:
  MockTransportStreamExecutor(const PlanNode *node, QueryContext *qctx)
      : StreamExecutor("MockTransportStreamExecutor", node, qctx) {
  }

  std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input, std::unordered_map<Value, nebula::storage::cpp2::ScanCursor> offset) override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_STREAM_MOCKTRANSPORTSTREAMEXECUTOR_H_
