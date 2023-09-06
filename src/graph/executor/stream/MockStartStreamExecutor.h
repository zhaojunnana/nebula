// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_STREAM_MOCKSTARTSTREAMEXECUTOR_H_
#define GRAPH_EXECUTOR_STREAM_MOCKSTARTSTREAMEXECUTOR_H_

#include "graph/executor/StreamExecutor.h"
#include "graph/planner/plan/Query.h"
// used in lookup and match scenarios.
// fetch data from storage layer, according to the index selected by the optimizer.
namespace nebula {
namespace graph {

class MockStartStreamExecutor final : public StreamExecutor {
 public:
  MockStartStreamExecutor(const PlanNode *node, QueryContext *qctx)
      : StreamExecutor("MockStartStreamExecutor", node, qctx) {
  }

  std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input, std::string offset) override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_STREAM_MOCKSTARTSTREAMEXECUTOR_H_
