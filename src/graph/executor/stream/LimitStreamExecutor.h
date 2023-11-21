// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_STREAM_LIMITSTREAMEXECUTOR_H_
#define GRAPH_EXECUTOR_STREAM_LIMITSTREAMEXECUTOR_H_

#include <atomic>
#include <cstdint>
#include "graph/executor/StreamExecutor.h"
#include "graph/planner/plan/Query.h"
// used in lookup and match scenarios.
// fetch data from storage layer, according to the index selected by the optimizer.
namespace nebula {
namespace graph {

class LimitStreamExecutor final : public StreamExecutor {
 public:
  LimitStreamExecutor(const PlanNode *node, QueryContext *qctx)
      : StreamExecutor("LimitStreamExecutor", node, qctx) {
    auto* limit = asNode<Limit>(node);
    QueryExpressionContext qec(ectx_);
    limit_ = static_cast<std::size_t>(limit->count(qec));
    DLOG(INFO) << "LimitStreamExecutor limit size is " << limit_;
  }

  std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input, Offset offset) override;
 private:
  std::atomic_int64_t counter_ = 0;
  int64_t limit_ = -1;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_STREAM_LIMITSTREAMEXECUTOR_H_
