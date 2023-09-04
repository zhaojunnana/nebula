// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_STREAM_STREAMCOLLECTEXECUTOR_H_
#define GRAPH_EXECUTOR_STREAM_STREAMCOLLECTEXECUTOR_H_

#include "common/datatypes/DataSet.h"
#include "graph/executor/StreamExecutor.h"
#include "graph/planner/plan/Query.h"
// used in lookup and match scenarios.
// fetch data from storage layer, according to the index selected by the optimizer.
namespace nebula {
namespace graph {

class StreamCollectExecutor final : public StreamExecutor {
 public:
  StreamCollectExecutor(const PlanNode *node, QueryContext *qctx)
      : StreamExecutor("StreamCollectExecutor", node, qctx), mutex_() {
  }

  std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input, int64_t offset) override;

 protected:
  void markFinishExecutor() override;

 private:
  DataSet finalDataSet_;
  std::mutex mutex_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_STREAM_STREAMCOLLECTEXECUTOR_H_
