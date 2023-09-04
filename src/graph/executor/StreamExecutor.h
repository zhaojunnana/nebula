// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_STREAMEXECUTOR_H_
#define GRAPH_EXECUTOR_STREAMEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;

class RoundResult {
    public:
     RoundResult() = default;
     RoundResult(std::shared_ptr<DataSet> out, bool hasNextRound, int offset):
       output_(out), hasNextRound_(hasNextRound), offset_(offset) {}
     std::shared_ptr<DataSet> getOutputData();
     bool hasNextRound();
     int getOffset();

    private:
     std::shared_ptr<DataSet> output_{nullptr};
     bool hasNextRound_;
     int offset_;
};

class StreamExecutor : public Executor {
 public:
  // Create stream executor according to plan node
  static StreamExecutor *createStream(const PlanNode *node, QueryContext *qctx);

  folly::Future<Status> execute() override;

  virtual std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input, int64_t offset) = 0;

  int32_t markSubmitTask();

  int32_t markFinishTask(bool hasNextRound);

  void setRootPromise(folly::Promise<Status>&& rootPromise);

 protected:
  static StreamExecutor *makeStreamExecutor(const PlanNode *node,
                                            QueryContext *qctx,
                                            std::unordered_map<int64_t, StreamExecutor *> *visited);

  static StreamExecutor *makeStreamExecutor(QueryContext *qctx, const PlanNode *node);

  StreamExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
      : Executor(name, node, qctx) {}

  virtual void markFinishExecutor();

 private:
  bool upStreamFinished();

 protected:
  folly::Promise<Status> rootPromise_;
  bool rootPromiseHasBeenSet_ = false;
  const int32_t batch = 10;

 private:
     std::atomic_int32_t taskCount = 0;
     std::atomic_int32_t upStreamFinishCount = 0;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_STREAMEXECUTOR_H_
