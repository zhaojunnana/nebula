// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_STREAMEXECUTOR_H_
#define GRAPH_EXECUTOR_STREAMEXECUTOR_H_

#include <atomic>
#include <memory>
#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;

class RoundResult {
    public:
     RoundResult() = default;
     RoundResult(std::shared_ptr<DataSet> out, bool hasNextRound, std::string offset):
       output_(out), hasNextRound_(hasNextRound), offset_(std::move(offset)) {}
     std::shared_ptr<DataSet> getOutputData();
     bool hasNextRound();
     std::string getOffset();

    private:
     std::shared_ptr<DataSet> output_{nullptr};
     bool hasNextRound_;
     std::string offset_;
};

class StreamExecutor : public Executor {
 public:
  // Create stream executor according to plan node
  static StreamExecutor *createStream(const PlanNode *node, QueryContext *qctx,
   std::shared_ptr<std::atomic_bool> stopFlag);

  folly::Future<Status> execute() override;

  virtual std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input, std::string offset) = 0;

  int32_t markSubmitTask();

  int32_t markFinishTask(bool hasNextRound);

  void setRootPromise(folly::Promise<Status>&& rootPromise);

  void setSharedStopFLag(std::shared_ptr<std::atomic_bool> stopFlag);

  bool isExecutorStopped();

 protected:
  static StreamExecutor *makeStreamExecutor(const PlanNode *node,
                                            QueryContext *qctx,
                                            std::unordered_map<int64_t, StreamExecutor *> *visited,
                                            std::shared_ptr<std::atomic_bool> stopFlag);

  static StreamExecutor *makeStreamExecutor(QueryContext *qctx, const PlanNode *node);

  StreamExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
      : Executor(name, node, qctx) {}

  void markStopExecutor();

  virtual void markFinishExecutor();

 private:
  bool upStreamFinished();

 protected:
  folly::Promise<Status> rootPromise_;
  bool rootPromiseHasBeenSet_ = false;
  const int32_t batch = 10;

 private:
     std::atomic_int32_t taskCount_ = 0;
     std::atomic_int32_t upStreamFinishCount_ = 0;
     std::shared_ptr<std::atomic_bool> stopFlag_ = {nullptr};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_STREAMEXECUTOR_H_
