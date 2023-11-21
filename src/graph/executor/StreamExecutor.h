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
using Offset = std::unordered_map<Value, nebula::storage::cpp2::ScanCursor>;

class PlanNode;
class QueryContext;

class RoundResult {
    public:
     RoundResult() = default;
     RoundResult(std::shared_ptr<DataSet> out, bool hasNextRound, Offset offset):
       output_(out), hasNextRound_(hasNextRound), offset_(std::move(offset)) {}
     std::shared_ptr<DataSet> getOutputData();
     bool hasNextRound();
     Offset getOffset();

    private:
     std::shared_ptr<DataSet> output_{nullptr};
     bool hasNextRound_;
     Offset offset_;
};

class StreamExecutor : public Executor {
 public:
  // Create stream executor according to plan node
  static StreamExecutor *createStream(const PlanNode *node, QueryContext *qctx);

  folly::Future<Status> execute() override;

  virtual std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input, Offset offset) = 0;

  int32_t markSubmitTask();

  int32_t markFinishTask(bool hasNextRound);

  void setRootPromise(folly::Promise<Status>&& rootPromise);

  void setSharedAbnormalStatus(std::shared_ptr<Status> abnormalStatus);

  bool isExecutorStopped();

 protected:
  static StreamExecutor *makeStreamExecutor(const PlanNode *node,
                                            QueryContext *qctx,
                                            std::unordered_map<int64_t, StreamExecutor *> *visited,
                                            std::shared_ptr<Status> sharedAbnormalStatus);

  static StreamExecutor *makeStreamExecutor(QueryContext *qctx, const PlanNode *node);

  StreamExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
      : Executor(name, node, qctx) {}

  void markStopExecutor();

  virtual void markFinishExecutor();

  int32_t getBatchSize();

 private:
  bool upStreamFinished();

 protected:
  folly::Promise<Status> rootPromise_;
  bool rootPromiseHasBeenSet_ = false;
  std::shared_ptr<Status> abnormalStatus_ = {nullptr};

 private:
  std::atomic_int32_t taskCount_ = 0;
  std::atomic_int32_t upStreamFinishCount_ = 0;
  std::atomic_bool stopFlag_ = std::atomic_bool(false);
  std::atomic_bool finishFlag_ = std::atomic_bool(false);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_STREAMEXECUTOR_H_
