/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SCHEDULER_ASYNCSTREAMBASEDSCHEDULER_H_
#define GRAPH_SCHEDULER_ASYNCSTREAMBASEDSCHEDULER_H_

#include "graph/scheduler/Scheduler.h"
#include "graph/executor/StreamExecutor.h"

namespace nebula {
namespace graph {

class AsyncStreamBasedScheduler final : public Scheduler {
 public:
  explicit AsyncStreamBasedScheduler(QueryContext* qctx);

  folly::Future<Status> schedule() override;

  void waitFinish() override;

 private:
  folly::Future<Status> doSchedule(StreamExecutor* root) const;

  void submitTask(folly::Executor& pool, StreamExecutor* executor,
    std::shared_ptr<DataSet> input, std::string offset) const;

  void addExecuting(Executor* executor) const;

  void removeExecuting(Executor* executor) const;

  void setFailStatus(Status status) const;

  bool hasFailStatus() const;

  static std::string formatPrettyId(Executor* executor);

  static std::string formatPrettyDependencyTree(Executor* root);

  static void appendExecutor(size_t tabs, Executor* executor, std::stringstream& ss);

 private:
  // base scheduler
  std::unique_ptr<Scheduler> baseScheduler_;
  mutable std::mutex emtx_;
  mutable std::condition_variable cv_;
  mutable size_t executing_{0};
  QueryContext* qctx_{nullptr};
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_SCHEDULER_ASYNCSTREAMBASEDSCHEDULER_H_
