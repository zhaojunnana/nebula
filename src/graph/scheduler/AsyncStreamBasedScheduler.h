/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SCHEDULER_ASYNCSTREAMBASEDSCHEDULER_H_
#define GRAPH_SCHEDULER_ASYNCSTREAMBASEDSCHEDULER_H_

#include <string>
#include <unordered_set>
#include <vector>
#include "graph/planner/plan/PlanNode.h"
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
    std::shared_ptr<DataSet> input,
    std::unordered_map<Value, nebula::storage::cpp2::ScanCursor> offset) const;

  void addExecuting(Executor* executor) const;

  void removeExecuting(Executor* executor) const;

  void setFailStatus(Status status) const;

  bool hasFailStatus() const;

  static std::string formatPrettyId(Executor* executor);

  static std::string formatPrettyDependencyTree(Executor* root);

  static void appendExecutor(size_t tabs, Executor* executor, std::stringstream& ss);

  static void buildPlanPattern(PlanNode* root, std::vector<PlanNode::Kind>& pattern);

 private:
  // base scheduler
  std::unique_ptr<Scheduler> baseScheduler_;
  mutable std::mutex emtx_;
  mutable std::condition_variable cv_;
  mutable size_t executing_{0};
  QueryContext* qctx_{nullptr};
};

static std::string buildPatternString(std::vector<PlanNode::Kind> pattern) {
  std::string str = "";
  for (size_t i = 0; i < pattern.size(); i++) {
    if (i > 0) {
      str.append("-");
    }
    auto part = std::to_string(static_cast<uint8_t>(pattern[i]));
    str.append(part);
  }
  return str;
}

static std::unordered_set<std::string> initStreamPatterns() {
  std::unordered_set<std::string> streamPatterns;
  streamPatterns.insert(buildPatternString({
    PlanNode::Kind::kProject,
    PlanNode::Kind::kLimit,
    PlanNode::Kind::kAppendVertices,
    PlanNode::Kind::kTraverse,
    PlanNode::Kind::kIndexScan,
    PlanNode::Kind::kStart
    }));
  streamPatterns.insert(buildPatternString({
    PlanNode::Kind::kProject,
    PlanNode::Kind::kLimit,
    PlanNode::Kind::kFilter,
    PlanNode::Kind::kAppendVertices,
    PlanNode::Kind::kTraverse,
    PlanNode::Kind::kIndexScan,
    PlanNode::Kind::kStart
    }));
  streamPatterns.insert(buildPatternString({
    PlanNode::Kind::kProject,
    PlanNode::Kind::kLimit,
    PlanNode::Kind::kFilter,
    PlanNode::Kind::kAppendVertices,
    PlanNode::Kind::kFilter,
    PlanNode::Kind::kTraverse,
    PlanNode::Kind::kIndexScan,
    PlanNode::Kind::kStart
    }));
  streamPatterns.insert(buildPatternString({
    PlanNode::Kind::kProject,
    PlanNode::Kind::kLimit,
    PlanNode::Kind::kAppendVertices,
    PlanNode::Kind::kTraverse,
    PlanNode::Kind::kTraverse,
    PlanNode::Kind::kIndexScan,
    PlanNode::Kind::kStart
    }));
  streamPatterns.insert(buildPatternString({
    PlanNode::Kind::kProject,
    PlanNode::Kind::kLimit,
    PlanNode::Kind::kFilter,
    PlanNode::Kind::kAppendVertices,
    PlanNode::Kind::kTraverse,
    PlanNode::Kind::kTraverse,
    PlanNode::Kind::kIndexScan,
    PlanNode::Kind::kStart
    }));
  streamPatterns.insert(buildPatternString({
    PlanNode::Kind::kProject,
    PlanNode::Kind::kLimit,
    PlanNode::Kind::kFilter,
    PlanNode::Kind::kAppendVertices,
    PlanNode::Kind::kFilter,
    PlanNode::Kind::kTraverse,
    PlanNode::Kind::kTraverse,
    PlanNode::Kind::kIndexScan,
    PlanNode::Kind::kStart
    }));
  return streamPatterns;
}

static std::unordered_set<std::string> STREAM_PATTERNS = initStreamPatterns();

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_SCHEDULER_ASYNCSTREAMBASEDSCHEDULER_H_
