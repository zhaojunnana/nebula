/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/plan/PlanNode.h"
#include "graph/scheduler/AsyncMsgNotifyBasedScheduler.h"
#include "graph/scheduler/AsyncStreamBasedScheduler.h"
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gflags/gflags.h>
#include <atomic>
#include <memory>
#include "graph/executor/StreamExecutor.h"

DEFINE_bool(stream_executor_enable, false, "whether the flow executor is enbale");
DECLARE_bool(enable_lifetime_optimize);

namespace nebula {
namespace graph {

AsyncStreamBasedScheduler::AsyncStreamBasedScheduler(QueryContext* qctx) : Scheduler() {
  qctx_ = qctx;
  query_ = qctx->rctx()->query();
  baseScheduler_ = std::make_unique<AsyncMsgNotifyBasedScheduler>(qctx);
}

void AsyncStreamBasedScheduler::waitFinish() {
  std::unique_lock<std::mutex> lck(emtx_);
  cv_.wait(lck, [this] {
    if (executing_ != 0) {
      DLOG(INFO) << "executing: " << executing_;
      return false;
    } else {
      DLOG(INFO) << " wait finish";
      return true;
    }
  });
}

folly::Future<Status> AsyncStreamBasedScheduler::schedule() {
  auto root = qctx_->plan()->root();

  // witch schedule use
  if (!FLAGS_stream_executor_enable) {
    return baseScheduler_->schedule();
  }
  std::vector<PlanNode::Kind> pattern;
  buildPlanPattern(root, pattern);
  auto patternStr = buildPatterString(pattern);
  if (STREAM_PATTERNS.find(patternStr) == STREAM_PATTERNS.end()) {
    return baseScheduler_->schedule();
  }

  if (FLAGS_enable_lifetime_optimize) {
    // special for root
    root->outputVarPtr()->userCount.store(std::numeric_limits<uint64_t>::max(),
                                          std::memory_order_relaxed);
    analyzeLifetime(root);
  }
  // plan node 1 to 1 create to stream executor
  auto executor = StreamExecutor::createStream(root, qctx_);
  DLOG(INFO) << formatPrettyDependencyTree(executor);
  return doSchedule(executor);
}

folly::Future<Status> AsyncStreamBasedScheduler::doSchedule(StreamExecutor* root) const {
  // pool release ？
  folly::CPUThreadPoolExecutor pool(4);
  folly::Promise<Status> promiseForRoot;
  auto resultFuture = promiseForRoot.getFuture();
  // set promise to root executor
  root->setRootPromise(std::move(promiseForRoot));

  // collect leaf executors
  std::queue<Executor*> queue;
  std::unordered_set<Executor*> visited;
  std::vector<StreamExecutor*> leafExeuctors;
  queue.push(root);
  visited.emplace(root);
  while (!queue.empty()) {
    auto* exe = queue.front();
    queue.pop();

    for (auto* dep : exe->depends()) {
      auto notVisited = visited.emplace(dep).second;
      if (notVisited) {
        queue.push(dep);
      }
    }

    if (exe->depends().empty()) {
      leafExeuctors.emplace_back(static_cast<StreamExecutor*>(exe));
    }
  }

  for (auto leaf : leafExeuctors) {
    leaf->markSubmitTask();
    submitTask(pool, leaf, nullptr, {});
  }
  return resultFuture;
}

void AsyncStreamBasedScheduler::submitTask(folly::Executor &pool,
                                           StreamExecutor* executor,
                                           std::shared_ptr<DataSet> input,
                                           std::unordered_map<Value,
                                            nebula::storage::cpp2::ScanCursor> offset) const {
  folly::via(&pool, [&pool, executor, input, offset, this] {
    auto r = executor->executeOneRound(input, offset);
    auto out = r->getOutputData();

    auto isStopped = executor->isExecutorStopped();
    if (isStopped) {
      DLOG(INFO) << "stream stopped " << executor->id() << " , igore next task submit.";
    }

    if (nullptr != out) {
      for (auto successor : executor->successors()) {
        auto next = static_cast<StreamExecutor*>(successor);
        next->markSubmitTask();
        submitTask(pool, next, out, {});
      }
    }

    if (!isStopped && r->hasNextRound()) {
      executor->markSubmitTask();
    }
    executor->markFinishTask(r->hasNextRound() && !isStopped);
    if (!isStopped && r->hasNextRound()) {
      submitTask(pool, executor, input, r->getOffset());
    }
  });
}

void AsyncStreamBasedScheduler::addExecuting(Executor* executor) const {
  std::unique_lock<std::mutex> lck(emtx_);
  executing_++;
  DLOG(INFO) << formatPrettyId(executor) << " add " << executing_;
}

void AsyncStreamBasedScheduler::removeExecuting(Executor* executor) const {
  std::unique_lock<std::mutex> lck(emtx_);
  executing_--;
  DLOG(INFO) << formatPrettyId(executor) << "remove: " << executing_;
  cv_.notify_one();
}

std::string AsyncStreamBasedScheduler::formatPrettyId(Executor* executor) {
  return fmt::format("[{},{}]", executor->name(), executor->id());
}

std::string AsyncStreamBasedScheduler::formatPrettyDependencyTree(Executor* root) {
  std::stringstream ss;
  size_t spaces = 0;
  appendExecutor(spaces, root, ss);
  return ss.str();
}

void AsyncStreamBasedScheduler::appendExecutor(size_t spaces,
                                               Executor* executor,
                                               std::stringstream& ss) {
  ss << std::string(spaces, ' ') << formatPrettyId(executor) << std::endl;
  for (auto depend : executor->depends()) {
    appendExecutor(spaces + 1, depend, ss);
  }
}

void AsyncStreamBasedScheduler::buildPlanPattern(PlanNode* root,
  std::vector<PlanNode::Kind>& pattern) {
  pattern.emplace_back(root->kind());
  for (auto depend : root->dependencies()) {
    buildPlanPattern(const_cast<PlanNode*>(depend), pattern);
  }
}

}  // namespace graph
}  // namespace nebula
