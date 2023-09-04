// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/StreamExecutor.h"
#include <iostream>
#include <utility>
#include "common/base/Status.h"
#include "graph/executor/stream/MockStartStreamExecutor.h"
#include "graph/executor/stream/MockTransportStreamExecutor.h"
#include "graph/executor/stream/StreamCollectExecutor.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {

// RoundResult
std::shared_ptr<DataSet> RoundResult::getOutputData() {
    return output_;
}

bool RoundResult::hasNextRound() {
    return hasNextRound_;
}

int RoundResult::getOffset() {
    return offset_;
}

// static
StreamExecutor *StreamExecutor::createStream(const PlanNode *node, QueryContext *qctx) {
  std::unordered_map<int64_t, StreamExecutor *> visited;
  return makeStreamExecutor(node, qctx, &visited);
}

// static
StreamExecutor *StreamExecutor::makeStreamExecutor(const PlanNode *node, QueryContext *qctx,
    std::unordered_map<int64_t, StreamExecutor *> *visited) {
  DCHECK(qctx != nullptr);
  DCHECK(node != nullptr);
  auto iter = visited->find(node->id());
  if (iter != visited->end()) {
    return iter->second;
  }

  StreamExecutor *exec = makeStreamExecutor(qctx, node);

  for (size_t i = 0; i < node->numDeps(); ++i) {
    exec->dependsOn(makeStreamExecutor(node->dep(i), qctx, visited));
  }

  visited->insert({node->id(), exec});
  return exec;
}

StreamExecutor *StreamExecutor::makeStreamExecutor(QueryContext *qctx, const PlanNode *node) {
  auto pool = qctx->objPool();
//   auto &spaceName = qctx->rctx() ? qctx->rctx()->session()->spaceName() : "";
  switch (node->kind()) {
    case PlanNode::Kind::kStart: {
        return pool->makeAndAdd<MockStartStreamExecutor>(node, qctx);
    }
    case PlanNode::Kind::kIndexScan:
    case PlanNode::Kind::kEdgeIndexFullScan:
    case PlanNode::Kind::kEdgeIndexPrefixScan:
    case PlanNode::Kind::kEdgeIndexRangeScan:
    case PlanNode::Kind::kTagIndexFullScan:
    case PlanNode::Kind::kTagIndexPrefixScan:
    case PlanNode::Kind::kTagIndexRangeScan:
    case PlanNode::Kind::kTraverse:
    case PlanNode::Kind::kAppendVertices:
    case PlanNode::Kind::kLimit: {
      return pool->makeAndAdd<MockTransportStreamExecutor>(node, qctx);
    }
    case PlanNode::Kind::kProject: {
      return pool->makeAndAdd<StreamCollectExecutor>(node, qctx);
    }
    case PlanNode::Kind::kUnknown: {
      DLOG(FATAL) << "Unknown plan node kind " << static_cast<int32_t>(node->kind());
      break;
    }
    default: {
      DLOG(FATAL) << "Unsupported node kind yet " << static_cast<int32_t>(node->kind());
      break;
    }
  }
  return nullptr;
}

bool StreamExecutor::upStreamFinished() {
    auto upStream = static_cast<int32_t>(depends_.size());
    return upStreamFinishCount == upStream;
}

int32_t StreamExecutor::markSubmitTask() {
    return ++taskCount;
}

int32_t StreamExecutor::markFinishTask(bool hasNextRound) {
    int32_t currentTaskCount = --taskCount;
    bool upStreamFinished = this->upStreamFinished();
    DLOG(INFO) << "markFinishTask " << id()
      << " with upStreamFinished:" << upStreamFinished
      << ", hasNextRound:" << hasNextRound
      << ", currentTaskCount:" << currentTaskCount;
    if (upStreamFinished && !hasNextRound && currentTaskCount == 0) {
        for (auto next : successors_) {
          static_cast<StreamExecutor*>(next)->markSubmitTask();
        }
        this->markFinishExecutor();
        // flush once
        for (auto next : successors_) {
          static_cast<StreamExecutor*>(next)->markFinishTask(false);
        }
    }
    return currentTaskCount;
}

void StreamExecutor::markFinishExecutor() {
    DLOG(INFO) << "markFinishExecutor: " << id();
    // mark executor finish
    for (auto next : successors_) {
        static_cast<StreamExecutor*>(next)->upStreamFinishCount++;
    }
    if (rootPromiseHasBeenSet_) {
        rootPromise_.setValue(Status::OK());
    }
}

void StreamExecutor::setRootPromise(folly::Promise<Status>&& rootPromise) {
    rootPromise_ = std::move(rootPromise);
    rootPromiseHasBeenSet_ = true;
}

folly::Future<Status> StreamExecutor::execute() {
    return folly::makeFuture(Status::Error(
      "Unsupported execute() in StreamExecutor, please use executeOneRound() instead."));
}

std::shared_ptr<RoundResult> StreamExecutor::executeOneRound(
  std::shared_ptr<DataSet> input, int64_t offset) {
    // TODO not yet, return output data set.
    std::cout << input << ", " << offset;
    return nullptr;
}


}  // namespace graph
}  // namespace nebula
