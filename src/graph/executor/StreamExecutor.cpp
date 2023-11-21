// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/StreamExecutor.h"
#include <atomic>
#include <iostream>
#include <memory>
#include <utility>
#include "common/base/Base.h"
#include "common/base/Status.h"
#include "graph/executor/stream/AppendVerticesStreamExecutor.h"
#include "graph/executor/stream/FilterStreamExecutor.h"
#include "graph/executor/stream/IndexScanStreamExecutor.h"
#include "graph/executor/stream/MockGetNeighborsStreamExecutor.h"
#include "graph/executor/stream/LimitStreamExecutor.h"
#include "graph/executor/stream/MockStartStreamExecutor.h"
#include "graph/executor/stream/MockTransportStreamExecutor.h"
#include "graph/executor/stream/LimitStreamExecutor.h"
#include "graph/executor/stream/ProjectStreamExecutor.h"
#include "graph/executor/stream/StreamCollectExecutor.h"
#include "graph/executor/stream/TraverseStreamExecutor.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

DEFINE_uint64(stream_executor_batch_size,
              100,
              "batch size of stream executor limit to storage query");

namespace nebula {
namespace graph {

// RoundResult
std::shared_ptr<DataSet> RoundResult::getOutputData() {
    return output_;
}

bool RoundResult::hasNextRound() {
    return hasNextRound_;
}

Offset RoundResult::getOffset() {
    return offset_;
}

// static
StreamExecutor *StreamExecutor::createStream(const PlanNode *node, QueryContext *qctx) {
  std::unordered_map<int64_t, StreamExecutor *> visited;
  auto abnormalStatus = std::make_shared<Status>(Status::OK());
  auto rootExecutor = makeStreamExecutor(node, qctx, &visited, abnormalStatus);
  auto collectorExecutor = qctx->objPool()->makeAndAdd<StreamCollectExecutor>(node, qctx);
  collectorExecutor->dependsOn(rootExecutor);
  collectorExecutor->setSharedAbnormalStatus(abnormalStatus);
  return collectorExecutor;
}

// static
StreamExecutor *StreamExecutor::makeStreamExecutor(const PlanNode *node, QueryContext *qctx,
    std::unordered_map<int64_t, StreamExecutor *> *visited,
    std::shared_ptr<Status> sharedAbnormalStatus) {
  DCHECK(qctx != nullptr);
  DCHECK(node != nullptr);
  auto iter = visited->find(node->id());
  if (iter != visited->end()) {
    return iter->second;
  }

  StreamExecutor *exec = makeStreamExecutor(qctx, node);
  exec->setSharedAbnormalStatus(sharedAbnormalStatus);

  for (size_t i = 0; i < node->numDeps(); ++i) {
    exec->dependsOn(makeStreamExecutor(node->dep(i), qctx, visited, sharedAbnormalStatus));
  }

  visited->insert({node->id(), exec});
  return exec;
}

// static
StreamExecutor *StreamExecutor::makeStreamExecutor(QueryContext *qctx, const PlanNode *node) {
  auto pool = qctx->objPool();
//   auto &spaceName = qctx->rctx() ? qctx->rctx()->session()->spaceName() : "";
  switch (node->kind()) {
    case PlanNode::Kind::kEdgeIndexFullScan:
    case PlanNode::Kind::kEdgeIndexPrefixScan:
    case PlanNode::Kind::kEdgeIndexRangeScan:
    case PlanNode::Kind::kTagIndexFullScan:
    case PlanNode::Kind::kTagIndexPrefixScan:
    case PlanNode::Kind::kTagIndexRangeScan:
    case PlanNode::Kind::kExpandAll: {
      return pool->makeAndAdd<MockTransportStreamExecutor>(node, qctx);
    }
    case PlanNode::Kind::kExpand: {
      return pool->makeAndAdd<MockGetNeighborsStreamExecutor>(node, qctx);
    }
    case PlanNode::Kind::kStart: {
        return pool->makeAndAdd<MockStartStreamExecutor>(node, qctx);
    }
    case PlanNode::Kind::kIndexScan: {
      return pool->makeAndAdd<IndexScanStreamExecutor>(node, qctx);
    }
    case PlanNode::Kind::kFilter: {
      return pool->makeAndAdd<FilterStreamExecutor>(node, qctx);
    }
    case PlanNode::Kind::kAppendVertices: {
      return pool->makeAndAdd<AppendVerticesStreamExecutor>(node, qctx);
    }
    case PlanNode::Kind::kTraverse: {
      return pool->makeAndAdd<TraverseStreamExecutor>(node, qctx);
    }
    case PlanNode::Kind::kProject: {
      return pool->makeAndAdd<ProjectStreamExecutor>(node, qctx);
    }
    case PlanNode::Kind::kLimit: {
      return pool->makeAndAdd<LimitStreamExecutor>(node, qctx);
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
    return upStreamFinishCount_ == upStream;
}

int32_t StreamExecutor::markSubmitTask() {
    return ++taskCount_;
}

int32_t StreamExecutor::markFinishTask(bool hasNextRound) {
    int32_t currentTaskCount = --taskCount_;
    bool upStreamFinished = this->upStreamFinished();
    DLOG(INFO) << "markFinishTask " << id()
      << " with upStreamFinished:" << upStreamFinished
      << ", hasNextRound:" << hasNextRound
      << ", currentTaskCount:" << currentTaskCount;
    if (upStreamFinished && !hasNextRound && currentTaskCount == 0) {
        for (auto next : successors_) {
          static_cast<StreamExecutor*>(next)->markSubmitTask();
        }
        // make sure the markup is only executed once
        auto oldFlag = finishFlag_.exchange(true);
        if (!oldFlag) {
          this->markFinishExecutor();
        }
        // flush once
        for (auto next : successors_) {
          static_cast<StreamExecutor*>(next)->markFinishTask(false);
        }
    }
    return currentTaskCount;
}

void StreamExecutor::setRootPromise(folly::Promise<Status>&& rootPromise) {
    rootPromise_ = std::move(rootPromise);
    rootPromiseHasBeenSet_ = true;
}

void StreamExecutor::setSharedAbnormalStatus(std::shared_ptr<Status> abnormalStatus) {
  abnormalStatus_ = abnormalStatus;
}

bool StreamExecutor::isExecutorStopped() {
  return stopFlag_;
}

void StreamExecutor::markStopExecutor() {
  DLOG(INFO) << "markStopExecutor: " << id();
  auto oldFlag = stopFlag_.exchange(true);
  if (!oldFlag) {
    for (auto upstream : depends_) {
      static_cast<StreamExecutor*>(upstream)->markStopExecutor();
    }
  }
}

void StreamExecutor::markFinishExecutor() {
    DLOG(INFO) << "markFinishExecutor: " << id();
    // mark executor finish
    for (auto next : successors_) {
        static_cast<StreamExecutor*>(next)->upStreamFinishCount_++;
    }
    if (rootPromiseHasBeenSet_) {
        rootPromise_.setValue(Status::OK());
    }
}

int32_t StreamExecutor::getBatchSize() {
  return FLAGS_stream_executor_batch_size;
}

folly::Future<Status> StreamExecutor::execute() {
    return folly::makeFuture(Status::Error(
      "Unsupported execute() in StreamExecutor, please use executeOneRound() instead."));
}


}  // namespace graph
}  // namespace nebula
