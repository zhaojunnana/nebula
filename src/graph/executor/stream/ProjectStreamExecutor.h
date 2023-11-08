// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_PROJECTSTREAMEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_PROJECTSTREAMEXECUTOR_H_

#include "graph/executor/StreamExecutor.h"
// select user-specified columns from a table
namespace nebula {
namespace graph {

class ProjectStreamExecutor final : public StreamExecutor {
 public:
  ProjectStreamExecutor(const PlanNode *node, QueryContext *qctx)
      : StreamExecutor("ProjectStreamExecutor", node, qctx) {}

  std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input, Offset offset) override;

  DataSet handleJob(size_t begin, size_t end, Iterator *iter);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_PROJECTSTREAMEXECUTOR_H_
