// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_PROCEDUREEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_PROCEDUREEXECUTOR_H_

#include "graph/executor/Executor.h"
// select user-specified columns from a table
namespace nebula {
namespace graph {

class ProcedureExecutor final : public Executor {
 public:
  ProcedureExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ProcedureExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

  DataSet handleJob(size_t begin, size_t end, Iterator *iter);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_PROCEDUREEXECUTOR_H_
