// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_FILTERSTREAMEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_FILTERSTREAMEXECUTOR_H_

#include "graph/executor/StreamExecutor.h"

// delete the corresponding iterator when the row in the dataset does not meet the conditions
// and save the filtered iterator to the result
namespace nebula {
namespace graph {

class FilterStreamExecutor final : public StreamExecutor {
 public:
  FilterStreamExecutor(const PlanNode *node, QueryContext *qctx)
      : StreamExecutor("FilterStreamExecutor", node, qctx) {}

  std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input, Offset offset) override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_FILTERSTREAMEXECUTOR_H_
