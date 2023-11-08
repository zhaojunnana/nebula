// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/stream/ProjectStreamExecutor.h"

#include "graph/planner/plan/Query.h"
#include "graph/service/GraphFlags.h"

namespace nebula {
namespace graph {

std::shared_ptr<RoundResult> ProjectStreamExecutor::executeOneRound(
    std::shared_ptr<DataSet> input, Offset offset) {
  // throw std::bad_alloc in MemoryCheckGuard verified
  SCOPED_TIMER(&execTime_);
  auto iter = ResultBuilder().value(Value(*input))
    .iter(Iterator::Kind::kProp).build().iter();
  DCHECK(!!iter);

  auto ds = handleJob(0, iter->size(), iter.get());
  return std::make_shared<RoundResult>(std::make_shared<DataSet>(std::move(ds)), false, offset);
}

DataSet ProjectStreamExecutor::handleJob(size_t begin, size_t end, Iterator *iter) {
  auto *project = asNode<Project>(node());
  auto columns = project->columns()->clone();
  DataSet ds;
  ds.colNames = project->colNames();
  QueryExpressionContext ctx(qctx()->ectx());
  ds.rows.reserve(end - begin);
  for (; iter->valid() && begin++ < end; iter->next()) {
    Row row;
    for (auto &col : columns->columns()) {
      Value val = col->expr()->eval(ctx(iter));
      row.values.emplace_back(std::move(val));
    }
    ds.rows.emplace_back(std::move(row));
  }
  return ds;
}

}  // namespace graph
}  // namespace nebula
