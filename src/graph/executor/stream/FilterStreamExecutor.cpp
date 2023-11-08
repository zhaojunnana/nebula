// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/stream/FilterStreamExecutor.h"

#include "graph/planner/plan/Query.h"
#include "graph/service/GraphFlags.h"

namespace nebula {
namespace graph {

std::shared_ptr<RoundResult> FilterStreamExecutor::executeOneRound(
    std::shared_ptr<DataSet> input, Offset offset) {
  SCOPED_TIMER(&execTime_);
  auto iter = ResultBuilder().value(Value(*input))
    .iter(Iterator::Kind::kProp).build().iter();
  if (iter == nullptr || iter->isDefaultIter()) {
    auto status = Status::Error("iterator is nullptr or DefaultIter");
    LOG(ERROR) << status;
    return std::make_shared<RoundResult>(std::make_shared<DataSet>(), false, offset);
  }
  QueryExpressionContext ctx(ectx_);
  auto *filter = asNode<Filter>(node());
  auto condition = filter->condition()->clone();
  DataSet ds;
  ds.colNames = input->colNames;
  ds.rows.reserve(iter->size());
  for (; iter->valid(); iter->next()) {
    auto val = condition->eval(ctx(iter.get()));
    if (val.isBadNull() || (!val.empty() && !val.isImplicitBool() && !val.isNull())) {
      auto status = Status::Error("Failed to evaluate condition: %s. %s%s",
                           condition->toString().c_str(),
                           "For boolean conditions, please write in their full forms like",
                           " <condition> == <true/false> or <condition> IS [NOT] NULL.");
      LOG(ERROR) << status;
      return std::make_shared<RoundResult>(std::make_shared<DataSet>(), false, offset);
    }
    if (val.isImplicitBool() && val.implicitBool()) {
      Row row;
      row = *iter->row();
      ds.rows.emplace_back(std::move(row));
    }
  }
  return std::make_shared<RoundResult>(std::make_shared<DataSet>(std::move(ds)), false, offset);
}

}  // namespace graph
}  // namespace nebula
