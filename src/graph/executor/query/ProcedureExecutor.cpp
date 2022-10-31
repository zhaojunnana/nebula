// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "common/expression/FunctionCallExpression.h"
#include "graph/executor/query/ProcedureExecutor.h"

#include "graph/planner/plan/Query.h"
#include "graph/service/GraphFlags.h"

namespace nebula {
namespace graph {

folly::Future<Status> ProcedureExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *procedure = asNode<Procedure>(node());
  auto iter = ectx_->getResult(procedure->inputVar()).iter();
  DCHECK(!!iter);
  QueryExpressionContext ctx(ectx_);

  if (FLAGS_max_job_size <= 1) {
    auto ds = handleJob(0, iter->size(), iter.get());
    return finish(ResultBuilder().value(Value(std::move(ds))).build());
  } else {
    DataSet ds;
    ds.colNames = procedure->colNames();
    ds.rows.reserve(iter->size());

    auto scatter = [this](size_t begin, size_t end, Iterator *tmpIter) -> StatusOr<DataSet> {
      return handleJob(begin, end, tmpIter);
    };

    auto gather = [this, result = std::move(ds)](auto &&results) mutable {
      for (auto &r : results) {
        auto &&rows = std::move(r).value();
        result.rows.insert(result.rows.end(),
                           std::make_move_iterator(rows.begin()),
                           std::make_move_iterator(rows.end()));
      }
      finish(ResultBuilder().value(Value(std::move(result))).build());
      return Status::OK();
    };

    return runMultiJobs(std::move(scatter), std::move(gather), iter.get());
  }
}

DataSet ProcedureExecutor::handleJob(size_t begin, size_t end, Iterator *iter) {
  auto *procedure = asNode<Procedure>(node());
  auto columns = procedure->columns()->clone();
  DataSet ds;
  ds.colNames = procedure->colNames();
  QueryExpressionContext ctx(qctx()->ectx());
  ds.rows.reserve(end - begin);
  for (; iter->valid() && begin++ < end; iter->next()) {
    for (auto &col : columns->columns()) {
      Value val = static_cast<FunctionCallApocExpression*>(col->expr())
        ->evalWithInternalCtx(ctx(iter), "executor ctx");
      // procedure return list<map>
      if (val.type() == Value::Type::LIST) {
        Row row;
        for (auto &value : val.getList().values) {
          if (value.type() == Value::Type::MAP) {
            // set values to rows
            for (auto &colName : ds.colNames) {
              auto colValue = value.getMap().at(colName);
              row.values.emplace_back(std::move(colValue));
            }
          }
        }
        ds.rows.emplace_back(std::move(row));
      }
    }
  }
  return ds;
}

}  // namespace graph
}  // namespace nebula
