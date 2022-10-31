/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/CallClausePlanner.h"
#include <vector>

#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> CallClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
  if (clauseCtx->kind != CypherClauseKind::kCall) {
    return Status::Error("Not a valid context for CallClausePlanner.");
  }
  auto* CallCtx = static_cast<CallClauseContext*>(clauseCtx);

  SubPlan callPlan;
  NG_RETURN_IF_ERROR(buildCall(CallCtx, callPlan));
  return callPlan;
}

Status CallClausePlanner::buildCall(CallClauseContext* cctx, SubPlan& subplan) {
  auto* currentRoot = subplan.root;
  DCHECK(!currentRoot);
  auto* newProjCols = cctx->qctx->objPool()->makeAndAdd<YieldColumns>();
  newProjCols->addColumn(new YieldColumn(cctx->callFuncExpr));

  auto* procedure = Procedure::make(cctx->qctx, currentRoot, newProjCols);
  procedure->setColNames(std::move(cctx->yield->projOutputColumnNames_));
  subplan.root = procedure;
  subplan.tail = procedure;

  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
