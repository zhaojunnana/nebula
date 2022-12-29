/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_CALLCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_CALLCLAUSEPLANNER_H_

#include "graph/planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
// The ReturnClausePlanner generates plan for return clause.
class CallClausePlanner final : public CypherClausePlanner {
 public:
  CallClausePlanner() = default;

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

  Status buildCall(CallClauseContext* cctx, SubPlan& subPlan);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_CALLCLAUSEPLANNER_H_
