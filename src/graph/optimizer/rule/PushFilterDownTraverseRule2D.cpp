/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownTraverseRule2D.h"

#include "common/expression/ConstantExpression.h"
#include "common/expression/Expression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::Expression;
using nebula::graph::Filter;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Traverse;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownTraverseRule2D::kInstance =
    std::unique_ptr<PushFilterDownTraverseRule2D>(new PushFilterDownTraverseRule2D());

PushFilterDownTraverseRule2D::PushFilterDownTraverseRule2D() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownTraverseRule2D::pattern() const {
  static Pattern pattern =
      Pattern::create(PlanNode::Kind::kFilter,
       {Pattern::create(PlanNode::Kind::kTraverse,
        {Pattern::create(PlanNode::Kind::kTraverse)})});
  return pattern;
}

bool PushFilterDownTraverseRule2D::match(OptContext* ctx, const MatchedResult& matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  DCHECK_EQ(matched.dependencies[0].dependencies[0].node->node()->kind(), PlanNode::Kind::kTraverse);
  auto traverse = static_cast<const Traverse*>(matched.dependencies[0].dependencies[0].node->node());
  return traverse->isOneStep();
}

StatusOr<OptRule::TransformResult> PushFilterDownTraverseRule2D::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* filterGNode = matched.node;
  auto* filterGroup = filterGNode->group();
  auto* filter = static_cast<graph::Filter*>(filterGNode->node());
  auto* condition = filter->condition();

  auto* tvGNode1D = matched.dependencies[0].node;
  auto* tvNode1D = static_cast<graph::Traverse*>(tvGNode1D->node());

  auto* tvGNode2D = matched.dependencies[0].dependencies[0].node;
  auto* tvNode2D = static_cast<graph::Traverse*>(tvGNode2D->node());
  auto& edgeAlias = tvNode2D->edgeAlias();

  auto qctx = octx->qctx();
  auto pool = qctx->objPool();

  // Pick the expr looks like `$-.e[0].likeness
  auto picker = [&edgeAlias](const Expression* expr) -> bool {
    bool shouldNotPick = false;
    auto finder = [&shouldNotPick, &edgeAlias](const Expression* e) -> bool {
      // When visiting the expression tree and find an expession node is a one step edge property
      // expression, stop visiting its children and return true.
      if (graph::ExpressionUtils::isOneStepEdgeProp(edgeAlias, e)) return true;
      // Otherwise, continue visiting its children. And if the following two conditions are met,
      // mark the expression as shouldNotPick and return false.
      if (e->kind() == Expression::Kind::kInputProperty ||
          e->kind() == Expression::Kind::kVarProperty) {
        shouldNotPick = true;
        return false;
      }
      // TODO(jie): Handle the strange exists expr. e.g. exists(e.likeness)
      if (e->kind() == Expression::Kind::kPredicate &&
          static_cast<const PredicateExpression*>(e)->name() == "exists") {
        shouldNotPick = true;
        return false;
      }
      return false;
    };
    graph::FindVisitor visitor(finder, true, true);
    const_cast<Expression*>(expr)->accept(&visitor);
    if (shouldNotPick) return false;
    if (!visitor.results().empty()) {
      return true;
    }
    return false;
  };
  Expression* filterPicked = nullptr;
  Expression* filterUnpicked = nullptr;
  graph::ExpressionUtils::splitFilter(condition, picker, &filterPicked, &filterUnpicked);

  if (!filterPicked) {
    return TransformResult::noTransform();
  }
  auto* newFilterPicked =
      graph::ExpressionUtils::rewriteEdgePropertyFilter(pool, edgeAlias, filterPicked->clone());
  auto* eFilter = tvNode2D->eFilter();
  Expression* newEFilter = eFilter
                               ? LogicalExpression::makeAnd(pool, newFilterPicked, eFilter->clone())
                               : newFilterPicked;

  // produce new Traverse node
  auto newTvNode1D = static_cast<graph::Traverse*>(tvNode1D->clone());
  auto* newTvNode2D = static_cast<graph::Traverse*>(tvNode2D->clone());
  newTvNode2D->setEdgeFilter(newEFilter);
  newTvNode2D->setInputVar(tvNode2D->inputVar());
  newTvNode2D->setColNames(tvNode2D->outputVarPtr()->colNames);

  // connect the optimized plan
  TransformResult result;
  result.eraseAll = true;
  if (filterUnpicked) {
    auto* newFilterNode = graph::Filter::make(qctx, newTvNode1D, filterUnpicked);
    newFilterNode->setOutputVar(filter->outputVar());
    newFilterNode->setColNames(filter->colNames());
    auto newFilterGNode = OptGroupNode::create(octx, newFilterNode, filterGroup);
    // assemble the new Traverse group below Filter
    auto newTvGroup1D = OptGroup::create(octx);
    auto newTvGNode1D = newTvGroup1D->makeGroupNode(newTvNode1D);
    auto newTvGroup2D = OptGroup::create(octx);
    auto newTvGNode2D = newTvGroup2D->makeGroupNode(newTvNode2D);
    newTvGNode2D->setDeps(tvGNode2D->dependencies());
    newFilterGNode->setDeps({newTvGroup1D});
    newFilterNode->setInputVar(newTvNode1D->outputVar());
    newTvGNode1D->setDeps({newTvGroup2D});
    newTvNode1D->setInputVar(newTvNode2D->outputVar());
    result.newGroupNodes.emplace_back(newFilterGNode);
  } else {
    // replace the new Traverse node with the old Filter group
    auto newTvGNode1D = OptGroupNode::create(octx, newTvNode1D, filterGroup);
    newTvNode1D->setOutputVar(filter->outputVar());
    auto newTvGroup2D = OptGroup::create(octx);
    auto newTvGNode2D = newTvGroup2D->makeGroupNode(newTvNode2D);
    newTvGNode2D->setDeps(tvGNode2D->dependencies());
    newTvNode1D->setInputVar(newTvNode2D->outputVar());
    newTvGNode1D->setDeps({newTvGroup2D});
    result.newGroupNodes.emplace_back(newTvGNode1D);
  }

  return result;
}

std::string PushFilterDownTraverseRule2D::toString() const {
  return "PushFilterDownTraverseRule2D";
}

}  // namespace opt
}  // namespace nebula
