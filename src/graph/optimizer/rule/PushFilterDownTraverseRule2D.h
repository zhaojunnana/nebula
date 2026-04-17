/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

/*
 * Before:
 * Filter(e.likeness > 78)
 *        |
 *   AppendVertices
 *        |
 *     Traverse
 *        |
 *     Traverse
 *
 * After :
 *   AppendVertices
 *        |
 *     Traverse
 *        |
 *     Traverse(eFilter_: *.likeness > 78)
 */
class PushFilterDownTraverseRule2D final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushFilterDownTraverseRule2D();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
