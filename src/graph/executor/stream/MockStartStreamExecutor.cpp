// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/stream/MockStartStreamExecutor.h"
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace nebula {
using Row = List;

namespace graph {

std::shared_ptr<RoundResult> MockStartStreamExecutor::executeOneRound(
  std::shared_ptr<DataSet> input, Offset offset) {
    UNUSED(input);
    auto ds = std::make_shared<nebula::DataSet>();
    return std::make_shared<RoundResult>(ds, false, offset);
}

}  // namespace graph
}  // namespace nebula
