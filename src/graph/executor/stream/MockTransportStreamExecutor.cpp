// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/stream/MockTransportStreamExecutor.h"

namespace nebula {
namespace graph {

std::shared_ptr<RoundResult> MockTransportStreamExecutor::executeOneRound(
  std::shared_ptr<DataSet> input, int64_t offset) {
    std::cout << "input: " << input << ", offset: " << offset << std::endl;
    return std::make_shared<RoundResult>(input, false, -1);
}

}  // namespace graph
}  // namespace nebula
