// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/stream/MockStartStreamExecutor.h"
#include <iostream>
#include <memory>
#include <vector>

namespace nebula {
using Row = List;

namespace graph {

std::shared_ptr<RoundResult> MockStartStreamExecutor::executeOneRound(
  std::shared_ptr<DataSet> input, int64_t offset) {
    std::cout << "input: " << input << ", offset: " << offset << std::endl;
    if (offset < 0) {
        offset = 0;
    }
    auto ds = std::make_shared<nebula::DataSet>();
    std::vector<std::string> colNames = {"var1", "var2",  "var3"};
    ds->colNames = std::move(colNames);
    Row row;
    row.emplace_back(Value(offset));
    row.emplace_back(Value("hello"));
    row.emplace_back(Value(1.23));
    ds->rows.emplace_back(std::move(row));
    return std::make_shared<RoundResult>(ds, offset < 3, offset+1);
}

}  // namespace graph
}  // namespace nebula
