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
  std::shared_ptr<DataSet> input, std::unordered_map<Value, nebula::storage::cpp2::ScanCursor> offset) {
    std::cout << "input: " << input << std::endl;
    // int64_t index = 0;
    // if (!offset->empty()) {
    //   index = offset->size();
    // }

    auto ds = std::make_shared<nebula::DataSet>();
    std::vector<std::string> colNames = {nebula::kVid};
    ds->colNames = std::move(colNames);
    Row row;
    row.emplace_back(Value("player101"));
    ds->rows.emplace_back(std::move(row));
    // std::unordered_map<Value, nebula::storage::cpp2::ScanCursor> cur;
    return std::make_shared<RoundResult>(ds, false, offset);

    // auto ds = std::make_shared<nebula::DataSet>();
    // std::vector<std::string> colNames = {"var1", "var2",  "var3"};
    // ds->colNames = std::move(colNames);
    // Row row;
    // row.emplace_back(Value(index));
    // row.emplace_back(Value("hello"));
    // row.emplace_back(Value(1.23));
    // ds->rows.emplace_back(std::move(row));
    // return std::make_shared<RoundResult>(ds, index < 3, *offset);
}

}  // namespace graph
}  // namespace nebula
