/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "FunctionApocManager.h"
#include <vector>
#include "common/base/Base.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"

namespace nebula {

std::unordered_map<std::string, FunctionApocManager::FunctionApocAttributes> functions_;

FunctionApocManager &FunctionApocManager::instance() {
  static FunctionApocManager instance;
  return instance;
}

FunctionApocManager::FunctionApocManager() = default;

StatusOr<Value::Type> FunctionApocManager::getApocReturnType(
    std::string func, const std::vector<Value::Type> &argsType) {
  LOG(INFO) << "Get apoc return type for function: " << func << "(" << argsType.size() << ").";
  if (func.find(".procedure.") != std::string::npos) {
    return Value::Type::LIST;
  }
  return Value::Type::STRING;
}

StatusOr<const FunctionManager::FunctionAttributes> nebula::FunctionApocManager::loadApocFunction(
    std::string func, size_t arity) {
  auto result = instance().getInternal(func, arity);
  NG_RETURN_IF_ERROR(result);
  return result.value().normalFuncAttr_;
}

void FunctionApocManager::addFakeApocFunction(std::string funcName) const {
  auto &attr = functions_[funcName];
  attr.normalFuncAttr_.minArity_ = 1;
  attr.normalFuncAttr_.maxArity_ = 1;
  attr.normalFuncAttr_.isAlwaysPure_ = true;
  if (funcName.find(".procedure.") != std::string::npos) {
    attr.normalFuncAttr_.body_ = [](const auto &args) -> Value {
      UNUSED(args);
      return Value::kNullBadType;
    };

    attr.returnEntryProps_ = std::vector<std::string>();
    attr.returnEntryProps_.emplace_back("col1");

    attr.apocBody_ = [funcName](const auto &args, const std::string ctx) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          // return list<map>
          Map row;
          std::string value = args[0].get().getStr();
          auto str = "apoc->" + funcName + "(" + value + ") with ctx:" + ctx;
          row.kvs.emplace("col1", str);
          List rows;
          rows.emplace_back(row);
          return rows;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  } else {
    attr.normalFuncAttr_.body_ = [funcName](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          std::string value(args[0].get().getStr());
          auto str = "apoc->" + funcName + "(" + value + ")";
          return str;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };

    attr.returnEntryProps_ = std::vector<std::string>();
    attr.returnEntryProps_.emplace_back("value");

    attr.apocBody_ = [funcName](const auto &args, const std::string ctx) -> Value {
      UNUSED(args);
      UNUSED(ctx);
      return Value::kNullBadType;
    };
  }
}

StatusOr<FunctionApocManager::ApocFunction>
FunctionApocManager::get(const std::string &func, size_t arity) {
  auto result = instance().getInternal(func, arity);
  NG_RETURN_IF_ERROR(result);
  return result.value().apocBody_;
}

StatusOr<std::vector<std::string>>
FunctionApocManager::getReturnEntryProps(const std::string &func, size_t arity) {
  auto result = instance().getInternal(func, arity);
  NG_RETURN_IF_ERROR(result);
  return result.value().returnEntryProps_;
}

StatusOr<const FunctionApocManager::FunctionApocAttributes> FunctionApocManager::getInternal(
    std::string func, size_t arity) const {
  auto iter = functions_.find(func);
  if (iter == functions_.end()) {
    addFakeApocFunction(func);
    iter = functions_.find(func);
    // return Status::Error("Function `%s' not defined", func.c_str());
  }
  auto minArity = iter->second.normalFuncAttr_.minArity_;
  auto maxArity = iter->second.normalFuncAttr_.maxArity_;
  if (arity < minArity || arity > maxArity) {
    if (minArity == maxArity) {
      return Status::Error(
          "Arity not match for function `%s': "
          "provided %lu but %lu expected.",
          func.c_str(),
          arity,
          minArity);
    } else {
      return Status::Error(
          "Arity not match for function `%s': "
          "provided %lu but %lu-%lu expected.",
          func.c_str(),
          arity,
          minArity,
          maxArity);
    }
  }
  return iter->second;
}

}  // namespace nebula
