/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "FunctionApocManager.h"
#include <vector>
#include "common/base/Base.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Vertex.h"

namespace nebula {

std::unordered_map<std::string, FunctionApocManager::FunctionApocAttributes> functions_;
std::unordered_map<std::string, std::vector<TypeSignature>> typeSignature_ = {
    {"apoc.bitwise.op",
     {TypeSignature({Value::Type::INT, Value::Type::STRING, Value::Type::INT}, Value::Type::INT)}},
    {"apoc.text.join",
     {TypeSignature({Value::Type::LIST, Value::Type::STRING}, Value::Type::STRING)}},
    {"apoc.label.exists",
     {TypeSignature({Value::Type::VERTEX, Value::Type::STRING}, Value::Type::BOOL)}},
    {"apoc.coll.contains",
     {TypeSignature({Value::Type::LIST, Value::Type::BOOL}, Value::Type::BOOL),
      TypeSignature({Value::Type::LIST, Value::Type::INT}, Value::Type::BOOL),
      TypeSignature({Value::Type::LIST, Value::Type::FLOAT}, Value::Type::BOOL),
      TypeSignature({Value::Type::LIST, Value::Type::STRING}, Value::Type::BOOL)}},
};

FunctionApocManager &FunctionApocManager::instance() {
  static FunctionApocManager instance;
  return instance;
}

FunctionApocManager::FunctionApocManager() {
  // built-in apoc functions
  {
    auto &attr = functions_["apoc.bitwise.op"];
    attr.normalFuncAttr_.minArity_ = 3;
    attr.normalFuncAttr_.maxArity_ = 3;
    attr.normalFuncAttr_.isAlwaysPure_ = true;
    std::unordered_map<std::string, int> opMap = {
      {"&", 1},
      {"|", 2},
      {"^", 3},
      {"~", 4},
      {">>", 5},
      {"<<", 6},
    };
    attr.normalFuncAttr_.body_ = [opMap](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          if (args[1].get().isNull() || !args[1].get().isStr()) {
            return Value::kNullValue;
          }
          if (args[2].get().isNull() || !args[2].get().isInt()) {
            return Value::kNullValue;
          }

          auto left = args[0].get().getInt();
          std::string option(args[1].get().getStr());
          auto right = args[2].get().getInt();
          switch (opMap.at(option)) {
            case 1:
              return left & right;
            case 2:
              return left | right;
            case 3:
              return left ^ right;
            case 4:
              return ~left;
            case 5:
              return left >> right;
            case 6:
              return left << right;
            default:
              return Value::kNullBadData;
          }
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };

    attr.returnEntryProps_ = std::vector<std::string>();
    attr.returnEntryProps_.emplace_back("value");

    attr.apocBody_ = [](const auto &args, const std::string ctx) -> Value {
      UNUSED(args);
      UNUSED(ctx);
      return Value::kNullBadType;
    };
  }
  {
    auto &attr = functions_["apoc.text.join"];
    attr.normalFuncAttr_.minArity_ = 2;
    attr.normalFuncAttr_.maxArity_ = 2;
    attr.normalFuncAttr_.isAlwaysPure_ = true;
    attr.normalFuncAttr_.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::LIST: {
          if (args[1].get().isNull() || !args[1].get().isStr()) {
            return Value::kNullValue;
          }
          std::string delimiter(args[1].get().getStr());
          std::string buffer;
          int index = 0;
          for (auto text : args[0].get().getList().values) {
            if (!text.isStr()) {
              return Value::kNullValue;
            }
            if (index++ != 0) {
              buffer.append(delimiter);
            }
            buffer.append(text.getStr());
          }
          return buffer;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };

    attr.returnEntryProps_ = std::vector<std::string>();
    attr.returnEntryProps_.emplace_back("value");

    attr.apocBody_ = [](const auto &args, const std::string ctx) -> Value {
      UNUSED(args);
      UNUSED(ctx);
      return Value::kNullBadType;
    };
  }
  {
    auto &attr = functions_["apoc.label.exists"];
    attr.normalFuncAttr_.minArity_ = 2;
    attr.normalFuncAttr_.maxArity_ = 2;
    attr.normalFuncAttr_.isAlwaysPure_ = true;
    attr.normalFuncAttr_.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::VERTEX: {
          if (args[1].get().isNull() || !args[1].get().isStr()) {
            return Value::kNullValue;
          }
          const auto &v = args[0].get().getVertex();
          std::string target(args[1].get().getStr());
          for (auto tag : v.tags) {
            if (target == tag.name) {
              return true;
            }
          }
          return false;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };

    attr.returnEntryProps_ = std::vector<std::string>();
    attr.returnEntryProps_.emplace_back("value");

    attr.apocBody_ = [](const auto &args, const std::string ctx) -> Value {
      UNUSED(args);
      UNUSED(ctx);
      return Value::kNullBadType;
    };
  }
  {
    auto &attr = functions_["apoc.coll.contains"];
    attr.normalFuncAttr_.minArity_ = 2;
    attr.normalFuncAttr_.maxArity_ = 2;
    attr.normalFuncAttr_.isAlwaysPure_ = true;
    attr.normalFuncAttr_.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::LIST: {
          if (args[1].get().isNull()) {
            return Value::kNullValue;
          }
          for (auto entry : args[0].get().getList().values) {
            if (args[1].get() == entry) {
              return true;
            }
          }
          return false;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };

    attr.returnEntryProps_ = std::vector<std::string>();
    attr.returnEntryProps_.emplace_back("value");

    attr.apocBody_ = [](const auto &args, const std::string ctx) -> Value {
      UNUSED(args);
      UNUSED(ctx);
      return Value::kNullBadType;
    };
  }
}

StatusOr<Value::Type> FunctionApocManager::getApocReturnType(
    std::string func, const std::vector<Value::Type> &argsType) {
  VLOG(2) << "Get apoc return type for function: " << func << "(" << argsType.size() << ").";

  auto iter = typeSignature_.find(func);
  if (iter == typeSignature_.end()) {
    // fake return type
    // if (func.find(".procedure.") != std::string::npos) {
    //   return Value::Type::LIST;
    // }
    // return Value::Type::STRING;
    return Status::Error("Function `%s' not defined", func.c_str());
  }

  for (const auto &args : iter->second) {
    if (argsType == args.argsType_) {
      return args.returnType_;
    }
  }

  for (auto &argType : argsType) {
    // Most functions do not accept NULL or EMPTY
    // but if the parameters are given by NULL or EMPTY ,
    // then we will tell that it returns NULL or EMPTY
    if (argType == Value::Type::__EMPTY__ || argType == Value::Type::NULLVALUE) {
      return argType;
    }
  }

  return Status::Error("Parameter's type error");
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
    // addFakeApocFunction(func);
    // iter = functions_.find(func);
    return Status::Error("Function `%s' not defined", func.c_str());
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
