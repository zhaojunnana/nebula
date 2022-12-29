/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_FUNCTION_FUNCTIONAPOCMANAGER_H_
#define COMMON_FUNCTION_FUNCTIONAPOCMANAGER_H_

#include <vector>
#include "FunctionManager.h"
#include "GraphFunction.h"

namespace nebula {

class FunctionApocManager {
 public:
  using ApocFunction =
  std::function<Value(const std::vector<FunctionManager::ArgType> &, const std::string)>;

  static StatusOr<Value::Type>
  getApocReturnType(const std::string functionName, const std::vector<Value::Type> &argsType);

  static StatusOr<const FunctionManager::FunctionAttributes> loadApocFunction(
      std::string functionName, size_t arity);

  static StatusOr<ApocFunction> get(const std::string &func, size_t arity);

  static StatusOr<std::vector<std::string>>
  getReturnEntryProps(const std::string &func, size_t arity);

  static FunctionApocManager &instance();

  // The attributes of the apoc function call
  struct FunctionApocAttributes final {
    FunctionManager::FunctionAttributes normalFuncAttr_;
    std::vector<std::string> returnEntryProps_;
    ApocFunction apocBody_;
  };

  FunctionApocManager();

 private:
  void addFakeApocFunction(std::string funcName) const;

  StatusOr<const FunctionApocAttributes> getInternal(std::string func, size_t arity) const;
};

}  // namespace nebula
#endif  // COMMON_FUNCTION_FUNCTIONAPOCMANAGER_H_
