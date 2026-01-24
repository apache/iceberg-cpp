/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "iceberg/expression/json_internal.h"

#include <format>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/literal.h"
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<Expression>> ExpressionFromJson(const nlohmann::json& json) {
  // Handle boolean literals
  if (json.is_boolean()) {
    return json.get<bool>() ? std::static_pointer_cast<Expression>(True::Instance())
                            : std::static_pointer_cast<Expression>(False::Instance());
  }
  return JsonParseError("Only boolean literals are currently supported");
}

nlohmann::json ToJson(const Expression& expr) {
  switch (expr.op()) {
    case Expression::Operation::kTrue:
      return true;

    case Expression::Operation::kFalse:
      return false;
    default:
      throw std::logic_error("Only booleans are currently supported");
  }
}

#define ICEBERG_DEFINE_FROM_JSON(Model)                                        \
  template <>                                                                  \
  Result<std::shared_ptr<Model>> FromJson<Model>(const nlohmann::json& json) { \
    return Model##FromJson(json);                                              \
  }

ICEBERG_DEFINE_FROM_JSON(Expression)

}  // namespace iceberg
