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

#include <format>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {
// Expression type strings
constexpr std::string_view kTypeTrue = "true";
constexpr std::string_view kTypeFalse = "false";
constexpr std::string_view kTypeEq = "eq";
constexpr std::string_view kTypeAnd = "and";
constexpr std::string_view kTypeOr = "or";
constexpr std::string_view kTypeNot = "not";
constexpr std::string_view kTypeIn = "in";
constexpr std::string_view kTypeNotIn = "not-in";
constexpr std::string_view kTypeLt = "lt";
constexpr std::string_view kTypeLtEq = "lt-eq";
constexpr std::string_view kTypeGt = "gt";
constexpr std::string_view kTypeGtEq = "gt-eq";
constexpr std::string_view kTypeNotEq = "not-eq";
constexpr std::string_view kTypeStartsWith = "starts-with";
constexpr std::string_view kTypeNotStartsWith = "not-starts-with";
constexpr std::string_view kTypeIsNull = "is-null";
constexpr std::string_view kTypeNotNull = "not-null";
constexpr std::string_view kTypeIsNan = "is-nan";
constexpr std::string_view kTypeNotNan = "not-nan";
}  // namespace

/// \brief Converts a JSON type string to an Expression::Operation.
///
/// \param type_str The JSON type string
/// \return The corresponding Operation or an error if unknown
Result<Expression::Operation> OperationTypeFromString(const std::string_view type_str) {
  if (type_str == kTypeTrue) return Expression::Operation::kTrue;
  if (type_str == kTypeFalse) return Expression::Operation::kFalse;
  if (type_str == kTypeAnd) return Expression::Operation::kAnd;
  if (type_str == kTypeOr) return Expression::Operation::kOr;
  if (type_str == kTypeNot) return Expression::Operation::kNot;
  if (type_str == kTypeEq) return Expression::Operation::kEq;
  if (type_str == kTypeNotEq) return Expression::Operation::kNotEq;
  if (type_str == kTypeLt) return Expression::Operation::kLt;
  if (type_str == kTypeLtEq) return Expression::Operation::kLtEq;
  if (type_str == kTypeGt) return Expression::Operation::kGt;
  if (type_str == kTypeGtEq) return Expression::Operation::kGtEq;
  if (type_str == kTypeIn) return Expression::Operation::kIn;
  if (type_str == kTypeNotIn) return Expression::Operation::kNotIn;
  if (type_str == kTypeIsNull) return Expression::Operation::kIsNull;
  if (type_str == kTypeNotNull) return Expression::Operation::kNotNull;
  if (type_str == kTypeIsNan) return Expression::Operation::kIsNan;
  if (type_str == kTypeNotNan) return Expression::Operation::kNotNan;
  if (type_str == kTypeStartsWith) return Expression::Operation::kStartsWith;
  if (type_str == kTypeNotStartsWith) return Expression::Operation::kNotStartsWith;

  return JsonParseError("Unknown expression type: {}", type_str);
}

/// \brief Converts an Expression::Operation to its JSON string representation.
///
/// \param op The operation to convert
/// \return The JSON type string (e.g., "eq", "lt-eq", "is-null")
std::string_view ToStringOperationType(Expression::Operation op) {
  switch (op) {
    case Expression::Operation::kTrue:
      return kTypeTrue;
    case Expression::Operation::kFalse:
      return kTypeFalse;
    case Expression::Operation::kAnd:
      return kTypeAnd;
    case Expression::Operation::kOr:
      return kTypeOr;
    case Expression::Operation::kNot:
      return kTypeNot;
    case Expression::Operation::kEq:
      return kTypeEq;
    case Expression::Operation::kNotEq:
      return kTypeNotEq;
    case Expression::Operation::kLt:
      return kTypeLt;
    case Expression::Operation::kLtEq:
      return kTypeLtEq;
    case Expression::Operation::kGt:
      return kTypeGt;
    case Expression::Operation::kGtEq:
      return kTypeGtEq;
    case Expression::Operation::kIn:
      return kTypeIn;
    case Expression::Operation::kNotIn:
      return kTypeNotIn;
    case Expression::Operation::kIsNull:
      return kTypeIsNull;
    case Expression::Operation::kNotNull:
      return kTypeNotNull;
    case Expression::Operation::kIsNan:
      return kTypeIsNan;
    case Expression::Operation::kNotNan:
      return kTypeNotNan;
    case Expression::Operation::kStartsWith:
      return kTypeStartsWith;
    case Expression::Operation::kNotStartsWith:
      return kTypeNotStartsWith;
    default:
      return "unknown";
  }
}

Result<std::shared_ptr<Expression>> ExpressionFromJson(const nlohmann::json& json) {
  // Handle boolean
  if (json.is_boolean()) {
    return json.get<bool>() ? std::static_pointer_cast<Expression>(True::Instance())
                            : std::static_pointer_cast<Expression>(False::Instance());
  }
  return JsonParseError("Only booleans are currently supported");
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
