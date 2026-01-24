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

#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/util/checked_cast.h"
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

bool IsUnaryOperation(Expression::Operation op) {
  switch (op) {
    case Expression::Operation::kIsNull:
    case Expression::Operation::kNotNull:
    case Expression::Operation::kIsNan:
    case Expression::Operation::kNotNan:
      return true;
    default:
      return false;
  }
}

bool IsSetOperation(Expression::Operation op) {
  switch (op) {
    case Expression::Operation::kIn:
    case Expression::Operation::kNotIn:
      return true;
    default:
      return false;
  }
}

Result<Expression::Operation> OperationTypeFromString(const std::string_view typeStr) {
  if (typeStr == kTypeTrue) return Expression::Operation::kTrue;
  if (typeStr == kTypeFalse) return Expression::Operation::kFalse;
  if (typeStr == kTypeAnd) return Expression::Operation::kAnd;
  if (typeStr == kTypeOr) return Expression::Operation::kOr;
  if (typeStr == kTypeNot) return Expression::Operation::kNot;
  if (typeStr == kTypeEq) return Expression::Operation::kEq;
  if (typeStr == kTypeNotEq) return Expression::Operation::kNotEq;
  if (typeStr == kTypeLt) return Expression::Operation::kLt;
  if (typeStr == kTypeLtEq) return Expression::Operation::kLtEq;
  if (typeStr == kTypeGt) return Expression::Operation::kGt;
  if (typeStr == kTypeGtEq) return Expression::Operation::kGtEq;
  if (typeStr == kTypeIn) return Expression::Operation::kIn;
  if (typeStr == kTypeNotIn) return Expression::Operation::kNotIn;
  if (typeStr == kTypeIsNull) return Expression::Operation::kIsNull;
  if (typeStr == kTypeNotNull) return Expression::Operation::kNotNull;
  if (typeStr == kTypeIsNan) return Expression::Operation::kIsNan;
  if (typeStr == kTypeNotNan) return Expression::Operation::kNotNan;
  if (typeStr == kTypeStartsWith) return Expression::Operation::kStartsWith;
  if (typeStr == kTypeNotStartsWith) return Expression::Operation::kNotStartsWith;

  return JsonParseError("Unknown expression type: {}", typeStr);
}

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
      ICEBERG_CHECK_OR_DIE(false, "Unknown expression operation.");
  }
}

Result<std::shared_ptr<Expression>> ExpressionFromJson(const nlohmann::json& json) {
  // Handle boolean
  if (json.is_boolean()) {
    return json.get<bool>()
               ? internal::checked_pointer_cast<Expression>(True::Instance())
               : internal::checked_pointer_cast<Expression>(False::Instance());
  }
  return JsonParseError("Only booleans are currently supported.");
}

nlohmann::json ExpressionToJson(const Expression& expr) {
  switch (expr.op()) {
    case Expression::Operation::kTrue:
      return true;

    case Expression::Operation::kFalse:
      return false;
    default:
      ICEBERG_CHECK_OR_DIE(false, "Only booleans are currently supported.");
  }
}

}  // namespace iceberg
