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
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/term.h"
#include "iceberg/transform.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {
// JSON field names
constexpr std::string_view kType = "type";
constexpr std::string_view kTerm = "term";
constexpr std::string_view kTransform = "transform";
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
constexpr std::string_view kTypeCount = "count";
constexpr std::string_view kTypeCountNull = "count-null";
constexpr std::string_view kTypeCountStar = "count-star";
constexpr std::string_view kTypeMin = "min";
constexpr std::string_view kTypeMax = "max";
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

Result<Expression::Operation> OperationTypeFromJson(const nlohmann::json& json) {
  if (!json.is_string()) {
    return JsonParseError("Unable to create operation. Json value is not a string");
  }
  auto typeStr = json.get<std::string>();
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
  if (typeStr == kTypeCount) return Expression::Operation::kCount;
  if (typeStr == kTypeCountNull) return Expression::Operation::kCountNull;
  if (typeStr == kTypeCountStar) return Expression::Operation::kCountStar;
  if (typeStr == kTypeMin) return Expression::Operation::kMin;
  if (typeStr == kTypeMax) return Expression::Operation::kMax;

  return JsonParseError("Unknown expression type: {}", typeStr);
}

nlohmann::json ToJson(Expression::Operation op) {
  std::string json(ToString(op));
  std::ranges::transform(json, json.begin(), [](unsigned char c) -> char {
    return (c == '_') ? '-' : static_cast<char>(std::tolower(c));
  });
  return json;
}

nlohmann::json ToJson(const NamedReference& ref) { return ref.name(); }

Result<std::shared_ptr<NamedReference>> NamedReferenceFromJson(
    const nlohmann::json& json) {
  if (!json.is_string()) [[unlikely]] {
    return JsonParseError("Expected string for named reference");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(json.get<std::string>()));
  return std::shared_ptr<NamedReference>(std::move(ref));
}

nlohmann::json ToJson(const UnboundTransform& transform) {
  auto& mutable_transform = const_cast<UnboundTransform&>(transform);
  nlohmann::json json;
  json[kType] = std::string(kTransform);
  json[kTransform] = transform.transform()->ToString();
  json[kTerm] = mutable_transform.reference()->name();
  return json;
}

Result<std::shared_ptr<UnboundTransform>> UnboundTransformFromJson(
    const nlohmann::json& json) {
  if (json.is_object() && json.contains(kType) &&
      json[kType] == std::string(kTransform) && json.contains(kTerm)) {
    ICEBERG_ASSIGN_OR_RAISE(auto transform_str,
                            GetJsonValue<std::string>(json, kTransform));
    ICEBERG_ASSIGN_OR_RAISE(auto transform, TransformFromString(transform_str));
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReferenceFromJson(json[kTerm]));
    ICEBERG_ASSIGN_OR_RAISE(auto result,
                            UnboundTransform::Make(std::move(ref), std::move(transform)));
    return std::shared_ptr<UnboundTransform>(std::move(result));
  }
  return JsonParseError("Invalid unbound transform json: {}", SafeDumpJson(json));
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

nlohmann::json ToJson(const Expression& expr) {
  switch (expr.op()) {
    case Expression::Operation::kTrue:
      return true;

    case Expression::Operation::kFalse:
      return false;
    default:
      // TODO(evindj): This code will be removed as we implemented the full expression
      // serialization.
      ICEBERG_CHECK_OR_DIE(false, "Only booleans are currently supported.");
  }
}

}  // namespace iceberg
