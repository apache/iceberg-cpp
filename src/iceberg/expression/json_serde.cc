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

#include <ranges>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/term.h"
#include "iceberg/transform.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/transform_util.h"

namespace iceberg {
namespace {
// JSON field names
constexpr std::string_view kType = "type";
constexpr std::string_view kTerm = "term";
constexpr std::string_view kTransform = "transform";
constexpr std::string_view kValue = "value";
constexpr std::string_view kValues = "values";
constexpr std::string_view kLeft = "left";
constexpr std::string_view kRight = "right";
constexpr std::string_view kChild = "child";
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
constexpr std::string_view kTypeLiteral = "literal";
constexpr std::string_view kTypeReference = "reference";

/// Helper to check if a JSON term represents a transform
bool IsTransformTerm(const nlohmann::json& json) {
  return json.is_object() && json.contains(kType) &&
         json[kType].get<std::string>() == kTransform && json.contains(kTerm);
}

/// Template helper to create predicates from JSON with the appropriate term type
template <typename B>
Result<std::unique_ptr<UnboundPredicate>> MakePredicateFromJson(
    Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term,
    const nlohmann::json& json) {
  if (IsUnaryOperation(op)) {
    if (json.contains(kValue)) [[unlikely]] {
      return JsonParseError("Unary predicate has invalid 'value' field: {}",
                            SafeDumpJson(json));
    }
    if (json.contains(kValues)) [[unlikely]] {
      return JsonParseError("Unary predicate has invalid 'values' field: {}",
                            SafeDumpJson(json));
    }
    return UnboundPredicateImpl<B>::Make(op, std::move(term));
  }

  if (IsSetOperation(op)) {
    std::vector<Literal> literals;
    if (!json.contains(kValues) || !json[kValues].is_array()) [[unlikely]] {
      return JsonParseError("Missing or invalid 'values' field for set operation: {}",
                            SafeDumpJson(json));
    }
    for (const auto& val : json[kValues]) {
      ICEBERG_ASSIGN_OR_RAISE(auto lit, LiteralFromJson(val));
      literals.push_back(std::move(lit));
    }
    return UnboundPredicateImpl<B>::Make(op, std::move(term), std::move(literals));
  }

  // Literal predicate
  if (!json.contains(kValue)) [[unlikely]] {
    return JsonParseError("Missing 'value' field for literal predicate: {}",
                          SafeDumpJson(json));
  }
  ICEBERG_ASSIGN_OR_RAISE(auto literal, LiteralFromJson(json[kValue]));
  return UnboundPredicateImpl<B>::Make(op, std::move(term), std::move(literal));
}
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
  if (!json.is_string()) [[unlikely]] {
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

Result<std::unique_ptr<NamedReference>> NamedReferenceFromJson(
    const nlohmann::json& json) {
  if (json.is_object() && json.contains(kType) &&
      json[kType].get<std::string>() == kTypeReference && json.contains(kTerm)) {
    return NamedReference::Make(json[kTerm].get<std::string>());
  }
  if (!json.is_string()) [[unlikely]] {
    return JsonParseError("Expected string for named reference");
  }
  return NamedReference::Make(json.get<std::string>());
}

nlohmann::json ToJson(const UnboundTransform& transform) {
  auto& mutable_transform = const_cast<UnboundTransform&>(transform);
  nlohmann::json json;
  json[kType] = kTransform;
  json[kTransform] = transform.transform()->ToString();
  json[kTerm] = mutable_transform.reference()->name();
  return json;
}

Result<std::unique_ptr<UnboundTransform>> UnboundTransformFromJson(
    const nlohmann::json& json) {
  if (IsTransformTerm(json)) {
    ICEBERG_ASSIGN_OR_RAISE(auto transform_str,
                            GetJsonValue<std::string>(json, kTransform));
    ICEBERG_ASSIGN_OR_RAISE(auto transform, TransformFromString(transform_str));
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReferenceFromJson(json[kTerm]));
    return UnboundTransform::Make(std::move(ref), std::move(transform));
  }
  return JsonParseError("Invalid unbound transform json: {}", SafeDumpJson(json));
}

nlohmann::json ToJson(const Literal& literal) {
  if (literal.IsNull()) {
    return nullptr;
  }

  const auto type_id = literal.type()->type_id();
  const auto& value = literal.value();

  switch (type_id) {
    case TypeId::kBoolean:
      return std::get<bool>(value);
    case TypeId::kInt:
      return std::get<int32_t>(value);
    case TypeId::kDate:
      return TransformUtil::HumanDay(std::get<int32_t>(value));
    case TypeId::kLong:
      return std::get<int64_t>(value);
    case TypeId::kTime:
      return TransformUtil::HumanTime(std::get<int64_t>(value));
    case TypeId::kTimestamp:
      return TransformUtil::HumanTimestamp(std::get<int64_t>(value));
    case TypeId::kTimestampTz:
      return TransformUtil::HumanTimestampWithZone(std::get<int64_t>(value));
    case TypeId::kFloat:
      return std::get<float>(value);
    case TypeId::kDouble:
      return std::get<double>(value);
    case TypeId::kString:
      return std::get<std::string>(value);
    case TypeId::kBinary:
    case TypeId::kFixed: {
      const auto& bytes = std::get<std::vector<uint8_t>>(value);
      std::string hex;
      hex.reserve(bytes.size() * 2);
      for (uint8_t byte : bytes) {
        hex += std::format("{:02X}", byte);
      }
      return hex;
    }
    case TypeId::kDecimal: {
      return literal.ToString();
    }
    case TypeId::kUuid:
      return std::get<Uuid>(value).ToString();
    default:
      nlohmann::json json;
      return json;
  }
}

Result<Literal> LiteralFromJson(const nlohmann::json& json) {
  // Unwrap {"type": "literal", "value": <actual>} wrapper
  if (json.is_object() && json.contains(kType) &&
      json[kType].get<std::string>() == kTypeLiteral && json.contains(kValue)) {
    return LiteralFromJson(json[kValue]);
  }
  if (json.is_null()) {
    return Literal::Null(nullptr);
  }
  if (json.is_boolean()) {
    return Literal::Boolean(json.get<bool>());
  }
  if (json.is_number_integer()) {
    return Literal::Long(json.get<int64_t>());
  }
  if (json.is_number_float()) {
    return Literal::Double(json.get<double>());
  }
  if (json.is_string()) {
    // All strings are returned as String literals.
    // Conversion to binary/date/time/etc. happens during binding
    // when schema type information is available.
    return Literal::String(json.get<std::string>());
  }
  return JsonParseError("Unsupported literal JSON type");
}

nlohmann::json TermToJson(const Term& term) {
  switch (term.kind()) {
    case Term::Kind::kReference:
      return ToJson(static_cast<const NamedReference&>(term));
    case Term::Kind::kTransform:
      return ToJson(static_cast<const UnboundTransform&>(term));
    default:
      return nullptr;
  }
}

nlohmann::json ToJson(const UnboundPredicate& pred) {
  nlohmann::json json;
  json[kType] = ToJson(pred.op());

  // Get term and literals by casting to the appropriate impl type
  std::span<const Literal> literals;

  if (auto* ref_pred = dynamic_cast<const UnboundPredicateImpl<BoundReference>*>(&pred)) {
    json[kTerm] = TermToJson(*ref_pred->term());
    literals = ref_pred->literals();
  } else if (auto* transform_pred =
                 dynamic_cast<const UnboundPredicateImpl<BoundTransform>*>(&pred)) {
    json[kTerm] = TermToJson(*transform_pred->term());
    literals = transform_pred->literals();
  }

  if (!IsUnaryOperation(pred.op())) {
    if (IsSetOperation(pred.op())) {
      nlohmann::json values = nlohmann::json::array();
      for (const auto& lit : literals) {
        values.push_back(ToJson(lit));
      }
      json[kValues] = std::move(values);
    } else if (!literals.empty()) {
      json[kValue] = ToJson(literals[0]);
    }
  }
  return json;
}

Result<std::unique_ptr<UnboundPredicate>> UnboundPredicateFromJson(
    const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto op, OperationTypeFromJson(json[kType]));

  const auto& term_json = json[kTerm];

  if (IsTransformTerm(term_json)) {
    ICEBERG_ASSIGN_OR_RAISE(auto term, UnboundTransformFromJson(term_json));
    return MakePredicateFromJson<BoundTransform>(op, std::move(term), json);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto term, NamedReferenceFromJson(term_json));
  return MakePredicateFromJson<BoundReference>(op, std::move(term), json);
}

Result<std::shared_ptr<Expression>> ExpressionFromJson(const nlohmann::json& json) {
  // Handle boolean constants
  if (json.is_boolean()) {
    return json.get<bool>()
               ? internal::checked_pointer_cast<Expression>(True::Instance())
               : internal::checked_pointer_cast<Expression>(False::Instance());
  }

  if (!json.is_object()) [[unlikely]] {
    return JsonParseError("Expression must be boolean or object");
  }

  ICEBERG_ASSIGN_OR_RAISE(auto op, OperationTypeFromJson(json[kType]));

  switch (op) {
    case Expression::Operation::kAnd: {
      if (!json.contains(kLeft) || !json.contains(kRight)) [[unlikely]] {
        return JsonParseError("AND expression missing 'left' or 'right' field");
      }
      ICEBERG_ASSIGN_OR_RAISE(auto left, ExpressionFromJson(json[kLeft]));
      ICEBERG_ASSIGN_OR_RAISE(auto right, ExpressionFromJson(json[kRight]));
      ICEBERG_ASSIGN_OR_RAISE(auto result, And::Make(std::move(left), std::move(right)));
      return std::shared_ptr<Expression>(std::move(result));
    }
    case Expression::Operation::kOr: {
      if (!json.contains(kLeft) || !json.contains(kRight)) [[unlikely]] {
        return JsonParseError("OR expression missing 'left' or 'right' field");
      }
      ICEBERG_ASSIGN_OR_RAISE(auto left, ExpressionFromJson(json[kLeft]));
      ICEBERG_ASSIGN_OR_RAISE(auto right, ExpressionFromJson(json[kRight]));
      ICEBERG_ASSIGN_OR_RAISE(auto result, Or::Make(std::move(left), std::move(right)));
      return std::shared_ptr<Expression>(std::move(result));
    }
    case Expression::Operation::kNot: {
      if (!json.contains(kChild)) [[unlikely]] {
        return JsonParseError("NOT expression missing 'child' field");
      }
      ICEBERG_ASSIGN_OR_RAISE(auto child, ExpressionFromJson(json[kChild]));
      ICEBERG_ASSIGN_OR_RAISE(auto result, Not::Make(std::move(child)));
      return std::shared_ptr<Expression>(std::move(result));
    }
    default:
      // All other operations are predicates
      return UnboundPredicateFromJson(json);
  }
}

nlohmann::json ToJson(const Expression& expr) {
  switch (expr.op()) {
    case Expression::Operation::kTrue:
      return true;
    case Expression::Operation::kFalse:
      return false;
    case Expression::Operation::kAnd: {
      const auto& and_expr = static_cast<const And&>(expr);
      nlohmann::json json;
      json[kType] = ToJson(expr.op());
      json[kLeft] = ToJson(*and_expr.left());
      json[kRight] = ToJson(*and_expr.right());
      return json;
    }
    case Expression::Operation::kOr: {
      const auto& or_expr = static_cast<const Or&>(expr);
      nlohmann::json json;
      json[kType] = ToJson(expr.op());
      json[kLeft] = ToJson(*or_expr.left());
      json[kRight] = ToJson(*or_expr.right());
      return json;
    }
    case Expression::Operation::kNot: {
      const auto& not_expr = static_cast<const Not&>(expr);
      nlohmann::json json;
      json[kType] = ToJson(expr.op());
      json[kChild] = ToJson(*not_expr.child());
      return json;
    }
    default:
      return ToJson(dynamic_cast<const UnboundPredicate&>(expr));
  }
}

}  // namespace iceberg
