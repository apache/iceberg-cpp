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
constexpr std::string_view kTrue = "true";
constexpr std::string_view kFalse = "false";
constexpr std::string_view kEq = "eq";
constexpr std::string_view kAnd = "and";
constexpr std::string_view kOr = "or";
constexpr std::string_view kNot = "not";
constexpr std::string_view kIn = "in";
constexpr std::string_view kNotIn = "not-in";
constexpr std::string_view kLt = "lt";
constexpr std::string_view kLtEq = "lt-eq";
constexpr std::string_view kGt = "gt";
constexpr std::string_view kGtEq = "gt-eq";
constexpr std::string_view kNotEq = "not-eq";
constexpr std::string_view kStartsWith = "starts-with";
constexpr std::string_view kNotStartsWith = "not-starts-with";
constexpr std::string_view kIsNull = "is-null";
constexpr std::string_view kNotNull = "not-null";
constexpr std::string_view kIsNan = "is-nan";
constexpr std::string_view kNotNan = "not-nan";
constexpr std::string_view kCount = "count";
constexpr std::string_view kCountNull = "count-null";
constexpr std::string_view kCountStar = "count-star";
constexpr std::string_view kMin = "min";
constexpr std::string_view kMax = "max";
constexpr std::string_view kLiteral = "literal";
constexpr std::string_view kReference = "reference";

/// Helper to build the transform JSON object shared by Unbound/BoundTransform
nlohmann::json MakeTransformJson(std::string_view transform_str,
                                 std::string_view ref_name) {
  nlohmann::json json;
  json[kType] = kTransform;
  json[kTransform] = transform_str;
  json[kTerm] = ref_name;
  return json;
}

/// Helper to check if a JSON term represents a transform
bool IsTransformTerm(const nlohmann::json& json) {
  return json.is_object() && json.contains(kType) &&
         json[kType].get<std::string>() == kTransform && json.contains(kTerm);
}

/// Template helper to create predicates from JSON with the appropriate term type
template <typename B>
Result<std::unique_ptr<UnboundPredicate>> PredicateFromJson(
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
    if (!json.contains(kValues) || !json[kValues].is_array() || json.contains(kValue))
        [[unlikely]] {
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
  if (!json.contains(kValue) || json.contains(kValues)) [[unlikely]] {
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
  if (typeStr == kTrue) return Expression::Operation::kTrue;
  if (typeStr == kFalse) return Expression::Operation::kFalse;
  if (typeStr == kAnd) return Expression::Operation::kAnd;
  if (typeStr == kOr) return Expression::Operation::kOr;
  if (typeStr == kNot) return Expression::Operation::kNot;
  if (typeStr == kEq) return Expression::Operation::kEq;
  if (typeStr == kNotEq) return Expression::Operation::kNotEq;
  if (typeStr == kLt) return Expression::Operation::kLt;
  if (typeStr == kLtEq) return Expression::Operation::kLtEq;
  if (typeStr == kGt) return Expression::Operation::kGt;
  if (typeStr == kGtEq) return Expression::Operation::kGtEq;
  if (typeStr == kIn) return Expression::Operation::kIn;
  if (typeStr == kNotIn) return Expression::Operation::kNotIn;
  if (typeStr == kIsNull) return Expression::Operation::kIsNull;
  if (typeStr == kNotNull) return Expression::Operation::kNotNull;
  if (typeStr == kIsNan) return Expression::Operation::kIsNan;
  if (typeStr == kNotNan) return Expression::Operation::kNotNan;
  if (typeStr == kStartsWith) return Expression::Operation::kStartsWith;
  if (typeStr == kNotStartsWith) return Expression::Operation::kNotStartsWith;
  if (typeStr == kCount) return Expression::Operation::kCount;
  if (typeStr == kCountNull) return Expression::Operation::kCountNull;
  if (typeStr == kCountStar) return Expression::Operation::kCountStar;
  if (typeStr == kMin) return Expression::Operation::kMin;
  if (typeStr == kMax) return Expression::Operation::kMax;

  return JsonParseError("Unknown expression type: {}", typeStr);
}

Result<nlohmann::json> ToJson(Expression::Operation op) {
  std::string json(ToString(op));
  std::ranges::transform(json, json.begin(), [](unsigned char c) -> char {
    return (c == '_') ? '-' : static_cast<char>(std::tolower(c));
  });
  return nlohmann::json(std::move(json));
}

Result<nlohmann::json> ToJson(const NamedReference& ref) {
  return nlohmann::json(ref.name());
}

Result<std::unique_ptr<NamedReference>> NamedReferenceFromJson(
    const nlohmann::json& json) {
  if (json.is_object() && json.contains(kType) &&
      json[kType].get<std::string>() == kReference && json.contains(kTerm)) {
    return NamedReference::Make(json[kTerm].get<std::string>());
  }
  if (!json.is_string()) [[unlikely]] {
    return JsonParseError("Expected string for named reference");
  }
  return NamedReference::Make(json.get<std::string>());
}

Result<nlohmann::json> ToJson(const UnboundTransform& transform) {
  auto& mut = const_cast<UnboundTransform&>(transform);
  return MakeTransformJson(transform.transform()->ToString(), mut.reference()->name());
}

Result<nlohmann::json> ToJson(const BoundReference& ref) {
  return nlohmann::json(ref.name());
}

Result<nlohmann::json> ToJson(const BoundTransform& transform) {
  auto& mut = const_cast<BoundTransform&>(transform);
  return MakeTransformJson(transform.transform()->ToString(), mut.reference()->name());
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

Result<nlohmann::json> ToJson(const Literal& literal) {
  if (literal.IsNull()) {
    return nlohmann::json(nullptr);
  }

  const auto type_id = literal.type()->type_id();
  const auto& value = literal.value();

  switch (type_id) {
    case TypeId::kBoolean:
      return nlohmann::json(std::get<bool>(value));
    case TypeId::kInt:
      return nlohmann::json(std::get<int32_t>(value));
    case TypeId::kDate:
      return nlohmann::json(TransformUtil::HumanDay(std::get<int32_t>(value)));
    case TypeId::kLong:
      return nlohmann::json(std::get<int64_t>(value));
    case TypeId::kTime:
      return nlohmann::json(TransformUtil::HumanTime(std::get<int64_t>(value)));
    case TypeId::kTimestamp:
      return nlohmann::json(TransformUtil::HumanTimestamp(std::get<int64_t>(value)));
    case TypeId::kTimestampTz:
      return nlohmann::json(
          TransformUtil::HumanTimestampWithZone(std::get<int64_t>(value)));
    case TypeId::kFloat:
      return nlohmann::json(std::get<float>(value));
    case TypeId::kDouble:
      return nlohmann::json(std::get<double>(value));
    case TypeId::kString:
      return nlohmann::json(std::get<std::string>(value));
    case TypeId::kBinary:
    case TypeId::kFixed: {
      // base 16 encoding for binary data
      const auto& bytes = std::get<std::vector<uint8_t>>(value);
      std::string hex;
      hex.reserve(bytes.size() * 2);
      for (uint8_t byte : bytes) {
        hex += std::format("{:02X}", byte);
      }
      return nlohmann::json(std::move(hex));
    }
    case TypeId::kDecimal: {
      return nlohmann::json(literal.ToString());
    }
    case TypeId::kUuid:
      return nlohmann::json(std::get<Uuid>(value).ToString());
    default:
      return NotSupported("Unsupported literal type for JSON serialization: {}",
                          literal.type()->ToString());
  }
}

Result<Literal> LiteralFromJson(const nlohmann::json& json) {
  // Unwrap {"type": "literal", "value": <actual>} wrapper
  if (json.is_object() && json.contains(kType) &&
      json[kType].get<std::string>() == kLiteral && json.contains(kValue)) {
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

Result<nlohmann::json> ToJson(const Term& term) {
  switch (term.kind()) {
    case Term::Kind::kReference:
      if (term.is_unbound())
        return ToJson(internal::checked_cast<const NamedReference&>(term));
      return ToJson(internal::checked_cast<const BoundReference&>(term));
    case Term::Kind::kTransform:
      if (term.is_unbound())
        return ToJson(internal::checked_cast<const UnboundTransform&>(term));
      return ToJson(internal::checked_cast<const BoundTransform&>(term));
    default:
      return NotSupported("Unsupported term kind for JSON serialization");
  }
}

Result<nlohmann::json> ToJson(const UnboundPredicate& pred) {
  nlohmann::json json;
  ICEBERG_ASSIGN_OR_RAISE(json[kType], ToJson(pred.op()));

  ICEBERG_ASSIGN_OR_RAISE(json[kTerm], ToJson(pred.predicate_term()));
  std::span<const Literal> literals = pred.literals();

  if (IsSetOperation(pred.op())) {
    nlohmann::json values = nlohmann::json::array();
    for (const auto& lit : literals) {
      ICEBERG_ASSIGN_OR_RAISE(auto lit_json, ToJson(lit));
      values.push_back(std::move(lit_json));
    }
    json[kValues] = std::move(values);
  } else if (!literals.empty()) {
    ICEBERG_DCHECK(literals.size() == 1,
                   "Expected exactly one literal for non-set predicate");
    ICEBERG_ASSIGN_OR_RAISE(json[kValue], ToJson(literals[0]));
  }
  return json;
}

Result<nlohmann::json> ToJson(const BoundPredicate& pred) {
  nlohmann::json json;
  ICEBERG_ASSIGN_OR_RAISE(json[kType], ToJson(pred.op()));
  ICEBERG_ASSIGN_OR_RAISE(json[kTerm], ToJson(*pred.term()));

  if (IsSetOperation(pred.op())) {
    const auto& sp = internal::checked_cast<const BoundSetPredicate&>(pred);
    nlohmann::json values = nlohmann::json::array();
    for (const auto& lit : sp.literal_set()) {
      ICEBERG_ASSIGN_OR_RAISE(auto lit_json, ToJson(lit));
      values.push_back(std::move(lit_json));
    }
    json[kValues] = std::move(values);
  } else if (!IsUnaryOperation(pred.op())) {
    const auto& lp = internal::checked_cast<const BoundLiteralPredicate&>(pred);
    ICEBERG_ASSIGN_OR_RAISE(json[kValue], ToJson(lp.literal()));
  }
  return json;
}

Result<std::unique_ptr<UnboundPredicate>> UnboundPredicateFromJson(
    const nlohmann::json& json) {
  if (!json.contains(kType) || !json.contains(kTerm)) [[unlikely]] {
    return JsonParseError(
        "Invalid predicate JSON: unexpected 'type' or 'term' field : {}",
        SafeDumpJson(json));
  }
  ICEBERG_ASSIGN_OR_RAISE(auto op, OperationTypeFromJson(json[kType]));

  const auto& term_json = json[kTerm];

  if (IsTransformTerm(term_json)) {
    ICEBERG_ASSIGN_OR_RAISE(auto term, UnboundTransformFromJson(term_json));
    return PredicateFromJson<BoundTransform>(op, std::move(term), json);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto term, NamedReferenceFromJson(term_json));
  return PredicateFromJson<BoundReference>(op, std::move(term), json);
}

Result<std::shared_ptr<Expression>> ExpressionFromJson(const nlohmann::json& json) {
  // Handle boolean constants
  if (json.is_boolean()) {
    return json.get<bool>()
               ? internal::checked_pointer_cast<Expression>(True::Instance())
               : internal::checked_pointer_cast<Expression>(False::Instance());
  }
  if (json.is_string()) {
    auto s = json.get<std::string>();
    std::ranges::transform(s, s.begin(), [](unsigned char c) -> char {
      return static_cast<char>(std::tolower(c));
    });
    if (s == kTrue) return internal::checked_pointer_cast<Expression>(True::Instance());
    if (s == kFalse) return internal::checked_pointer_cast<Expression>(False::Instance());
  }

  if (!json.is_object() || !json.contains(kType)) [[unlikely]] {
    return JsonParseError("expresion JSON must be an object with a 'type' field: {}",
                          SafeDumpJson(json));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto op, OperationTypeFromJson(json[kType]));

  switch (op) {
    case Expression::Operation::kAnd: {
      if (!json.contains(kLeft) || !json.contains(kRight)) [[unlikely]] {
        return JsonParseError("AND expression missing 'left' or 'right' field");
      }
      ICEBERG_ASSIGN_OR_RAISE(auto left, ExpressionFromJson(json[kLeft]));
      ICEBERG_ASSIGN_OR_RAISE(auto right, ExpressionFromJson(json[kRight]));
      return And::Make(std::move(left), std::move(right));
    }
    case Expression::Operation::kOr: {
      if (!json.contains(kLeft) || !json.contains(kRight)) [[unlikely]] {
        return JsonParseError("OR expression missing 'left' or 'right' field");
      }
      ICEBERG_ASSIGN_OR_RAISE(auto left, ExpressionFromJson(json[kLeft]));
      ICEBERG_ASSIGN_OR_RAISE(auto right, ExpressionFromJson(json[kRight]));
      return Or::Make(std::move(left), std::move(right));
    }
    case Expression::Operation::kNot: {
      if (!json.contains(kChild)) [[unlikely]] {
        return JsonParseError("NOT expression missing 'child' field");
      }
      ICEBERG_ASSIGN_OR_RAISE(auto child, ExpressionFromJson(json[kChild]));
      return Not::Make(std::move(child));
    }
    default:
      // All other operations are predicates however since binding does not happen during
      // JSON deserialization, we will always deserialize to an unbound predicate.
      return UnboundPredicateFromJson(json);
  }
}

Result<nlohmann::json> ToJson(const Expression& expr) {
  switch (expr.op()) {
    case Expression::Operation::kTrue:
      return nlohmann::json(true);
    case Expression::Operation::kFalse:
      return nlohmann::json(false);
    case Expression::Operation::kAnd: {
      const auto& and_expr = internal::checked_cast<const And&>(expr);
      nlohmann::json json;
      ICEBERG_ASSIGN_OR_RAISE(json[kType], ToJson(expr.op()));
      ICEBERG_ASSIGN_OR_RAISE(json[kLeft], ToJson(*and_expr.left()));
      ICEBERG_ASSIGN_OR_RAISE(json[kRight], ToJson(*and_expr.right()));
      return json;
    }
    case Expression::Operation::kOr: {
      const auto& or_expr = internal::checked_cast<const Or&>(expr);
      nlohmann::json json;
      ICEBERG_ASSIGN_OR_RAISE(json[kType], ToJson(expr.op()));
      ICEBERG_ASSIGN_OR_RAISE(json[kLeft], ToJson(*or_expr.left()));
      ICEBERG_ASSIGN_OR_RAISE(json[kRight], ToJson(*or_expr.right()));
      return json;
    }
    case Expression::Operation::kNot: {
      const auto& not_expr = internal::checked_cast<const Not&>(expr);
      nlohmann::json json;
      ICEBERG_ASSIGN_OR_RAISE(json[kType], ToJson(expr.op()));
      ICEBERG_ASSIGN_OR_RAISE(json[kChild], ToJson(*not_expr.child()));
      return json;
    }
    default:
      if (expr.is_unbound_predicate())
        return ToJson(dynamic_cast<const UnboundPredicate&>(expr));
      if (expr.is_bound_predicate())
        return ToJson(dynamic_cast<const BoundPredicate&>(expr));
      return NotSupported("Unsupported expression type for JSON serialization");
  }
}

}  // namespace iceberg
