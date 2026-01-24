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
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/term.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

// JSON field names
constexpr std::string_view kType = "type";
constexpr std::string_view kTerm = "term";
constexpr std::string_view kValue = "value";
constexpr std::string_view kValues = "values";
constexpr std::string_view kLeft = "left";
constexpr std::string_view kRight = "right";
constexpr std::string_view kChild = "child";
constexpr std::string_view kTransform = "transform";

// Expression type strings
constexpr std::string_view kTypeTrue = "true";
constexpr std::string_view kTypeFalse = "false";
constexpr std::string_view kTypeAnd = "and";
constexpr std::string_view kTypeOr = "or";
constexpr std::string_view kTypeNot = "not";
constexpr std::string_view kTypeEq = "eq";
constexpr std::string_view kTypeNotEq = "not-eq";
constexpr std::string_view kTypeLt = "lt";
constexpr std::string_view kTypeLtEq = "lt-eq";
constexpr std::string_view kTypeGt = "gt";
constexpr std::string_view kTypeGtEq = "gt-eq";
constexpr std::string_view kTypeIn = "in";
constexpr std::string_view kTypeNotIn = "not-in";
constexpr std::string_view kTypeIsNull = "is-null";
constexpr std::string_view kTypeNotNull = "not-null";
constexpr std::string_view kTypeIsNan = "is-nan";
constexpr std::string_view kTypeNotNan = "not-nan";
constexpr std::string_view kTypeStartsWith = "starts-with";
constexpr std::string_view kTypeNotStartsWith = "not-starts-with";
constexpr std::string_view kTypeReference = "reference";

// Term type for transform
constexpr std::string_view kTypeTransform = "transform";

/// Serialize a term (NamedReference or UnboundTransform) to JSON
nlohmann::json TermToJson(const Term& term) {
  if (term.kind() == Term::Kind::kReference) {
    // Simple references are serialized as plain strings
    return std::string(dynamic_cast<const NamedReference&>(term).name());
  } else if (term.kind() == Term::Kind::kTransform) {
    // Note: const_cast is safe here because reference() just returns a shared_ptr
    // and we're only reading from it. The method is not const due to interface design.
    auto& transform_term =
        const_cast<UnboundTransform&>(dynamic_cast<const UnboundTransform&>(term));
    nlohmann::json json;
    json[kType] = kTypeTransform;
    json[kTransform] = transform_term.transform()->ToString();
    json[kTerm] = std::string(transform_term.reference()->name());
    return json;
  }
  // Fallback for unknown term types
  return nlohmann::json{};
}

/// Parse a term from JSON (returns NamedReference or UnboundTransform)
Result<std::shared_ptr<UnboundTerm<BoundReference>>> TermFromJsonAsReference(
    const nlohmann::json& json) {
  // Handle string term (simple reference)
  if (json.is_string()) {
    ICEBERG_ASSIGN_OR_RAISE(auto name, GetTypedJsonValue<std::string>(json));
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::move(name)));
    return std::shared_ptr<NamedReference>(std::move(ref));
  }

  // Handle object term
  if (json.is_object()) {
    ICEBERG_ASSIGN_OR_RAISE(auto type_str, GetJsonValue<std::string>(json, kType));

    if (type_str == kTypeReference) {
      ICEBERG_ASSIGN_OR_RAISE(auto name, GetJsonValue<std::string>(json, kTerm));
      ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::move(name)));
      return std::shared_ptr<NamedReference>(std::move(ref));
    }

    if (type_str == kTypeTransform) {
      ICEBERG_ASSIGN_OR_RAISE(auto transform_str, GetJsonValue<std::string>(json, kTransform));
      ICEBERG_ASSIGN_OR_RAISE(auto term_name, GetJsonValue<std::string>(json, kTerm));
      ICEBERG_ASSIGN_OR_RAISE(auto transform, TransformFromString(transform_str));
      ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::move(term_name)));
      // For UnboundTransform, we need to return it as UnboundTerm<BoundReference>
      // However, UnboundTransform binds to BoundTransform, not BoundReference.
      // The Java implementation handles this by using a common Term interface.
      // For now, we'll handle this case by returning an error for transforms
      // when expecting a reference term.
      return JsonParseError("Transform terms are not supported in this context: {}",
                            SafeDumpJson(json));
    }

    return JsonParseError("Unknown term type '{}' in {}", type_str, SafeDumpJson(json));
  }

  return JsonParseError("Invalid term format, expected string or object: {}",
                        SafeDumpJson(json));
}

/// Check if an operation is a unary predicate (no values)
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

/// Check if an operation is a set predicate (multiple values)
bool IsSetOperation(Expression::Operation op) {
  switch (op) {
    case Expression::Operation::kIn:
    case Expression::Operation::kNotIn:
      return true;
    default:
      return false;
  }
}

}  // namespace

std::string_view OperationToJsonType(Expression::Operation op) {
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

Result<Expression::Operation> OperationFromJsonType(std::string_view type_str) {
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

nlohmann::json LiteralToJson(const Literal& literal) {
  if (literal.IsNull()) {
    return nlohmann::json(nullptr);
  }

  const auto& value = literal.value();
  const auto type_id = literal.type()->type_id();

  // Handle based on the variant type
  if (std::holds_alternative<bool>(value)) {
    return std::get<bool>(value);
  }
  if (std::holds_alternative<int32_t>(value)) {
    return std::get<int32_t>(value);
  }
  if (std::holds_alternative<int64_t>(value)) {
    return std::get<int64_t>(value);
  }
  if (std::holds_alternative<float>(value)) {
    return std::get<float>(value);
  }
  if (std::holds_alternative<double>(value)) {
    return std::get<double>(value);
  }
  if (std::holds_alternative<std::string>(value)) {
    return std::get<std::string>(value);
  }
  if (std::holds_alternative<Uuid>(value)) {
    return std::get<Uuid>(value).ToString();
  }
  if (std::holds_alternative<std::vector<uint8_t>>(value)) {
    // Binary and Fixed are serialized as hex strings
    const auto& bytes = std::get<std::vector<uint8_t>>(value);
    std::string hex;
    hex.reserve(bytes.size() * 2);
    for (uint8_t byte : bytes) {
      hex += std::format("{:02x}", byte);
    }
    return hex;
  }
  if (std::holds_alternative<Decimal>(value)) {
    // Decimal is serialized as string representation
    const auto& decimal = std::get<Decimal>(value);
    if (type_id == TypeId::kDecimal) {
      const auto& decimal_type = static_cast<const DecimalType&>(*literal.type());
      auto result = decimal.ToString(decimal_type.scale());
      if (result.has_value()) {
        return result.value();
      }
    }
    // Fallback to integer string representation
    return decimal.ToIntegerString();
  }

  // Fallback: use ToString()
  return literal.ToString();
}

Result<Literal> LiteralFromJson(const nlohmann::json& json) {
  if (json.is_null()) {
    // We don't have type information, so we can't create a proper null literal
    return JsonParseError("Cannot deserialize null literal without type information");
  }

  if (json.is_boolean()) {
    return Literal::Boolean(json.get<bool>());
  }

  if (json.is_number_integer()) {
    // Try to fit into int32, otherwise use int64
    auto val = json.get<int64_t>();
    if (val >= std::numeric_limits<int32_t>::min() &&
        val <= std::numeric_limits<int32_t>::max()) {
      return Literal::Int(static_cast<int32_t>(val));
    }
    return Literal::Long(val);
  }

  if (json.is_number_float()) {
    return Literal::Double(json.get<double>());
  }

  if (json.is_string()) {
    return Literal::String(json.get<std::string>());
  }

  return JsonParseError("Unsupported JSON literal type: {}", SafeDumpJson(json));
}

nlohmann::json ToJson(const NamedReference& ref) { return std::string(ref.name()); }

nlohmann::json ToJson(const UnboundTransform& transform) {
  // Note: const_cast is safe here because reference() just returns a shared_ptr
  // and we're only reading from it. The method is not const due to interface design.
  auto& mutable_transform = const_cast<UnboundTransform&>(transform);
  nlohmann::json json;
  json[kType] = kTypeTransform;
  json[kTransform] = transform.transform()->ToString();
  json[kTerm] = std::string(mutable_transform.reference()->name());
  return json;
}

nlohmann::json ToJson(const UnboundPredicate& predicate) {
  nlohmann::json json;
  json[kType] = OperationToJsonType(predicate.op());

  // Get the term from the predicate
  // Note: const_cast is safe here because reference() just returns a shared_ptr
  // and we're only reading from it. The method is not const due to interface design.
  auto& mutable_predicate = const_cast<UnboundPredicate&>(predicate);
  auto ref = mutable_predicate.reference();
  if (ref) {
    json[kTerm] = std::string(ref->name());
  }

  // For predicates with values, we need to cast to the concrete type
  // UnboundPredicateImpl<BoundReference> to access literals()
  const auto* pred_impl =
      dynamic_cast<const UnboundPredicateImpl<BoundReference>*>(&predicate);
  if (pred_impl) {
    auto literals = pred_impl->literals();
    if (!literals.empty()) {
      if (IsSetOperation(predicate.op())) {
        nlohmann::json values_array = nlohmann::json::array();
        for (const auto& lit : literals) {
          values_array.push_back(LiteralToJson(lit));
        }
        json[kValues] = std::move(values_array);
      } else if (literals.size() == 1) {
        json[kValue] = LiteralToJson(literals[0]);
      }
    }
  }

  return json;
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
      json[kType] = kTypeAnd;
      json[kLeft] = ToJson(*and_expr.left());
      json[kRight] = ToJson(*and_expr.right());
      return json;
    }

    case Expression::Operation::kOr: {
      const auto& or_expr = static_cast<const Or&>(expr);
      nlohmann::json json;
      json[kType] = kTypeOr;
      json[kLeft] = ToJson(*or_expr.left());
      json[kRight] = ToJson(*or_expr.right());
      return json;
    }

    case Expression::Operation::kNot: {
      const auto& not_expr = static_cast<const Not&>(expr);
      nlohmann::json json;
      json[kType] = kTypeNot;
      json[kChild] = ToJson(*not_expr.child());
      return json;
    }

    default:
      // Handle predicates
      if (expr.is_unbound_predicate()) {
        // Use dynamic_cast due to virtual inheritance
        const auto* pred = dynamic_cast<const UnboundPredicate*>(&expr);
        if (pred) {
          return ToJson(*pred);
        }
      }
      // Fallback for unknown expression types
      nlohmann::json json;
      json[kType] = OperationToJsonType(expr.op());
      return json;
  }
}

Result<std::shared_ptr<UnboundPredicate>> UnboundPredicateFromJson(
    const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto type_str, GetJsonValue<std::string>(json, kType));
  ICEBERG_ASSIGN_OR_RAISE(auto op, OperationFromJsonType(type_str));

  // Parse the term
  ICEBERG_ASSIGN_OR_RAISE(auto term_json, GetJsonValue<nlohmann::json>(json, kTerm));
  ICEBERG_ASSIGN_OR_RAISE(auto term, TermFromJsonAsReference(term_json));

  // Handle unary predicates (no value)
  if (IsUnaryOperation(op)) {
    ICEBERG_ASSIGN_OR_RAISE(auto pred, UnboundPredicateImpl<BoundReference>::Make(
                                           op, std::move(term)));
    return std::shared_ptr<UnboundPredicate>(std::move(pred));
  }

  // Handle set predicates (multiple values)
  if (IsSetOperation(op)) {
    if (!json.contains(kValues)) {
      return JsonParseError("Missing '{}' for set predicate in {}", kValues,
                            SafeDumpJson(json));
    }
    ICEBERG_ASSIGN_OR_RAISE(auto values_json, GetJsonValue<nlohmann::json>(json, kValues));
    if (!values_json.is_array()) {
      return JsonParseError("Expected array for '{}' in {}", kValues, SafeDumpJson(json));
    }

    std::vector<Literal> values;
    values.reserve(values_json.size());
    for (const auto& val_json : values_json) {
      ICEBERG_ASSIGN_OR_RAISE(auto lit, LiteralFromJson(val_json));
      values.push_back(std::move(lit));
    }

    ICEBERG_ASSIGN_OR_RAISE(
        auto pred,
        UnboundPredicateImpl<BoundReference>::Make(op, std::move(term), std::move(values)));
    return std::shared_ptr<UnboundPredicate>(std::move(pred));
  }

  // Handle literal predicates (single value)
  if (!json.contains(kValue)) {
    return JsonParseError("Missing '{}' for predicate in {}", kValue, SafeDumpJson(json));
  }
  ICEBERG_ASSIGN_OR_RAISE(auto value_json, GetJsonValue<nlohmann::json>(json, kValue));
  ICEBERG_ASSIGN_OR_RAISE(auto value, LiteralFromJson(value_json));

  ICEBERG_ASSIGN_OR_RAISE(auto pred, UnboundPredicateImpl<BoundReference>::Make(
                                         op, std::move(term), std::move(value)));
  return std::shared_ptr<UnboundPredicate>(std::move(pred));
}

Result<std::shared_ptr<Expression>> ExpressionFromJson(const nlohmann::json& json) {
  // Handle boolean literals
  if (json.is_boolean()) {
    return json.get<bool>() ? std::static_pointer_cast<Expression>(True::Instance())
                            : std::static_pointer_cast<Expression>(False::Instance());
  }

  if (!json.is_object()) {
    return JsonParseError("Expected boolean or object for expression: {}",
                          SafeDumpJson(json));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto type_str, GetJsonValue<std::string>(json, kType));
  ICEBERG_ASSIGN_OR_RAISE(auto op, OperationFromJsonType(type_str));

  switch (op) {
    case Expression::Operation::kTrue:
      return True::Instance();

    case Expression::Operation::kFalse:
      return False::Instance();

    case Expression::Operation::kAnd: {
      ICEBERG_ASSIGN_OR_RAISE(auto left_json, GetJsonValue<nlohmann::json>(json, kLeft));
      ICEBERG_ASSIGN_OR_RAISE(auto right_json, GetJsonValue<nlohmann::json>(json, kRight));
      ICEBERG_ASSIGN_OR_RAISE(auto left, ExpressionFromJson(left_json));
      ICEBERG_ASSIGN_OR_RAISE(auto right, ExpressionFromJson(right_json));
      return And::MakeFolded(std::move(left), std::move(right));
    }

    case Expression::Operation::kOr: {
      ICEBERG_ASSIGN_OR_RAISE(auto left_json, GetJsonValue<nlohmann::json>(json, kLeft));
      ICEBERG_ASSIGN_OR_RAISE(auto right_json, GetJsonValue<nlohmann::json>(json, kRight));
      ICEBERG_ASSIGN_OR_RAISE(auto left, ExpressionFromJson(left_json));
      ICEBERG_ASSIGN_OR_RAISE(auto right, ExpressionFromJson(right_json));
      return Or::MakeFolded(std::move(left), std::move(right));
    }

    case Expression::Operation::kNot: {
      ICEBERG_ASSIGN_OR_RAISE(auto child_json, GetJsonValue<nlohmann::json>(json, kChild));
      ICEBERG_ASSIGN_OR_RAISE(auto child, ExpressionFromJson(child_json));
      return Not::MakeFolded(std::move(child));
    }

    default:
      // Handle predicates
      ICEBERG_ASSIGN_OR_RAISE(auto pred, UnboundPredicateFromJson(json));
      return pred;
  }
}

}  // namespace iceberg
