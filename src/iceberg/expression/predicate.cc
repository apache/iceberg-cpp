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

#include "iceberg/expression/predicate.h"

#include <algorithm>
#include <cmath>
#include <format>

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/literal.h"
#include "iceberg/result.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

// Predicate template implementations
template <TermType T>
Predicate<T>::Predicate(Expression::Operation op, std::shared_ptr<T> term)
    : operation_(op), term_(std::move(term)) {}

template <TermType T>
Predicate<T>::~Predicate() = default;

// UnboundPredicate template implementations
template <typename B>
UnboundPredicate<B>::UnboundPredicate(Expression::Operation op,
                                      std::shared_ptr<UnboundTerm<B>> term)
    : BASE(op, std::move(term)) {}

template <typename B>
UnboundPredicate<B>::UnboundPredicate(Expression::Operation op,
                                      std::shared_ptr<UnboundTerm<B>> term, Literal value)
    : BASE(op, std::move(term)), values_{std::move(value)} {}

template <typename B>
UnboundPredicate<B>::UnboundPredicate(Expression::Operation op,
                                      std::shared_ptr<UnboundTerm<B>> term,
                                      std::vector<Literal> values)
    : BASE(op, std::move(term)), values_(std::move(values)) {}

template <typename B>
UnboundPredicate<B>::~UnboundPredicate() = default;

namespace {}

template <typename B>
std::string UnboundPredicate<B>::ToString() const {
  auto invalid_predicate_string = [](Expression::Operation op) {
    return std::format("Invalid predicate: operation = {}", op);
  };

  const auto& term = *BASE::term();
  const auto op = BASE::op();

  switch (op) {
    case Expression::Operation::kIsNull:
      return std::format("is_null({})", term);
    case Expression::Operation::kNotNull:
      return std::format("not_null({})", term);
    case Expression::Operation::kIsNan:
      return std::format("is_nan({})", term);
    case Expression::Operation::kNotNan:
      return std::format("not_nan({})", term);
    case Expression::Operation::kLt:
      return values_.size() == 1 ? std::format("{} < {}", term, values_[0])
                                 : invalid_predicate_string(op);
    case Expression::Operation::kLtEq:
      return values_.size() == 1 ? std::format("{} <= {}", term, values_[0])
                                 : invalid_predicate_string(op);
    case Expression::Operation::kGt:
      return values_.size() == 1 ? std::format("{} > {}", term, values_[0])
                                 : invalid_predicate_string(op);
    case Expression::Operation::kGtEq:
      return values_.size() == 1 ? std::format("{} >= {}", term, values_[0])
                                 : invalid_predicate_string(op);
    case Expression::Operation::kEq:
      return values_.size() == 1 ? std::format("{} == {}", term, values_[0])
                                 : invalid_predicate_string(op);
    case Expression::Operation::kNotEq:
      return values_.size() == 1 ? std::format("{} != {}", term, values_[0])
                                 : invalid_predicate_string(op);
    case Expression::Operation::kStartsWith:
      return values_.size() == 1 ? std::format("{} startsWith {}", term, values_[0])
                                 : invalid_predicate_string(op);
    case Expression::Operation::kNotStartsWith:
      return values_.size() == 1 ? std::format("{} notStartsWith {}", term, values_[0])
                                 : invalid_predicate_string(op);
    case Expression::Operation::kIn:
      return std::format("{} in {}", term, values_);
    case Expression::Operation::kNotIn:
      return std::format("{} not in {}", term, values_);
    default:
      return invalid_predicate_string(op);
  }
}

template <typename B>
Result<std::shared_ptr<Expression>> UnboundPredicate<B>::Negate() const {
  ICEBERG_ASSIGN_OR_RAISE(auto negated_op, ::iceberg::Negate(BASE::op()));
  return std::make_shared<UnboundPredicate>(negated_op, BASE::term(), values_);
}

template <typename B>
Result<std::shared_ptr<Expression>> UnboundPredicate<B>::Bind(const Schema& schema,
                                                              bool case_sensitive) const {
  ICEBERG_ASSIGN_OR_RAISE(auto bound_term, BASE::term()->Bind(schema, case_sensitive));

  if (values_.empty()) {
    return BindUnaryOperation(std::move(bound_term));
  }

  if (BASE::op() == Expression::Operation::kIn ||
      BASE::op() == Expression::Operation::kNotIn) {
    return BindInOperation(std::move(bound_term));
  }

  return BindLiteralOperation(std::move(bound_term));
}

namespace {

bool IsFloatingType(TypeId type) {
  return type == TypeId::kFloat || type == TypeId::kDouble;
}

bool IsNan(const Literal& literal) {
  const auto& value = literal.value();
  if (std::holds_alternative<float>(value)) {
    return std::isnan(std::get<float>(value));
  } else if (std::holds_alternative<double>(value)) {
    return std::isnan(std::get<double>(value));
  }
  return false;
}

bool StartsWith(const Literal& lhs, const Literal& rhs) {
  const auto& lhs_value = lhs.value();
  const auto& rhs_value = rhs.value();
  if (std::holds_alternative<std::string>(lhs_value) &&
      std::holds_alternative<std::string>(rhs_value)) {
    return std::get<std::string>(lhs_value).starts_with(std::get<std::string>(rhs_value));
  }
  return false;
}

}  // namespace

template <typename B>
Result<std::shared_ptr<Expression>> UnboundPredicate<B>::BindUnaryOperation(
    std::shared_ptr<B> bound_term) const {
  switch (BASE::op()) {
    case Expression::Operation::kIsNull:
      if (!bound_term->MayProduceNull()) {
        return Expressions::AlwaysFalse();
      }
      // TODO(gangwu): deal with UnknownType
      return std::make_shared<BoundUnaryPredicate>(Expression::Operation::kIsNull,
                                                   std::move(bound_term));
    case Expression::Operation::kNotNull:
      if (!bound_term->MayProduceNull()) {
        return Expressions::AlwaysTrue();
      }
      return std::make_shared<BoundUnaryPredicate>(Expression::Operation::kNotNull,
                                                   std::move(bound_term));
    case Expression::Operation::kIsNan:
    case Expression::Operation::kNotNan:
      if (!IsFloatingType(bound_term->type()->type_id())) {
        return InvalidExpression("{} cannot be used with a non-floating-point column",
                                 BASE::op());
      }
      return std::make_shared<BoundUnaryPredicate>(BASE::op(), std::move(bound_term));
    default:
      return InvalidExpression("Operation must be IS_NULL, NOT_NULL, IS_NAN, or NOT_NAN");
  }
}

template <typename B>
Result<std::shared_ptr<Expression>> UnboundPredicate<B>::BindLiteralOperation(
    std::shared_ptr<B> bound_term) const {
  if (BASE::op() == Expression::Operation::kStartsWith ||
      BASE::op() == Expression::Operation::kNotStartsWith) {
    if (bound_term->type()->type_id() != TypeId::kString) {
      return InvalidExpression(
          "Term for STARTS_WITH or NOT_STARTS_WITH must produce a string: {}: {}",
          *bound_term, *bound_term->type());
    }
  }

  if (values_.size() != 1) {
    return InvalidExpression("Literal operation requires a single value but got {}",
                             values_.size());
  }

  ICEBERG_ASSIGN_OR_RAISE(auto literal,
                          values_[0].CastTo(internal::checked_pointer_cast<PrimitiveType>(
                              bound_term->type())));

  if (literal.IsNull()) {
    return InvalidExpression("Invalid value for conversion to type {}: {} ({})",
                             *bound_term->type(), literal.ToString(), *literal.type());
  } else if (literal.IsAboveMax()) {
    switch (BASE::op()) {
      case Expression::Operation::kLt:
      case Expression::Operation::kLtEq:
      case Expression::Operation::kNotEq:
        return Expressions::AlwaysTrue();
      case Expression::Operation::kGt:
      case Expression::Operation::kGtEq:
      case Expression::Operation::kEq:
        return Expressions::AlwaysFalse();
      default:
        break;
    }
  } else if (literal.IsBelowMin()) {
    switch (BASE::op()) {
      case Expression::Operation::kGt:
      case Expression::Operation::kGtEq:
      case Expression::Operation::kNotEq:
        return Expressions::AlwaysTrue();
      case Expression::Operation::kLt:
      case Expression::Operation::kLtEq:
      case Expression::Operation::kEq:
        return Expressions::AlwaysFalse();
      default:
        break;
    }
  }

  // TODO(gangwu): translate truncate(col) == value to startsWith(value)
  return std::make_shared<BoundLiteralPredicate>(BASE::op(), std::move(bound_term),
                                                 std::move(literal));
}

template <typename B>
Result<std::shared_ptr<Expression>> UnboundPredicate<B>::BindInOperation(
    std::shared_ptr<B> bound_term) const {
  std::vector<Literal> converted_literals;
  for (const auto& literal : values_) {
    auto primitive_type =
        internal::checked_pointer_cast<PrimitiveType>(bound_term->type());
    ICEBERG_ASSIGN_OR_RAISE(auto converted, literal.CastTo(primitive_type));
    if (converted.IsNull()) {
      return InvalidExpression("Invalid value for conversion to type {}: {} ({})",
                               *bound_term->type(), literal.ToString(), *literal.type());
    }
    // Filter out literals that are out of range after conversion.
    if (!converted.IsBelowMin() && !converted.IsAboveMax()) {
      converted_literals.push_back(std::move(converted));
    }
  }

  // If no valid literals remain after conversion and filtering
  if (converted_literals.empty()) {
    switch (BASE::op()) {
      case Expression::Operation::kIn:
        return Expressions::AlwaysFalse();
      case Expression::Operation::kNotIn:
        return Expressions::AlwaysTrue();
      default:
        return InvalidExpression("Operation must be IN or NOT_IN");
    }
  }

  // If only one unique literal remains, convert to equality/inequality
  if (converted_literals.size() == 1) {
    const auto& single_literal = converted_literals[0];
    switch (BASE::op()) {
      case Expression::Operation::kIn:
        return std::make_shared<BoundLiteralPredicate>(
            Expression::Operation::kEq, std::move(bound_term), single_literal);
      case Expression::Operation::kNotIn:
        return std::make_shared<BoundLiteralPredicate>(
            Expression::Operation::kNotEq, std::move(bound_term), single_literal);
      default:
        return InvalidExpression("Operation must be IN or NOT_IN");
    }
  }

  // Multiple literals - create a set predicate
  return std::make_shared<BoundSetPredicate>(
      BASE::op(), std::move(bound_term), std::span<const Literal>(converted_literals));
}

// BoundPredicate implementation
BoundPredicate::BoundPredicate(Expression::Operation op, std::shared_ptr<BoundTerm> term)
    : Predicate<BoundTerm>(op, std::move(term)) {}

BoundPredicate::~BoundPredicate() = default;

Result<Literal> BoundPredicate::Evaluate(const StructLike& data) const {
  ICEBERG_ASSIGN_OR_RAISE(auto eval_result, term_->Evaluate(data));
  ICEBERG_ASSIGN_OR_RAISE(auto test_result, Test(eval_result));
  return Literal::Boolean(test_result);
}

// BoundUnaryPredicate implementation
BoundUnaryPredicate::BoundUnaryPredicate(Expression::Operation op,
                                         std::shared_ptr<BoundTerm> term)
    : BoundPredicate(op, std::move(term)) {}

BoundUnaryPredicate::~BoundUnaryPredicate() = default;

Result<bool> BoundUnaryPredicate::Test(const Literal& literal) const {
  switch (op()) {
    case Expression::Operation::kIsNull:
      return literal.IsNull();
    case Expression::Operation::kNotNull:
      return !literal.IsNull();
    case Expression::Operation::kIsNan:
      return IsNan(literal);
    case Expression::Operation::kNotNan:
      return !IsNan(literal);
    default:
      return InvalidExpression("Invalid operation for BoundUnaryPredicate: {}", op());
  }
}

Result<std::shared_ptr<Expression>> BoundUnaryPredicate::Negate() const {
  ICEBERG_ASSIGN_OR_RAISE(auto negated_op, ::iceberg::Negate(op()));
  return std::make_shared<BoundUnaryPredicate>(negated_op, term_);
}

bool BoundUnaryPredicate::Equals(const Expression& other) const {
  if (op() != other.op()) {
    return false;
  }

  if (const auto* other_pred = dynamic_cast<const BoundUnaryPredicate*>(&other);
      other_pred) {
    return term_->Equals(*other_pred->term());
  }

  return false;
}

std::string BoundUnaryPredicate::ToString() const {
  switch (op()) {
    case Expression::Operation::kIsNull:
      return std::format("is_null({})", *term());
    case Expression::Operation::kNotNull:
      return std::format("not_null({})", *term());
    case Expression::Operation::kIsNan:
      return std::format("is_nan({})", *term());
    case Expression::Operation::kNotNan:
      return std::format("not_nan({})", *term());
    default:
      return std::format("Invalid unary predicate: operation = {}", op());
  }
}

// BoundLiteralPredicate implementation
BoundLiteralPredicate::BoundLiteralPredicate(Expression::Operation op,
                                             std::shared_ptr<BoundTerm> term,
                                             Literal literal)
    : BoundPredicate(op, std::move(term)), literal_(std::move(literal)) {}

BoundLiteralPredicate::~BoundLiteralPredicate() = default;

Result<bool> BoundLiteralPredicate::Test(const Literal& value) const {
  switch (op()) {
    case Expression::Operation::kLt:
      return value < literal_;
    case Expression::Operation::kLtEq:
      return value <= literal_;
    case Expression::Operation::kGt:
      return value > literal_;
    case Expression::Operation::kGtEq:
      return value >= literal_;
    case Expression::Operation::kEq:
      return value == literal_;
    case Expression::Operation::kNotEq:
      return value != literal_;
    case Expression::Operation::kStartsWith:
      return StartsWith(value, literal_);
    case Expression::Operation::kNotStartsWith:
      return !StartsWith(value, literal_);
    default:
      return InvalidExpression("Invalid operation for BoundLiteralPredicate: {}", op());
  }
}

Result<std::shared_ptr<Expression>> BoundLiteralPredicate::Negate() const {
  ICEBERG_ASSIGN_OR_RAISE(auto negated_op, ::iceberg::Negate(op()));
  return std::make_shared<BoundLiteralPredicate>(negated_op, term_, literal_);
}

bool BoundLiteralPredicate::Equals(const Expression& other) const {
  const auto* other_pred = dynamic_cast<const BoundLiteralPredicate*>(&other);
  if (!other_pred) {
    return false;
  }

  if (op() == other.op()) {
    if (term_->Equals(*other_pred->term())) {
      // because the term is equivalent, the literal must have the same type
      return literal_ == other_pred->literal();
    }
  }

  // TODO(gangwu): add TypeId::kTimestampNano
  static const std::unordered_set<TypeId> kIntegralTypes = {
      TypeId::kInt,  TypeId::kLong,      TypeId::kDate,
      TypeId::kTime, TypeId::kTimestamp, TypeId::kTimestampTz};

  if (kIntegralTypes.contains(term_->type()->type_id()) &&
      term_->Equals(*other_pred->term())) {
    auto get_long = [](const Literal& lit) -> std::optional<int64_t> {
      const auto& val = lit.value();
      if (std::holds_alternative<int32_t>(val)) {
        return std::get<int32_t>(val);
      } else if (std::holds_alternative<int64_t>(val)) {
        return std::get<int64_t>(val);
      }
      return std::nullopt;
    };

    auto this_val = get_long(literal_);
    auto other_val = get_long(other_pred->literal());
    if (this_val && other_val) {
      switch (op()) {
        case Expression::Operation::kLt:
          // < 6 is equivalent to <= 5
          return other_pred->op() == Expression::Operation::kLtEq &&
                 *this_val == *other_val + 1;
        case Expression::Operation::kLtEq:
          // <= 5 is equivalent to < 6
          return other_pred->op() == Expression::Operation::kLt &&
                 *this_val == *other_val - 1;
        case Expression::Operation::kGt:
          // > 5 is equivalent to >= 6
          return other_pred->op() == Expression::Operation::kGtEq &&
                 *this_val == *other_val - 1;
        case Expression::Operation::kGtEq:
          // >= 6 is equivalent to > 5
          return other_pred->op() == Expression::Operation::kGt &&
                 *this_val == *other_val + 1;
        default:
          return false;
      }
    }
  }

  return false;
}

std::string BoundLiteralPredicate::ToString() const {
  switch (op()) {
    case Expression::Operation::kLt:
      return std::format("{} < {}", *term(), literal());
    case Expression::Operation::kLtEq:
      return std::format("{} <= {}", *term(), literal());
    case Expression::Operation::kGt:
      return std::format("{} > {}", *term(), literal());
    case Expression::Operation::kGtEq:
      return std::format("{} >= {}", *term(), literal());
    case Expression::Operation::kEq:
      return std::format("{} == {}", *term(), literal());
    case Expression::Operation::kNotEq:
      return std::format("{} != {}", *term(), literal());
    case Expression::Operation::kStartsWith:
      return std::format("{} startsWith \"{}\"", *term(), literal());
    case Expression::Operation::kNotStartsWith:
      return std::format("{} notStartsWith \"{}\"", *term(), literal());
    case Expression::Operation::kIn:
      return std::format("{} in ({})", *term(), literal());
    case Expression::Operation::kNotIn:
      return std::format("{} not in ({})", *term(), literal());
    default:
      return std::format("Invalid literal predicate: operation = {}", op());
  }
}

// BoundSetPredicate implementation
BoundSetPredicate::BoundSetPredicate(Expression::Operation op,
                                     std::shared_ptr<BoundTerm> term,
                                     std::span<const Literal> literals)
    : BoundPredicate(op, std::move(term)), value_set_(literals.begin(), literals.end()) {}

BoundSetPredicate::BoundSetPredicate(Expression::Operation op,
                                     std::shared_ptr<BoundTerm> term,
                                     LiteralSet value_set)
    : BoundPredicate(op, std::move(term)), value_set_(std::move(value_set)) {}

BoundSetPredicate::~BoundSetPredicate() = default;

Result<bool> BoundSetPredicate::Test(const Literal& value) const {
  switch (op()) {
    case Expression::Operation::kIn:
      return value_set_.contains(value);
    case Expression::Operation::kNotIn:
      return !value_set_.contains(value);
    default:
      return InvalidExpression("Invalid operation for BoundSetPredicate: {}", op());
  }
}

Result<std::shared_ptr<Expression>> BoundSetPredicate::Negate() const {
  ICEBERG_ASSIGN_OR_RAISE(auto negated_op, ::iceberg::Negate(op()));
  return std::make_shared<BoundSetPredicate>(negated_op, term_, value_set_);
}

bool BoundSetPredicate::Equals(const Expression& other) const {
  if (op() != other.op()) {
    return false;
  }

  if (const auto* other_pred = dynamic_cast<const BoundSetPredicate*>(&other);
      other_pred) {
    return value_set_ == other_pred->value_set_;
  }

  return false;
}

std::string BoundSetPredicate::ToString() const {
  switch (op()) {
    case Expression::Operation::kIn:
      return std::format("{} in {}", *term(), FormatRange(value_set_, ", ", "(", ")"));
    case Expression::Operation::kNotIn:
      return std::format("{} not in {}", *term(),
                         FormatRange(value_set_, ", ", "(", ")"));
    default:
      return std::format("Invalid set predicate: operation = {}", op());
  }
}

// Explicit template instantiations
template class Predicate<UnboundTerm<BoundReference>>;
template class Predicate<UnboundTerm<BoundTransform>>;
template class Predicate<BoundTerm>;

template class UnboundPredicate<BoundReference>;
template class UnboundPredicate<BoundTransform>;

}  // namespace iceberg
