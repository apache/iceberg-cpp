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

#include "iceberg/expression/expressions.h"

#include "iceberg/exception.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

// Logical NOT operation
std::shared_ptr<Expression> Expressions::Not(std::shared_ptr<Expression> child) {
  if (child->op() == Expression::Operation::kTrue) {
    return AlwaysFalse();
  }

  if (child->op() == Expression::Operation::kFalse) {
    return AlwaysTrue();
  }

  // not(not(x)) = x
  if (child->op() == Expression::Operation::kNot) {
    const auto& not_expr = static_cast<const ::iceberg::Not&>(*child);
    return not_expr.child();
  }

  return std::make_shared<::iceberg::Not>(std::move(child));
}

// Transform functions

std::shared_ptr<UnboundTransform> Expressions::Bucket(std::string name,
                                                      int32_t num_buckets) {
  return std::make_shared<UnboundTransform>(Ref(std::move(name)),
                                            Transform::Bucket(num_buckets));
}

std::shared_ptr<UnboundTransform> Expressions::Year(std::string name) {
  return std::make_shared<UnboundTransform>(Ref(std::move(name)), Transform::Year());
}

std::shared_ptr<UnboundTransform> Expressions::Month(std::string name) {
  return std::make_shared<UnboundTransform>(Ref(std::move(name)), Transform::Month());
}

std::shared_ptr<UnboundTransform> Expressions::Day(std::string name) {
  return std::make_shared<UnboundTransform>(Ref(std::move(name)), Transform::Day());
}

std::shared_ptr<UnboundTransform> Expressions::Hour(std::string name) {
  return std::make_shared<UnboundTransform>(Ref(std::move(name)), Transform::Hour());
}

std::shared_ptr<UnboundTransform> Expressions::Truncate(std::string name, int32_t width) {
  return std::make_shared<UnboundTransform>(Ref(std::move(name)),
                                            Transform::Truncate(width));
}

std::shared_ptr<UnboundTransform> Expressions::Transform(
    std::string name, std::shared_ptr<::iceberg::Transform> transform) {
  return std::make_shared<UnboundTransform>(Ref(std::move(name)), std::move(transform));
}

// Template implementations for unary predicates

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::IsNull(std::string name) {
  return IsNull<BoundReference>(Ref(std::move(name)));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::IsNull(
    std::shared_ptr<UnboundTerm<B>> expr) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kIsNull,
                                               std::move(expr));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::NotNull(std::string name) {
  return NotNull<BoundReference>(Ref(std::move(name)));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::NotNull(
    std::shared_ptr<UnboundTerm<B>> expr) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kNotNull,
                                               std::move(expr));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::IsNaN(std::string name) {
  return IsNaN<BoundReference>(Ref(std::move(name)));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::IsNaN(
    std::shared_ptr<UnboundTerm<B>> expr) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kIsNan,
                                               std::move(expr));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::NotNaN(std::string name) {
  return NotNaN<BoundReference>(Ref(std::move(name)));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::NotNaN(
    std::shared_ptr<UnboundTerm<B>> expr) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kNotNan,
                                               std::move(expr));
}

// Template implementations for comparison predicates

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::LessThan(std::string name,
                                                                        Literal value) {
  return LessThan<BoundReference>(Ref(std::move(name)), std::move(value));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::LessThan(
    std::shared_ptr<UnboundTerm<B>> expr, Literal value) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kLt,
                                               std::move(expr), std::move(value));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::LessThanOrEqual(
    std::string name, Literal value) {
  return LessThanOrEqual<BoundReference>(Ref(std::move(name)), std::move(value));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::LessThanOrEqual(
    std::shared_ptr<UnboundTerm<B>> expr, Literal value) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kLtEq,
                                               std::move(expr), std::move(value));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::GreaterThan(
    std::string name, Literal value) {
  return GreaterThan<BoundReference>(Ref(std::move(name)), std::move(value));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::GreaterThan(
    std::shared_ptr<UnboundTerm<B>> expr, Literal value) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kGt,
                                               std::move(expr), std::move(value));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::GreaterThanOrEqual(
    std::string name, Literal value) {
  return GreaterThanOrEqual<BoundReference>(Ref(std::move(name)), std::move(value));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::GreaterThanOrEqual(
    std::shared_ptr<UnboundTerm<B>> expr, Literal value) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kGtEq,
                                               std::move(expr), std::move(value));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::Equal(std::string name,
                                                                     Literal value) {
  return Equal<BoundReference>(Ref(std::move(name)), std::move(value));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::Equal(
    std::shared_ptr<UnboundTerm<B>> expr, Literal value) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kEq,
                                               std::move(expr), std::move(value));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::NotEqual(std::string name,
                                                                        Literal value) {
  return NotEqual<BoundReference>(Ref(std::move(name)), std::move(value));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::NotEqual(
    std::shared_ptr<UnboundTerm<B>> expr, Literal value) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kNotEq,
                                               std::move(expr), std::move(value));
}

// String predicates

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::StartsWith(
    std::string name, std::string value) {
  return StartsWith<BoundReference>(Ref(std::move(name)), std::move(value));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::StartsWith(
    std::shared_ptr<UnboundTerm<B>> expr, std::string value) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kStartsWith,
                                               std::move(expr),
                                               Literal::String(std::move(value)));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::NotStartsWith(
    std::string name, std::string value) {
  return NotStartsWith<BoundReference>(Ref(std::move(name)), std::move(value));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::NotStartsWith(
    std::shared_ptr<UnboundTerm<B>> expr, std::string value) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kNotStartsWith,
                                               std::move(expr),
                                               Literal::String(std::move(value)));
}

// Template implementations for set predicates

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::In(
    std::string name, std::vector<Literal> values) {
  return In<BoundReference>(Ref(std::move(name)), std::move(values));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::In(std::shared_ptr<UnboundTerm<B>> expr,
                                                     std::vector<Literal> values) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kIn,
                                               std::move(expr), std::move(values));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::In(
    std::string name, std::initializer_list<Literal> values) {
  return In<BoundReference>(Ref(std::move(name)), std::vector<Literal>(values));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::In(
    std::shared_ptr<UnboundTerm<B>> expr, std::initializer_list<Literal> values) {
  return In<B>(std::move(expr), std::vector<Literal>(values));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::NotIn(
    std::string name, std::vector<Literal> values) {
  return NotIn<BoundReference>(Ref(std::move(name)), std::move(values));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::NotIn(
    std::shared_ptr<UnboundTerm<B>> expr, std::vector<Literal> values) {
  return std::make_shared<UnboundPredicate<B>>(Expression::Operation::kNotIn,
                                               std::move(expr), std::move(values));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::NotIn(
    std::string name, std::initializer_list<Literal> values) {
  return NotIn<BoundReference>(Ref(std::move(name)), std::vector<Literal>(values));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::NotIn(
    std::shared_ptr<UnboundTerm<B>> expr, std::initializer_list<Literal> values) {
  return NotIn<B>(expr, std::vector<Literal>(values));
}

// Template implementations for generic predicate factory

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::Predicate(
    Expression::Operation op, std::string name, Literal value) {
  return std::make_shared<UnboundPredicate<BoundReference>>(op, Ref(std::move(name)),
                                                            std::move(value));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::Predicate(
    Expression::Operation op, std::string name, std::vector<Literal> values) {
  return std::make_shared<UnboundPredicate<BoundReference>>(op, Ref(std::move(name)),
                                                            std::move(values));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::Predicate(
    Expression::Operation op, std::string name, std::initializer_list<Literal> values) {
  return Predicate(op, name, std::vector<Literal>(values));
}

std::shared_ptr<UnboundPredicate<BoundReference>> Expressions::Predicate(
    Expression::Operation op, std::string name) {
  return std::make_shared<UnboundPredicate<BoundReference>>(op, Ref(std::move(name)));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::Predicate(
    Expression::Operation op, std::shared_ptr<UnboundTerm<B>> expr,
    std::vector<Literal> values) {
  return std::make_shared<UnboundPredicate<B>>(op, std::move(expr), std::move(values));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::Predicate(
    Expression::Operation op, std::shared_ptr<UnboundTerm<B>> expr,
    std::initializer_list<Literal> values) {
  return Predicate<B>(op, std::move(expr), std::vector<Literal>(values));
}

template <typename B>
std::shared_ptr<UnboundPredicate<B>> Expressions::Predicate(
    Expression::Operation op, std::shared_ptr<UnboundTerm<B>> expr) {
  return std::make_shared<UnboundPredicate<B>>(op, std::move(expr));
}

// Constants

std::shared_ptr<True> Expressions::AlwaysTrue() { return True::Instance(); }

std::shared_ptr<False> Expressions::AlwaysFalse() { return False::Instance(); }

// Utilities

std::shared_ptr<NamedReference> Expressions::Ref(std::string name) {
  return std::make_shared<NamedReference>(std::move(name));
}

Literal Expressions::Lit(Literal::Value value, std::shared_ptr<PrimitiveType> type) {
  throw IcebergError("Literal creation is not implemented");
}

}  // namespace iceberg
