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
#include "iceberg/expression/aggregate.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

// Logical NOT operation
std::shared_ptr<Expression> Expressions::Not(std::shared_ptr<Expression> child) {
  ICEBERG_ASSIGN_OR_THROW(auto not_expr, ::iceberg::Not::MakeFolded(std::move(child)));
  return not_expr;
}

// Transform functions

std::shared_ptr<UnboundTransform> Expressions::Bucket(std::string name,
                                                      int32_t num_buckets) {
  ICEBERG_ASSIGN_OR_THROW(
      auto transform,
      UnboundTransform::Make(Ref(std::move(name)), Transform::Bucket(num_buckets)));
  return transform;
}

std::shared_ptr<UnboundTransform> Expressions::Year(std::string name) {
  ICEBERG_ASSIGN_OR_THROW(
      auto transform, UnboundTransform::Make(Ref(std::move(name)), Transform::Year()));
  return transform;
}

std::shared_ptr<UnboundTransform> Expressions::Month(std::string name) {
  ICEBERG_ASSIGN_OR_THROW(
      auto transform, UnboundTransform::Make(Ref(std::move(name)), Transform::Month()));
  return transform;
}

std::shared_ptr<UnboundTransform> Expressions::Day(std::string name) {
  ICEBERG_ASSIGN_OR_THROW(auto transform,
                          UnboundTransform::Make(Ref(std::move(name)), Transform::Day()));
  return transform;
}

std::shared_ptr<UnboundTransform> Expressions::Hour(std::string name) {
  ICEBERG_ASSIGN_OR_THROW(
      auto transform, UnboundTransform::Make(Ref(std::move(name)), Transform::Hour()));
  return transform;
}

std::shared_ptr<UnboundTransform> Expressions::Truncate(std::string name, int32_t width) {
  ICEBERG_ASSIGN_OR_THROW(
      auto transform,
      UnboundTransform::Make(Ref(std::move(name)), Transform::Truncate(width)));
  return transform;
}

std::shared_ptr<UnboundTransform> Expressions::Transform(
    std::string name, std::shared_ptr<::iceberg::Transform> transform) {
  ICEBERG_ASSIGN_OR_THROW(
      auto unbound_transform,
      UnboundTransform::Make(Ref(std::move(name)), std::move(transform)));
  return unbound_transform;
}

// Aggregates

std::shared_ptr<UnboundAggregateImpl<BoundReference>> Expressions::Count(
    std::string name) {
  return Count(Ref(std::move(name)));
}

std::shared_ptr<UnboundAggregateImpl<BoundReference>> Expressions::Count(
    std::shared_ptr<UnboundTerm<BoundReference>> expr) {
  ICEBERG_ASSIGN_OR_THROW(auto agg, UnboundAggregateImpl<BoundReference>::Make(
                                        Expression::Operation::kCount, std::move(expr)));
  return agg;
}

std::shared_ptr<UnboundAggregateImpl<BoundReference>> Expressions::CountNull(
    std::string name) {
  return CountNull(Ref(std::move(name)));
}

std::shared_ptr<UnboundAggregateImpl<BoundReference>> Expressions::CountNull(
    std::shared_ptr<UnboundTerm<BoundReference>> expr) {
  ICEBERG_ASSIGN_OR_THROW(auto agg,
                          UnboundAggregateImpl<BoundReference>::Make(
                              Expression::Operation::kCountNull, std::move(expr)));
  return agg;
}

std::shared_ptr<UnboundAggregateImpl<BoundReference>> Expressions::CountNotNull(
    std::string name) {
  return CountNotNull(Ref(std::move(name)));
}

std::shared_ptr<UnboundAggregateImpl<BoundReference>> Expressions::CountNotNull(
    std::shared_ptr<UnboundTerm<BoundReference>> expr) {
  ICEBERG_ASSIGN_OR_THROW(auto agg, UnboundAggregateImpl<BoundReference>::Make(
                                        Expression::Operation::kCount, std::move(expr)));
  return agg;
}

std::shared_ptr<UnboundAggregateImpl<BoundReference>> Expressions::CountStar() {
  ICEBERG_ASSIGN_OR_THROW(auto agg, UnboundAggregateImpl<BoundReference>::Make(
                                        Expression::Operation::kCountStar, nullptr));
  return agg;
}

std::shared_ptr<UnboundAggregateImpl<BoundReference>> Expressions::Max(std::string name) {
  return Max(Ref(std::move(name)));
}

std::shared_ptr<UnboundAggregateImpl<BoundReference>> Expressions::Max(
    std::shared_ptr<UnboundTerm<BoundReference>> expr) {
  ICEBERG_ASSIGN_OR_THROW(auto agg, UnboundAggregateImpl<BoundReference>::Make(
                                        Expression::Operation::kMax, std::move(expr)));
  return agg;
}

std::shared_ptr<UnboundAggregateImpl<BoundTransform>> Expressions::Max(
    std::shared_ptr<UnboundTerm<BoundTransform>> expr) {
  ICEBERG_ASSIGN_OR_THROW(auto agg, UnboundAggregateImpl<BoundTransform>::Make(
                                        Expression::Operation::kMax, std::move(expr)));
  return agg;
}

std::shared_ptr<UnboundAggregateImpl<BoundReference>> Expressions::Min(std::string name) {
  return Min(Ref(std::move(name)));
}

std::shared_ptr<UnboundAggregateImpl<BoundReference>> Expressions::Min(
    std::shared_ptr<UnboundTerm<BoundReference>> expr) {
  ICEBERG_ASSIGN_OR_THROW(auto agg, UnboundAggregateImpl<BoundReference>::Make(
                                        Expression::Operation::kMin, std::move(expr)));
  return agg;
}

std::shared_ptr<UnboundAggregateImpl<BoundTransform>> Expressions::Min(
    std::shared_ptr<UnboundTerm<BoundTransform>> expr) {
  ICEBERG_ASSIGN_OR_THROW(auto agg, UnboundAggregateImpl<BoundTransform>::Make(
                                        Expression::Operation::kMin, std::move(expr)));
  return agg;
}

// Template implementations for unary predicates

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::IsNull(
    std::string name) {
  return IsNull<BoundReference>(Ref(std::move(name)));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::NotNull(
    std::string name) {
  return NotNull<BoundReference>(Ref(std::move(name)));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::IsNaN(
    std::string name) {
  return IsNaN<BoundReference>(Ref(std::move(name)));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::NotNaN(
    std::string name) {
  return NotNaN<BoundReference>(Ref(std::move(name)));
}

// Template implementations for comparison predicates

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::LessThan(
    std::string name, Literal value) {
  return LessThan<BoundReference>(Ref(std::move(name)), std::move(value));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::LessThanOrEqual(
    std::string name, Literal value) {
  return LessThanOrEqual<BoundReference>(Ref(std::move(name)), std::move(value));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::GreaterThan(
    std::string name, Literal value) {
  return GreaterThan<BoundReference>(Ref(std::move(name)), std::move(value));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::GreaterThanOrEqual(
    std::string name, Literal value) {
  return GreaterThanOrEqual<BoundReference>(Ref(std::move(name)), std::move(value));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::Equal(std::string name,
                                                                         Literal value) {
  return Equal<BoundReference>(Ref(std::move(name)), std::move(value));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::NotEqual(
    std::string name, Literal value) {
  return NotEqual<BoundReference>(Ref(std::move(name)), std::move(value));
}

// String predicates

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::StartsWith(
    std::string name, std::string value) {
  return StartsWith<BoundReference>(Ref(std::move(name)), std::move(value));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::NotStartsWith(
    std::string name, std::string value) {
  return NotStartsWith<BoundReference>(Ref(std::move(name)), std::move(value));
}

// Template implementations for set predicates

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::In(
    std::string name, std::vector<Literal> values) {
  return In<BoundReference>(Ref(std::move(name)), std::move(values));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::In(
    std::string name, std::initializer_list<Literal> values) {
  return In<BoundReference>(Ref(std::move(name)), std::vector<Literal>(values));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::NotIn(
    std::string name, std::vector<Literal> values) {
  return NotIn<BoundReference>(Ref(std::move(name)), std::move(values));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::NotIn(
    std::string name, std::initializer_list<Literal> values) {
  return NotIn<BoundReference>(Ref(std::move(name)), std::vector<Literal>(values));
}

// Template implementations for generic predicate factory

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::Predicate(
    Expression::Operation op, std::string name, Literal value) {
  ICEBERG_ASSIGN_OR_THROW(auto pred, UnboundPredicateImpl<BoundReference>::Make(
                                         op, Ref(std::move(name)), std::move(value)));
  return pred;
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::Predicate(
    Expression::Operation op, std::string name, std::vector<Literal> values) {
  ICEBERG_ASSIGN_OR_THROW(auto pred, UnboundPredicateImpl<BoundReference>::Make(
                                         op, Ref(std::move(name)), std::move(values)));
  return pred;
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::Predicate(
    Expression::Operation op, std::string name, std::initializer_list<Literal> values) {
  return Predicate(op, name, std::vector<Literal>(values));
}

std::shared_ptr<UnboundPredicateImpl<BoundReference>> Expressions::Predicate(
    Expression::Operation op, std::string name) {
  ICEBERG_ASSIGN_OR_THROW(
      auto pred, UnboundPredicateImpl<BoundReference>::Make(op, Ref(std::move(name))));
  return pred;
}

// Constants

std::shared_ptr<True> Expressions::AlwaysTrue() { return True::Instance(); }

std::shared_ptr<False> Expressions::AlwaysFalse() { return False::Instance(); }

// Utilities

std::shared_ptr<NamedReference> Expressions::Ref(std::string name) {
  ICEBERG_ASSIGN_OR_THROW(auto ref, NamedReference::Make(std::move(name)));
  return ref;
}

Literal Expressions::Lit(Literal::Value value, std::shared_ptr<PrimitiveType> type) {
  throw ExpressionError("Literal creation is not implemented");
}

}  // namespace iceberg
