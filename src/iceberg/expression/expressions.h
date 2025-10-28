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

#pragma once

/// \file iceberg/expression/expressions.h
/// Factory methods for creating expressions.

#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/term.h"
#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Factory methods for creating expressions.
class ICEBERG_EXPORT Expressions {
 public:
  // Logical operations

  /// \brief Create an AND expression.
  template <typename... Args>
  static std::shared_ptr<Expression> And(std::shared_ptr<Expression> left,
                                         std::shared_ptr<Expression> right,
                                         Args&&... args)
    requires std::conjunction_v<std::is_same<Args, std::shared_ptr<Expression>>...>
  {
    if constexpr (sizeof...(args) == 0) {
      if (left->op() == Expression::Operation::kFalse ||
          right->op() == Expression::Operation::kFalse) {
        return AlwaysFalse();
      }

      if (left->op() == Expression::Operation::kTrue) {
        return right;
      }

      if (right->op() == Expression::Operation::kTrue) {
        return left;
      }

      return std::make_shared<::iceberg::And>(std::move(left), std::move(right));
    } else {
      return And(And(std::move(left), std::move(right)), std::forward<Args>(args)...);
    }
  }

  /// \brief Create an OR expression.
  template <typename... Args>
  static std::shared_ptr<Expression> Or(std::shared_ptr<Expression> left,
                                        std::shared_ptr<Expression> right, Args&&... args)
    requires std::conjunction_v<std::is_same<Args, std::shared_ptr<Expression>>...>
  {
    if constexpr (sizeof...(args) == 0) {
      if (left->op() == Expression::Operation::kTrue ||
          right->op() == Expression::Operation::kTrue) {
        return AlwaysTrue();
      }

      if (left->op() == Expression::Operation::kFalse) {
        return right;
      }

      if (right->op() == Expression::Operation::kFalse) {
        return left;
      }

      return std::make_shared<::iceberg::Or>(std::move(left), std::move(right));
    } else {
      return Or(Or(std::move(left), std::move(right)), std::forward<Args>(args)...);
    }
  }

  /// \brief Create a NOT expression.
  ///
  /// \param child The expression to negate
  /// \return A negated expression with optimizations applied:
  ///   - not(true) returns false
  ///   - not(false) returns true
  ///   - not(not(x)) returns x
  static std::shared_ptr<Expression> Not(std::shared_ptr<Expression> child);

  // Transform functions

  /// \brief Create a bucket transform term.
  static std::shared_ptr<UnboundTransform> Bucket(std::string name, int32_t num_buckets);

  /// \brief Create a year transform term.
  static std::shared_ptr<UnboundTransform> Year(std::string name);

  /// \brief Create a month transform term.
  static std::shared_ptr<UnboundTransform> Month(std::string name);

  /// \brief Create a day transform term.
  static std::shared_ptr<UnboundTransform> Day(std::string name);

  /// \brief Create an hour transform term.
  static std::shared_ptr<UnboundTransform> Hour(std::string name);

  /// \brief Create a truncate transform term.
  static std::shared_ptr<UnboundTransform> Truncate(std::string name, int32_t width);

  /// \brief Create a transform expression.
  static std::shared_ptr<UnboundTransform> Transform(
      std::string name, std::shared_ptr<Transform> transform);

  // Unary predicates

  /// \brief Create an IS NULL predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> IsNull(std::string name);

  /// \brief Create an IS NULL predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> IsNull(
      std::shared_ptr<UnboundTerm<B>> expr);

  /// \brief Create a NOT NULL predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> NotNull(std::string name);

  /// \brief Create a NOT NULL predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> NotNull(
      std::shared_ptr<UnboundTerm<B>> expr);

  /// \brief Create an IS NaN predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> IsNaN(std::string name);

  /// \brief Create an IS NaN predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> IsNaN(std::shared_ptr<UnboundTerm<B>> expr);

  /// \brief Create a NOT NaN predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> NotNaN(std::string name);

  /// \brief Create a NOT NaN predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> NotNaN(
      std::shared_ptr<UnboundTerm<B>> expr);

  // Comparison predicates

  /// \brief Create a less than predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> LessThan(std::string name,
                                                                    Literal value);

  /// \brief Create a less than predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> LessThan(
      std::shared_ptr<UnboundTerm<B>> expr, Literal value);

  /// \brief Create a less than or equal predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> LessThanOrEqual(
      std::string name, Literal value);

  /// \brief Create a less than or equal predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> LessThanOrEqual(
      std::shared_ptr<UnboundTerm<B>> expr, Literal value);

  /// \brief Create a greater than predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> GreaterThan(std::string name,
                                                                       Literal value);

  /// \brief Create a greater than predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> GreaterThan(
      std::shared_ptr<UnboundTerm<B>> expr, Literal value);

  /// \brief Create a greater than or equal predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> GreaterThanOrEqual(
      std::string name, Literal value);

  /// \brief Create a greater than or equal predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> GreaterThanOrEqual(
      std::shared_ptr<UnboundTerm<B>> expr, Literal value);

  /// \brief Create an equal predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> Equal(std::string name,
                                                                 Literal value);

  /// \brief Create an equal predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> Equal(std::shared_ptr<UnboundTerm<B>> expr,
                                                    Literal value);

  /// \brief Create a not equal predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> NotEqual(std::string name,
                                                                    Literal value);

  /// \brief Create a not equal predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> NotEqual(
      std::shared_ptr<UnboundTerm<B>> expr, Literal value);

  // String predicates

  /// \brief Create a starts with predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> StartsWith(std::string name,
                                                                      std::string value);

  /// \brief Create a starts with predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> StartsWith(
      std::shared_ptr<UnboundTerm<B>> expr, std::string value);

  /// \brief Create a not starts with predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> NotStartsWith(
      std::string name, std::string value);

  /// \brief Create a not starts with predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> NotStartsWith(
      std::shared_ptr<UnboundTerm<B>> expr, std::string value);

  // Set predicates

  /// \brief Create an IN predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> In(
      std::string name, std::vector<Literal> values);

  /// \brief Create an IN predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> In(std::shared_ptr<UnboundTerm<B>> expr,
                                                 std::vector<Literal> values);

  /// \brief Create an IN predicate for a field name with initializer list.
  static std::shared_ptr<UnboundPredicate<BoundReference>> In(
      std::string name, std::initializer_list<Literal> values);

  /// \brief Create an IN predicate for an unbound term with initializer list.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> In(std::shared_ptr<UnboundTerm<B>> expr,
                                                 std::initializer_list<Literal> values);

  /// \brief Create a NOT IN predicate for a field name.
  static std::shared_ptr<UnboundPredicate<BoundReference>> NotIn(
      std::string name, std::vector<Literal> values);

  /// \brief Create a NOT IN predicate for an unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> NotIn(std::shared_ptr<UnboundTerm<B>> expr,
                                                    std::vector<Literal> values);

  /// \brief Create a NOT IN predicate for a field name with initializer list.
  static std::shared_ptr<UnboundPredicate<BoundReference>> NotIn(
      std::string name, std::initializer_list<Literal> values);

  /// \brief Create a NOT IN predicate for an unbound term with initializer list.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> NotIn(
      std::shared_ptr<UnboundTerm<B>> expr, std::initializer_list<Literal> values);

  // Generic predicate factory

  /// \brief Create a predicate with operation and single value.
  static std::shared_ptr<UnboundPredicate<BoundReference>> Predicate(
      Expression::Operation op, std::string name, Literal value);

  /// \brief Create a predicate with operation and multiple values.
  static std::shared_ptr<UnboundPredicate<BoundReference>> Predicate(
      Expression::Operation op, std::string name, std::vector<Literal> values);

  /// \brief Create a predicate with operation and multiple values.
  static std::shared_ptr<UnboundPredicate<BoundReference>> Predicate(
      Expression::Operation op, std::string name, std::initializer_list<Literal> values);

  /// \brief Create a unary predicate (no values).
  static std::shared_ptr<UnboundPredicate<BoundReference>> Predicate(
      Expression::Operation op, std::string name);

  /// \brief Create a predicate for unbound term with multiple values.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> Predicate(
      Expression::Operation op, std::shared_ptr<UnboundTerm<B>> expr,
      std::vector<Literal> values);

  /// \brief Create a predicate with operation and multiple values.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> Predicate(
      Expression::Operation op, std::shared_ptr<UnboundTerm<B>> expr,
      std::initializer_list<Literal> values);

  /// \brief Create a unary predicate for unbound term.
  template <typename B>
  static std::shared_ptr<UnboundPredicate<B>> Predicate(
      Expression::Operation op, std::shared_ptr<UnboundTerm<B>> expr);

  // Constants

  /// \brief Return the always true expression.
  static std::shared_ptr<True> AlwaysTrue();

  /// \brief Return the always false expression.
  static std::shared_ptr<False> AlwaysFalse();

  // Utilities

  /// \brief Create a named reference to a field.
  static std::shared_ptr<NamedReference> Ref(std::string name);

  /// \brief Create a literal from a value.
  static Literal Lit(Literal::Value value, std::shared_ptr<PrimitiveType> type);
};

}  // namespace iceberg
