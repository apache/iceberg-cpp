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

/// \file iceberg/expression/expression.h
/// Expression interface for Iceberg table operations.

#include <memory>
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"

namespace iceberg {

/// Operation types for expressions
enum class Operation {
  kTrue,
  kFalse,
  kIsNull,
  kNotNull,
  kIsNan,
  kNotNan,
  kLt,
  kLtEq,
  kGt,
  kGtEq,
  kEq,
  kNotEq,
  kIn,
  kNotIn,
  kNot,
  kAnd,
  kOr,
  kStartsWith,
  kNotStartsWith,
  kCount,
  kCountStar,
  kMax,
  kMin
};

/// \brief Returns whether the operation is a predicate operation.
constexpr bool IsPredicate(Operation op) {
  switch (op) {
    case Operation::kTrue:
    case Operation::kFalse:
    case Operation::kIsNull:
    case Operation::kNotNull:
    case Operation::kIsNan:
    case Operation::kNotNan:
    case Operation::kLt:
    case Operation::kLtEq:
    case Operation::kGt:
    case Operation::kGtEq:
    case Operation::kEq:
    case Operation::kNotEq:
    case Operation::kIn:
    case Operation::kNotIn:
    case Operation::kNot:
    case Operation::kAnd:
    case Operation::kOr:
    case Operation::kStartsWith:
    case Operation::kNotStartsWith:
      return true;
    case Operation::kCount:
    case Operation::kCountStar:
    case Operation::kMax:
    case Operation::kMin:
      return false;
  }
  return false;
}

class BoundExpression;

/// \brief Represents a boolean expression tree.
class ICEBERG_EXPORT Expression {
 public:
  using BoundType = BoundExpression;

  virtual ~Expression() = default;

  /// \brief Returns the operation for an expression node.
  virtual Operation op() const = 0;

  /// \brief Returns whether this expression will accept the same values as another.
  /// \param other another expression
  /// \return true if the expressions are equivalent
  virtual bool Equals(const Expression& other) const {
    // only bound predicates can be equivalent
    return false;
  }

  virtual std::string ToString() const = 0;

  virtual Result<std::unique_ptr<BoundExpression>> Bind(
      const Schema& schema, bool case_sensitive) const {
    return NotImplemented("Binding of Expression is not implemented");
  }
};

class ICEBERG_EXPORT Predicate : public Expression {
 public:
  /// \brief Returns a negated version of this predicate.
  virtual std::shared_ptr<Predicate> Negate() const = 0;

  // Factory functions for creating predicates

  /// \brief Creates a True predicate that always evaluates to true.
  /// \return A shared pointer to a True predicate
  static const std::shared_ptr<Predicate>& AlwaysTrue();

  /// \brief Creates a False predicate that always evaluates to false.
  /// \return A shared pointer to a False predicate
  static const std::shared_ptr<Predicate>& AlwaysFalse();

  /// \brief Creates an And predicate that represents logical AND of two predicates.
  /// \param left The left operand of the AND predicate
  /// \param right The right operand of the AND predicate
  /// \return A shared pointer to an And predicate
  static std::shared_ptr<Predicate> And(std::shared_ptr<Predicate> left,
                                        std::shared_ptr<Predicate> right);

  /// \brief Creates an Or predicate that represents logical OR of two predicates.
  /// \param left The left operand of the OR predicate
  /// \param right The right operand of the OR predicate
  /// \return A shared pointer to an Or predicate
  static std::shared_ptr<Predicate> Or(std::shared_ptr<Predicate> left,
                                       std::shared_ptr<Predicate> right);
};

class ICEBERG_EXPORT BoundExpression {
 public:
  virtual ~BoundExpression() = default;

  /// \brief Returns the operation for a bound expression node.
  virtual Operation op() const = 0;

  /// \brief Returns whether this expression will accept the same values as another.
  virtual bool Equals(const BoundExpression& other) const = 0;

  /// \brief Returns a string representation of this bound expression.
  virtual std::string ToString() const = 0;
};

class ICEBERG_EXPORT BoundPredicate : public BoundExpression {};

}  // namespace iceberg
