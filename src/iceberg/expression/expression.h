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

#include "iceberg/expression/common.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/term.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"

namespace iceberg {

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

  /// \brief Creates an IsNull predicate
  static std::shared_ptr<Predicate> IsNull(Reference reference);

  /// \brief Creates an IsNotNull predicate
  static std::shared_ptr<Predicate> IsNotNull(Reference reference);

  /// \brief Creates an IsNan predicate
  static std::shared_ptr<Predicate> IsNan(Reference reference);

  /// \brief Creates an IsNotNan predicate
  static std::shared_ptr<Predicate> IsNotNan(Reference reference);

  /// \brief Creates an equal-to predicate: reference = literal
  static std::shared_ptr<Predicate> Equal(Reference reference, Literal literal);

  /// \brief Creates a not-equal-to predicate: reference != literal
  static std::shared_ptr<Predicate> NotEqual(Reference reference, Literal literal);

  /// \brief Creates a less-than predicate: reference < literal
  static std::shared_ptr<Predicate> LessThan(Reference reference, Literal literal);

  /// \brief Creates a less-than-or-equal predicate: reference <= literal
  static std::shared_ptr<Predicate> LessThanOrEqual(Reference reference, Literal literal);

  /// \brief Creates a greater-than predicate: reference > literal
  static std::shared_ptr<Predicate> GreaterThan(Reference reference, Literal literal);

  /// \brief Creates a greater-than-or-equal predicate: reference >= literal
  static std::shared_ptr<Predicate> GreaterThanOrEqual(Reference reference,
                                                       Literal literal);

  /// \brief Creates a starts-with predicate: reference STARTS WITH literal
  static std::shared_ptr<Predicate> StartsWith(Reference reference, Literal literal);

  /// \brief Creates a not-starts-with predicate: reference NOT STARTS WITH literal
  static std::shared_ptr<Predicate> NotStartsWith(Reference reference, Literal literal);
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
