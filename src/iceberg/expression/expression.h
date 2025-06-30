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

#include "iceberg/exception.h"
#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Represents a boolean expression tree.
class ICEBERG_EXPORT Expression {
 public:
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

  virtual ~Expression() = default;

  /// \brief Returns the operation for an expression node.
  virtual Operation op() const = 0;

  /// \brief Returns the negation of this expression, equivalent to not(this).
  virtual std::shared_ptr<Expression> Negate() const {
    throw IcebergError("Expression cannot be negated");
  }

  /// \brief Returns whether this expression will accept the same values as another.
  /// \param other another expression
  /// \return true if the expressions are equivalent
  virtual bool Equals(const Expression& other) const {
    // only bound predicates can be equivalent
    return false;
  }

  virtual std::string ToString() const { return "Expression"; }
};

/// \brief An Expression that is always true.
///
/// Represents a boolean predicate that always evaluates to true.
class ICEBERG_EXPORT True : public Expression {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<True>& Instance();

  Operation op() const override { return Operation::kTrue; }

  std::string ToString() const override { return "true"; }

  std::shared_ptr<Expression> Negate() const override;

  bool Equals(const Expression& other) const override {
    return other.op() == Operation::kTrue;
  }

 private:
  constexpr True() = default;
};

/// \brief An expression that is always false.
class ICEBERG_EXPORT False : public Expression {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<False>& Instance();

  Operation op() const override { return Operation::kFalse; }

  std::string ToString() const override { return "false"; }

  std::shared_ptr<Expression> Negate() const override;

  bool Equals(const Expression& other) const override {
    return other.op() == Operation::kFalse;
  }

 private:
  constexpr False() = default;
};

/// \brief An Expression that represents a logical AND operation between two expressions.
///
/// This expression evaluates to true if and only if both of its child expressions
/// evaluate to true.
class ICEBERG_EXPORT And : public Expression {
 public:
  /// \brief Constructs an And expression from two sub-expressions.
  ///
  /// \param left The left operand of the AND expression
  /// \param right The right operand of the AND expression
  And(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

  /// \brief Returns the left operand of the AND expression.
  ///
  /// \return The left operand of the AND expression
  const std::shared_ptr<Expression>& left() const { return left_; }

  /// \brief Returns the right operand of the AND expression.
  ///
  /// \return The right operand of the AND expression
  const std::shared_ptr<Expression>& right() const { return right_; }

  Operation op() const override { return Operation::kAnd; }

  std::string ToString() const override;

  std::shared_ptr<Expression> Negate() const override;

  bool Equals(const Expression& other) const override;

 private:
  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;
};

/// \brief An Expression that represents a logical OR operation between two expressions.
///
/// This expression evaluates to true if at least one of its child expressions
/// evaluates to true.
class ICEBERG_EXPORT Or : public Expression {
 public:
  /// \brief Constructs an Or expression from two sub-expressions.
  ///
  /// \param left The left operand of the OR expression
  /// \param right The right operand of the OR expression
  Or(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

  /// \brief Returns the left operand of the OR expression.
  ///
  /// \return The left operand of the OR expression
  const std::shared_ptr<Expression>& left() const { return left_; }

  /// \brief Returns the right operand of the OR expression.
  ///
  /// \return The right operand of the OR expression
  const std::shared_ptr<Expression>& right() const { return right_; }

  Operation op() const override { return Operation::kOr; }

  std::string ToString() const override;

  std::shared_ptr<Expression> Negate() const override;

  bool Equals(const Expression& other) const override;

 private:
  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;
};

}  // namespace iceberg
