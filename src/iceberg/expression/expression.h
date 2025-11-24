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
#include "iceberg/util/formattable.h"
#include "iceberg/util/macros.h"

namespace iceberg {

/// \brief Represents a boolean expression tree.
class ICEBERG_EXPORT Expression : public util::Formattable {
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
    kCountNull,
    kCountStar,
    kMax,
    kMin
  };

  virtual ~Expression() = default;

  /// \brief Returns the operation for an expression node.
  virtual Operation op() const = 0;

  /// \brief Returns the negation of this expression, equivalent to not(this).
  virtual Result<std::shared_ptr<Expression>> Negate() const {
    return NotSupported("Expression cannot be negated");
  }

  /// \brief Returns whether this expression will accept the same values as another.
  /// \param other another expression
  /// \return true if the expressions are equivalent
  virtual bool Equals(const Expression& other) const {
    // only bound predicates can be equivalent
    return false;
  }

  std::string ToString() const override { return "Expression"; }

  virtual bool is_unbound_predicate() const { return false; }
  virtual bool is_bound_predicate() const { return false; }
  virtual bool is_unbound_aggregate() const { return false; }
  virtual bool is_bound_aggregate() const { return false; }
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

  Result<std::shared_ptr<Expression>> Negate() const override;

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

  Result<std::shared_ptr<Expression>> Negate() const override;

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
  /// \brief Creates an And expression from two sub-expressions.
  ///
  /// \param left The left operand of the AND expression
  /// \param right The right operand of the AND expression
  static Result<std::unique_ptr<And>> Make(std::shared_ptr<Expression> left,
                                           std::shared_ptr<Expression> right);

  /// \brief Creates a folded And expression from two sub-expressions.
  ///
  /// \param left The left operand of the AND expression
  /// \param right The right operand of the AND expression
  /// \param args Additional operands of the AND expression
  /// \return A Result containing a shared pointer to the folded And expression, or an
  /// error if left or right is nullptr
  /// \note A folded And expression is an expression that is equivalent to the original
  /// expression, but with the And operation removed. For example, (true and x) = x.
  template <typename... Args>
  static Result<std::shared_ptr<Expression>> MakeFolded(std::shared_ptr<Expression> left,
                                                        std::shared_ptr<Expression> right,
                                                        Args&&... args)
    requires std::conjunction_v<std::is_same<Args, std::shared_ptr<Expression>>...>
  {
    if constexpr (sizeof...(args) == 0) {
      if (left->op() == Expression::Operation::kFalse ||
          right->op() == Expression::Operation::kFalse) {
        return False::Instance();
      }

      if (left->op() == Expression::Operation::kTrue) {
        return right;
      }

      if (right->op() == Expression::Operation::kTrue) {
        return left;
      }

      return And::Make(std::move(left), std::move(right));
    } else {
      ICEBERG_ASSIGN_OR_THROW(auto and_expr,
                              And::Make(std::move(left), std::move(right)));

      return And::MakeFolded(std::move(and_expr), std::forward<Args>(args)...);
    }
  }

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

  Result<std::shared_ptr<Expression>> Negate() const override;

  bool Equals(const Expression& other) const override;

 private:
  And(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;
};

/// \brief An Expression that represents a logical OR operation between two expressions.
///
/// This expression evaluates to true if at least one of its child expressions
/// evaluates to true.
class ICEBERG_EXPORT Or : public Expression {
 public:
  /// \brief Creates an Or expression from two sub-expressions.
  ///
  /// \param left The left operand of the OR expression
  /// \param right The right operand of the OR expression
  static Result<std::unique_ptr<Or>> Make(std::shared_ptr<Expression> left,
                                          std::shared_ptr<Expression> right);

  /// \brief Creates a folded Or expression from two sub-expressions.
  ///
  /// \param left The left operand of the OR expression
  /// \param right The right operand of the OR expression
  /// \param args Additional operands of the OR expression
  /// \return A Result containing a shared pointer to the folded Or expression, or an
  /// error if left or right is nullptr
  /// \note A folded Or expression is an expression that is equivalent to the original
  /// expression, but with the Or operation removed. For example, (false or x) = x.
  template <typename... Args>
  static Result<std::shared_ptr<Expression>> MakeFolded(std::shared_ptr<Expression> left,
                                                        std::shared_ptr<Expression> right,
                                                        Args&&... args)
    requires std::conjunction_v<std::is_same<Args, std::shared_ptr<Expression>>...>
  {
    if constexpr (sizeof...(args) == 0) {
      if (left->op() == Expression::Operation::kTrue ||
          right->op() == Expression::Operation::kTrue) {
        return True::Instance();
      }

      if (left->op() == Expression::Operation::kFalse) {
        return right;
      }

      if (right->op() == Expression::Operation::kFalse) {
        return left;
      }

      return Or::Make(std::move(left), std::move(right));
    } else {
      ICEBERG_ASSIGN_OR_THROW(auto or_expr, Or::Make(std::move(left), std::move(right)));

      return Or::MakeFolded(std::move(or_expr), std::forward<Args>(args)...);
    }
  }

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

  Result<std::shared_ptr<Expression>> Negate() const override;

  bool Equals(const Expression& other) const override;

 private:
  Or(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;
};

/// \brief An Expression that represents logical NOT operation.
///
/// This expression negates its child expression.
class ICEBERG_EXPORT Not : public Expression {
 public:
  /// \brief Creates a Not expression from a child expression.
  ///
  /// \param child The expression to negate
  /// \return A Result containing a unique pointer to Not, or an error if child is nullptr
  static Result<std::unique_ptr<Not>> Make(std::shared_ptr<Expression> child);

  /// \brief Creates a folded Not expression from a child expression.
  ///
  /// \param child The expression to negate
  /// \return A Result containing a shared pointer to the folded Not expression, or an
  /// error if child is nullptr
  /// \note A folded Not expression is an expression that is equivalent to the original
  /// expression, but with the Not operation removed. For example, not(not(x)) = x.
  static Result<std::shared_ptr<Expression>> MakeFolded(
      std::shared_ptr<Expression> child);

  /// \brief Returns the child expression.
  ///
  /// \return The child expression being negated
  const std::shared_ptr<Expression>& child() const { return child_; }

  Operation op() const override { return Operation::kNot; }

  std::string ToString() const override;

  Result<std::shared_ptr<Expression>> Negate() const override;

  bool Equals(const Expression& other) const override;

 private:
  explicit Not(std::shared_ptr<Expression> child);

  std::shared_ptr<Expression> child_;
};

/// \brief Returns a string representation of an expression operation.
ICEBERG_EXPORT std::string_view ToString(Expression::Operation op);

/// \brief Returns the negated operation.
ICEBERG_EXPORT Result<Expression::Operation> Negate(Expression::Operation op);

}  // namespace iceberg
