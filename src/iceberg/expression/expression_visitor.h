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

/// \file iceberg/expression/expression_visitor.h
/// Visitor pattern implementation for traversing Iceberg expression trees.

#include <concepts>
#include <memory>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/term.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

/// \brief Base visitor for traversing expression trees.
///
/// This visitor traverses expression trees in postorder traversal and calls appropriate
/// visitor methods for each node. Subclasses can override specific methods to implement
/// custom behavior.
///
/// \tparam R The return type produced by visitor methods
template <typename R>
class ICEBERG_EXPORT ExpressionVisitor {
  using ParamType = std::conditional_t<std::is_fundamental_v<R>, R, const R&>;

 public:
  virtual ~ExpressionVisitor() = default;

  /// \brief Visit a True expression (always evaluates to true).
  virtual Result<R> AlwaysTrue() = 0;

  /// \brief Visit a False expression (always evaluates to false).
  virtual Result<R> AlwaysFalse() = 0;

  /// \brief Visit a Not expression.
  /// \param child_result The result from visiting the child expression
  virtual Result<R> Not(ParamType child_result) = 0;

  /// \brief Visit an And expression.
  /// \param left_result The result from visiting the left child
  /// \param right_result The result from visiting the right child
  virtual Result<R> And(ParamType left_result, ParamType right_result) = 0;

  /// \brief Visit an Or expression.
  /// \param left_result The result from visiting the left child
  /// \param right_result The result from visiting the right child
  virtual Result<R> Or(ParamType left_result, ParamType right_result) = 0;

  /// \brief Visit a bound predicate.
  /// \param pred The bound predicate to visit
  virtual Result<R> Predicate(const std::shared_ptr<BoundPredicate>& pred) = 0;

  /// \brief Visit an unbound predicate.
  /// \param pred The unbound predicate to visit
  virtual Result<R> Predicate(const std::shared_ptr<UnboundPredicate>& pred) = 0;
};

/// \brief Visitor for bound expressions.
///
/// This visitor is for traversing bound expression trees.
///
/// \tparam R The return type produced by visitor methods
template <typename R>
class ICEBERG_EXPORT BoundVisitor : public ExpressionVisitor<R> {
 public:
  ~BoundVisitor() override = default;

  /// \brief Visit an IS_NULL unary predicate.
  /// \param term The bound term being tested
  virtual Result<R> IsNull(const std::shared_ptr<BoundTerm>& term) = 0;

  /// \brief Visit a NOT_NULL unary predicate.
  /// \param term The bound term being tested
  virtual Result<R> NotNull(const std::shared_ptr<BoundTerm>& term) = 0;

  /// \brief Visit an IS_NAN unary predicate.
  /// \param term The bound term being tested
  virtual Result<R> IsNaN(const std::shared_ptr<BoundTerm>& term) {
    return NotSupported("IsNaN operation is not supported by this visitor");
  }

  /// \brief Visit a NOT_NAN unary predicate.
  /// \param term The bound term being tested
  virtual Result<R> NotNaN(const std::shared_ptr<BoundTerm>& term) {
    return NotSupported("NotNaN operation is not supported by this visitor");
  }

  /// \brief Visit a less-than predicate.
  /// \param term The bound term
  /// \param lit The literal value to compare against
  virtual Result<R> Lt(const std::shared_ptr<BoundTerm>& term, const Literal& lit) = 0;

  /// \brief Visit a less-than-or-equal predicate.
  /// \param term The bound term
  /// \param lit The literal value to compare against
  virtual Result<R> LtEq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) = 0;

  /// \brief Visit a greater-than predicate.
  /// \param term The bound term
  /// \param lit The literal value to compare against
  virtual Result<R> Gt(const std::shared_ptr<BoundTerm>& term, const Literal& lit) = 0;

  /// \brief Visit a greater-than-or-equal predicate.
  /// \param term The bound term
  /// \param lit The literal value to compare against
  virtual Result<R> GtEq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) = 0;

  /// \brief Visit an equality predicate.
  /// \param term The bound term
  /// \param lit The literal value to compare against
  virtual Result<R> Eq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) = 0;

  /// \brief Visit a not-equal predicate.
  /// \param term The bound term
  /// \param lit The literal value to compare against
  virtual Result<R> NotEq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) = 0;

  /// \brief Visit a starts-with predicate.
  /// \param term The bound term
  /// \param lit The literal value to check for prefix match
  virtual Result<R> StartsWith([[maybe_unused]] const std::shared_ptr<BoundTerm>& term,
                               [[maybe_unused]] const Literal& lit) {
    return NotSupported("StartsWith operation is not supported by this visitor");
  }

  /// \brief Visit a not-starts-with predicate.
  /// \param term The bound term
  /// \param lit The literal value to check for prefix match
  virtual Result<R> NotStartsWith([[maybe_unused]] const std::shared_ptr<BoundTerm>& term,
                                  [[maybe_unused]] const Literal& lit) {
    return NotSupported("NotStartsWith operation is not supported by this visitor");
  }

  /// \brief Visit an IN set predicate.
  /// \param term The bound term
  /// \param literal_set The set of literal values to test membership
  virtual Result<R> In(
      [[maybe_unused]] const std::shared_ptr<BoundTerm>& term,
      [[maybe_unused]] const BoundSetPredicate::LiteralSet& literal_set) {
    return NotSupported("In operation is not supported by this visitor");
  }

  /// \brief Visit a NOT_IN set predicate.
  /// \param term The bound term
  /// \param literal_set The set of literal values to test membership
  virtual Result<R> NotIn(
      [[maybe_unused]] const std::shared_ptr<BoundTerm>& term,
      [[maybe_unused]] const BoundSetPredicate::LiteralSet& literal_set) {
    return NotSupported("NotIn operation is not supported by this visitor");
  }

  /// \brief Visit a bound predicate.
  ///
  /// This method dispatches to specific visitor methods based on the predicate
  /// type and operation.
  ///
  /// \param pred The bound predicate to visit
  Result<R> Predicate(const std::shared_ptr<BoundPredicate>& pred) override {
    ICEBERG_DCHECK(pred != nullptr, "BoundPredicate cannot be null");

    switch (pred->kind()) {
      case BoundPredicate::Kind::kUnary: {
        switch (pred->op()) {
          case Expression::Operation::kIsNull:
            return IsNull(pred->term());
          case Expression::Operation::kNotNull:
            return NotNull(pred->term());
          case Expression::Operation::kIsNan:
            return IsNaN(pred->term());
          case Expression::Operation::kNotNan:
            return NotNaN(pred->term());
          default:
            return InvalidExpression("Invalid operation for BoundUnaryPredicate: {}",
                                     ToString(pred->op()));
        }
      }
      case BoundPredicate::Kind::kLiteral: {
        const auto& literal_pred =
            internal::checked_cast<const BoundLiteralPredicate&>(*pred);
        switch (pred->op()) {
          case Expression::Operation::kLt:
            return Lt(pred->term(), literal_pred.literal());
          case Expression::Operation::kLtEq:
            return LtEq(pred->term(), literal_pred.literal());
          case Expression::Operation::kGt:
            return Gt(pred->term(), literal_pred.literal());
          case Expression::Operation::kGtEq:
            return GtEq(pred->term(), literal_pred.literal());
          case Expression::Operation::kEq:
            return Eq(pred->term(), literal_pred.literal());
          case Expression::Operation::kNotEq:
            return NotEq(pred->term(), literal_pred.literal());
          case Expression::Operation::kStartsWith:
            return StartsWith(pred->term(), literal_pred.literal());
          case Expression::Operation::kNotStartsWith:
            return NotStartsWith(pred->term(), literal_pred.literal());
          default:
            return InvalidExpression("Invalid operation for BoundLiteralPredicate: {}",
                                     ToString(pred->op()));
        }
      }
      case BoundPredicate::Kind::kSet: {
        const auto& set_pred = internal::checked_cast<const BoundSetPredicate&>(*pred);
        switch (pred->op()) {
          case Expression::Operation::kIn:
            return In(pred->term(), set_pred.literal_set());
          case Expression::Operation::kNotIn:
            return NotIn(pred->term(), set_pred.literal_set());
          default:
            return InvalidExpression("Invalid operation for BoundSetPredicate: {}",
                                     ToString(pred->op()));
        }
      }
    }

    return InvalidExpression("Unsupported bound predicate: {}", pred->ToString());
  }

  /// \brief Visit an unbound predicate.
  ///
  /// Bound visitors do not support unbound predicates.
  ///
  /// \param pred The unbound predicate
  Result<R> Predicate(const std::shared_ptr<UnboundPredicate>& pred) final {
    ICEBERG_DCHECK(pred != nullptr, "UnboundPredicate cannot be null");
    return NotSupported("Not a bound predicate: {}", pred->ToString());
  }
};

/// \brief Traverse an expression tree with a visitor.
///
/// This function traverses the given expression tree in postorder traversal and calls
/// appropriate visitor methods for each node. Results from child nodes are passed to
/// parent nodes.
///
/// \tparam R The return type produced by the visitor
/// \tparam V The visitor type (must derive from ExpressionVisitor<R>)
/// \param expr The expression to traverse
/// \param visitor The visitor to use for traversal
/// \return The result produced by the visitor for the root expression node
template <typename R, typename V>
  requires std::derived_from<V, ExpressionVisitor<R>>
Result<R> Visit(const std::shared_ptr<Expression>& expr, V& visitor) {
  ICEBERG_DCHECK(expr != nullptr, "Expression cannot be null");

  if (expr->is_bound_predicate()) {
    return visitor.Predicate(std::dynamic_pointer_cast<BoundPredicate>(expr));
  }

  if (expr->is_unbound_predicate()) {
    return visitor.Predicate(std::dynamic_pointer_cast<UnboundPredicate>(expr));
  }

  // TODO(gangwu): handle aggregate expression

  switch (expr->op()) {
    case Expression::Operation::kTrue:
      return visitor.AlwaysTrue();
    case Expression::Operation::kFalse:
      return visitor.AlwaysFalse();
    case Expression::Operation::kNot: {
      const auto& not_expr = internal::checked_pointer_cast<Not>(expr);
      ICEBERG_ASSIGN_OR_RAISE(auto child_result,
                              (Visit<R, V>(not_expr->child(), visitor)));
      return visitor.Not(std::move(child_result));
    }
    case Expression::Operation::kAnd: {
      const auto& and_expr = internal::checked_pointer_cast<And>(expr);
      ICEBERG_ASSIGN_OR_RAISE(auto left_result, (Visit<R, V>(and_expr->left(), visitor)));
      ICEBERG_ASSIGN_OR_RAISE(auto right_result,
                              (Visit<R, V>(and_expr->right(), visitor)));
      return visitor.And(std::move(left_result), std::move(right_result));
    }
    case Expression::Operation::kOr: {
      const auto& or_expr = internal::checked_pointer_cast<Or>(expr);
      ICEBERG_ASSIGN_OR_RAISE(auto left_result, (Visit<R, V>(or_expr->left(), visitor)));
      ICEBERG_ASSIGN_OR_RAISE(auto right_result,
                              (Visit<R, V>(or_expr->right(), visitor)));
      return visitor.Or(std::move(left_result), std::move(right_result));
    }
    default:
      return InvalidExpression("Unknown expression operation: {}", expr->ToString());
  }
}

}  // namespace iceberg
