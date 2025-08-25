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

/// \file iceberg/expression/predicate.h
/// Predicate interface for boolean expressions that test terms.

#include <concepts>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/term.h"

namespace iceberg {

template <typename T>
concept TermType = std::derived_from<T, Term>;

/// \brief A predicate is a boolean expression that tests a term against some criteria.
///
/// \tparam TermType The type of the term being tested
template <TermType T>
class ICEBERG_EXPORT Predicate : public Expression {
 public:
  /// \brief Create a predicate with an operation and term.
  ///
  /// \param op The operation this predicate performs
  /// \param term The term this predicate tests
  Predicate(Expression::Operation op, std::shared_ptr<T> term);

  ~Predicate() override;

  Expression::Operation op() const override { return operation_; }

  /// \brief Returns the term this predicate tests.
  const std::shared_ptr<T>& term() const { return term_; }

 protected:
  Expression::Operation operation_;
  std::shared_ptr<T> term_;
};

/// \brief Unbound predicates contain unbound terms and must be bound to a concrete schema
/// before they can be evaluated.
///
/// \tparam B The bound type this predicate produces when binding is successful
template <typename B>
class ICEBERG_EXPORT UnboundPredicate : public Predicate<UnboundTerm<B>>,
                                        public Unbound<Expression> {
  using BASE = Predicate<UnboundTerm<B>>;

 public:
  UnboundPredicate(Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term);
  UnboundPredicate(Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term,
                   Literal value);
  UnboundPredicate(Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term,
                   std::vector<Literal> values);

  ~UnboundPredicate() override;

  std::shared_ptr<NamedReference> reference() override {
    return BASE::term()->reference();
  }

  std::string ToString() const override;

  /// \brief Bind this UnboundPredicate.
  Result<std::shared_ptr<Expression>> Bind(const Schema& schema,
                                           bool case_sensitive) const override;

  Result<std::shared_ptr<Expression>> Negate() const override;

 private:
  Result<std::shared_ptr<Expression>> BindUnaryOperation(
      std::shared_ptr<B> bound_term) const;
  Result<std::shared_ptr<Expression>> BindLiteralOperation(
      std::shared_ptr<B> bound_term) const;
  Result<std::shared_ptr<Expression>> BindInOperation(
      std::shared_ptr<B> bound_term) const;

 private:
  std::vector<Literal> values_;
};

/// \brief Bound predicates contain bound terms and can be evaluated.
class ICEBERG_EXPORT BoundPredicate : public Predicate<BoundTerm>, public Bound {
 public:
  BoundPredicate(Expression::Operation op, std::shared_ptr<BoundTerm> term);

  ~BoundPredicate() override;

  using Predicate<BoundTerm>::op;

  using Predicate<BoundTerm>::term;

  std::shared_ptr<BoundReference> reference() override { return term_->reference(); }

  Result<Literal::Value> Evaluate(const StructLike& data) const override;

  /// \brief Test a value against this predicate.
  ///
  /// \param value The value to test
  /// \return true if the predicate passes, false otherwise
  virtual Result<bool> Test(const Literal::Value& value) const = 0;

  enum class Kind : int8_t {
    // A unary predicate (tests for null, not-null, etc.).
    kUnary = 0,
    // A literal predicate (compares against a literal).
    kLiteral,
    // A set predicate (tests membership in a set).
    kSet,
  };

  /// \brief Returns the kind of this bound predicate.
  virtual Kind kind() const = 0;
};

/// \brief Bound unary predicate (null, not-null, etc.).
class ICEBERG_EXPORT BoundUnaryPredicate : public BoundPredicate {
 public:
  /// \brief Create a bound unary predicate.
  ///
  /// \param op The unary operation (kIsNull, kNotNull, kIsNan, kNotNan)
  /// \param term The bound term to test
  BoundUnaryPredicate(Expression::Operation op, std::shared_ptr<BoundTerm> term);

  ~BoundUnaryPredicate() override;

  Result<bool> Test(const Literal::Value& value) const override;

  Kind kind() const override { return Kind::kUnary; }

  std::string ToString() const override;

  bool Equals(const Expression& other) const override;
};

/// \brief Bound literal predicate (comparison against a single value).
class ICEBERG_EXPORT BoundLiteralPredicate : public BoundPredicate {
 public:
  /// \brief Create a bound literal predicate.
  ///
  /// \param op The comparison operation (kLt, kLtEq, kGt, kGtEq, kEq, kNotEq)
  /// \param term The bound term to compare
  /// \param literal The literal value to compare against
  BoundLiteralPredicate(Expression::Operation op, std::shared_ptr<BoundTerm> term,
                        Literal literal);

  ~BoundLiteralPredicate() override;

  /// \brief Returns the literal being compared against.
  const Literal& literal() const { return literal_; }

  Result<bool> Test(const Literal::Value& value) const override;

  Kind kind() const override { return Kind::kLiteral; }

  std::string ToString() const override;

  bool Equals(const Expression& other) const override;

 private:
  Literal literal_;
};

/// \brief Bound set predicate (membership testing against a set of values).
class ICEBERG_EXPORT BoundSetPredicate : public BoundPredicate {
 public:
  /// \brief Create a bound set predicate.
  ///
  /// \param op The set operation (kIn, kNotIn)
  /// \param term The bound term to test for membership
  /// \param literals The set of literal values to test against
  BoundSetPredicate(Expression::Operation op, std::shared_ptr<BoundTerm> term,
                    std::span<const Literal> literals);

  ~BoundSetPredicate() override;

  /// \brief Returns the set of literals to test against.
  const std::vector<Literal::Value>& literal_set() const { return value_set_; }

  Result<bool> Test(const Literal::Value& value) const override;

  Kind kind() const override { return Kind::kSet; }

  std::string ToString() const override;

  bool Equals(const Expression& other) const override;

 private:
  /// FIXME: Literal::Value does not have hash support. We need to add this
  /// and replace the vector with a unordered_set.
  std::vector<Literal::Value> value_set_;
};

}  // namespace iceberg
