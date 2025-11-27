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
#include <unordered_set>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/term.h"

namespace iceberg {

template <typename T>
concept TermType = std::derived_from<T, Term>;

/// \brief A predicate is a boolean expression that tests a term against some criteria.
///
/// \tparam TermType The type of the term being tested
template <TermType T>
class ICEBERG_EXPORT Predicate : public virtual Expression {
 public:
  ~Predicate() override;

  Expression::Operation op() const override { return operation_; }

  /// \brief Returns the term this predicate tests.
  const std::shared_ptr<T>& term() const { return term_; }

 protected:
  /// \brief Create a predicate with an operation and term.
  ///
  /// \param op The operation this predicate performs
  /// \param term The term this predicate tests
  Predicate(Expression::Operation op, std::shared_ptr<T> term);

  Expression::Operation operation_;
  std::shared_ptr<T> term_;
};

/// \brief Non-template base class for all UnboundPredicate instances.
///
/// This class enables type erasure for template-based UnboundPredicate classes,
/// allowing them to be used in non-template visitor interfaces.
class ICEBERG_EXPORT UnboundPredicate : public virtual Expression,
                                        public Unbound<Expression> {
 public:
  ~UnboundPredicate() override = default;

  /// \brief Returns the reference of this UnboundPredicate.
  std::shared_ptr<NamedReference> reference() override = 0;

  /// \brief Bind this UnboundPredicate.
  Result<std::shared_ptr<Expression>> Bind(const Schema& schema,
                                           bool case_sensitive) const override = 0;

  /// \brief Negate this UnboundPredicate.
  Result<std::shared_ptr<Expression>> Negate() const override = 0;

  bool is_unbound_predicate() const override { return true; }

 protected:
  UnboundPredicate() = default;
};

/// \brief Unbound predicates contain unbound terms and must be bound to a concrete schema
/// before they can be evaluated.
///
/// \tparam B The bound type this predicate produces when binding is successful
template <typename B>
class ICEBERG_EXPORT UnboundPredicateImpl : public UnboundPredicate,
                                            public Predicate<UnboundTerm<B>> {
  using BASE = Predicate<UnboundTerm<B>>;

 public:
  /// \brief Create an unbound predicate (unary operation).
  ///
  /// \param op The operation (kIsNull, kNotNull, kIsNan, kNotNan)
  /// \param term The unbound term
  /// \return Result containing the unbound predicate or an error
  static Result<std::unique_ptr<UnboundPredicateImpl<B>>> Make(
      Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term);

  /// \brief Create an unbound predicate with a single value.
  ///
  /// \param op The operation
  /// \param term The unbound term
  /// \param value The literal value
  /// \return Result containing the unbound predicate or an error
  static Result<std::unique_ptr<UnboundPredicateImpl<B>>> Make(
      Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term, Literal value);

  /// \brief Create an unbound predicate with multiple values.
  ///
  /// \param op The operation (typically kIn or kNotIn)
  /// \param term The unbound term
  /// \param values Vector of literal values
  /// \return Result containing the unbound predicate or an error
  static Result<std::unique_ptr<UnboundPredicateImpl<B>>> Make(
      Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term,
      std::vector<Literal> values);

  ~UnboundPredicateImpl() override;

  std::shared_ptr<NamedReference> reference() override {
    return BASE::term()->reference();
  }

  std::string ToString() const override;

  Result<std::shared_ptr<Expression>> Bind(const Schema& schema,
                                           bool case_sensitive) const override;

  Result<std::shared_ptr<Expression>> Negate() const override;

  std::span<const Literal> literals() const { return values_; }

 private:
  UnboundPredicateImpl(Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term);
  UnboundPredicateImpl(Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term,
                       Literal value);
  UnboundPredicateImpl(Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term,
                       std::vector<Literal> values);

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
  ~BoundPredicate() override;

  using Predicate<BoundTerm>::op;

  using Predicate<BoundTerm>::term;

  std::shared_ptr<BoundReference> reference() override { return term_->reference(); }

  Result<Literal> Evaluate(const StructLike& data) const override;

  bool is_bound_predicate() const override { return true; }

  /// \brief Test a value against this predicate.
  ///
  /// \param value The literal value to test
  /// \return true if the predicate passes, false otherwise
  virtual Result<bool> Test(const Literal& value) const = 0;

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

 protected:
  BoundPredicate(Expression::Operation op, std::shared_ptr<BoundTerm> term);
};

/// \brief Bound unary predicate (null, not-null, etc.).
class ICEBERG_EXPORT BoundUnaryPredicate : public BoundPredicate {
 public:
  /// \brief Create a bound unary predicate.
  ///
  /// \param op The unary operation (kIsNull, kNotNull, kIsNan, kNotNan)
  /// \param term The bound term to test
  /// \return Result containing the bound unary predicate or an error
  static Result<std::unique_ptr<BoundUnaryPredicate>> Make(
      Expression::Operation op, std::shared_ptr<BoundTerm> term);

  ~BoundUnaryPredicate() override;

  Result<bool> Test(const Literal& value) const override;

  Kind kind() const override { return Kind::kUnary; }

  std::string ToString() const override;

  Result<std::shared_ptr<Expression>> Negate() const override;

  bool Equals(const Expression& other) const override;

 private:
  BoundUnaryPredicate(Expression::Operation op, std::shared_ptr<BoundTerm> term);
};

/// \brief Bound literal predicate (comparison against a single value).
class ICEBERG_EXPORT BoundLiteralPredicate : public BoundPredicate {
 public:
  /// \brief Create a bound literal predicate.
  ///
  /// \param op The comparison operation (kLt, kLtEq, kGt, kGtEq, kEq, kNotEq)
  /// \param term The bound term to compare
  /// \param literal The literal value to compare against
  /// \return Result containing the bound literal predicate or an error
  static Result<std::unique_ptr<BoundLiteralPredicate>> Make(
      Expression::Operation op, std::shared_ptr<BoundTerm> term, Literal literal);

  ~BoundLiteralPredicate() override;

  /// \brief Returns the literal being compared against.
  const Literal& literal() const { return literal_; }

  Result<bool> Test(const Literal& value) const override;

  Kind kind() const override { return Kind::kLiteral; }

  std::string ToString() const override;

  Result<std::shared_ptr<Expression>> Negate() const override;

  bool Equals(const Expression& other) const override;

 private:
  BoundLiteralPredicate(Expression::Operation op, std::shared_ptr<BoundTerm> term,
                        Literal literal);

  Literal literal_;
};

/// \brief Bound set predicate (membership testing against a set of values).
class ICEBERG_EXPORT BoundSetPredicate : public BoundPredicate {
 public:
  using LiteralSet = std::unordered_set<Literal, LiteralHash>;

  /// \brief Create a bound set predicate.
  ///
  /// \param op The set operation (kIn, kNotIn)
  /// \param term The bound term to test for membership
  /// \param literals The set of literal values to test against
  /// \return Result containing the bound set predicate or an error
  static Result<std::unique_ptr<BoundSetPredicate>> Make(
      Expression::Operation op, std::shared_ptr<BoundTerm> term,
      std::span<const Literal> literals);

  /// \brief Create a bound set predicate using a set of literals.
  ///
  /// \param op The set operation (kIn, kNotIn)
  /// \param term The bound term to test for membership
  /// \param value_set The set of literal values to test against
  /// \return Result containing the bound set predicate or an error
  static Result<std::unique_ptr<BoundSetPredicate>> Make(Expression::Operation op,
                                                         std::shared_ptr<BoundTerm> term,
                                                         LiteralSet value_set);

  ~BoundSetPredicate() override;

  /// \brief Returns the set of literals to test against.
  const LiteralSet& literal_set() const { return value_set_; }

  Result<bool> Test(const Literal& value) const override;

  Kind kind() const override { return Kind::kSet; }

  std::string ToString() const override;

  Result<std::shared_ptr<Expression>> Negate() const override;

  bool Equals(const Expression& other) const override;

 private:
  BoundSetPredicate(Expression::Operation op, std::shared_ptr<BoundTerm> term,
                    std::span<const Literal> literals);

  BoundSetPredicate(Expression::Operation op, std::shared_ptr<BoundTerm> term,
                    LiteralSet value_set);

  LiteralSet value_set_;
};

}  // namespace iceberg
