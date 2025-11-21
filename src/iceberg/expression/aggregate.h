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

/// \file iceberg/expression/aggregate.h
/// Aggregate expression definitions.

#include <memory>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/term.h"
#include "iceberg/result.h"

namespace iceberg {

class AggregateEvaluator;

/// \brief Base class for aggregate expressions.
class ICEBERG_EXPORT Aggregate : public Expression {
 public:
  ~Aggregate() override = default;

  Expression::Operation op() const override { return operation_; }

  bool is_unbound_aggregate() const override { return false; }
  bool is_bound_aggregate() const override { return false; }

 protected:
  explicit Aggregate(Expression::Operation op) : operation_(op) {}

 private:
  Expression::Operation operation_;
};

/// \brief Unbound aggregate with an optional term.
class ICEBERG_EXPORT UnboundAggregate : public Aggregate, public Unbound<Expression> {
 public:
  ~UnboundAggregate() override = default;

  bool is_unbound_aggregate() const override { return true; }

  /// \brief Returns the unbound reference if the aggregate has a term.
  std::shared_ptr<NamedReference> reference() override = 0;

 protected:
  explicit UnboundAggregate(Expression::Operation op) : Aggregate(op) {}
};

/// \brief Bound aggregate with an optional term.
class ICEBERG_EXPORT BoundAggregate : public Aggregate {
 public:
  ~BoundAggregate() override = default;

  bool is_bound_aggregate() const override { return true; }

  const std::shared_ptr<BoundTerm>& term() const { return term_; }

 protected:
  BoundAggregate(Expression::Operation op, std::shared_ptr<BoundTerm> term)
      : Aggregate(op), term_(std::move(term)) {}

 private:
  std::shared_ptr<BoundTerm> term_;
};

/// \brief COUNT aggregate variants.
class ICEBERG_EXPORT CountAggregate : public UnboundAggregate {
 public:
  enum class Mode { kNonNull, kNull, kStar };

  static Result<std::unique_ptr<CountAggregate>> Count(
      std::shared_ptr<UnboundTerm<BoundReference>> term);

  static Result<std::unique_ptr<CountAggregate>> CountNull(
      std::shared_ptr<UnboundTerm<BoundReference>> term);

  static std::unique_ptr<CountAggregate> CountStar();

  ~CountAggregate() override = default;

  Mode mode() const { return mode_; }

  const std::shared_ptr<UnboundTerm<BoundReference>>& term() const { return term_; }

  std::shared_ptr<NamedReference> reference() override { return reference_; }

  std::string ToString() const override;

  Result<std::shared_ptr<Expression>> Bind(const Schema& schema,
                                           bool case_sensitive) const override;

 private:
  CountAggregate(Expression::Operation op, Mode mode,
                 std::shared_ptr<UnboundTerm<BoundReference>> term,
                 std::shared_ptr<NamedReference> reference);

  Mode mode_;
  std::shared_ptr<UnboundTerm<BoundReference>> term_;
  std::shared_ptr<NamedReference> reference_;
};

/// \brief Bound COUNT aggregate.
class ICEBERG_EXPORT BoundCountAggregate : public BoundAggregate {
 public:
  BoundCountAggregate(Expression::Operation op, CountAggregate::Mode mode,
                      std::shared_ptr<BoundTerm> term);

  CountAggregate::Mode mode() const { return mode_; }

  std::string ToString() const override;

 private:
  CountAggregate::Mode mode_;
};

/// \brief MAX/MIN aggregate on a single term.
class ICEBERG_EXPORT ValueAggregate : public UnboundAggregate {
 public:
  static Result<std::unique_ptr<ValueAggregate>> Max(
      std::shared_ptr<UnboundTerm<BoundReference>> term);

  static Result<std::unique_ptr<ValueAggregate>> Min(
      std::shared_ptr<UnboundTerm<BoundReference>> term);

  ~ValueAggregate() override = default;

  std::shared_ptr<NamedReference> reference() override { return reference_; }

  const std::shared_ptr<UnboundTerm<BoundReference>>& term() const { return term_; }

  std::string ToString() const override;

  Result<std::shared_ptr<Expression>> Bind(const Schema& schema,
                                           bool case_sensitive) const override;

 private:
  ValueAggregate(Expression::Operation op,
                 std::shared_ptr<UnboundTerm<BoundReference>> term,
                 std::shared_ptr<NamedReference> reference);

  std::shared_ptr<UnboundTerm<BoundReference>> term_;
  std::shared_ptr<NamedReference> reference_;
};

/// \brief Bound MAX/MIN aggregate.
class ICEBERG_EXPORT BoundValueAggregate : public BoundAggregate {
 public:
  BoundValueAggregate(Expression::Operation op, std::shared_ptr<BoundTerm> term);

  std::string ToString() const override;
};

/// \brief Evaluates bound aggregates over StructLike rows.
class ICEBERG_EXPORT AggregateEvaluator {
 public:
  virtual ~AggregateEvaluator() = default;

  /// \brief Create an evaluator for a bound aggregate.
  static Result<std::unique_ptr<AggregateEvaluator>> Make(
      std::shared_ptr<BoundAggregate> aggregate);

  /// \brief Add a row to the aggregate.
  virtual Status Add(const StructLike& row) = 0;

  /// \brief Final aggregated value.
  virtual Result<Literal> ResultLiteral() const = 0;
};

}  // namespace iceberg
