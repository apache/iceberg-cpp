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

#include <concepts>
#include <memory>
#include <vector>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/term.h"
#include "iceberg/result.h"

namespace iceberg {

template <typename T>
concept AggregateTermType = std::derived_from<T, Term>;

/// \brief Base aggregate holding an operation and a term.
template <AggregateTermType T>
class ICEBERG_EXPORT Aggregate : public virtual Expression {
 public:
  ~Aggregate() override = default;

  Expression::Operation op() const override { return operation_; }

  const std::shared_ptr<T>& term() const { return term_; }

 protected:
  Aggregate(Expression::Operation op, std::shared_ptr<T> term)
      : operation_(op), term_(std::move(term)) {}

  Expression::Operation operation_;
  std::shared_ptr<T> term_;
};

/// \brief Base class for unbound aggregates.
class ICEBERG_EXPORT UnboundAggregate : public virtual Expression,
                                        public Unbound<Expression> {
 public:
  ~UnboundAggregate() override = default;

  bool is_unbound_aggregate() const override { return true; }
};

/// \brief Template for unbound aggregates that carry a term and operation.
template <typename B>
class ICEBERG_EXPORT UnboundAggregateImpl : public UnboundAggregate,
                                            public Aggregate<UnboundTerm<B>> {
  using BASE = Aggregate<UnboundTerm<B>>;

 public:
  enum class CountMode { kNonNull, kNull, kStar, kNone };

  UnboundAggregateImpl(Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term,
                       CountMode count_mode = CountMode::kNone)
      : BASE(op, std::move(term)), count_mode_(count_mode) {}

  std::shared_ptr<NamedReference> reference() override {
    return BASE::term() ? BASE::term()->reference() : nullptr;
  }

  Result<std::shared_ptr<Expression>> Bind(const Schema& schema,
                                           bool case_sensitive) const override;

  CountMode count_mode() const { return count_mode_; }

 private:
  CountMode count_mode_;
};

/// \brief Base class for bound aggregates.
class ICEBERG_EXPORT BoundAggregate : public Aggregate<BoundTerm>, public Bound {
 public:
  using Aggregate<BoundTerm>::op;
  using Aggregate<BoundTerm>::term;

  std::shared_ptr<BoundReference> reference() override {
    return term_ ? term_->reference() : nullptr;
  }

  Result<Literal> Evaluate(const StructLike& data) const override = 0;

  bool is_bound_aggregate() const override { return true; }

  enum class Kind : int8_t {
    // Count aggregates (COUNT, COUNT_STAR, COUNT_NULL)
    kCount = 0,
    // Value aggregates (MIN, MAX)
    kValue,
  };

  virtual Kind kind() const = 0;

 protected:
  BoundAggregate(Expression::Operation op, std::shared_ptr<BoundTerm> term)
      : Aggregate<BoundTerm>(op, std::move(term)) {}
};

/// \brief Bound COUNT aggregate.
class ICEBERG_EXPORT CountAggregate : public BoundAggregate {
 public:
  enum class Mode { kNonNull, kNull, kStar };

  BoundCountAggregate(Expression::Operation op, Mode mode,
                      std::shared_ptr<BoundTerm> term);

  Mode mode() const { return mode_; }

  Kind kind() const override { return Kind::kCount; }

  std::string ToString() const override;
  Result<Literal> Evaluate(const StructLike& data) const override;

 private:
  Mode mode_;
};

/// \brief Bound MAX/MIN aggregate.
class ICEBERG_EXPORT BoundValueAggregate : public BoundAggregate {
 public:
  BoundValueAggregate(Expression::Operation op, std::shared_ptr<BoundTerm> term);

  Kind kind() const override { return Kind::kValue; }

  std::string ToString() const override;
  Result<Literal> Evaluate(const StructLike& data) const override;
};

/// \brief Evaluates bound aggregates over StructLike rows.
class ICEBERG_EXPORT AggregateEvaluator {
 public:
  virtual ~AggregateEvaluator() = default;

  /// \brief Create an evaluator for a single bound aggregate.
  /// \param aggregate The bound aggregate to evaluate across rows.
  static Result<std::unique_ptr<AggregateEvaluator>> Make(
      std::shared_ptr<BoundAggregate> aggregate);

  /// \brief Create an evaluator for multiple bound aggregates.
  /// \param aggregates Aggregates to evaluate in one pass; order is preserved in
  /// Results().
  static Result<std::unique_ptr<AggregateEvaluator>> MakeList(
      std::vector<std::shared_ptr<BoundAggregate>> aggregates);

  /// \brief Add a row to the aggregate.
  virtual Status Add(const StructLike& row) = 0;

  /// \brief Final aggregated value.
  virtual Result<std::vector<Literal>> Results() const = 0;

  /// \brief Convenience accessor when only one aggregate is evaluated.
  virtual Result<Literal> ResultLiteral() const = 0;
};

}  // namespace iceberg
