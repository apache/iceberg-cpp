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
#include <optional>
#include <string>
#include <vector>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/term.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base aggregate holding an operation and a term.
template <TermType T>
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
  static Result<std::shared_ptr<UnboundAggregateImpl<B>>> Make(
      Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term);

  std::shared_ptr<NamedReference> reference() override {
    return BASE::term() ? BASE::term()->reference() : nullptr;
  }

  Result<std::shared_ptr<Expression>> Bind(const Schema& schema,
                                           bool case_sensitive) const override;

  std::string ToString() const override;

 private:
  static constexpr bool IsSupportedOp(Expression::Operation op) {
    return op == Expression::Operation::kCount ||
           op == Expression::Operation::kCountNull ||
           op == Expression::Operation::kCountStar || op == Expression::Operation::kMax ||
           op == Expression::Operation::kMin;
  }

  UnboundAggregateImpl(Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term)
      : BASE(op, std::move(term)) {
    ICEBERG_DCHECK(IsSupportedOp(op), "Unexpected aggregate operation");
    ICEBERG_DCHECK(op == Expression::Operation::kCountStar || BASE::term() != nullptr,
                   "Aggregate term cannot be null unless COUNT(*)");
  }
};

/// \brief Base class for bound aggregates.
class ICEBERG_EXPORT BoundAggregate : public Aggregate<BoundTerm>, public Bound {
 public:
  using Aggregate<BoundTerm>::op;
  using Aggregate<BoundTerm>::term;

  class ICEBERG_EXPORT Aggregator {
   public:
    virtual ~Aggregator() = default;

    virtual Status Update(const StructLike& row) = 0;
    virtual Status Update(const DataFile& file) {
      return NotSupported("Aggregating DataFile not supported");
    }
    virtual Result<Literal> ResultLiteral() const = 0;
  };

  std::shared_ptr<BoundReference> reference() override {
    ICEBERG_DCHECK(term_ != nullptr || op() == Expression::Operation::kCountStar,
                   "Bound aggregate term should not be null unless COUNT(*)");
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
  virtual Result<std::unique_ptr<Aggregator>> NewAggregator() const = 0;

 protected:
  BoundAggregate(Expression::Operation op, std::shared_ptr<BoundTerm> term)
      : Aggregate<BoundTerm>(op, std::move(term)) {}
};

/// \brief Base class for COUNT aggregates.
class ICEBERG_EXPORT CountAggregate : public BoundAggregate {
 public:
  Kind kind() const override { return Kind::kCount; }

 protected:
  CountAggregate(Expression::Operation op, std::shared_ptr<BoundTerm> term)
      : BoundAggregate(op, std::move(term)) {}
};

/// \brief COUNT(term) aggregate.
class ICEBERG_EXPORT CountNonNullAggregate : public CountAggregate {
 public:
  static Result<std::shared_ptr<CountNonNullAggregate>> Make(
      std::shared_ptr<BoundTerm> term);

  std::string ToString() const override;
  Result<Literal> Evaluate(const StructLike& data) const override;
  Result<std::unique_ptr<Aggregator>> NewAggregator() const override;

 private:
  explicit CountNonNullAggregate(std::shared_ptr<BoundTerm> term);
};

/// \brief COUNT_NULL(term) aggregate.
class ICEBERG_EXPORT CountNullAggregate : public CountAggregate {
 public:
  static Result<std::shared_ptr<CountNullAggregate>> Make(
      std::shared_ptr<BoundTerm> term);

  std::string ToString() const override;
  Result<Literal> Evaluate(const StructLike& data) const override;
  Result<std::unique_ptr<Aggregator>> NewAggregator() const override;

 private:
  explicit CountNullAggregate(std::shared_ptr<BoundTerm> term);
};

/// \brief COUNT(*) aggregate.
class ICEBERG_EXPORT CountStarAggregate : public CountAggregate {
 public:
  static Result<std::shared_ptr<CountStarAggregate>> Make();

  std::string ToString() const override;
  Result<Literal> Evaluate(const StructLike& data) const override;
  Result<std::unique_ptr<Aggregator>> NewAggregator() const override;

 private:
  CountStarAggregate();
};

/// \brief Bound MAX/MIN aggregate.
class ICEBERG_EXPORT ValueAggregate : public BoundAggregate {
 public:
  ValueAggregate(Expression::Operation op, std::shared_ptr<BoundTerm> term);

  Kind kind() const override { return Kind::kValue; }

  std::string ToString() const override;
  Result<Literal> Evaluate(const StructLike& data) const override;
  Result<std::unique_ptr<Aggregator>> NewAggregator() const override;
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

  /// \brief Update aggregates with a row.
  virtual Status Update(const StructLike& row) = 0;

  /// \brief Final aggregated value.
  virtual Result<std::vector<Literal>> Results() const = 0;

  /// \brief Convenience accessor when only one aggregate is evaluated.
  virtual Result<Literal> ResultLiteral() const = 0;
};

}  // namespace iceberg
