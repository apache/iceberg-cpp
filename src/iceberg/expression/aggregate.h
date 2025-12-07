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
#include <span>
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

  std::string ToString() const override;

 protected:
  Aggregate(Expression::Operation op, std::shared_ptr<T> term)
      : operation_(op), term_(std::move(term)) {}

  static constexpr bool IsSupportedOp(Expression::Operation op) {
    return op == Expression::Operation::kCount ||
           op == Expression::Operation::kCountNull ||
           op == Expression::Operation::kCountStar || op == Expression::Operation::kMax ||
           op == Expression::Operation::kMin;
  }

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

 private:
  UnboundAggregateImpl(Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term)
      : BASE(op, std::move(term)) {
    ICEBERG_DCHECK(BASE::IsSupportedOp(op), "Unexpected aggregate operation");
    ICEBERG_DCHECK(op == Expression::Operation::kCountStar || BASE::term() != nullptr,
                   "Aggregate term cannot be null except for COUNT(*)");
  }
};

/// \brief Base class for bound aggregates.
class ICEBERG_EXPORT BoundAggregate : public Aggregate<BoundTerm>, public Bound {
 public:
  using Aggregate<BoundTerm>::op;
  using Aggregate<BoundTerm>::term;

  /// \brief Base class for aggregators.
  class Aggregator {
   public:
    virtual ~Aggregator() = default;

    virtual Status Update(const StructLike& data) = 0;

    virtual Status Update(const DataFile& file) = 0;

    /// \brief Whether the aggregator is still valid.
    virtual bool IsValid() const = 0;

    /// \brief Get the result of the aggregation.
    /// \return The result of the aggregation.
    /// \note It is an undefined behavior to call this method if any previous Update call
    /// has returned an error or if IsValid() returns false.
    virtual Literal GetResult() const = 0;
  };

  std::shared_ptr<BoundReference> reference() override {
    ICEBERG_DCHECK(term() != nullptr || op() == Expression::Operation::kCountStar,
                   "Bound aggregate term should not be null except for COUNT(*)");
    return term() ? term()->reference() : nullptr;
  }

  Result<Literal> Evaluate(const StructLike& data) const override = 0;

  virtual Result<Literal> Evaluate(const DataFile& file) const = 0;

  /// \brief Whether metrics in the data file are sufficient to evaluate.
  virtual bool HasValue(const DataFile& file) const = 0;

  bool is_bound_aggregate() const override { return true; }

  /// \brief Create a new aggregator for this aggregate.
  /// \note The returned aggregator cannot outlive the BoundAggregate that creates it.
  virtual std::unique_ptr<Aggregator> NewAggregator() const = 0;

 protected:
  BoundAggregate(Expression::Operation op, std::shared_ptr<BoundTerm> term)
      : Aggregate<BoundTerm>(op, std::move(term)) {}
};

/// \brief Base class for COUNT aggregates.
class ICEBERG_EXPORT CountAggregate : public BoundAggregate {
 public:
  Result<Literal> Evaluate(const StructLike& data) const override;
  Result<Literal> Evaluate(const DataFile& file) const override;

  std::unique_ptr<Aggregator> NewAggregator() const override;

  /// \brief Count for a single row. Subclasses implement this.
  virtual Result<int64_t> CountFor(const StructLike& data) const = 0;
  /// \brief Count using metrics from a data file.
  virtual Result<int64_t> CountFor(const DataFile& file) const = 0;

 protected:
  CountAggregate(Expression::Operation op, std::shared_ptr<BoundTerm> term)
      : BoundAggregate(op, std::move(term)) {}
};

/// \brief COUNT(term) aggregate.
class ICEBERG_EXPORT CountNonNullAggregate : public CountAggregate {
 public:
  static Result<std::unique_ptr<CountNonNullAggregate>> Make(
      std::shared_ptr<BoundTerm> term);

  Result<int64_t> CountFor(const StructLike& data) const override;
  Result<int64_t> CountFor(const DataFile& file) const override;
  bool HasValue(const DataFile& file) const override;

 private:
  explicit CountNonNullAggregate(std::shared_ptr<BoundTerm> term);
};

/// \brief COUNT_NULL(term) aggregate.
class ICEBERG_EXPORT CountNullAggregate : public CountAggregate {
 public:
  static Result<std::unique_ptr<CountNullAggregate>> Make(
      std::shared_ptr<BoundTerm> term);

  Result<int64_t> CountFor(const StructLike& data) const override;
  Result<int64_t> CountFor(const DataFile& file) const override;
  bool HasValue(const DataFile& file) const override;

 private:
  explicit CountNullAggregate(std::shared_ptr<BoundTerm> term);
};

/// \brief COUNT(*) aggregate.
class ICEBERG_EXPORT CountStarAggregate : public CountAggregate {
 public:
  static Result<std::unique_ptr<CountStarAggregate>> Make();

  Result<int64_t> CountFor(const StructLike& data) const override;
  Result<int64_t> CountFor(const DataFile& file) const override;
  bool HasValue(const DataFile& file) const override;

 private:
  CountStarAggregate();
};

/// \brief Bound MAX aggregate.
class ICEBERG_EXPORT MaxAggregate : public BoundAggregate {
 public:
  static Result<std::unique_ptr<MaxAggregate>> Make(std::shared_ptr<BoundTerm> term);

  Result<Literal> Evaluate(const StructLike& data) const override;
  Result<Literal> Evaluate(const DataFile& file) const override;
  bool HasValue(const DataFile& file) const override;

  std::unique_ptr<Aggregator> NewAggregator() const override;

 private:
  explicit MaxAggregate(std::shared_ptr<BoundTerm> term);
};

/// \brief Bound MIN aggregate.
class ICEBERG_EXPORT MinAggregate : public BoundAggregate {
 public:
  static Result<std::unique_ptr<MinAggregate>> Make(std::shared_ptr<BoundTerm> term);

  Result<Literal> Evaluate(const StructLike& data) const override;
  Result<Literal> Evaluate(const DataFile& file) const override;
  bool HasValue(const DataFile& file) const override;

  std::unique_ptr<Aggregator> NewAggregator() const override;

 private:
  explicit MinAggregate(std::shared_ptr<BoundTerm> term);
};

/// \brief Evaluates bound aggregates over StructLike data.
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
  static Result<std::unique_ptr<AggregateEvaluator>> Make(
      std::vector<std::shared_ptr<BoundAggregate>> aggregates);

  /// \brief Update aggregates with a row.
  virtual Status Update(const StructLike& data) = 0;

  /// \brief Update aggregates using data file metrics.
  virtual Status Update(const DataFile& file) = 0;

  /// \brief Final aggregated value.
  virtual Result<std::span<const Literal>> GetResults() const = 0;

  /// \brief Convenience accessor when only one aggregate is evaluated.
  virtual Result<Literal> GetResult() const = 0;

  /// \brief Whether all aggregators are still valid (metrics present).
  virtual bool AllAggregatorsValid() const = 0;
};

}  // namespace iceberg
