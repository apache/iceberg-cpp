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

#include "iceberg/expression/aggregate.h"

#include <format>
#include <optional>
#include <vector>

#include "iceberg/expression/literal.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

std::shared_ptr<PrimitiveType> GetPrimitiveType(const BoundTerm& term) {
  ICEBERG_DCHECK(term.type()->is_primitive(), "Value aggregate term should be primitive");
  return internal::checked_pointer_cast<PrimitiveType>(term.type());
}

class CountAggregator : public BoundAggregate::Aggregator {
 public:
  explicit CountAggregator(const CountAggregate& aggregate) : aggregate_(aggregate) {}

  Status Update(const StructLike& row) override {
    ICEBERG_ASSIGN_OR_RAISE(auto count, aggregate_.CountFor(row));
    count_ += count;
    return {};
  }

  Literal GetResult() const override { return Literal::Long(count_); }

 private:
  const CountAggregate& aggregate_;
  int64_t count_ = 0;
};

class MaxAggregator : public BoundAggregate::Aggregator {
 public:
  explicit MaxAggregator(const MaxAggregate& aggregate)
      : aggregate_(aggregate),
        current_(Literal::Null(GetPrimitiveType(*aggregate_.term()))) {}

  Status Update(const StructLike& data) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, aggregate_.Evaluate(data));
    if (value.IsNull()) {
      return {};
    }
    if (current_.IsNull()) {
      current_ = std::move(value);
      return {};
    }

    if (auto ordering = value <=> current_;
        ordering == std::partial_ordering::unordered) {
      return InvalidArgument("Cannot compare literal {} with current value {}",
                             value.ToString(), current_.ToString());
    } else if (ordering == std::partial_ordering::greater) {
      current_ = std::move(value);
    }

    return {};
  }

  Literal GetResult() const override { return current_; }

 private:
  const MaxAggregate& aggregate_;
  Literal current_;
};

class MinAggregator : public BoundAggregate::Aggregator {
 public:
  explicit MinAggregator(const MinAggregate& aggregate)
      : aggregate_(aggregate),
        current_(Literal::Null(GetPrimitiveType(*aggregate_.term()))) {}

  Status Update(const StructLike& data) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, aggregate_.Evaluate(data));
    if (value.IsNull()) {
      return {};
    }
    if (current_.IsNull()) {
      current_ = std::move(value);
      return {};
    }

    if (auto ordering = value <=> current_;
        ordering == std::partial_ordering::unordered) {
      return InvalidArgument("Cannot compare literal {} with current value {}",
                             value.ToString(), current_.ToString());
    } else if (ordering == std::partial_ordering::less) {
      current_ = std::move(value);
    }
    return {};
  }

  Literal GetResult() const override { return current_; }

 private:
  const MinAggregate& aggregate_;
  Literal current_;
};

}  // namespace

template <TermType T>
std::string Aggregate<T>::ToString() const {
  ICEBERG_DCHECK(IsSupportedOp(op()), "Unexpected aggregate operation");
  ICEBERG_DCHECK(op() == Expression::Operation::kCountStar || term() != nullptr,
                 "Aggregate term should not be null except for COUNT(*)");

  switch (op()) {
    case Expression::Operation::kCount:
      return std::format("count({})", term()->ToString());
    case Expression::Operation::kCountNull:
      return std::format("count_if({} is null)", term()->ToString());
    case Expression::Operation::kCountStar:
      return "count(*)";
    case Expression::Operation::kMax:
      return std::format("max({})", term()->ToString());
    case Expression::Operation::kMin:
      return std::format("min({})", term()->ToString());
    default:
      return std::format("Invalid aggregate: {}", ::iceberg::ToString(op()));
  }
}

// -------------------- CountAggregate --------------------

Result<Literal> CountAggregate::Evaluate(const StructLike& data) const {
  return CountFor(data).transform([](int64_t count) { return Literal::Long(count); });
}

std::unique_ptr<BoundAggregate::Aggregator> CountAggregate::NewAggregator() const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new CountAggregator(*this));
}

CountNonNullAggregate::CountNonNullAggregate(std::shared_ptr<BoundTerm> term)
    : CountAggregate(Expression::Operation::kCount, std::move(term)) {}

Result<std::unique_ptr<CountNonNullAggregate>> CountNonNullAggregate::Make(
    std::shared_ptr<BoundTerm> term) {
  if (!term) {
    return InvalidExpression("Bound count aggregate requires non-null term");
  }
  return std::unique_ptr<CountNonNullAggregate>(
      new CountNonNullAggregate(std::move(term)));
}

Result<int64_t> CountNonNullAggregate::CountFor(const StructLike& data) const {
  return term()->Evaluate(data).transform(
      [](const auto& val) { return val.IsNull() ? 0 : 1; });
}

CountNullAggregate::CountNullAggregate(std::shared_ptr<BoundTerm> term)
    : CountAggregate(Expression::Operation::kCountNull, std::move(term)) {}

Result<std::unique_ptr<CountNullAggregate>> CountNullAggregate::Make(
    std::shared_ptr<BoundTerm> term) {
  if (!term) {
    return InvalidExpression("Bound count aggregate requires non-null term");
  }
  return std::unique_ptr<CountNullAggregate>(new CountNullAggregate(std::move(term)));
}

Result<int64_t> CountNullAggregate::CountFor(const StructLike& data) const {
  return term()->Evaluate(data).transform(
      [](const auto& val) { return val.IsNull() ? 1 : 0; });
}

CountStarAggregate::CountStarAggregate()
    : CountAggregate(Expression::Operation::kCountStar, nullptr) {}

Result<std::unique_ptr<CountStarAggregate>> CountStarAggregate::Make() {
  return std::unique_ptr<CountStarAggregate>(new CountStarAggregate());
}

Result<int64_t> CountStarAggregate::CountFor(const StructLike& /*data*/) const {
  return 1;
}

MaxAggregate::MaxAggregate(std::shared_ptr<BoundTerm> term)
    : BoundAggregate(Expression::Operation::kMax, std::move(term)) {}

std::shared_ptr<MaxAggregate> MaxAggregate::Make(std::shared_ptr<BoundTerm> term) {
  return std::shared_ptr<MaxAggregate>(new MaxAggregate(std::move(term)));
}

Result<Literal> MaxAggregate::Evaluate(const StructLike& data) const {
  return term()->Evaluate(data);
}

std::unique_ptr<BoundAggregate::Aggregator> MaxAggregate::NewAggregator() const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new MaxAggregator(*this));
}

MinAggregate::MinAggregate(std::shared_ptr<BoundTerm> term)
    : BoundAggregate(Expression::Operation::kMin, std::move(term)) {}

std::shared_ptr<MinAggregate> MinAggregate::Make(std::shared_ptr<BoundTerm> term) {
  return std::shared_ptr<MinAggregate>(new MinAggregate(std::move(term)));
}

Result<Literal> MinAggregate::Evaluate(const StructLike& data) const {
  return term()->Evaluate(data);
}

std::unique_ptr<BoundAggregate::Aggregator> MinAggregate::NewAggregator() const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new MinAggregator(*this));
}

// -------------------- Unbound binding --------------------

template <typename B>
Result<std::shared_ptr<Expression>> UnboundAggregateImpl<B>::Bind(
    const Schema& schema, bool case_sensitive) const {
  ICEBERG_DCHECK(UnboundAggregateImpl<B>::IsSupportedOp(this->op()),
                 "Unexpected aggregate operation");

  std::shared_ptr<B> bound_term;
  if (this->term()) {
    ICEBERG_ASSIGN_OR_RAISE(bound_term, this->term()->Bind(schema, case_sensitive));
  }

  switch (this->op()) {
    case Expression::Operation::kCountStar:
      return CountStarAggregate::Make();
    case Expression::Operation::kCount:
      return CountNonNullAggregate::Make(std::move(bound_term));
    case Expression::Operation::kCountNull:
      return CountNullAggregate::Make(std::move(bound_term));
    case Expression::Operation::kMax:
      return MaxAggregate::Make(std::move(bound_term));
    case Expression::Operation::kMin:
      return MinAggregate::Make(std::move(bound_term));
    default:
      return NotSupported("Unsupported aggregate operation: {}",
                          ::iceberg::ToString(this->op()));
  }
}

template <typename B>
Result<std::shared_ptr<UnboundAggregateImpl<B>>> UnboundAggregateImpl<B>::Make(
    Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term) {
  if (!Aggregate<UnboundTerm<B>>::IsSupportedOp(op)) {
    return NotSupported("Unsupported aggregate operation: {}", ::iceberg::ToString(op));
  }
  if (op != Expression::Operation::kCountStar && !term) {
    return InvalidExpression("Aggregate term cannot be null unless COUNT(*)");
  }

  return std::shared_ptr<UnboundAggregateImpl<B>>(
      new UnboundAggregateImpl<B>(op, std::move(term)));
}

template class Aggregate<UnboundTerm<BoundReference>>;
template class Aggregate<BoundTerm>;
template class UnboundAggregateImpl<BoundReference>;

// -------------------- AggregateEvaluator --------------------

namespace {

class AggregateEvaluatorImpl : public AggregateEvaluator {
 public:
  AggregateEvaluatorImpl(
      std::vector<std::shared_ptr<BoundAggregate>> aggregates,
      std::vector<std::unique_ptr<BoundAggregate::Aggregator>> aggregators)
      : aggregates_(std::move(aggregates)), aggregators_(std::move(aggregators)) {}

  Status Update(const StructLike& data) override {
    for (auto& aggregator : aggregators_) {
      ICEBERG_RETURN_UNEXPECTED(aggregator->Update(data));
    }
    return {};
  }

  Result<std::span<const Literal>> GetResults() const override {
    results_.clear();
    results_.reserve(aggregates_.size());
    for (const auto& aggregator : aggregators_) {
      results_.emplace_back(aggregator->GetResult());
    }
    return std::span<const Literal>(results_);
  }

  Result<Literal> GetResult() const override {
    if (aggregates_.size() != 1) {
      return InvalidArgument(
          "GetResult() is only valid when evaluating a single aggregate");
    }

    ICEBERG_ASSIGN_OR_RAISE(auto all, GetResults());
    return all.front();
  }

 private:
  std::vector<std::shared_ptr<BoundAggregate>> aggregates_;
  std::vector<std::unique_ptr<BoundAggregate::Aggregator>> aggregators_;
  mutable std::vector<Literal> results_;
};

}  // namespace

Result<std::unique_ptr<AggregateEvaluator>> AggregateEvaluator::Make(
    std::shared_ptr<BoundAggregate> aggregate) {
  std::vector<std::shared_ptr<BoundAggregate>> aggs;
  aggs.push_back(std::move(aggregate));
  return Make(std::move(aggs));
}

Result<std::unique_ptr<AggregateEvaluator>> AggregateEvaluator::Make(
    std::vector<std::shared_ptr<BoundAggregate>> aggregates) {
  if (aggregates.empty()) {
    return InvalidArgument("AggregateEvaluator requires at least one aggregate");
  }
  std::vector<std::unique_ptr<BoundAggregate::Aggregator>> aggregators;
  aggregators.reserve(aggregates.size());
  for (const auto& agg : aggregates) {
    aggregators.push_back(agg->NewAggregator());
  }

  return std::unique_ptr<AggregateEvaluator>(
      new AggregateEvaluatorImpl(std::move(aggregates), std::move(aggregators)));
}

}  // namespace iceberg
