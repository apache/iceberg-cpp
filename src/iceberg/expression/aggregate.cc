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

#include "iceberg/exception.h"
#include "iceberg/expression/binder.h"
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

class CountNonNullAggregator : public BoundAggregate::Aggregator {
 public:
  explicit CountNonNullAggregator(std::shared_ptr<BoundTerm> term)
      : term_(std::move(term)) {}

  Status Update(const StructLike& row) override {
    ICEBERG_ASSIGN_OR_RAISE(auto literal, term_->Evaluate(row));
    if (!literal.IsNull()) {
      ++count_;
    }
    return {};
  }

  Literal GetResult() const override { return Literal::Long(count_); }

 private:
  std::shared_ptr<BoundTerm> term_;
  int64_t count_ = 0;
};

class CountNullAggregator : public BoundAggregate::Aggregator {
 public:
  explicit CountNullAggregator(std::shared_ptr<BoundTerm> term)
      : term_(std::move(term)) {}

  Status Update(const StructLike& row) override {
    ICEBERG_ASSIGN_OR_RAISE(auto literal, term_->Evaluate(row));
    if (literal.IsNull()) {
      ++count_;
    }
    return {};
  }

  Literal GetResult() const override { return Literal::Long(count_); }

 private:
  std::shared_ptr<BoundTerm> term_;
  int64_t count_ = 0;
};

class CountStarAggregator : public BoundAggregate::Aggregator {
 public:
  Status Update(const StructLike& /*row*/) override {
    ++count_;
    return {};
  }

  Literal GetResult() const override { return Literal::Long(count_); }

 private:
  int64_t count_ = 0;
};

class MaxAggregator : public BoundAggregate::Aggregator {
 public:
  explicit MaxAggregator(std::shared_ptr<BoundTerm> term) : term_(std::move(term)) {}

  Status Update(const StructLike& row) override {
    ICEBERG_ASSIGN_OR_RAISE(auto val_literal, term_->Evaluate(row));
    if (val_literal.IsNull()) {
      return {};
    }
    if (!current_) {
      current_ = std::move(val_literal);
      return {};
    }

    auto ordering = val_literal <=> *current_;
    if (ordering == std::partial_ordering::unordered) {
      return InvalidExpression("Cannot compare literals of type {}",
                               val_literal.type()->ToString());
    }

    if (ordering == std::partial_ordering::greater) {
      current_ = std::move(val_literal);
    }
    return {};
  }

  Literal GetResult() const override {
    return current_.value_or(Literal::Null(GetPrimitiveType(*term_)));
  }

 private:
  std::shared_ptr<BoundTerm> term_;
  std::optional<Literal> current_;
};

class MinAggregator : public BoundAggregate::Aggregator {
 public:
  explicit MinAggregator(std::shared_ptr<BoundTerm> term) : term_(std::move(term)) {}

  Status Update(const StructLike& row) override {
    ICEBERG_ASSIGN_OR_RAISE(auto val_literal, term_->Evaluate(row));
    if (val_literal.IsNull()) {
      return {};
    }
    if (!current_) {
      current_ = std::move(val_literal);
      return {};
    }

    auto ordering = val_literal <=> *current_;
    if (ordering == std::partial_ordering::unordered) {
      return InvalidExpression("Cannot compare literals of type {}",
                               val_literal.type()->ToString());
    }

    if (ordering == std::partial_ordering::less) {
      current_ = std::move(val_literal);
    }
    return {};
  }

  Literal GetResult() const override {
    return current_.value_or(Literal::Null(GetPrimitiveType(*term_)));
  }

 private:
  std::shared_ptr<BoundTerm> term_;
  std::optional<Literal> current_;
};

}  // namespace

// -------------------- Bound aggregates --------------------

std::string BoundAggregate::ToString() const {
  std::string term_name;
  if (op() != Expression::Operation::kCountStar) {
    ICEBERG_DCHECK(term() != nullptr, "Bound aggregate should have term unless COUNT(*)");
    term_name = term()->reference()->name();
  }

  switch (op()) {
    case Expression::Operation::kCount:
      return std::format("count({})", term_name);
    case Expression::Operation::kCountNull:
      return std::format("count_null({})", term_name);
    case Expression::Operation::kCountStar:
      return "count(*)";
    case Expression::Operation::kMax:
      return std::format("max({})", term_name);
    case Expression::Operation::kMin:
      return std::format("min({})", term_name);
    default:
      return "Aggregate";
  }
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

Result<Literal> CountNonNullAggregate::Evaluate(const StructLike& data) const {
  ICEBERG_ASSIGN_OR_RAISE(auto literal, term()->Evaluate(data));
  return Literal::Long(literal.IsNull() ? 0 : 1);
}

std::unique_ptr<BoundAggregate::Aggregator> CountNonNullAggregate::NewAggregator() const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new CountNonNullAggregator(term()));
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

Result<Literal> CountNullAggregate::Evaluate(const StructLike& data) const {
  ICEBERG_ASSIGN_OR_RAISE(auto literal, term()->Evaluate(data));
  return Literal::Long(literal.IsNull() ? 1 : 0);
}

std::unique_ptr<BoundAggregate::Aggregator> CountNullAggregate::NewAggregator() const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new CountNullAggregator(term()));
}

CountStarAggregate::CountStarAggregate()
    : CountAggregate(Expression::Operation::kCountStar, nullptr) {}

Result<std::unique_ptr<CountStarAggregate>> CountStarAggregate::Make() {
  return std::unique_ptr<CountStarAggregate>(new CountStarAggregate());
}

Result<Literal> CountStarAggregate::Evaluate(const StructLike& data) const {
  return Literal::Long(1);
}

std::unique_ptr<BoundAggregate::Aggregator> CountStarAggregate::NewAggregator() const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new CountStarAggregator());
}

MaxAggregate::MaxAggregate(std::shared_ptr<BoundTerm> term)
    : BoundAggregate(Expression::Operation::kMax, std::move(term)) {}

std::shared_ptr<MaxAggregate> MaxAggregate::Make(std::shared_ptr<BoundTerm> term) {
  return std::shared_ptr<MaxAggregate>(new MaxAggregate(std::move(term)));
}

Result<Literal> MaxAggregate::Evaluate(const StructLike& data) const {
  ICEBERG_ASSIGN_OR_RAISE(auto literal, term()->Evaluate(data));
  return literal;
}

std::unique_ptr<BoundAggregate::Aggregator> MaxAggregate::NewAggregator() const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new MaxAggregator(term()));
}

MinAggregate::MinAggregate(std::shared_ptr<BoundTerm> term)
    : BoundAggregate(Expression::Operation::kMin, std::move(term)) {}

std::shared_ptr<MinAggregate> MinAggregate::Make(std::shared_ptr<BoundTerm> term) {
  return std::shared_ptr<MinAggregate>(new MinAggregate(std::move(term)));
}

Result<Literal> MinAggregate::Evaluate(const StructLike& data) const {
  ICEBERG_ASSIGN_OR_RAISE(auto literal, term()->Evaluate(data));
  return literal;
}

std::unique_ptr<BoundAggregate::Aggregator> MinAggregate::NewAggregator() const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new MinAggregator(term()));
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
      return CountStarAggregate::Make().transform([](auto aggregate) {
        return std::shared_ptr<CountStarAggregate>(std::move(aggregate));
      });
    case Expression::Operation::kCount:
      return CountNonNullAggregate::Make(std::move(bound_term))
          .transform([](auto aggregate) {
            return std::shared_ptr<CountNonNullAggregate>(std::move(aggregate));
          });
    case Expression::Operation::kCountNull:
      return CountNullAggregate::Make(std::move(bound_term))
          .transform([](auto aggregate) {
            return std::shared_ptr<CountNullAggregate>(std::move(aggregate));
          });
    case Expression::Operation::kMax:
    case Expression::Operation::kMin: {
      if (!bound_term) {
        return InvalidExpression("Aggregate requires a term");
      }
      if (!bound_term->type()->is_primitive()) {
        return InvalidExpression("Aggregate requires primitive type, got {}",
                                 bound_term->type()->ToString());
      }
      if (this->op() == Expression::Operation::kMax) {
        return MaxAggregate::Make(std::move(bound_term));
      }
      return MinAggregate::Make(std::move(bound_term));
    }
    default:
      return NotSupported("Unsupported aggregate operation");
  }
}

template <typename B>
Result<std::shared_ptr<UnboundAggregateImpl<B>>> UnboundAggregateImpl<B>::Make(
    Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term) {
  if (!IsSupportedOp(op)) {
    return NotSupported("Unsupported aggregate operation: {}", ::iceberg::ToString(op));
  }
  if (op != Expression::Operation::kCountStar && !term) {
    return InvalidExpression("Aggregate term cannot be null unless COUNT(*)");
  }

  return std::shared_ptr<UnboundAggregateImpl<B>>(
      new UnboundAggregateImpl<B>(op, std::move(term)));
}

template <typename B>
std::string UnboundAggregateImpl<B>::ToString() const {
  ICEBERG_DCHECK(UnboundAggregateImpl<B>::IsSupportedOp(this->op()),
                 "Unexpected aggregate operation");
  ICEBERG_DCHECK(
      this->op() == Expression::Operation::kCountStar || this->term() != nullptr,
      "Aggregate term should not be null unless COUNT(*)");

  auto term_str = this->term() ? this->term()->ToString() : std::string{};
  switch (this->op()) {
    case Expression::Operation::kCount:
      return std::format("count({})", term_str);
    case Expression::Operation::kCountNull:
      return std::format("count_if({} is null)", term_str);
    case Expression::Operation::kCountStar:
      return "count(*)";
    case Expression::Operation::kMax:
      return std::format("max({})", term_str);
    case Expression::Operation::kMin:
      return std::format("min({})", term_str);
    default:
      return "Aggregate";
  }
}

template class UnboundAggregateImpl<BoundReference>;

// -------------------- AggregateEvaluator --------------------

namespace {

class AggregateEvaluatorImpl : public AggregateEvaluator {
 public:
  AggregateEvaluatorImpl(
      std::vector<std::shared_ptr<BoundAggregate>> aggregates,
      std::vector<std::unique_ptr<BoundAggregate::Aggregator>> aggregators)
      : aggregates_(std::move(aggregates)), aggregators_(std::move(aggregators)) {}

  Status Update(const StructLike& row) override {
    for (auto& aggregator : aggregators_) {
      ICEBERG_RETURN_UNEXPECTED(aggregator->Update(row));
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
