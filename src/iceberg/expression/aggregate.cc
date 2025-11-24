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

Result<std::shared_ptr<PrimitiveType>> GetPrimitiveType(const BoundTerm& term) {
  if (!term.type().is_primitive()) {
    return InvalidExpression("Aggregate requires primitive type, got {}",
                             term.type()->ToString());
  }
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
    return Status::OK();
  }

  Result<Literal> ResultLiteral() const override { return Literal::Long(count_); }

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
    return Status::OK();
  }

  Result<Literal> ResultLiteral() const override { return Literal::Long(count_); }

 private:
  std::shared_ptr<BoundTerm> term_;
  int64_t count_ = 0;
};

class CountStarAggregator : public BoundAggregate::Aggregator {
 public:
  Status Update(const StructLike& /*row*/) override {
    ++count_;
    return Status::OK();
  }

  Result<Literal> ResultLiteral() const override { return Literal::Long(count_); }

 private:
  int64_t count_ = 0;
};

class ValueAggregatorImpl : public BoundAggregate::Aggregator {
 public:
  ValueAggregatorImpl(bool is_max, std::shared_ptr<BoundTerm> term)
      : is_max_(is_max), term_(std::move(term)) {}

  Status Update(const StructLike& row) override {
    ICEBERG_ASSIGN_OR_RAISE(auto val_literal, term_->Evaluate(row));
    if (val_literal.IsNull()) {
      return Status::OK();
    }
    if (!current_) {
      current_ = std::move(val_literal);
      return Status::OK();
    }

    auto ordering = val_literal <=> *current_;
    if (ordering == std::partial_ordering::unordered) {
      return InvalidExpression("Cannot compare literals of type {}",
                               val_literal.type()->ToString());
    }

    if (is_max_) {
      if (ordering == std::partial_ordering::greater) {
        current_ = std::move(val_literal);
      }
    } else {
      if (ordering == std::partial_ordering::less) {
        current_ = std::move(val_literal);
      }
    }
    return Status::OK();
  }

  Result<Literal> ResultLiteral() const override {
    if (current_) {
      return *current_;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto type, GetPrimitiveType(*term_));
    return Literal::Null(type);
  }

 private:
  bool is_max_;
  std::shared_ptr<BoundTerm> term_;
  std::optional<Literal> current_;
};

}  // namespace

// -------------------- Bound aggregates --------------------

CountNonNullAggregate::CountNonNullAggregate(std::shared_ptr<BoundTerm> term)
    : CountAggregate(Expression::Operation::kCount, std::move(term)) {}

Result<std::shared_ptr<CountNonNullAggregate>> CountNonNullAggregate::Make(
    std::shared_ptr<BoundTerm> term) {
  if (!term) {
    return InvalidExpression("Bound count aggregate requires non-null term");
  }
  return std::shared_ptr<CountNonNullAggregate>(
      new CountNonNullAggregate(std::move(term)));
}

std::string CountNonNullAggregate::ToString() const {
  ICEBERG_DCHECK(term() != nullptr, "Bound count aggregate should have term");
  return std::format("count({})", term()->reference()->name());
}

Result<Literal> CountNonNullAggregate::Evaluate(const StructLike& data) const {
  ICEBERG_ASSIGN_OR_RAISE(auto literal, term()->Evaluate(data));
  return Literal::Long(literal.IsNull() ? 0 : 1);
}

Result<std::unique_ptr<BoundAggregate::Aggregator>> CountNonNullAggregate::NewAggregator()
    const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new CountNonNullAggregator(term()));
}

CountNullAggregate::CountNullAggregate(std::shared_ptr<BoundTerm> term)
    : CountAggregate(Expression::Operation::kCountNull, std::move(term)) {}

Result<std::shared_ptr<CountNullAggregate>> CountNullAggregate::Make(
    std::shared_ptr<BoundTerm> term) {
  if (!term) {
    return InvalidExpression("Bound count aggregate requires non-null term");
  }
  return std::shared_ptr<CountNullAggregate>(new CountNullAggregate(std::move(term)));
}

std::string CountNullAggregate::ToString() const {
  ICEBERG_DCHECK(term() != nullptr, "Bound count aggregate should have term");
  return std::format("count_null({})", term()->reference()->name());
}

Result<Literal> CountNullAggregate::Evaluate(const StructLike& data) const {
  ICEBERG_ASSIGN_OR_RAISE(auto literal, term()->Evaluate(data));
  return Literal::Long(literal.IsNull() ? 1 : 0);
}

Result<std::unique_ptr<BoundAggregate::Aggregator>> CountNullAggregate::NewAggregator()
    const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new CountNullAggregator(term()));
}

CountStarAggregate::CountStarAggregate()
    : CountAggregate(Expression::Operation::kCountStar, nullptr) {}

Result<std::shared_ptr<CountStarAggregate>> CountStarAggregate::Make() {
  return std::shared_ptr<CountStarAggregate>(new CountStarAggregate());
}

std::string CountStarAggregate::ToString() const { return "count(*)"; }

Result<Literal> CountStarAggregate::Evaluate(const StructLike& data) const {
  return Literal::Long(1);
}

Result<std::unique_ptr<BoundAggregate::Aggregator>> CountStarAggregate::NewAggregator()
    const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new CountStarAggregator());
}

ValueAggregate::ValueAggregate(Expression::Operation op, std::shared_ptr<BoundTerm> term)
    : BoundAggregate(op, std::move(term)) {}

std::string ValueAggregate::ToString() const {
  ICEBERG_DCHECK(term() != nullptr, "Bound value aggregate should have term");
  auto prefix = op() == Expression::Operation::kMax ? "max" : "min";
  return std::format("{}({})", prefix, term()->reference()->name());
}

Result<Literal> ValueAggregate::Evaluate(const StructLike& data) const {
  ICEBERG_ASSIGN_OR_RAISE(auto literal, term()->Evaluate(data));
  return literal;
}

Result<std::unique_ptr<BoundAggregate::Aggregator>> ValueAggregate::NewAggregator()
    const {
  bool is_max = op() == Expression::Operation::kMax;
  return std::unique_ptr<BoundAggregate::Aggregator>(
      new ValueAggregatorImpl(is_max, term()));
}

// -------------------- Unbound binding --------------------

template <typename B>
Result<std::shared_ptr<Expression>> UnboundAggregateImpl<B>::Bind(
    const Schema& schema, bool case_sensitive) const {
  ICEBERG_DCHECK(UnboundAggregateImpl<B>::IsSupportedOp(this->op()),
                 "Unexpected aggregate operation");

  std::shared_ptr<B> bound_term;
  if (this->term()) {
    ICEBERG_ASSIGN_OR_THROW(bound_term, this->term()->Bind(schema, case_sensitive));
  }

  switch (this->op()) {
    case Expression::Operation::kCountStar: {
      ICEBERG_ASSIGN_OR_THROW(auto aggregate, CountStarAggregate::Make());
      return aggregate;
    }
    case Expression::Operation::kCount: {
      if (!bound_term) {
        return InvalidExpression("Aggregate requires a term");
      }
      ICEBERG_ASSIGN_OR_THROW(auto aggregate,
                              CountNonNullAggregate::Make(std::move(bound_term)));
      return aggregate;
    }
    case Expression::Operation::kCountNull: {
      if (!bound_term) {
        return InvalidExpression("Aggregate requires a term");
      }
      ICEBERG_ASSIGN_OR_THROW(auto aggregate,
                              CountNullAggregate::Make(std::move(bound_term)));
      return aggregate;
    }
    case Expression::Operation::kMax:
    case Expression::Operation::kMin: {
      if (!bound_term) {
        return InvalidExpression("Aggregate requires a term");
      }
      auto aggregate =
          std::make_shared<ValueAggregate>(this->op(), std::move(bound_term));
      return aggregate;
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
      ICEBERG_RETURN_NOT_OK(aggregator->Update(row));
    }
    return {};
  }

  Result<std::vector<Literal>> Results() const override {
    std::vector<Literal> out;
    out.reserve(aggregates_.size());
    for (const auto& aggregator : aggregators_) {
      ICEBERG_ASSIGN_OR_RAISE(auto literal, aggregator->ResultLiteral());
      out.emplace_back(std::move(literal));
    }
    return out;
  }

  Result<Literal> ResultLiteral() const override {
    if (aggregates_.size() != 1) {
      return InvalidArgument(
          "ResultLiteral() is only valid when evaluating a single aggregate");
    }

    ICEBERG_ASSIGN_OR_RAISE(auto all, Results());
    return all.front();
  }

 private:
  std::vector<std::shared_ptr<BoundAggregate>> aggregates_;
  std::vector<std::unique_ptr<BoundAggregate::Aggregator>> aggregators_;
};

}  // namespace

Result<std::unique_ptr<AggregateEvaluator>> AggregateEvaluator::Make(
    std::shared_ptr<BoundAggregate> aggregate) {
  std::vector<std::shared_ptr<BoundAggregate>> aggs;
  aggs.push_back(std::move(aggregate));
  return MakeList(std::move(aggs));
}

Result<std::unique_ptr<AggregateEvaluator>> AggregateEvaluator::MakeList(
    std::vector<std::shared_ptr<BoundAggregate>> aggregates) {
  if (aggregates.empty()) {
    return InvalidArgument("AggregateEvaluator requires at least one aggregate");
  }
  std::vector<std::unique_ptr<BoundAggregate::Aggregator>> aggregators;
  aggregators.reserve(aggregates.size());
  for (const auto& agg : aggregates) {
    ICEBERG_ASSIGN_OR_RAISE(auto aggregator, agg->NewAggregator());
    aggregators.push_back(std::move(aggregator));
  }

  return std::unique_ptr<AggregateEvaluator>(
      new AggregateEvaluatorImpl(std::move(aggregates), std::move(aggregators)));
}

}  // namespace iceberg
