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

#include "iceberg/exception.h"
#include "iceberg/expression/binder.h"
#include "iceberg/expression/expression.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

std::string OperationToPrefix(Expression::Operation op) {
  switch (op) {
    case Expression::Operation::kMax:
      return "max";
    case Expression::Operation::kMin:
      return "min";
    case Expression::Operation::kCount:
    case Expression::Operation::kCountStar:
      return "count";
    default:
      break;
  }
  return "aggregate";
}

Result<std::shared_ptr<PrimitiveType>> GetPrimitiveType(const BoundTerm& term) {
  auto primitive = std::dynamic_pointer_cast<PrimitiveType>(term.type());
  if (primitive == nullptr) {
    return InvalidExpression("Aggregate requires primitive type, got {}",
                             term.type()->ToString());
  }
  return primitive;
}

}  // namespace

CountAggregate::CountAggregate(Expression::Operation op, Mode mode,
                               std::shared_ptr<UnboundTerm<BoundReference>> term,
                               std::shared_ptr<NamedReference> reference)
    : UnboundAggregate(op),
      mode_(mode),
      term_(std::move(term)),
      reference_(std::move(reference)) {}

Result<std::unique_ptr<CountAggregate>> CountAggregate::Count(
    std::shared_ptr<UnboundTerm<BoundReference>> term) {
  auto ref = term->reference();
  return std::unique_ptr<CountAggregate>(new CountAggregate(
      Expression::Operation::kCount, Mode::kNonNull, std::move(term), std::move(ref)));
}

Result<std::unique_ptr<CountAggregate>> CountAggregate::CountNull(
    std::shared_ptr<UnboundTerm<BoundReference>> term) {
  auto ref = term->reference();
  return std::unique_ptr<CountAggregate>(new CountAggregate(
      Expression::Operation::kCount, Mode::kNull, std::move(term), std::move(ref)));
}

std::unique_ptr<CountAggregate> CountAggregate::CountStar() {
  return std::unique_ptr<CountAggregate>(new CountAggregate(
      Expression::Operation::kCountStar, Mode::kStar, nullptr, nullptr));
}

std::string CountAggregate::ToString() const {
  if (mode_ == Mode::kStar) {
    return "count(*)";
  }
  ICEBERG_DCHECK(reference_ != nullptr, "Count aggregate should have reference");
  switch (mode_) {
    case Mode::kNull:
      return std::format("count_null({})", reference_->name());
    case Mode::kNonNull:
      return std::format("count({})", reference_->name());
    case Mode::kStar:
      break;
  }
  std::unreachable();
}

Result<std::shared_ptr<Expression>> CountAggregate::Bind(const Schema& schema,
                                                         bool case_sensitive) const {
  std::shared_ptr<BoundTerm> bound_term;
  if (term_ != nullptr) {
    ICEBERG_ASSIGN_OR_THROW(auto bound, term_->Bind(schema, case_sensitive));
    bound_term = std::move(bound);
  }
  auto aggregate =
      std::make_shared<BoundCountAggregate>(op(), mode_, std::move(bound_term));
  return aggregate;
}

BoundCountAggregate::BoundCountAggregate(Expression::Operation op,
                                         CountAggregate::Mode mode,
                                         std::shared_ptr<BoundTerm> term)
    : BoundAggregate(op, std::move(term)), mode_(mode) {}

std::string BoundCountAggregate::ToString() const {
  if (mode_ == CountAggregate::Mode::kStar) {
    return "count(*)";
  }
  ICEBERG_DCHECK(term() != nullptr, "Bound count aggregate should have term");
  switch (mode_) {
    case CountAggregate::Mode::kNull:
      return std::format("count_null({})", term()->reference()->name());
    case CountAggregate::Mode::kNonNull:
      return std::format("count({})", term()->reference()->name());
    case CountAggregate::Mode::kStar:
      break;
  }
  std::unreachable();
}

ValueAggregate::ValueAggregate(Expression::Operation op,
                               std::shared_ptr<UnboundTerm<BoundReference>> term,
                               std::shared_ptr<NamedReference> reference)
    : UnboundAggregate(op), term_(std::move(term)), reference_(std::move(reference)) {}

Result<std::unique_ptr<ValueAggregate>> ValueAggregate::Max(
    std::shared_ptr<UnboundTerm<BoundReference>> term) {
  auto ref = term->reference();
  return std::unique_ptr<ValueAggregate>(
      new ValueAggregate(Expression::Operation::kMax, std::move(term), std::move(ref)));
}

Result<std::unique_ptr<ValueAggregate>> ValueAggregate::Min(
    std::shared_ptr<UnboundTerm<BoundReference>> term) {
  auto ref = term->reference();
  return std::unique_ptr<ValueAggregate>(
      new ValueAggregate(Expression::Operation::kMin, std::move(term), std::move(ref)));
}

std::string ValueAggregate::ToString() const {
  return std::format("{}({})", OperationToPrefix(op()), reference_->name());
}

Result<std::shared_ptr<Expression>> ValueAggregate::Bind(const Schema& schema,
                                                         bool case_sensitive) const {
  ICEBERG_ASSIGN_OR_THROW(auto bound, term_->Bind(schema, case_sensitive));
  auto aggregate = std::make_shared<BoundValueAggregate>(
      op(), std::shared_ptr<BoundTerm>(std::move(bound)));
  return aggregate;
}

BoundValueAggregate::BoundValueAggregate(Expression::Operation op,
                                         std::shared_ptr<BoundTerm> term)
    : BoundAggregate(op, std::move(term)) {}

std::string BoundValueAggregate::ToString() const {
  ICEBERG_DCHECK(term() != nullptr, "Bound value aggregate should have term");
  return std::format("{}({})", OperationToPrefix(op()), term()->reference()->name());
}

namespace {

class CountEvaluator : public AggregateEvaluator {
 public:
  CountEvaluator(CountAggregate::Mode mode, std::shared_ptr<BoundTerm> term)
      : mode_(mode), term_(std::move(term)) {}

  Status Add(const StructLike& row) override {
    switch (mode_) {
      case CountAggregate::Mode::kStar:
        ++count_;
        return {};
      case CountAggregate::Mode::kNonNull: {
        ICEBERG_ASSIGN_OR_RAISE(auto literal, term_->Evaluate(row));
        if (!literal.IsNull()) {
          ++count_;
        }
        return {};
      }
      case CountAggregate::Mode::kNull: {
        ICEBERG_ASSIGN_OR_RAISE(auto literal, term_->Evaluate(row));
        if (literal.IsNull()) {
          ++count_;
        }
        return {};
      }
    }
    std::unreachable();
  }

  Result<Literal> ResultLiteral() const override { return Literal::Long(count_); }

 private:
  CountAggregate::Mode mode_;
  std::shared_ptr<BoundTerm> term_;
  int64_t count_ = 0;
};

class ValueAggregateEvaluator : public AggregateEvaluator {
 public:
  ValueAggregateEvaluator(Expression::Operation op, std::shared_ptr<BoundTerm> term,
                          std::shared_ptr<PrimitiveType> type)
      : op_(op), term_(std::move(term)), type_(std::move(type)) {}

  Status Add(const StructLike& row) override {
    ICEBERG_ASSIGN_OR_RAISE(auto literal, term_->Evaluate(row));
    if (literal.IsNull()) {
      return {};
    }

    if (!current_) {
      current_ = std::move(literal);
      return {};
    }

    auto ordering = literal <=> *current_;
    if (ordering == std::partial_ordering::unordered) {
      return InvalidExpression("Cannot compare literals of type {}",
                               literal.type()->ToString());
    }

    if (op_ == Expression::Operation::kMax) {
      if (ordering == std::partial_ordering::greater) {
        current_ = std::move(literal);
      }
    } else {
      if (ordering == std::partial_ordering::less) {
        current_ = std::move(literal);
      }
    }
    return {};
  }

  Result<Literal> ResultLiteral() const override {
    if (!current_) {
      return Literal::Null(type_);
    }
    return *current_;
  }

 private:
  Expression::Operation op_;
  std::shared_ptr<BoundTerm> term_;
  std::shared_ptr<PrimitiveType> type_;
  std::optional<Literal> current_;
};

}  // namespace

Result<std::unique_ptr<AggregateEvaluator>> AggregateEvaluator::Make(
    std::shared_ptr<BoundAggregate> aggregate) {
  ICEBERG_DCHECK(aggregate != nullptr, "Aggregate cannot be null");

  if (auto count = std::dynamic_pointer_cast<BoundCountAggregate>(aggregate)) {
    if (count->mode() != CountAggregate::Mode::kStar && !count->term()) {
      return InvalidExpression("Count aggregate requires a term");
    }
    return std::unique_ptr<AggregateEvaluator>(
        new CountEvaluator(count->mode(), count->term()));
  }

  if (auto value = std::dynamic_pointer_cast<BoundValueAggregate>(aggregate)) {
    ICEBERG_ASSIGN_OR_RAISE(auto type, GetPrimitiveType(*value->term()));
    return std::unique_ptr<AggregateEvaluator>(
        new ValueAggregateEvaluator(value->op(), value->term(), std::move(type)));
  }

  return NotSupported("Unsupported aggregate: {}", aggregate->ToString());
}

}  // namespace iceberg
