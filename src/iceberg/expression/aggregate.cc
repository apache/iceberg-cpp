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

}  // namespace

// -------------------- Bound aggregates --------------------

BoundCountAggregate::BoundCountAggregate(Expression::Operation op, Mode mode,
                                         std::shared_ptr<BoundTerm> term)
    : BoundAggregate(op, std::move(term)), mode_(mode) {}

std::string BoundCountAggregate::ToString() const {
  if (mode_ == Mode::kStar) {
    return "count(*)";
  }
  ICEBERG_DCHECK(term() != nullptr, "Bound count aggregate should have term");
  switch (mode_) {
    case Mode::kNull:
      return std::format("count_null({})", term()->reference()->name());
    case Mode::kNonNull:
      return std::format("count({})", term()->reference()->name());
    case Mode::kStar:
      break;
  }
  std::unreachable();
}

Result<Literal> BoundCountAggregate::Evaluate(const StructLike& data) const {
  switch (mode_) {
    case Mode::kStar:
      return Literal::Long(1);
    case Mode::kNonNull: {
      ICEBERG_ASSIGN_OR_RAISE(auto literal, term()->Evaluate(data));
      return Literal::Long(literal.IsNull() ? 0 : 1);
    }
    case Mode::kNull: {
      ICEBERG_ASSIGN_OR_RAISE(auto literal, term()->Evaluate(data));
      return Literal::Long(literal.IsNull() ? 1 : 0);
    }
  }
  std::unreachable();
}

BoundValueAggregate::BoundValueAggregate(Expression::Operation op,
                                         std::shared_ptr<BoundTerm> term)
    : BoundAggregate(op, std::move(term)) {}

std::string BoundValueAggregate::ToString() const {
  ICEBERG_DCHECK(term() != nullptr, "Bound value aggregate should have term");
  auto prefix = op() == Expression::Operation::kMax ? "max" : "min";
  return std::format("{}({})", prefix, term()->reference()->name());
}

Result<Literal> BoundValueAggregate::Evaluate(const StructLike& data) const {
  ICEBERG_ASSIGN_OR_RAISE(auto literal, term()->Evaluate(data));
  return literal;
}

// -------------------- Unbound binding --------------------

template <typename B>
Result<std::shared_ptr<Expression>> UnboundAggregateImpl<B>::Bind(
    const Schema& schema, bool case_sensitive) const {
  std::shared_ptr<B> bound_term;
  if (this->term()) {
    ICEBERG_ASSIGN_OR_THROW(bound_term, this->term()->Bind(schema, case_sensitive));
  }

  switch (count_mode_) {
    case CountMode::kStar:
    case CountMode::kNull:
    case CountMode::kNonNull: {
      auto op = this->op() == Expression::Operation::kCountStar
                    ? Expression::Operation::kCountStar
                    : Expression::Operation::kCount;
      auto mode =
          count_mode_ == CountMode::kNull
              ? BoundCountAggregate::Mode::kNull
              : (count_mode_ == CountMode::kStar ? BoundCountAggregate::Mode::kStar
                                                 : BoundCountAggregate::Mode::kNonNull);
      auto aggregate =
          std::make_shared<BoundCountAggregate>(op, mode, std::move(bound_term));
      return aggregate;
    }
    case CountMode::kNone: {
      if (this->op() != Expression::Operation::kMax &&
          this->op() != Expression::Operation::kMin) {
        return NotSupported("Unsupported aggregate operation");
      }
      if (!bound_term) {
        return InvalidExpression("Aggregate requires a term");
      }
      auto aggregate =
          std::make_shared<BoundValueAggregate>(this->op(), std::move(bound_term));
      return aggregate;
    }
  }
  std::unreachable();
}

template class UnboundAggregateImpl<BoundReference>;

// -------------------- AggregateEvaluator --------------------

namespace {

class AggregateEvaluatorImpl : public AggregateEvaluator {
 public:
  explicit AggregateEvaluatorImpl(std::vector<std::shared_ptr<BoundAggregate>> aggregates)
      : aggregates_(std::move(aggregates)), counts_(aggregates_.size(), 0) {
    values_.resize(aggregates_.size());
  }

  Status Add(const StructLike& row) override {
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      const auto& agg = aggregates_[i];
      switch (agg->kind()) {
        case BoundAggregate::Kind::kCount: {
          auto count_agg = internal::checked_pointer_cast<BoundCountAggregate>(agg);
          ICEBERG_ASSIGN_OR_RAISE(auto contribution, count_agg->Evaluate(row));
          counts_[i] += std::get<int64_t>(contribution.value());
          break;
        }
        case BoundAggregate::Kind::kValue: {
          auto value_agg = internal::checked_pointer_cast<BoundValueAggregate>(agg);
          ICEBERG_ASSIGN_OR_RAISE(auto val_literal, value_agg->Evaluate(row));
          if (val_literal.IsNull()) {
            break;
          }
          auto& current = values_[i];
          if (!current) {
            current = std::move(val_literal);
            break;
          }
          auto ordering = val_literal <=> *current;
          if (ordering == std::partial_ordering::unordered) {
            return InvalidExpression("Cannot compare literals of type {}",
                                     val_literal.type()->ToString());
          }
          if (agg->op() == Expression::Operation::kMax) {
            if (ordering == std::partial_ordering::greater) {
              current = std::move(val_literal);
            }
          } else {
            if (ordering == std::partial_ordering::less) {
              current = std::move(val_literal);
            }
          }
          break;
        }
      }
    }
    return {};
  }

  Result<std::vector<Literal>> Results() const override {
    std::vector<Literal> out;
    out.reserve(aggregates_.size());
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      switch (aggregates_[i]->kind()) {
        case BoundAggregate::Kind::kCount:
          out.emplace_back(Literal::Long(counts_[i]));
          break;
        case BoundAggregate::Kind::kValue: {
          if (values_[i]) {
            out.emplace_back(*values_[i]);
          } else {
            auto value_agg =
                internal::checked_pointer_cast<BoundValueAggregate>(aggregates_[i]);
            ICEBERG_ASSIGN_OR_RAISE(auto type, GetPrimitiveType(*value_agg->term()));
            out.emplace_back(Literal::Null(type));
          }
          break;
        }
      }
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
  std::vector<int64_t> counts_;
  std::vector<std::optional<Literal>> values_;
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
  return std::unique_ptr<AggregateEvaluator>(
      new AggregateEvaluatorImpl(std::move(aggregates)));
}

}  // namespace iceberg
