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
#include <string_view>
#include <vector>

#include "iceberg/expression/literal.h"
#include "iceberg/manifest_entry.h"
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

Result<Scalar> LiteralToScalar(const Literal& literal) {
  if (literal.IsNull()) {
    return Scalar{std::monostate{}};
  }

  switch (literal.type()->type_id()) {
    case TypeId::kBoolean:
      return Scalar{std::get<bool>(literal.value())};
    case TypeId::kInt:
    case TypeId::kDate:
      return Scalar{std::get<int32_t>(literal.value())};
    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      return Scalar{std::get<int64_t>(literal.value())};
    case TypeId::kFloat:
      return Scalar{std::get<float>(literal.value())};
    case TypeId::kDouble:
      return Scalar{std::get<double>(literal.value())};
    case TypeId::kString: {
      const auto& str = std::get<std::string>(literal.value());
      return Scalar{std::string_view(str)};
    }
    case TypeId::kBinary:
    case TypeId::kFixed: {
      const auto& bytes = std::get<std::vector<uint8_t>>(literal.value());
      return Scalar{
          std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size())};
    }
    case TypeId::kDecimal:
      return Scalar{std::get<Decimal>(literal.value())};
    default:
      return NotSupported("Cannot convert literal of type {} to Scalar",
                          literal.type()->ToString());
  }
}

class SingleValueStructLike : public StructLike {
 public:
  explicit SingleValueStructLike(Literal literal) : literal_(std::move(literal)) {}

  Result<Scalar> GetField(size_t /*pos*/) const override { return LiteralToScalar(literal_); }

  size_t num_fields() const override { return 1; }

 private:
  Literal literal_;
};

class CountAggregator : public BoundAggregate::Aggregator {
 public:
  explicit CountAggregator(const CountAggregate& aggregate) : aggregate_(aggregate) {}

  Status Update(const StructLike& row) override {
    ICEBERG_ASSIGN_OR_RAISE(auto count, aggregate_.CountFor(row));
    count_ += count;
    return {};
  }

  Status Update(const DataFile& file) override {
    if (!valid_) {
      return {};
    }
    if (!aggregate_.HasValue(file)) {
      valid_ = false;
      return {};
    }
    ICEBERG_ASSIGN_OR_RAISE(auto maybe_count, aggregate_.CountFor(file));
    if (!maybe_count.has_value()) {
      valid_ = false;
      return {};
    }
    count_ += *maybe_count;
    return {};
  }

  Literal GetResult() const override {
    if (!valid_) {
      return Literal::Null(int64());
    }
    return Literal::Long(count_);
  }

  bool IsValid() const override { return valid_; }

 private:
  const CountAggregate& aggregate_;
  int64_t count_ = 0;
  bool valid_ = true;
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

  Status Update(const DataFile& file) override {
    if (!valid_) {
      return {};
    }
    if (!aggregate_.HasValue(file)) {
      valid_ = false;
      return {};
    }

    ICEBERG_ASSIGN_OR_RAISE(auto value, aggregate_.Evaluate(file));
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

  Literal GetResult() const override {
    if (!valid_) {
      return Literal::Null(GetPrimitiveType(*aggregate_.term()));
    }
    return current_;
  }

  bool IsValid() const override { return valid_; }

 private:
  const MaxAggregate& aggregate_;
  Literal current_;
  bool valid_ = true;
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

  Status Update(const DataFile& file) override {
    if (!valid_) {
      return {};
    }
    if (!aggregate_.HasValue(file)) {
      valid_ = false;
      return {};
    }

    ICEBERG_ASSIGN_OR_RAISE(auto value, aggregate_.Evaluate(file));
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

  Literal GetResult() const override {
    if (!valid_) {
      return Literal::Null(GetPrimitiveType(*aggregate_.term()));
    }
    return current_;
  }

  bool IsValid() const override { return valid_; }

 private:
  const MinAggregate& aggregate_;
  Literal current_;
  bool valid_ = true;
};

bool HasMapKey(const std::map<int32_t, int64_t>& map, int32_t key) {
  return map.contains(key);
}

bool HasMapKey(const std::map<int32_t, std::vector<uint8_t>>& map, int32_t key) {
  return map.contains(key);
}

template <typename T>
std::optional<T> GetMapValue(const std::map<int32_t, T>& map, int32_t key) {
  auto iter = map.find(key);
  if (iter == map.end()) {
    return std::nullopt;
  }
  return iter->second;
}

int32_t GetFieldId(const std::shared_ptr<BoundTerm>& term) {
  ICEBERG_DCHECK(term != nullptr, "Aggregate term should not be null");
  auto ref = term->reference();
  ICEBERG_DCHECK(ref != nullptr, "Aggregate term reference should not be null");
  return ref->field().field_id();
}

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

Result<Literal> BoundAggregate::Evaluate(const DataFile& /*file*/) const {
  return NotImplemented("Evaluate(DataFile) not implemented");
}

// -------------------- CountAggregate --------------------

Result<Literal> CountAggregate::Evaluate(const StructLike& data) const {
  return CountFor(data).transform([](int64_t count) { return Literal::Long(count); });
}

Result<Literal> CountAggregate::Evaluate(const DataFile& file) const {
  ICEBERG_ASSIGN_OR_RAISE(auto count, CountFor(file));
  if (!count.has_value()) {
    return Literal::Null(int64());
  }
  return Literal::Long(*count);
}

Result<std::optional<int64_t>> CountAggregate::CountFor(const DataFile& file) const {
  return NotImplemented("CountFor(DataFile) not implemented");
}

bool CountAggregate::HasValue(const DataFile& /*file*/) const { return false; }

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

Result<std::optional<int64_t>> CountNonNullAggregate::CountFor(
    const DataFile& file) const {
  auto field_id = GetFieldId(term());
  auto value_count = GetMapValue(file.value_counts, field_id);
  auto null_count = GetMapValue(file.null_value_counts, field_id).value_or(0);
  if (!value_count.has_value()) {
    return std::nullopt;
  }
  return *value_count - null_count;
}

bool CountNonNullAggregate::HasValue(const DataFile& file) const {
  auto field_id = GetFieldId(term());
  return HasMapKey(file.value_counts, field_id) && HasMapKey(file.null_value_counts, field_id);
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

Result<std::optional<int64_t>> CountNullAggregate::CountFor(const DataFile& file) const {
  auto field_id = GetFieldId(term());
  auto null_count = GetMapValue(file.null_value_counts, field_id);
  if (!null_count.has_value()) {
    return std::nullopt;
  }
  return *null_count;
}

bool CountNullAggregate::HasValue(const DataFile& file) const {
  return HasMapKey(file.null_value_counts, GetFieldId(term()));
}

CountStarAggregate::CountStarAggregate()
    : CountAggregate(Expression::Operation::kCountStar, nullptr) {}

Result<std::unique_ptr<CountStarAggregate>> CountStarAggregate::Make() {
  return std::unique_ptr<CountStarAggregate>(new CountStarAggregate());
}

Result<int64_t> CountStarAggregate::CountFor(const StructLike& /*data*/) const {
  return 1;
}

Result<std::optional<int64_t>> CountStarAggregate::CountFor(const DataFile& file) const {
  if (!HasValue(file)) {
    return std::nullopt;
  }
  return file.record_count;
}

bool CountStarAggregate::HasValue(const DataFile& file) const {
  return file.record_count >= 0;
}

MaxAggregate::MaxAggregate(std::shared_ptr<BoundTerm> term)
    : BoundAggregate(Expression::Operation::kMax, std::move(term)) {}

std::shared_ptr<MaxAggregate> MaxAggregate::Make(std::shared_ptr<BoundTerm> term) {
  return std::shared_ptr<MaxAggregate>(new MaxAggregate(std::move(term)));
}

Result<Literal> MaxAggregate::Evaluate(const StructLike& data) const {
  return term()->Evaluate(data);
}

Result<Literal> MaxAggregate::Evaluate(const DataFile& file) const {
  auto field_id = GetFieldId(term());
  auto upper = GetMapValue(file.upper_bounds, field_id);
  auto ptype = GetPrimitiveType(*term());
  if (!upper.has_value()) {
    SingleValueStructLike data(Literal::Null(ptype));
    return term()->Evaluate(data);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto literal, Literal::Deserialize(*upper, ptype));
  SingleValueStructLike data(std::move(literal));
  return term()->Evaluate(data);
}

std::unique_ptr<BoundAggregate::Aggregator> MaxAggregate::NewAggregator() const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new MaxAggregator(*this));
}

bool MaxAggregate::HasValue(const DataFile& file) const {
  auto field_id = GetFieldId(term());
  bool has_bound = HasMapKey(file.upper_bounds, field_id);
  auto value_count = GetMapValue(file.value_counts, field_id);
  auto null_count = GetMapValue(file.null_value_counts, field_id);
  bool all_null =
      value_count.has_value() && *value_count > 0 && null_count.has_value() &&
      null_count.value() == value_count.value();
  return has_bound || all_null;
}

MinAggregate::MinAggregate(std::shared_ptr<BoundTerm> term)
    : BoundAggregate(Expression::Operation::kMin, std::move(term)) {}

std::shared_ptr<MinAggregate> MinAggregate::Make(std::shared_ptr<BoundTerm> term) {
  return std::shared_ptr<MinAggregate>(new MinAggregate(std::move(term)));
}

Result<Literal> MinAggregate::Evaluate(const StructLike& data) const {
  return term()->Evaluate(data);
}

Result<Literal> MinAggregate::Evaluate(const DataFile& file) const {
  auto field_id = GetFieldId(term());
  auto lower = GetMapValue(file.lower_bounds, field_id);
  auto ptype = GetPrimitiveType(*term());
  if (!lower.has_value()) {
    SingleValueStructLike data(Literal::Null(ptype));
    return term()->Evaluate(data);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto literal, Literal::Deserialize(*lower, ptype));
  SingleValueStructLike data(std::move(literal));
  return term()->Evaluate(data);
}

std::unique_ptr<BoundAggregate::Aggregator> MinAggregate::NewAggregator() const {
  return std::unique_ptr<BoundAggregate::Aggregator>(new MinAggregator(*this));
}

bool MinAggregate::HasValue(const DataFile& file) const {
  auto field_id = GetFieldId(term());
  bool has_bound = HasMapKey(file.lower_bounds, field_id);
  auto value_count = GetMapValue(file.value_counts, field_id);
  auto null_count = GetMapValue(file.null_value_counts, field_id);
  bool all_null =
      value_count.has_value() && *value_count > 0 && null_count.has_value() &&
      null_count.value() == value_count.value();
  return has_bound || all_null;
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
template class Aggregate<UnboundTerm<BoundTransform>>;
template class Aggregate<BoundTerm>;
template class UnboundAggregateImpl<BoundReference>;
template class UnboundAggregateImpl<BoundTransform>;

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

  Status Update(const DataFile& file) override {
    for (auto& aggregator : aggregators_) {
      ICEBERG_RETURN_UNEXPECTED(aggregator->Update(file));
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

  bool AllAggregatorsValid() const override {
    for (const auto& aggregator : aggregators_) {
      if (!aggregator->IsValid()) {
        return false;
      }
    }
    return true;
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
