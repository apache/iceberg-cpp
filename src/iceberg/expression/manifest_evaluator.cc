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

#include "iceberg/expression/manifest_evaluator.h"

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {
constexpr bool kRowsMightMatch = true;
constexpr bool kRowCannotMatch = false;
constexpr int32_t kInPredicateLimit = 200;
}  // namespace

class ManifestEvalVisitor : public BoundVisitor<bool> {
 public:
  explicit ManifestEvalVisitor(const ManifestFile& manifest)
      : stats_(manifest.partitions) {}

  Result<bool> AlwaysTrue() override { return kRowsMightMatch; }

  Result<bool> AlwaysFalse() override { return kRowCannotMatch; }

  Result<bool> Not(bool child_result) override { return !child_result; }

  Result<bool> And(bool left_result, bool right_result) override {
    return left_result && right_result;
  }

  Result<bool> Or(bool left_result, bool right_result) override {
    return left_result || right_result;
  }

  Result<bool> IsNull(const std::shared_ptr<Bound>& expr) override {
    // no need to check whether the field is required because binding evaluates that case
    // if the column has no null values, the expression cannot match
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    if (!stats_.at(pos).contains_null) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> NotNull(const std::shared_ptr<Bound>& expr) override {
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    if (AllValuesAreNull(stats_.at(pos), ref->type()->type_id())) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> IsNaN(const std::shared_ptr<Bound>& expr) override {
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    if (stats_.at(pos).contains_nan.has_value() && !stats_.at(pos).contains_nan.value()) {
      return kRowCannotMatch;
    }
    if (AllValuesAreNull(stats_.at(pos), ref->type()->type_id())) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> NotNaN(const std::shared_ptr<Bound>& expr) override {
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    const auto& summary = stats_.at(pos);
    // if containsNaN is true, containsNull is false and lowerBound is null, all values
    // are NaN
    if (summary.contains_nan.has_value() && summary.contains_nan.value() &&
        !summary.contains_null && !summary.lower_bound.has_value()) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> Lt(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    const auto& summary = stats_.at(pos);
    if (!summary.lower_bound.has_value()) {
      return kRowCannotMatch;  // values are all null
    }
    ICEBERG_ASSIGN_OR_RAISE(
        auto lower, DeserializeBoundLiteral(summary.lower_bound.value(), ref->type()));
    if (lower >= lit) {
      return kRowCannotMatch;
    }
    return kRowsMightMatch;
  }

  Result<bool> LtEq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    const auto& summary = stats_.at(pos);
    if (!summary.lower_bound.has_value()) {
      return kRowCannotMatch;  // values are all null
    }
    ICEBERG_ASSIGN_OR_RAISE(
        auto lower, DeserializeBoundLiteral(summary.lower_bound.value(), ref->type()));
    if (lower > lit) {
      return kRowCannotMatch;
    }
    return kRowsMightMatch;
  }

  Result<bool> Gt(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    const auto& summary = stats_.at(pos);
    if (!summary.upper_bound.has_value()) {
      return kRowCannotMatch;  // values are all null
    }
    ICEBERG_ASSIGN_OR_RAISE(
        auto upper, DeserializeBoundLiteral(summary.upper_bound.value(), ref->type()));
    if (upper <= lit) {
      return kRowCannotMatch;
    }
    return kRowsMightMatch;
  }

  Result<bool> GtEq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    const auto& summary = stats_.at(pos);
    if (!summary.upper_bound.has_value()) {
      return kRowCannotMatch;  // values are all null
    }
    ICEBERG_ASSIGN_OR_RAISE(
        auto upper,
        DeserializeBoundLiteral(summary.upper_bound.value(), expr->reference()->type()));
    if (upper < lit) {
      return kRowCannotMatch;
    }
    return kRowsMightMatch;
  }

  Result<bool> Eq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    const auto& summary = stats_.at(pos);
    if (!summary.lower_bound.has_value() || !summary.upper_bound.has_value()) {
      return kRowCannotMatch;  // values are all null and literal cannot contain null
    }
    ICEBERG_ASSIGN_OR_RAISE(
        auto lower, DeserializeBoundLiteral(summary.lower_bound.value(), ref->type()));
    if (lower > lit) {
      return kRowCannotMatch;
    }

    ICEBERG_ASSIGN_OR_RAISE(
        auto upper, DeserializeBoundLiteral(summary.upper_bound.value(), ref->type()));
    if (upper < lit) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> NotEq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    // because the bounds are not necessarily a min or max value, this cannot be answered
    // using them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
    return kRowsMightMatch;
  }

  Result<bool> In(const std::shared_ptr<Bound>& expr,
                  const BoundSetPredicate::LiteralSet& literal_set) override {
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    const auto& summary = stats_.at(pos);
    if (!summary.lower_bound.has_value() || !summary.upper_bound.has_value()) {
      // values are all null and literalSet cannot contain null.
      return kRowCannotMatch;
    }
    if (literal_set.size() > kInPredicateLimit) {
      // skip evaluating the predicate if the number of values is too big
      return kRowsMightMatch;
    }

    ICEBERG_ASSIGN_OR_RAISE(
        auto lower, DeserializeBoundLiteral(summary.lower_bound.value(), ref->type()));
    ICEBERG_ASSIGN_OR_RAISE(
        auto upper, DeserializeBoundLiteral(summary.upper_bound.value(), ref->type()));

    if (std::ranges::all_of(literal_set, [&](const Literal& lit) {
          return lit < lower || lit > upper;
        })) {
      // if all values are less than lower bound or greater than upper bound,
      // rows cannot match.
      return kRowCannotMatch;
    }
    return kRowsMightMatch;
  }

  Result<bool> NotIn(const std::shared_ptr<Bound>& expr,
                     const BoundSetPredicate::LiteralSet& literal_set) override {
    // because the bounds are not necessarily a min or max value, this cannot be answered
    // using them. notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in
    // col.
    return kRowsMightMatch;
  }

  Result<bool> StartsWith(const std::shared_ptr<Bound>& expr,
                          const Literal& lit) override {
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    const auto& summary = stats_.at(pos);
    if (!summary.lower_bound.has_value() || !summary.upper_bound.has_value()) {
      return kRowCannotMatch;
    }
    if (lit.type()->type_id() != TypeId::kString) {
      return InvalidExpression("Invalid literal: not a string, cannot use StartsWith");
    }
    const auto& prefix = std::get<std::string>(lit.value());
    ICEBERG_ASSIGN_OR_RAISE(
        auto lower, DeserializeBoundLiteral(summary.lower_bound.value(), ref->type()));
    ICEBERG_ASSIGN_OR_RAISE(
        auto upper, DeserializeBoundLiteral(summary.upper_bound.value(), ref->type()));
    const auto& lower_bound = std::get<std::string>(lower.value());
    const auto& upper_bound = std::get<std::string>(upper.value());
    // truncate lower bound so that its length in bytes is not greater than the length of
    // prefix
    size_t length = std::min(prefix.size(), lower_bound.size());
    if (lower_bound.substr(0, length) > prefix) {
      return kRowCannotMatch;
    }
    length = std::min(prefix.size(), upper_bound.size());
    if (upper_bound.substr(0, length) < prefix) {
      return kRowCannotMatch;
    }
    return kRowsMightMatch;
  }

  Result<bool> NotStartsWith(const std::shared_ptr<Bound>& expr,
                             const Literal& lit) override {
    const auto& ref = expr->reference();
    ICEBERG_ASSIGN_OR_RAISE(auto pos, GetPosition(*ref));
    const auto& summary = stats_.at(pos);
    if (summary.contains_null || !summary.lower_bound.has_value() ||
        !summary.upper_bound.has_value()) {
      return kRowsMightMatch;
    }
    if (lit.type()->type_id() != TypeId::kString) {
      return InvalidExpression("Invalid literal: not a string, cannot use notStartsWith");
    }
    // notStartsWith will match unless all values must start with the prefix. This happens
    // when the lower and upper bounds both start with the prefix.
    const auto& prefix = std::get<std::string>(lit.value());
    ICEBERG_ASSIGN_OR_RAISE(
        auto lower, DeserializeBoundLiteral(summary.lower_bound.value(), ref->type()));
    ICEBERG_ASSIGN_OR_RAISE(
        auto upper, DeserializeBoundLiteral(summary.upper_bound.value(), ref->type()));
    const auto& lower_bound = std::get<std::string>(lower.value());
    const auto& upper_bound = std::get<std::string>(upper.value());

    // if lower is shorter than the prefix, it can't start with the prefix
    if (lower_bound.size() < prefix.size()) {
      return kRowsMightMatch;
    }
    if (lower_bound.starts_with(prefix)) {
      // the lower bound starts with the prefix; check the upper bound
      // if upper is shorter than the prefix, it can't start with the prefix
      if (upper_bound.size() < prefix.size()) {
        return kRowsMightMatch;
      }
      // truncate upper bound so that its length in bytes is not greater than the length
      // of prefix
      if (upper_bound.starts_with(prefix)) {
        return kRowCannotMatch;
      }
    }
    return kRowsMightMatch;
  }

 private:
  Result<size_t> GetPosition(const BoundReference& ref) const {
    const auto& accessor = ref.accessor();
    const auto& position_path = accessor.position_path();
    if (position_path.empty()) {
      return InvalidArgument("Invalid accessor: empty position path.");
    }
    // nested accessors are not supported for partition fields
    if (position_path.size() > 1) {
      return InvalidArgument("Cannot convert nested accessor to position");
    }
    auto pos = position_path.at(0);
    if (pos >= stats_.size()) {
      return InvalidArgument("Position {} is out of partition field range {}", pos,
                             stats_.size());
    }
    return pos;
  }

  bool AllValuesAreNull(const PartitionFieldSummary& summary, TypeId typeId) {
    // containsNull encodes whether at least one partition value is null,
    // lowerBound is null if all partition values are null
    bool allNull = summary.contains_null && !summary.lower_bound.has_value();

    if (allNull && (typeId == TypeId::kDouble || typeId == TypeId::kFloat)) {
      // floating point types may include NaN values, which we check separately.
      // In case bounds don't include NaN value, containsNaN needs to be checked against.
      allNull = summary.contains_nan.has_value() && !summary.contains_nan.value();
    }
    return allNull;
  }

  Result<Literal> DeserializeBoundLiteral(const std::vector<uint8_t>& bound,
                                          const std::shared_ptr<Type>& type) const {
    if (!type->is_primitive()) {
      return NotSupported("Bounds of non-primitive partition fields are not supported.");
    }
    return Literal::Deserialize(
        bound, std::move(internal::checked_pointer_cast<PrimitiveType>(type)));
  }

 private:
  const std::vector<PartitionFieldSummary>& stats_;
};

ManifestEvaluator::ManifestEvaluator(std::shared_ptr<Expression> expr)
    : expr_(std::move(expr)) {}

ManifestEvaluator::~ManifestEvaluator() = default;

Result<std::unique_ptr<ManifestEvaluator>> ManifestEvaluator::MakeRowFilter(
    [[maybe_unused]] std::shared_ptr<Expression> expr,
    [[maybe_unused]] const std::shared_ptr<PartitionSpec>& spec,
    [[maybe_unused]] const Schema& schema, [[maybe_unused]] bool case_sensitive) {
  // TODO(xiao.dong) we need a projection util to project row filter to the partition col
  return NotImplemented("ManifestEvaluator::MakeRowFilter");
}

Result<std::unique_ptr<ManifestEvaluator>> ManifestEvaluator::MakePartitionFilter(
    std::shared_ptr<Expression> expr, const std::shared_ptr<PartitionSpec>& spec,
    const Schema& schema, bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec->PartitionType(schema));
  ICEBERG_ASSIGN_OR_RAISE(auto rewrite_expr, RewriteNot::Visit(std::move(expr)));
  ICEBERG_ASSIGN_OR_RAISE(
      auto partition_expr,
      Binder::Bind(*partition_type->ToSchema(), rewrite_expr, case_sensitive));
  return std::unique_ptr<ManifestEvaluator>(
      new ManifestEvaluator(std::move(partition_expr)));
}

Result<bool> ManifestEvaluator::Evaluate(const ManifestFile& manifest) const {
  if (manifest.partitions.empty()) {
    return kRowsMightMatch;
  }
  ManifestEvalVisitor visitor(manifest);
  return Visit<bool, ManifestEvalVisitor>(expr_, visitor);
}

}  // namespace iceberg
