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

#include "iceberg/expression/inclusive_metrics_evaluator.h"

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/transform.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {
constexpr bool kRowsMightMatch = true;
constexpr bool kRowCannotMatch = false;
constexpr int32_t kInPredicateLimit = 200;
}  // namespace

class InclusiveMetricsVisitor : public BoundVisitor<bool> {
 public:
  explicit InclusiveMetricsVisitor(const DataFile& data_file) : data_file_(data_file) {}

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
    if (IsNonNullPreserving(expr)) {
      // number of non-nulls is the same as for the ref
      int32_t id = expr->reference()->field().field_id();
      if (!MayContainNull(id)) {
        return kRowCannotMatch;
      }
    }
    return kRowsMightMatch;
  }

  Result<bool> NotNull(const std::shared_ptr<Bound>& expr) override {
    // no need to check whether the field is required because binding evaluates that case
    // if the column has no non-null values, the expression cannot match

    // all terms are null preserving. see #isNullPreserving(Bound)
    int32_t id = expr->reference()->field().field_id();
    if (ContainsNullsOnly(id)) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> IsNaN(const std::shared_ptr<Bound>& expr) override {
    // when there's no nanCounts information, but we already know the column only contains
    // null, it's guaranteed that there's no NaN value
    int32_t id = expr->reference()->field().field_id();
    if (ContainsNullsOnly(id)) {
      return kRowCannotMatch;
    }
    if (dynamic_cast<const BoundReference*>(expr.get()) == nullptr) {
      return kRowsMightMatch;
    }
    auto it = data_file_.nan_value_counts.find(id);
    if (it != data_file_.nan_value_counts.end() && it->second == 0) {
      return kRowCannotMatch;
    }
    return kRowsMightMatch;
  }

  Result<bool> NotNaN(const std::shared_ptr<Bound>& expr) override {
    if (dynamic_cast<const BoundReference*>(expr.get()) == nullptr) {
      // identity transforms are already removed by this time
      return kRowsMightMatch;
    }

    int32_t id = expr->reference()->field().field_id();
    if (ContainsNaNsOnly(id)) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> Lt(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int32_t id = expr->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return kRowCannotMatch;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto lower, LowerBound(expr));
    if (!lower.has_value() || lower->IsNull() || lower->IsNaN()) {
      // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
      return kRowsMightMatch;
    }

    // this also works for transforms that are order preserving:
    // if a transform f is order preserving, a < b means that f(a) <= f(b).
    // because lower <= a for all values of a in the file, f(lower) <= f(a).
    // when f(lower) >= X then f(a) >= f(lower) >= X, so there is no a such that f(a) < X
    // f(lower) >= X means rows cannot match
    if (lower.value() >= lit) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> LtEq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int32_t id = expr->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return kRowCannotMatch;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto lower, LowerBound(expr));
    if (!lower.has_value() || lower->IsNull() || lower->IsNaN()) {
      // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
      return kRowsMightMatch;
    }

    // this also works for transforms that are order preserving:
    // if a transform f is order preserving, a < b means that f(a) <= f(b).
    // because lower <= a for all values of a in the file, f(lower) <= f(a).
    // when f(lower) > X then f(a) >= f(lower) > X, so there is no a such that f(a) <= X
    // f(lower) > X means rows cannot match
    if (lower.value() > lit) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> Gt(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int32_t id = expr->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return kRowCannotMatch;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto upper, UpperBound(expr));
    if (!upper.has_value() || upper->IsNull()) {
      return kRowsMightMatch;
    }
    if (upper.value() <= lit) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> GtEq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int32_t id = expr->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return kRowCannotMatch;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto upper, UpperBound(expr));
    if (!upper.has_value() || upper->IsNull()) {
      return kRowsMightMatch;
    }
    if (upper.value() < lit) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> Eq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int32_t id = expr->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return kRowCannotMatch;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto lower, LowerBound(expr));
    if (lower.has_value() && !lower->IsNull() && !lower->IsNaN()) {
      if (lower.value() > lit) {
        return kRowCannotMatch;
      }
    }

    ICEBERG_ASSIGN_OR_RAISE(auto upper, UpperBound(expr));
    if (!upper.has_value() || upper->IsNull()) {
      return kRowsMightMatch;
    }
    if (upper.value() < lit) {
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
    // all terms are null preserving. see #isNullPreserving(Bound)
    int32_t id = expr->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return kRowCannotMatch;
    }

    if (literal_set.size() > kInPredicateLimit) {
      // skip evaluating the predicate if the number of values is too big
      return kRowsMightMatch;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto lower, LowerBound(expr));
    if (!lower.has_value() || lower->IsNull() || lower->IsNaN()) {
      // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
      return kRowsMightMatch;
    }
    auto literals_view = literal_set | std::views::filter([&](const Literal& lit) {
                           return lower.value() <= lit;
                         });
    // if all values are less than lower bound, rows cannot match
    if (literals_view.empty()) {
      return kRowCannotMatch;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto upper, UpperBound(expr));
    if (!upper.has_value() || upper->IsNull()) {
      return kRowsMightMatch;
    }
    auto filtered_view = literals_view | std::views::filter([&](const Literal& lit) {
                           return upper.value() >= lit;
                         });
    // if remaining values are greater than upper bound, rows cannot match
    if (filtered_view.empty()) {
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
    if (auto transform = dynamic_cast<const BoundTransform*>(expr.get());
        transform != nullptr &&
        transform->transform()->transform_type() != TransformType::kIdentity) {
      // truncate must be rewritten in binding. the result is either always or never
      // compatible
      return kRowsMightMatch;
    }

    int32_t id = expr->reference()->field().field_id();
    if (ContainsNullsOnly(id)) {
      return kRowCannotMatch;
    }
    if (lit.type()->type_id() != TypeId::kString) {
      return kRowCannotMatch;
    }
    const auto& prefix = std::get<std::string>(lit.value());

    ICEBERG_ASSIGN_OR_RAISE(auto lower, LowerBound(expr));
    if (!lower.has_value() || lower->IsNull()) {
      return kRowsMightMatch;
    }
    const auto& lower_str = std::get<std::string>(lower->value());
    // truncate lower bound so that its length in bytes is not greater than the length of
    // prefix
    size_t length = std::min(prefix.size(), lower_str.size());
    // if prefix of lower bound is greater than prefix, rows cannot match
    if (lower_str.substr(0, length) > prefix) {
      return kRowCannotMatch;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto upper, UpperBound(expr));
    if (!upper.has_value() || upper->IsNull()) {
      return kRowsMightMatch;
    }
    const auto& upper_str = std::get<std::string>(upper->value());
    // truncate upper bound so that its length in bytes is not greater than the length of
    // prefix
    length = std::min(prefix.size(), upper_str.size());
    // if prefix of upper bound is less than prefix, rows cannot match
    if (upper_str.substr(0, length) < prefix) {
      return kRowCannotMatch;
    }

    return kRowsMightMatch;
  }

  Result<bool> NotStartsWith(const std::shared_ptr<Bound>& expr,
                             const Literal& lit) override {
    // the only transforms that produce strings are truncate and identity, which work with
    // this
    int32_t id = expr->reference()->field().field_id();
    if (MayContainNull(id)) {
      return kRowsMightMatch;
    }

    if (lit.type()->type_id() != TypeId::kString) {
      return kRowCannotMatch;
    }
    const auto& prefix = std::get<std::string>(lit.value());

    // notStartsWith will match unless all values must start with the prefix. This happens
    // when the lower and upper bounds both start with the prefix.
    ICEBERG_ASSIGN_OR_RAISE(auto lower, LowerBound(expr));
    ICEBERG_ASSIGN_OR_RAISE(auto upper, UpperBound(expr));
    if (!lower.has_value() || lower->IsNull() || !upper.has_value() || upper->IsNull()) {
      return kRowsMightMatch;
    }
    const auto& lower_str = std::get<std::string>(lower->value());
    const auto& upper_str = std::get<std::string>(upper->value());

    // if lower is shorter than the prefix then lower doesn't start with the prefix
    if (lower_str.size() < prefix.size()) {
      return kRowsMightMatch;
    }

    if (lower_str.starts_with(prefix)) {
      // if upper is shorter than the prefix then upper can't start with the prefix
      if (upper_str.size() < prefix.size()) {
        return kRowsMightMatch;
      }
      if (upper_str.starts_with(prefix)) {
        // both bounds match the prefix, so all rows must match the prefix and therefore
        // do not satisfy the predicate
        return kRowCannotMatch;
      }
    }

    return kRowsMightMatch;
  }

 private:
  bool MayContainNull(int32_t id) {
    return data_file_.null_value_counts.empty() ||
           !data_file_.null_value_counts.contains(id) ||
           data_file_.null_value_counts.at(id) != 0;
  }

  bool ContainsNullsOnly(int32_t id) {
    auto val_it = data_file_.value_counts.find(id);
    auto null_it = data_file_.null_value_counts.find(id);
    return val_it != data_file_.value_counts.cend() &&
           null_it != data_file_.null_value_counts.cend() &&
           val_it->second == null_it->second;
  }

  bool ContainsNaNsOnly(int32_t id) {
    auto val_it = data_file_.value_counts.find(id);
    auto nan_it = data_file_.nan_value_counts.find(id);
    return val_it != data_file_.value_counts.cend() &&
           nan_it != data_file_.nan_value_counts.cend() &&
           val_it->second == nan_it->second;
  }

  Result<std::optional<Literal>> LowerBound(const std::shared_ptr<Bound>& expr) {
    if (auto reference = dynamic_cast<const BoundReference*>(expr.get());
        reference != nullptr) {
      return ParseLowerBound(*reference);
    } else if (auto transform = dynamic_cast<BoundTransform*>(expr.get());
               transform != nullptr) {
      return TransformLowerBound(*transform);
    } else {
      return std::nullopt;
    }
    // TODO(xiao.dong) handle extract lower and upper bounds
  }

  Result<std::optional<Literal>> UpperBound(const std::shared_ptr<Bound>& expr) {
    if (auto reference = dynamic_cast<const BoundReference*>(expr.get());
        reference != nullptr) {
      return ParseUpperBound(*reference);
    } else if (auto transform = dynamic_cast<BoundTransform*>(expr.get());
               transform != nullptr) {
      return TransformUpperBound(*transform);
    } else {
      return std::nullopt;
    }
    // TODO(xiao.dong) handle extract lower and upper bounds
  }

  Result<std::optional<Literal>> ParseLowerBound(const BoundReference& ref) {
    int32_t id = ref.field().field_id();
    auto type = ref.type();
    if (!type->is_primitive()) {
      return NotSupported("Lower bound of non-primitive type is not supported.");
    }
    auto primitive_type = internal::checked_pointer_cast<PrimitiveType>(type);
    if (data_file_.lower_bounds.contains(id)) {
      return Literal::Deserialize(data_file_.lower_bounds.at(id), primitive_type);
    }

    return std::nullopt;
  }

  Result<std::optional<Literal>> ParseUpperBound(const BoundReference& ref) {
    int32_t id = ref.field().field_id();
    auto type = ref.type();
    if (!type->is_primitive()) {
      return NotSupported("Upper bound of non-primitive type is not supported.");
    }
    auto primitive_type = internal::checked_pointer_cast<PrimitiveType>(type);
    if (data_file_.upper_bounds.contains(id)) {
      return Literal::Deserialize(data_file_.upper_bounds.at(id), primitive_type);
    }

    return std::nullopt;
  }

  Result<std::optional<Literal>> TransformLowerBound(BoundTransform& boundTransform) {
    auto transform = boundTransform.transform();
    if (transform->PreservesOrder()) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower, ParseLowerBound(*boundTransform.reference()));
      if (lower.has_value()) {
        ICEBERG_ASSIGN_OR_RAISE(auto transform_func,
                                transform->Bind(boundTransform.reference()->type()));
        return transform_func->Transform(lower.value());
      }
    }

    return std::nullopt;
  }

  Result<std::optional<Literal>> TransformUpperBound(BoundTransform& boundTransform) {
    auto transform = boundTransform.transform();
    if (transform->PreservesOrder()) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper, ParseUpperBound(*boundTransform.reference()));
      if (upper.has_value()) {
        ICEBERG_ASSIGN_OR_RAISE(auto transform_func,
                                transform->Bind(boundTransform.reference()->type()));
        return transform_func->Transform(upper.value());
      }
    }

    return std::nullopt;
  }

  /** Returns true if the expression term produces a non-null value for non-null input. */
  bool IsNonNullPreserving(const std::shared_ptr<Bound>& expr) {
    if (auto reference = dynamic_cast<const BoundReference*>(expr.get());
        reference != nullptr) {
      return true;
    } else if (auto transform = dynamic_cast<BoundTransform*>(expr.get());
               transform != nullptr) {
      return transform->transform()->PreservesOrder();
    }
    //  a non-null variant does not necessarily contain a specific field
    //  and unknown bound terms are not non-null preserving
    return false;
  }

 private:
  const DataFile& data_file_;
};

InclusiveMetricsEvaluator::InclusiveMetricsEvaluator(std::shared_ptr<Expression> expr)
    : expr_(std::move(expr)) {}

InclusiveMetricsEvaluator::~InclusiveMetricsEvaluator() = default;

Result<std::unique_ptr<InclusiveMetricsEvaluator>> InclusiveMetricsEvaluator::Make(
    std::shared_ptr<Expression> expr, const Schema& schema, bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto rewrite_expr, RewriteNot::Visit(std::move(expr)));
  ICEBERG_ASSIGN_OR_RAISE(auto bound_expr,
                          Binder::Bind(schema, rewrite_expr, case_sensitive));
  return std::unique_ptr<InclusiveMetricsEvaluator>(
      new InclusiveMetricsEvaluator(std::move(bound_expr)));
}

Result<bool> InclusiveMetricsEvaluator::Evaluate(const DataFile& data_file) const {
  if (data_file.record_count == 0) {
    return kRowCannotMatch;
  }
  if (data_file.record_count < 0) {
    // we haven't implemented parsing record count from avro file and thus set record
    // count -1 when importing avro tables to iceberg tables. This should be updated once
    // we implemented and set correct record count.
    return kRowsMightMatch;
  }
  InclusiveMetricsVisitor visitor(data_file);
  return Visit<bool, InclusiveMetricsVisitor>(expr_, visitor);
}

}  // namespace iceberg
