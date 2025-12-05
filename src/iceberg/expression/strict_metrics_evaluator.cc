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

#include "iceberg/expression/strict_metrics_evaluator.h"

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/expression/term.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {
constexpr bool kRowsMustMatch = true;
constexpr bool kRowsMightNotMatch = false;
}  // namespace

// If the term in any expression is not a direct reference, assume that rows may not
// match. This happens when transforms or other expressions are passed to this evaluator.
// For example, bucket16(x) = 0 can't be determined because this visitor operates on data
// metrics and not partition values. It may be possible to un-transform expressions for
// order preserving transforms in the future, but this is not currently supported.
#define RETURN_IF_NOT_REFERENCE(expr)                                         \
  if (auto ref = dynamic_cast<BoundReference*>(expr.get()); ref == nullptr) { \
    return kRowsMightNotMatch;                                                \
  }

class StrictMetricsVisitor : public BoundVisitor<bool> {
 public:
  explicit StrictMetricsVisitor(const DataFile& data_file, const Schema& schema)
      : data_file_(data_file), schema_(schema) {}

  Result<bool> AlwaysTrue() override { return kRowsMustMatch; }

  Result<bool> AlwaysFalse() override { return kRowsMightNotMatch; }

  Result<bool> Not(bool child_result) override { return !child_result; }

  Result<bool> And(bool left_result, bool right_result) override {
    return left_result && right_result;
  }

  Result<bool> Or(bool left_result, bool right_result) override {
    return left_result || right_result;
  }

  Result<bool> IsNull(const std::shared_ptr<Bound>& expr) override {
    RETURN_IF_NOT_REFERENCE(expr);

    // no need to check whether the field is required because binding evaluates that case
    // if the column has any non-null values, the expression does not match
    int32_t id = expr->reference()->field().field_id();

    ICEBERG_ASSIGN_OR_RAISE(auto is_nested, IsNestedColumn(id));
    if (is_nested) {
      return kRowsMightNotMatch;
    }

    if (ContainsNullsOnly(id)) {
      return kRowsMustMatch;
    }
    return kRowsMightNotMatch;
  }

  Result<bool> NotNull(const std::shared_ptr<Bound>& expr) override {
    RETURN_IF_NOT_REFERENCE(expr);

    // no need to check whether the field is required because binding evaluates that case
    // if the column has any null values, the expression does not match
    int32_t id = expr->reference()->field().field_id();

    ICEBERG_ASSIGN_OR_RAISE(auto is_nested, IsNestedColumn(id));
    if (is_nested) {
      return kRowsMightNotMatch;
    }

    auto it = data_file_.null_value_counts.find(id);
    if (it != data_file_.null_value_counts.cend() && it->second == 0) {
      return kRowsMustMatch;
    }

    return kRowsMightNotMatch;
  }

  Result<bool> IsNaN(const std::shared_ptr<Bound>& expr) override {
    RETURN_IF_NOT_REFERENCE(expr);

    int32_t id = expr->reference()->field().field_id();

    if (ContainsNaNsOnly(id)) {
      return kRowsMustMatch;
    }

    return kRowsMightNotMatch;
  }

  Result<bool> NotNaN(const std::shared_ptr<Bound>& expr) override {
    RETURN_IF_NOT_REFERENCE(expr);

    int32_t id = expr->reference()->field().field_id();

    auto it = data_file_.nan_value_counts.find(id);
    if (it != data_file_.nan_value_counts.cend() && it->second == 0) {
      return kRowsMustMatch;
    }

    if (ContainsNullsOnly(id)) {
      return kRowsMustMatch;
    }

    return kRowsMightNotMatch;
  }

  Result<bool> Lt(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    RETURN_IF_NOT_REFERENCE(expr);

    // Rows must match when: <----------Min----Max---X------->
    int32_t id = expr->reference()->field().field_id();

    ICEBERG_ASSIGN_OR_RAISE(auto is_nested, IsNestedColumn(id));
    if (is_nested) {
      return kRowsMightNotMatch;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return kRowsMightNotMatch;
    }

    auto it = data_file_.upper_bounds.find(id);
    if (it != data_file_.upper_bounds.cend()) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper, ParseBound(expr, it->second));
      if (upper < lit) {
        return kRowsMustMatch;
      }
    }

    return kRowsMightNotMatch;
  }

  Result<bool> LtEq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    RETURN_IF_NOT_REFERENCE(expr);

    // Rows must match when: <----------Min----Max---X------->
    int32_t id = expr->reference()->field().field_id();

    ICEBERG_ASSIGN_OR_RAISE(auto is_nested, IsNestedColumn(id));
    if (is_nested) {
      return kRowsMightNotMatch;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return kRowsMightNotMatch;
    }

    auto it = data_file_.upper_bounds.find(id);
    if (it != data_file_.upper_bounds.cend()) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper, ParseBound(expr, it->second));
      if (upper <= lit) {
        return kRowsMustMatch;
      }
    }

    return kRowsMightNotMatch;
  }

  Result<bool> Gt(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    RETURN_IF_NOT_REFERENCE(expr);

    // Rows must match when: <-------X---Min----Max---------->
    int32_t id = expr->reference()->field().field_id();

    ICEBERG_ASSIGN_OR_RAISE(auto is_nested, IsNestedColumn(id));
    if (is_nested) {
      return kRowsMightNotMatch;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return kRowsMightNotMatch;
    }

    auto it = data_file_.lower_bounds.find(id);
    if (it != data_file_.lower_bounds.cend()) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower, ParseBound(expr, it->second));
      if (lower.IsNaN()) {
        // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for
        // more.
        return kRowsMightNotMatch;
      }

      if (lower > lit) {
        return kRowsMustMatch;
      }
    }

    return kRowsMightNotMatch;
  }

  Result<bool> GtEq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    RETURN_IF_NOT_REFERENCE(expr);

    // Rows must match when: <-------X---Min----Max---------->
    int32_t id = expr->reference()->field().field_id();

    ICEBERG_ASSIGN_OR_RAISE(auto is_nested, IsNestedColumn(id));
    if (is_nested) {
      return kRowsMightNotMatch;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return kRowsMightNotMatch;
    }

    auto it = data_file_.lower_bounds.find(id);
    if (it != data_file_.lower_bounds.cend()) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower, ParseBound(expr, it->second));
      if (lower.IsNaN()) {
        // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for
        // more.
        return kRowsMightNotMatch;
      }

      if (lower >= lit) {
        return kRowsMustMatch;
      }
    }

    return kRowsMightNotMatch;
  }

  Result<bool> Eq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    RETURN_IF_NOT_REFERENCE(expr);

    // Rows must match when Min == X == Max
    int32_t id = expr->reference()->field().field_id();

    ICEBERG_ASSIGN_OR_RAISE(auto is_nested, IsNestedColumn(id));
    if (is_nested) {
      return kRowsMightNotMatch;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return kRowsMightNotMatch;
    }
    auto lower_it = data_file_.lower_bounds.find(id);
    auto upper_it = data_file_.upper_bounds.find(id);
    if (lower_it != data_file_.lower_bounds.cend() &&
        upper_it != data_file_.upper_bounds.cend()) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower, ParseBound(expr, lower_it->second));
      if (lower != lit) {
        return kRowsMightNotMatch;
      }
      ICEBERG_ASSIGN_OR_RAISE(auto upper, ParseBound(expr, upper_it->second));
      if (upper != lit) {
        return kRowsMightNotMatch;
      }

      return kRowsMustMatch;
    }

    return kRowsMightNotMatch;
  }

  Result<bool> NotEq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    RETURN_IF_NOT_REFERENCE(expr);

    // Rows must match when X < Min or Max < X because it is not in the range
    int32_t id = expr->reference()->field().field_id();

    ICEBERG_ASSIGN_OR_RAISE(auto is_nested, IsNestedColumn(id));
    if (is_nested) {
      return kRowsMightNotMatch;
    }

    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return kRowsMustMatch;
    }

    auto lower_it = data_file_.lower_bounds.find(id);
    if (lower_it != data_file_.lower_bounds.cend()) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower, ParseBound(expr, lower_it->second));
      if (lower.IsNaN()) {
        // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for
        // more.
        return kRowsMightNotMatch;
      }
      if (lower > lit) {
        return kRowsMustMatch;
      }
    }

    auto upper_it = data_file_.upper_bounds.find(id);
    if (upper_it != data_file_.upper_bounds.cend()) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper, ParseBound(expr, upper_it->second));
      if (upper < lit) {
        return kRowsMustMatch;
      }
    }

    return kRowsMightNotMatch;
  }

  Result<bool> In(const std::shared_ptr<Bound>& expr,
                  const BoundSetPredicate::LiteralSet& literal_set) override {
    RETURN_IF_NOT_REFERENCE(expr);

    int32_t id = expr->reference()->field().field_id();

    ICEBERG_ASSIGN_OR_RAISE(auto is_nested, IsNestedColumn(id));
    if (is_nested) {
      return kRowsMightNotMatch;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return kRowsMightNotMatch;
    }
    auto lower_it = data_file_.lower_bounds.find(id);
    auto upper_it = data_file_.upper_bounds.find(id);
    if (lower_it != data_file_.lower_bounds.cend() &&
        upper_it != data_file_.upper_bounds.cend()) {
      // similar to the implementation in eq, first check if the lower bound is in the
      // set
      ICEBERG_ASSIGN_OR_RAISE(auto lower, ParseBound(expr, lower_it->second));
      if (!literal_set.contains(lower)) {
        return kRowsMightNotMatch;
      }
      // check if the upper bound is in the set
      ICEBERG_ASSIGN_OR_RAISE(auto upper, ParseBound(expr, upper_it->second));
      if (!literal_set.contains(upper)) {
        return kRowsMightNotMatch;
      }
      // finally check if the lower bound and the upper bound are equal
      if (lower != upper) {
        return kRowsMightNotMatch;
      }

      // All values must be in the set if the lower bound and the upper bound are in the
      // set and are equal.
      return kRowsMustMatch;
    }

    return kRowsMightNotMatch;
  }

  Result<bool> NotIn(const std::shared_ptr<Bound>& expr,
                     const BoundSetPredicate::LiteralSet& literal_set) override {
    RETURN_IF_NOT_REFERENCE(expr);

    int32_t id = expr->reference()->field().field_id();

    ICEBERG_ASSIGN_OR_RAISE(auto is_nested, IsNestedColumn(id));
    if (is_nested) {
      return kRowsMightNotMatch;
    }

    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return kRowsMustMatch;
    }
    std::optional<Literal> lower_bound;
    auto lower_it = data_file_.lower_bounds.find(id);
    if (lower_it != data_file_.lower_bounds.cend()) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower, ParseBound(expr, lower_it->second));
      if (lower.IsNaN()) {
        // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for
        // more.
        return kRowsMightNotMatch;
      }
      lower_bound = std::move(lower);
    }
    auto literals_view = literal_set | std::views::filter([&](const Literal& lit) {
                           return lower_bound.has_value() && lower_bound.value() <= lit;
                         });
    // if all values are less than lower bound, rows must
    // match (notIn).
    if (lower_bound.has_value() && literals_view.empty()) {
      return kRowsMustMatch;
    }

    auto upper_it = data_file_.upper_bounds.find(id);
    if (upper_it != data_file_.upper_bounds.cend()) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper, ParseBound(expr, upper_it->second));
      auto filtered_view = literals_view | std::views::filter([&](const Literal& lit) {
                             return upper >= lit;
                           });
      if (filtered_view.empty()) {
        // if all remaining values are greater than upper bound,
        // rows must match
        // (notIn).
        return kRowsMustMatch;
      }
    }
    return kRowsMightNotMatch;
  }

  Result<bool> StartsWith(const std::shared_ptr<Bound>& expr,
                          const Literal& lit) override {
    return kRowsMightNotMatch;
  }

  Result<bool> NotStartsWith(const std::shared_ptr<Bound>& expr,
                             const Literal& lit) override {
    // TODO(xiao.dong) Handle cases that definitely cannot match,
    // such as notStartsWith("x") when
    // the bounds are ["a", "b"].
    return kRowsMightNotMatch;
  }

 private:
  Result<Literal> ParseBound(const std::shared_ptr<Bound>& expr,
                             const std::vector<uint8_t>& stats) {
    auto type = expr->reference()->type();
    if (!type->is_primitive()) {
      return NotSupported("Bound of non-primitive type is not supported.");
    }
    auto primitive_type = internal::checked_pointer_cast<PrimitiveType>(type);
    return Literal::Deserialize(stats, primitive_type);
  }

  bool CanContainNulls(int32_t id) {
    if (data_file_.null_value_counts.empty()) {
      return true;
    }
    auto it = data_file_.null_value_counts.find(id);
    return it != data_file_.null_value_counts.cend() && it->second > 0;
  }

  bool CanContainNaNs(int32_t id) {
    // nan counts might be null for early version writers when nan counters are not
    // populated.
    auto it = data_file_.nan_value_counts.find(id);
    return it != data_file_.nan_value_counts.cend() && it->second > 0;
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

  Result<bool> IsNestedColumn(int32_t id) {
    // XXX: null_count might be missing from nested columns but required  by
    // StrictMetricsEvaluator.
    // See https://github.com/apache/iceberg/pull/11261.
    ICEBERG_ASSIGN_OR_RAISE(auto field, schema_.GetFieldById(id));
    return !field.has_value() || field->get().type()->is_nested();
  }

 private:
  const DataFile& data_file_;
  const Schema& schema_;
};

StrictMetricsEvaluator::StrictMetricsEvaluator(std::shared_ptr<Expression> expr,
                                               std::shared_ptr<Schema> schema)
    : expr_(std::move(expr)), schema_(std::move(schema)) {}

StrictMetricsEvaluator::~StrictMetricsEvaluator() = default;

Result<std::unique_ptr<StrictMetricsEvaluator>> StrictMetricsEvaluator::Make(
    std::shared_ptr<Expression> expr, std::shared_ptr<Schema> schema,
    bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto rewrite_expr, RewriteNot::Visit(std::move(expr)));
  ICEBERG_ASSIGN_OR_RAISE(auto bound_expr,
                          Binder::Bind(*schema, rewrite_expr, case_sensitive));
  return std::unique_ptr<StrictMetricsEvaluator>(
      new StrictMetricsEvaluator(std::move(bound_expr), std::move(schema)));
}

Result<bool> StrictMetricsEvaluator::Evaluate(const DataFile& data_file) const {
  if (data_file.record_count <= 0) {
    return kRowsMustMatch;
  }
  StrictMetricsVisitor visitor(data_file, *schema_);
  return Visit<bool, StrictMetricsVisitor>(expr_, visitor);
}

}  // namespace iceberg
