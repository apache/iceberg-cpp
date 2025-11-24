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
#include "iceberg/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {
static const bool ROWS_MUST_MATCH = true;
static const bool ROWS_MIGHT_NOT_MATCH = false;
}  // namespace

class StrictMetricsVisitor : public BoundVisitor<bool> {
 public:
  explicit StrictMetricsVisitor(const DataFile& data_file, std::shared_ptr<Schema> schema)
      : data_file_(data_file), schema_(std::move(schema)) {}

  // TODO(xiao.dong) handleNonReference not implement

  Result<bool> AlwaysTrue() override { return ROWS_MUST_MATCH; }

  Result<bool> AlwaysFalse() override { return ROWS_MIGHT_NOT_MATCH; }

  Result<bool> Not(bool child_result) override { return !child_result; }

  Result<bool> And(bool left_result, bool right_result) override {
    return left_result && right_result;
  }

  Result<bool> Or(bool left_result, bool right_result) override {
    return left_result || right_result;
  }

  Result<bool> IsNull(const std::shared_ptr<BoundTerm>& term) override {
    int id = term->reference()->field().field_id();
    if (IsNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }
    if (ContainsNullsOnly(id)) {
      return ROWS_MUST_MATCH;
    }
    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> NotNull(const std::shared_ptr<BoundTerm>& term) override {
    // no need to check whether the field is required because binding evaluates that case
    // if the column has any null values, the expression does not match
    int id = term->reference()->field().field_id();
    if (IsNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (!data_file_.null_value_counts.empty() &&
        data_file_.null_value_counts.contains(id) &&
        data_file_.null_value_counts.at(id) == 0) {
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> IsNaN(const std::shared_ptr<BoundTerm>& term) override {
    int id = term->reference()->field().field_id();

    if (ContainsNaNsOnly(id)) {
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> NotNaN(const std::shared_ptr<BoundTerm>& term) override {
    int id = term->reference()->field().field_id();

    if (!data_file_.nan_value_counts.empty() &&
        data_file_.nan_value_counts.contains(id) &&
        data_file_.nan_value_counts.at(id) == 0) {
      return ROWS_MUST_MATCH;
    }

    if (ContainsNullsOnly(id)) {
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> Lt(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // Rows must match when: <----------Min----Max---X------->
    int id = term->reference()->field().field_id();
    if (IsNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (!data_file_.upper_bounds.empty() && data_file_.upper_bounds.contains(id)) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper,
                              ParseBound(term, data_file_.upper_bounds.at(id)));
      if (upper < lit) {
        return ROWS_MUST_MATCH;
      }
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> LtEq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // Rows must match when: <----------Min----Max---X------->
    int id = term->reference()->field().field_id();
    if (IsNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (!data_file_.upper_bounds.empty() && data_file_.upper_bounds.contains(id)) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper,
                              ParseBound(term, data_file_.upper_bounds.at(id)));

      if (upper <= lit) {
        return ROWS_MUST_MATCH;
      }
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> Gt(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // Rows must match when: <-------X---Min----Max---------->
    int id = term->reference()->field().field_id();
    if (IsNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (!data_file_.lower_bounds.empty() && data_file_.lower_bounds.contains(id)) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower,
                              ParseBound(term, data_file_.lower_bounds.at(id)));

      if (lower.IsNaN()) {
        // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for
        // more.
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (lower > lit) {
        return ROWS_MUST_MATCH;
      }
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> GtEq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // Rows must match when: <-------X---Min----Max---------->
    int id = term->reference()->field().field_id();
    if (IsNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (!data_file_.lower_bounds.empty() && data_file_.lower_bounds.contains(id)) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower,
                              ParseBound(term, data_file_.lower_bounds.at(id)));

      if (lower.IsNaN()) {
        // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for
        // more.
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (lower >= lit) {
        return ROWS_MUST_MATCH;
      }
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> Eq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // Rows must match when Min == X == Max
    int id = term->reference()->field().field_id();
    if (IsNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (!data_file_.lower_bounds.empty() && data_file_.lower_bounds.contains(id) &&
        !data_file_.upper_bounds.empty() && data_file_.upper_bounds.contains(id)) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower,
                              ParseBound(term, data_file_.lower_bounds.at(id)));

      if (lower != lit) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      ICEBERG_ASSIGN_OR_RAISE(auto upper,
                              ParseBound(term, data_file_.upper_bounds.at(id)));

      if (upper != lit) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> NotEq(const std::shared_ptr<BoundTerm>& term,
                     const Literal& lit) override {
    // Rows must match when X < Min or Max < X because it is not in the range
    int id = term->reference()->field().field_id();
    if (IsNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return ROWS_MUST_MATCH;
    }

    if (!data_file_.lower_bounds.empty() && data_file_.lower_bounds.contains(id)) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower,
                              ParseBound(term, data_file_.lower_bounds.at(id)));

      if (lower.IsNaN()) {
        // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for
        // more.
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (lower > lit) {
        return ROWS_MUST_MATCH;
      }
    }

    if (!data_file_.upper_bounds.empty() && data_file_.upper_bounds.contains(id)) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper,
                              ParseBound(term, data_file_.upper_bounds.at(id)));

      if (upper < lit) {
        return ROWS_MUST_MATCH;
      }
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> In(const std::shared_ptr<BoundTerm>& term,
                  const BoundSetPredicate::LiteralSet& literal_set) override {
    int id = term->reference()->field().field_id();
    if (IsNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (CanContainNulls(id) || CanContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (!data_file_.lower_bounds.empty() && data_file_.lower_bounds.contains(id) &&
        !data_file_.upper_bounds.empty() && data_file_.upper_bounds.contains(id)) {
      // similar to the implementation in eq, first check if the lower bound is in the
      // set
      ICEBERG_ASSIGN_OR_RAISE(auto lower,
                              ParseBound(term, data_file_.lower_bounds.at(id)));

      if (!literal_set.contains(lower)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      // check if the upper bound is in the set
      ICEBERG_ASSIGN_OR_RAISE(auto upper,
                              ParseBound(term, data_file_.upper_bounds.at(id)));
      if (!literal_set.contains(upper)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      // finally check if the lower bound and the upper bound are equal
      if (lower != upper) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      // All values must be in the set if the lower bound and the upper bound are in the
      // set and are equal.
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> NotIn(const std::shared_ptr<BoundTerm>& term,
                     const BoundSetPredicate::LiteralSet& literal_set) override {
    int id = term->reference()->field().field_id();
    if (IsNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return ROWS_MUST_MATCH;
    }
    std::vector<Literal> literals;
    if (!data_file_.lower_bounds.empty() && data_file_.lower_bounds.contains(id)) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower,
                              ParseBound(term, data_file_.lower_bounds.at(id)));

      if (lower.IsNaN()) {
        // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for
        // more.
        return ROWS_MIGHT_NOT_MATCH;
      }

      for (const auto& lit : literal_set) {
        if (lit >= lower) {
          literals.emplace_back(lit);
        }
      }
      // if all values are less than lower bound, rows must
      // match (notIn).
      if (literals.empty()) {
        return ROWS_MUST_MATCH;
      }
    }

    if (!data_file_.upper_bounds.empty() && data_file_.upper_bounds.contains(id)) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper,
                              ParseBound(term, data_file_.upper_bounds.at(id)));
      std::erase_if(literals, [&](const Literal& x) { return x > upper; });
      if (literals.empty()) {
        // if all remaining values are greater than upper bound,
        // rows must match
        // (notIn).
        return ROWS_MUST_MATCH;
      }
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> StartsWith(const std::shared_ptr<BoundTerm>& term,
                          const Literal& lit) override {
    return ROWS_MIGHT_NOT_MATCH;
  }

  Result<bool> NotStartsWith(const std::shared_ptr<BoundTerm>& term,
                             const Literal& lit) override {
    // TODO(xiao.dong) Handle cases that definitely cannot match,
    // such as notStartsWith("x") when
    // the bounds are ["a", "b"].
    return ROWS_MIGHT_NOT_MATCH;
  }

 private:
  Result<Literal> ParseBound(const std::shared_ptr<BoundTerm>& term,
                             const std::vector<uint8_t>& stats) {
    auto type = term->reference()->type();
    if (!type->is_primitive()) {
      return InvalidStats("Bound of non-primitive type is not supported.");
    }
    auto primitive_type = std::dynamic_pointer_cast<PrimitiveType>(type);
    ICEBERG_ASSIGN_OR_RAISE(auto bound, Literal::Deserialize(stats, primitive_type));
    return bound;
  }

  bool CanContainNulls(int32_t id) {
    return data_file_.null_value_counts.empty() ||
           (data_file_.null_value_counts.contains(id) &&
            data_file_.null_value_counts.at(id) > 0);
  }

  bool CanContainNaNs(int32_t id) {
    // nan counts might be null for early version writers when nan counters are not
    // populated.
    return !data_file_.nan_value_counts.empty() &&
           data_file_.nan_value_counts.contains(id) &&
           data_file_.nan_value_counts.at(id) > 0;
  }

  bool ContainsNullsOnly(int32_t id) {
    return !data_file_.value_counts.empty() && data_file_.value_counts.contains(id) &&
           !data_file_.null_value_counts.empty() &&
           data_file_.null_value_counts.contains(id) &&
           data_file_.value_counts.at(id) - data_file_.null_value_counts.at(id) == 0;
  }

  bool ContainsNaNsOnly(int32_t id) {
    return !data_file_.nan_value_counts.empty() &&
           data_file_.nan_value_counts.contains(id) && !data_file_.value_counts.empty() &&
           data_file_.value_counts.at(id) == data_file_.nan_value_counts.at(id);
  }

  bool IsNestedColumn(int id) {
    auto field = schema_->GetFieldById(id);
    return !field.has_value() || !field.value().has_value() ||
           field.value()->get().type()->is_nested();
  }

 private:
  const DataFile& data_file_;
  std::shared_ptr<Schema> schema_;
};

StrictMetricsEvaluator::StrictMetricsEvaluator(const std::shared_ptr<Expression>& expr,
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

Result<bool> StrictMetricsEvaluator::Eval(const DataFile& data_file) const {
  if (data_file.record_count <= 0) {
    return ROWS_MIGHT_NOT_MATCH;
  }
  StrictMetricsVisitor visitor(data_file, schema_);
  return Visit<bool, StrictMetricsVisitor>(expr_, visitor);
}

}  // namespace iceberg
