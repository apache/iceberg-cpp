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
#include "iceberg/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/transform.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {
static const bool ROWS_MIGHT_MATCH = true;
static const bool ROWS_CANNOT_MATCH = false;
static const int32_t IN_PREDICATE_LIMIT = 200;
}  // namespace

class InclusiveMetricsVisitor : public BoundVisitor<bool> {
 public:
  explicit InclusiveMetricsVisitor(const DataFile& data_file) : data_file_(data_file) {}

  Result<bool> AlwaysTrue() override { return ROWS_MIGHT_MATCH; }

  Result<bool> AlwaysFalse() override { return ROWS_CANNOT_MATCH; }

  Result<bool> Not(bool child_result) override { return !child_result; }

  Result<bool> And(bool left_result, bool right_result) override {
    return left_result && right_result;
  }

  Result<bool> Or(bool left_result, bool right_result) override {
    return left_result || right_result;
  }

  Result<bool> IsNull(const std::shared_ptr<BoundTerm>& term) override {
    if (IsNonNullPreserving(term)) {
      // number of non-nulls is the same as for the ref
      int id = term->reference()->field().field_id();
      if (!MayContainNull(id)) {
        return ROWS_CANNOT_MATCH;
      }
    }
    return ROWS_MIGHT_MATCH;
  }

  Result<bool> NotNull(const std::shared_ptr<BoundTerm>& term) override {
    // no need to check whether the field is required because binding evaluates that case
    // if the column has no non-null values, the expression cannot match

    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term->reference()->field().field_id();
    if (ContainsNullsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  Result<bool> IsNaN(const std::shared_ptr<BoundTerm>& term) override {
    // when there's no nanCounts information, but we already know the column only contains
    // null, it's guaranteed that there's no NaN value
    int id = term->reference()->field().field_id();
    if (ContainsNullsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }
    auto ptr = std::dynamic_pointer_cast<BoundReference>(term);
    if (ptr == nullptr) {
      return ROWS_MIGHT_MATCH;
    }
    if (!data_file_.nan_value_counts.empty() &&
        data_file_.nan_value_counts.contains(id) &&
        data_file_.nan_value_counts.at(id) == 0) {
      return ROWS_CANNOT_MATCH;
    }
    return ROWS_MIGHT_MATCH;
  }

  Result<bool> NotNaN(const std::shared_ptr<BoundTerm>& term) override {
    auto ptr = std::dynamic_pointer_cast<BoundReference>(term);
    if (ptr == nullptr) {
      // identity transforms are already removed by this time
      return ROWS_MIGHT_MATCH;
    }

    int id = term->reference()->field().field_id();

    if (ContainsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  Result<bool> Lt(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }
    auto lower_result = LowerBound(term);
    if (!lower_result.has_value() || lower_result.value().IsNaN()) {
      // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
      return ROWS_MIGHT_MATCH;
    }
    const auto& lower = lower_result.value();

    // this also works for transforms that are order preserving:
    // if a transform f is order preserving, a < b means that f(a) <= f(b).
    // because lower <= a for all values of a in the file, f(lower) <= f(a).
    // when f(lower) >= X then f(a) >= f(lower) >= X, so there is no a such that f(a) < X
    // f(lower) >= X means rows cannot match
    if (lower >= lit) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  Result<bool> LtEq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    auto lower_result = LowerBound(term);
    if (!lower_result.has_value() || lower_result.value().IsNaN()) {
      // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
      return ROWS_MIGHT_MATCH;
    }
    const auto& lower = lower_result.value();

    // this also works for transforms that are order preserving:
    // if a transform f is order preserving, a < b means that f(a) <= f(b).
    // because lower <= a for all values of a in the file, f(lower) <= f(a).
    // when f(lower) > X then f(a) >= f(lower) > X, so there is no a such that f(a) <= X
    // f(lower) > X means rows cannot match
    if (lower > lit) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  Result<bool> Gt(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    auto upper_result = UpperBound(term);
    if (!upper_result.has_value()) {
      return ROWS_MIGHT_MATCH;
    }
    const auto& upper = upper_result.value();

    if (upper <= lit) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  Result<bool> GtEq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    auto upper_result = UpperBound(term);
    if (!upper_result.has_value()) {
      return ROWS_MIGHT_MATCH;
    }
    const auto& upper = upper_result.value();
    if (upper < lit) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  Result<bool> Eq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    auto lower_result = LowerBound(term);
    if (lower_result.has_value()) {
      const auto& lower = lower_result.value();
      if (!lower.IsNaN() && lower > lit) {
        return ROWS_CANNOT_MATCH;
      }
    }

    auto upper_result = UpperBound(term);
    if (!upper_result.has_value()) {
      return ROWS_MIGHT_MATCH;
    }
    const auto& upper = upper_result.value();
    if (upper < lit) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  Result<bool> NotEq(const std::shared_ptr<BoundTerm>& term,
                     const Literal& lit) override {
    // because the bounds are not necessarily a min or max value, this cannot be answered
    // using them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
    return ROWS_MIGHT_MATCH;
  }

  Result<bool> In(const std::shared_ptr<BoundTerm>& term,
                  const BoundSetPredicate::LiteralSet& literal_set) override {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term->reference()->field().field_id();
    if (ContainsNullsOnly(id) || ContainsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    if (literal_set.size() > IN_PREDICATE_LIMIT) {
      // skip evaluating the predicate if the number of values is too big
      return ROWS_MIGHT_MATCH;
    }

    auto lower_result = LowerBound(term);
    if (!lower_result.has_value() || lower_result.value().IsNaN()) {
      // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
      return ROWS_MIGHT_MATCH;
    }
    const auto& lower = lower_result.value();
    std::vector<Literal> literals;
    for (const auto& lit : literal_set) {
      if (lower <= lit) {
        literals.emplace_back(lit);
      }
    }
    // if all values are less than lower bound, rows cannot match
    if (literals.empty()) {
      return ROWS_CANNOT_MATCH;
    }

    auto upper_result = UpperBound(term);
    if (!upper_result.has_value()) {
      return ROWS_MIGHT_MATCH;
    }
    const auto& upper = upper_result.value();
    std::erase_if(literals, [&](const Literal& x) { return x > upper; });
    // if remaining values are greater than upper bound, rows cannot match
    if (literals.empty()) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  Result<bool> NotIn(const std::shared_ptr<BoundTerm>& term,
                     const BoundSetPredicate::LiteralSet& literal_set) override {
    // because the bounds are not necessarily a min or max value, this cannot be answered
    // using them. notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in
    // col.
    return ROWS_MIGHT_MATCH;
  }

  Result<bool> StartsWith(const std::shared_ptr<BoundTerm>& term,
                          const Literal& lit) override {
    if (term->kind() == BoundTerm::Kind::kTransform &&
        std::dynamic_pointer_cast<BoundTransform>(term)->transform()->transform_type() !=
            TransformType::kIdentity) {
      // truncate must be rewritten in binding. the result is either always or never
      // compatible
      return ROWS_MIGHT_MATCH;
    }

    int id = term->reference()->field().field_id();
    if (ContainsNullsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }
    if (lit.type()->type_id() != TypeId::kString) {
      return ROWS_CANNOT_MATCH;
    }
    std::string prefix = get<std::string>(lit.value());

    auto lower_result = LowerBound(term);
    if (!lower_result.has_value()) {
      return ROWS_MIGHT_MATCH;
    }
    const auto& lower = lower_result.value();
    auto lower_str = get<std::string>(lower.value());
    // truncate lower bound so that its length in bytes is not greater than the length of
    // prefix
    int length = std::min(prefix.size(), lower_str.size());
    // if prefix of lower bound is greater than prefix, rows cannot match
    if (lower_str.substr(0, length) > prefix) {
      return ROWS_CANNOT_MATCH;
    }

    auto upper_result = UpperBound(term);
    if (!upper_result.has_value()) {
      return ROWS_MIGHT_MATCH;
    }
    const auto& upper = upper_result.value();
    auto upper_str = get<std::string>(upper.value());
    // truncate upper bound so that its length in bytes is not greater than the length of
    // prefix
    length = std::min(prefix.size(), upper_str.size());
    // if prefix of upper bound is less than prefix, rows cannot match
    if (upper_str.substr(0, length) < prefix) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  Result<bool> NotStartsWith(const std::shared_ptr<BoundTerm>& term,
                             const Literal& lit) override {
    // the only transforms that produce strings are truncate and identity, which work with
    // this
    int id = term->reference()->field().field_id();
    if (MayContainNull(id)) {
      return ROWS_MIGHT_MATCH;
    }

    if (lit.type()->type_id() != TypeId::kString) {
      return ROWS_CANNOT_MATCH;
    }
    std::string prefix = get<std::string>(lit.value());

    // notStartsWith will match unless all values must start with the prefix. This happens
    // when the lower and upper bounds both start with the prefix.
    auto lower_result = LowerBound(term);
    auto upper_result = UpperBound(term);
    if (!lower_result.has_value() || !upper_result.has_value()) {
      return ROWS_MIGHT_MATCH;
    }
    const auto& lower = lower_result.value();
    const auto& upper = upper_result.value();
    auto lower_str = get<std::string>(lower.value());
    auto upper_str = get<std::string>(upper.value());

    // if lower is shorter than the prefix then lower doesn't start with the prefix
    if (lower_str.size() < prefix.size()) {
      return ROWS_MIGHT_MATCH;
    }

    if (lower_str.starts_with(prefix)) {
      // if upper is shorter than the prefix then upper can't start with the prefix
      if (upper_str.size() < prefix.size()) {
        return ROWS_MIGHT_MATCH;
      }
      if (upper_str.starts_with(prefix)) {
        // both bounds match the prefix, so all rows must match the prefix and therefore
        // do not satisfy the predicate
        return ROWS_CANNOT_MATCH;
      }
    }

    return ROWS_MIGHT_MATCH;
  }

 private:
  bool MayContainNull(int32_t id) {
    return data_file_.null_value_counts.empty() ||
           !data_file_.null_value_counts.contains(id) ||
           data_file_.null_value_counts.at(id) != 0;
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

  Result<Literal> LowerBound(const std::shared_ptr<BoundTerm>& term) {
    if (term->kind() == BoundTerm::Kind::kReference) {
      return ParseLowerBound(*std::dynamic_pointer_cast<BoundReference>(term));
    } else if (term->kind() == BoundTerm::Kind::kTransform) {
      return TransformLowerBound(*std::dynamic_pointer_cast<BoundTransform>(term));
    } else if (term->kind() == BoundTerm::Kind::kExtract) {
      // TODO(xiao.dong) handle extract lower and upper bounds
      return NotImplemented("Extract lower bound not implemented.");
    } else {
      return NotFound("Lower bound not found.");
    }
  }

  Result<Literal> UpperBound(const std::shared_ptr<BoundTerm>& term) {
    if (term->kind() == BoundTerm::Kind::kReference) {
      return ParseUpperBound(*std::dynamic_pointer_cast<BoundReference>(term));
    } else if (term->kind() == BoundTerm::Kind::kTransform) {
      return TransformUpperBound(*std::dynamic_pointer_cast<BoundTransform>(term));
    } else if (term->kind() == BoundTerm::Kind::kExtract) {
      // TODO(xiao.dong) handle extract lower and upper bounds
      return NotImplemented("Extract upper bound not implemented.");
    } else {
      return NotFound("Upper bound not found.");
    }
  }

  Result<Literal> ParseLowerBound(const BoundReference& ref) {
    int id = ref.field().field_id();
    auto type = ref.type();
    if (!type->is_primitive()) {
      return InvalidStats("Lower bound of non-primitive type is not supported.");
    }
    auto primitive_type = std::dynamic_pointer_cast<PrimitiveType>(type);
    if (!data_file_.lower_bounds.empty() && data_file_.lower_bounds.contains(id)) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto lower,
          Literal::Deserialize(data_file_.lower_bounds.at(id), primitive_type));
      return lower;
    }

    return NotFound("Lower bound not found.");
  }

  Result<Literal> ParseUpperBound(const BoundReference& ref) {
    int id = ref.field().field_id();
    auto type = ref.type();
    if (!type->is_primitive()) {
      return InvalidStats("Upper bound of non-primitive type is not supported.");
    }
    auto primitive_type = std::dynamic_pointer_cast<PrimitiveType>(type);
    if (!data_file_.upper_bounds.empty() && data_file_.upper_bounds.contains(id)) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto upper,
          Literal::Deserialize(data_file_.upper_bounds.at(id), primitive_type));
      return upper;
    }

    return NotFound("Upper bound not found.");
  }

  Result<Literal> TransformLowerBound(BoundTransform& boundTransform) {
    auto transform = boundTransform.transform();
    if (transform->PreservesOrder()) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower, ParseLowerBound(*boundTransform.reference()));
      ICEBERG_ASSIGN_OR_RAISE(auto transform_func,
                              transform->Bind(boundTransform.reference()->type()));
      return transform_func->Transform(lower);
    }

    return NotFound("Transform lower bound not found.");
  }

  Result<Literal> TransformUpperBound(BoundTransform& boundTransform) {
    auto transform = boundTransform.transform();
    if (transform->PreservesOrder()) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper, ParseLowerBound(*boundTransform.reference()));
      ICEBERG_ASSIGN_OR_RAISE(auto transform_func,
                              transform->Bind(boundTransform.reference()->type()));
      return transform_func->Transform(upper);
    }

    return NotFound("Transform upper bound not found.");
  }

  // TODO(xiao.dong) handle extract lower and upper bounds
  /*
  Literal extractLowerBound(const BoundExtract& bound) {

  }

  Literal extractUpperBound(const BoundExtract& bound) {

  }
*/

  /** Returns true if the expression term produces a non-null value for non-null input. */
  bool IsNonNullPreserving(const std::shared_ptr<BoundTerm>& term) {
    if (term->kind() == BoundTerm::Kind::kReference) {
      return true;
    } else if (term->kind() == BoundTerm::Kind::kTransform) {
      return std::dynamic_pointer_cast<BoundTransform>(term)
          ->transform()
          ->PreservesOrder();
    }
    //  a non-null variant does not necessarily contain a specific field
    //  and unknown bound terms are not non-null preserving
    return false;
  }

 private:
  const DataFile& data_file_;
};

InclusiveMetricsEvaluator::InclusiveMetricsEvaluator(
    const std::shared_ptr<Expression>& expr)
    : expr_(std::move(expr)) {}

InclusiveMetricsEvaluator::~InclusiveMetricsEvaluator() = default;

Result<std::unique_ptr<InclusiveMetricsEvaluator>> InclusiveMetricsEvaluator::Make(
    std::shared_ptr<Expression> expr, const std::shared_ptr<Schema>& schema,
    bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto rewrite_expr, RewriteNot::Visit(std::move(expr)));
  ICEBERG_ASSIGN_OR_RAISE(auto bound_expr,
                          Binder::Bind(*schema, rewrite_expr, case_sensitive));
  return std::unique_ptr<InclusiveMetricsEvaluator>(
      new InclusiveMetricsEvaluator(std::move(bound_expr)));
}

Result<bool> InclusiveMetricsEvaluator::Eval(const DataFile& data_file) const {
  if (data_file.record_count <= 0) {
    return ROWS_CANNOT_MATCH;
  }
  InclusiveMetricsVisitor visitor(data_file);
  return Visit<bool, InclusiveMetricsVisitor>(expr_, visitor);
}

}  // namespace iceberg
