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

#include <algorithm>
#include <compare>
#include <functional>
#include <optional>
#include <unordered_map>

#include <parquet/statistics.h>

#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_metrics_internal.h"
#include "iceberg/parquet/parquet_metrics_row_group_filter_internal.h"
#include "iceberg/parquet/parquet_schema_util_internal.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg::parquet {

namespace {

constexpr size_t kInPredicateLimit = 200;
// True means a matching row may exist; false means the group can be skipped.
constexpr bool kRowsMightMatch = true;
constexpr bool kRowsCannotMatch = false;

class MetricsVisitor : public BoundVisitor<bool> {
 public:
  MetricsVisitor(const ::parquet::arrow::SchemaManifest& manifest,
                 const ::parquet::RowGroupMetaData& row_group,
                 const std::unordered_map<int32_t, int>& field_id_to_column_idx)
      : manifest_(manifest),
        row_group_(row_group),
        field_id_to_column_idx_(field_id_to_column_idx) {}

  Result<bool> AlwaysTrue() override { return kRowsMightMatch; }

  Result<bool> AlwaysFalse() override { return kRowsCannotMatch; }

  Result<bool> Not(bool) override {
    // Make() rewrites NOT before evaluation. If one remains, keep the group:
    // negating a "might match" result cannot prove that no rows match.
    return kRowsMightMatch;
  }

  Result<bool> And(bool left, bool right) override { return left && right; }

  Result<bool> Or(bool left, bool right) override { return left || right; }

  Result<bool> IsNull(const std::shared_ptr<Bound>& expr) override {
    return MayContainNull(std::dynamic_pointer_cast<BoundReference>(expr));
  }

  Result<bool> NotNull(const std::shared_ptr<Bound>& expr) override {
    if (ContainsNullsOnly(std::dynamic_pointer_cast<BoundReference>(expr))) {
      return kRowsCannotMatch;
    }
    return kRowsMightMatch;
  }

  Result<bool> IsNaN(const std::shared_ptr<Bound>& expr) override {
    if (ContainsNullsOnly(std::dynamic_pointer_cast<BoundReference>(expr))) {
      return kRowsCannotMatch;
    }
    return kRowsMightMatch;
  }

  Result<bool> NotNaN(const std::shared_ptr<Bound>&) override { return kRowsMightMatch; }

  Result<bool> Lt(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    return VisitInequality(std::dynamic_pointer_cast<BoundReference>(expr), value,
                           std::less<Literal>{}, /*use_lower_bound=*/true);
  }

  Result<bool> LtEq(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    return VisitInequality(std::dynamic_pointer_cast<BoundReference>(expr), value,
                           std::less_equal<Literal>{}, /*use_lower_bound=*/true);
  }

  Result<bool> Gt(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    return VisitInequality(std::dynamic_pointer_cast<BoundReference>(expr), value,
                           std::greater<Literal>{}, /*use_lower_bound=*/false);
  }

  Result<bool> GtEq(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    return VisitInequality(std::dynamic_pointer_cast<BoundReference>(expr), value,
                           std::greater_equal<Literal>{}, /*use_lower_bound=*/false);
  }

  Result<bool> Eq(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    const auto ref = std::dynamic_pointer_cast<BoundReference>(expr);
    if (ContainsNullsOnly(ref)) {
      return kRowsCannotMatch;
    }
    const auto lower = MinValue(ref);
    if (!lower) {
      return kRowsMightMatch;
    }
    if (*lower > value) {
      return kRowsCannotMatch;
    }
    const auto upper = MaxValue(ref);
    if (!upper) {
      return kRowsMightMatch;
    }
    return !(*upper < value);
  }

  Result<bool> NotEq(const std::shared_ptr<Bound>&, const Literal&) override {
    // Like Java, keep negative membership predicates inclusive.
    return kRowsMightMatch;
  }

  Result<bool> In(const std::shared_ptr<Bound>& expr,
                  const BoundSetPredicate::LiteralSet& values) override {
    const auto ref = std::dynamic_pointer_cast<BoundReference>(expr);
    if (ContainsNullsOnly(ref)) {
      return kRowsCannotMatch;
    }
    if (values.size() > kInPredicateLimit) {
      return kRowsMightMatch;
    }
    const auto lower = MinValue(ref);
    if (!lower) {
      return kRowsMightMatch;
    }
    if (std::ranges::all_of(values, [&](const auto& value) { return value < *lower; })) {
      return kRowsCannotMatch;
    }
    const auto upper = MaxValue(ref);
    if (!upper) {
      return kRowsMightMatch;
    }
    // Like Java, a single candidate must satisfy both bounds.
    for (const auto& value : values) {
      if (!(value < *lower || value > *upper)) {
        return kRowsMightMatch;
      }
    }
    return kRowsCannotMatch;
  }

  Result<bool> NotIn(const std::shared_ptr<Bound>&,
                     const BoundSetPredicate::LiteralSet&) override {
    return kRowsMightMatch;
  }

  Result<bool> StartsWith(const std::shared_ptr<Bound>& expr,
                          const Literal& value) override {
    const auto ref = std::dynamic_pointer_cast<BoundReference>(expr);
    if (ContainsNullsOnly(ref)) {
      return kRowsCannotMatch;
    }
    const auto lower = MinValue(ref);
    if (!lower) {
      return kRowsMightMatch;
    }
    const auto& prefix = std::get<std::string>(value.value());
    if (std::get<std::string>(lower->value()).compare(0, prefix.size(), prefix) > 0) {
      return kRowsCannotMatch;
    }
    const auto upper = MaxValue(ref);
    if (!upper) {
      return kRowsMightMatch;
    }
    return std::get<std::string>(upper->value()).compare(0, prefix.size(), prefix) >= 0;
  }

  Result<bool> NotStartsWith(const std::shared_ptr<Bound>& expr,
                             const Literal& value) override {
    const auto ref = std::dynamic_pointer_cast<BoundReference>(expr);
    if (MayContainNull(ref)) {
      return kRowsMightMatch;
    }
    const auto lower = MinValue(ref);
    if (!lower) {
      return kRowsMightMatch;
    }
    const auto& prefix = std::get<std::string>(value.value());
    if (!std::get<std::string>(lower->value()).starts_with(prefix)) {
      return kRowsMightMatch;
    }
    const auto upper = MaxValue(ref);
    if (!upper) {
      return kRowsMightMatch;
    }
    return !std::get<std::string>(upper->value()).starts_with(prefix);
  }

 private:
  bool ContainsNullsOnly(const std::shared_ptr<BoundReference>& ref) const {
    const auto stats = GetStatistics(ref);
    if (!stats || !stats->HasNullCount()) {
      return false;
    }
    const auto column =
        row_group_.ColumnChunk(field_id_to_column_idx_.at(ref->field_id()));
    return stats->null_count() == column->num_values();
  }

  bool MayContainNull(const std::shared_ptr<BoundReference>& ref) const {
    const auto stats = GetStatistics(ref);
    return !stats || !stats->HasNullCount() || stats->null_count() != 0;
  }

  std::shared_ptr<::parquet::Statistics> GetStatistics(
      const std::shared_ptr<BoundReference>& ref) const {
    if (!ref || !ref->type()->is_primitive() ||
        MetadataColumns::IsMetadataColumn(ref->field_id())) {
      return nullptr;
    }
    auto column = field_id_to_column_idx_.find(ref->field_id());
    // Missing columns can have initial defaults, so do not assume all nulls.
    if (column == field_id_to_column_idx_.end()) {
      return nullptr;
    }
    auto field = manifest_.column_index_to_field.find(column->second);
    if (field == manifest_.column_index_to_field.end() ||
        !ValidateParquetTypeCompatibility(*ref->type(), *field->second)) {
      return nullptr;
    }
    return row_group_.ColumnChunk(column->second)->statistics();
  }

  std::optional<Literal> MinValue(const std::shared_ptr<BoundReference>& ref) const {
    return GetBound(ref, /*is_min=*/true);
  }

  std::optional<Literal> MaxValue(const std::shared_ptr<BoundReference>& ref) const {
    return GetBound(ref, /*is_min=*/false);
  }

  std::optional<Literal> GetBound(const std::shared_ptr<BoundReference>& ref,
                                  bool is_min) const {
    const auto stats = GetStatistics(ref);
    if (!stats || !stats->HasMinMax()) {
      return std::nullopt;
    }
    const auto& type = static_cast<const PrimitiveType&>(*ref->type());
    // Parquet floating-point bounds can exclude NaNs.
    // Treat both bounds as undefined, preserving null-count pruning.
    if (type.type_id() == TypeId::kFloat || type.type_id() == TypeId::kDouble) {
      return std::nullopt;
    }
    auto result =
        ParquetMetrics::StatsValueToLiteral(*stats->descr(), type, *stats, is_min);
    if (!result || result->IsNaN()) {
      return std::nullopt;
    }
    return std::move(*result);
  }

  template <typename Comparator>
  bool VisitInequality(const std::shared_ptr<BoundReference>& ref, const Literal& value,
                       Comparator compare, bool lower_bound) const {
    if (ContainsNullsOnly(ref)) {
      return kRowsCannotMatch;
    }
    if (value.IsNaN()) {
      return kRowsMightMatch;
    }
    const auto bound = lower_bound ? MinValue(ref) : MaxValue(ref);
    if (!bound) {
      return kRowsMightMatch;
    }
    // An unordered comparison cannot rule out matching rows.
    if ((*bound <=> value) == std::partial_ordering::unordered) {
      return kRowsMightMatch;
    }
    return compare(*bound, value);
  }

  const ::parquet::arrow::SchemaManifest& manifest_;
  const ::parquet::RowGroupMetaData& row_group_;
  const std::unordered_map<int32_t, int>& field_id_to_column_idx_;
};

}  // namespace

Result<std::unique_ptr<ParquetMetricsRowGroupFilter>> ParquetMetricsRowGroupFilter::Make(
    const std::shared_ptr<Expression>& filter,
    const ::parquet::SchemaDescriptor& file_schema) {
  auto result =
      std::unique_ptr<ParquetMetricsRowGroupFilter>(new ParquetMetricsRowGroupFilter());
  for (int i = 0; i < file_schema.num_columns(); ++i) {
    auto id = file_schema.Column(i)->schema_node()->field_id();
    if (id >= 0) {
      result->field_id_to_column_idx_.emplace(id, i);
    }
  }
  result->bound_ = True::Instance();
  if (!filter) {
    return result;
  }
  ICEBERG_ASSIGN_OR_RAISE(result->bound_, RewriteNot::Visit(filter));
  return result;
}

Result<bool> ParquetMetricsRowGroupFilter::ShouldRead(
    const ::parquet::arrow::SchemaManifest& manifest,
    const ::parquet::RowGroupMetaData& row_group) const {
  if (row_group.num_rows() <= 0) {
    return kRowsCannotMatch;
  }
  try {
    MetricsVisitor visitor(manifest, row_group, field_id_to_column_idx_);
    return Visit<bool>(bound_, visitor);
  } catch (const ::parquet::ParquetException&) {
    // Unusable optional statistics must never turn into false negatives.
    return kRowsMightMatch;
  }
}

}  // namespace iceberg::parquet
