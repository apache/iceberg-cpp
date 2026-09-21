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
#include <unordered_map>

#include <parquet/statistics.h>

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/metrics.h"
#include "iceberg/parquet/parquet_metrics_internal.h"
#include "iceberg/parquet/parquet_metrics_row_group_filter_internal.h"
#include "iceberg/parquet/parquet_schema_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg::parquet {

namespace {

constexpr size_t kInPredicateLimit = 200;

class BindFilter : public Binder {
 public:
  BindFilter(const Schema& schema, bool case_sensitive,
             const ::parquet::arrow::SchemaManifest& manifest)
      : Binder(schema, case_sensitive) {
    for (const auto& [index, field] : manifest.column_index_to_field) {
      const auto* column = manifest.descr->Column(index);
      // Repeated-column counts and bounds do not describe individual rows.
      if (column->max_repetition_level() == 0) {
        fields_.emplace(column->schema_node()->field_id(), field);
      }
    }
  }

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<UnboundPredicate>& pred) override {
    auto bound = Binder::Predicate(pred);
    // Direct format readers may only supply a projection, not the complete schema.
    if (!bound) {
      return True::Instance();
    }
    return Visit<std::shared_ptr<Expression>>(*bound, *this);
  }

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override {
    auto ref = std::dynamic_pointer_cast<BoundReference>(pred->term());
    if (!ref || !ref->type()->is_primitive() ||
        MetadataColumns::IsMetadataColumn(ref->field_id()) ||
        MetadataColumns::IsRowLineageColumn(ref->field_id())) {
      return True::Instance();
    }
    auto field = fields_.find(ref->field_id());
    if (field == fields_.end() ||
        !ValidateParquetTypeCompatibility(*ref->type(), *field->second)) {
      return True::Instance();
    }
    return pred;
  }

 private:
  std::unordered_map<int32_t, const ::parquet::arrow::SchemaField*> fields_;
};

class MetricsVisitor : public BoundVisitor<bool> {
 public:
  MetricsVisitor(const ::parquet::SchemaDescriptor& schema,
                 const ::parquet::RowGroupMetaData& row_group)
      : schema_(schema), row_group_(row_group) {
    for (int i = 0; i < schema.num_columns(); ++i) {
      auto id = schema.Column(i)->schema_node()->field_id();
      if (id >= 0) {
        columns_.emplace(id, i);
      }
    }
  }

  Result<bool> AlwaysTrue() override { return true; }

  Result<bool> AlwaysFalse() override { return false; }

  Result<bool> Not(bool) override { return true; }

  Result<bool> And(bool left, bool right) override { return left && right; }

  Result<bool> Or(bool left, bool right) override { return left || right; }

  Result<bool> IsNull(const std::shared_ptr<Bound>& expr) override {
    return MayContainNull(GetMetrics(expr, false));
  }

  Result<bool> NotNull(const std::shared_ptr<Bound>& expr) override {
    return !ContainsNullsOnly(GetMetrics(expr, false));
  }

  Result<bool> IsNaN(const std::shared_ptr<Bound>& expr) override {
    return !ContainsNullsOnly(GetMetrics(expr, false));
  }

  Result<bool> NotNaN(const std::shared_ptr<Bound>&) override { return true; }

  Result<bool> Lt(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    const auto metrics = GetMetrics(expr);
    if (ContainsNullsOnly(metrics)) {
      return false;
    }
    if (!metrics.lower_bound || !ComparableLiteral(value)) {
      return true;
    }
    return !(*metrics.lower_bound >= value);
  }

  Result<bool> LtEq(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    const auto metrics = GetMetrics(expr);
    if (ContainsNullsOnly(metrics)) {
      return false;
    }
    if (!metrics.lower_bound || !ComparableLiteral(value)) {
      return true;
    }
    return !(*metrics.lower_bound > value);
  }

  Result<bool> Gt(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    const auto metrics = GetMetrics(expr);
    if (ContainsNullsOnly(metrics)) {
      return false;
    }
    if (!metrics.upper_bound || !ComparableLiteral(value)) {
      return true;
    }
    return !(*metrics.upper_bound <= value);
  }

  Result<bool> GtEq(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    const auto metrics = GetMetrics(expr);
    if (ContainsNullsOnly(metrics)) {
      return false;
    }
    if (!metrics.upper_bound || !ComparableLiteral(value)) {
      return true;
    }
    return !(*metrics.upper_bound < value);
  }

  Result<bool> Eq(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    const auto metrics = GetMetrics(expr);
    if (ContainsNullsOnly(metrics)) {
      return false;
    }
    if (!metrics.lower_bound || !metrics.upper_bound || !ComparableLiteral(value)) {
      return true;
    }
    return !(*metrics.lower_bound > value || *metrics.upper_bound < value);
  }

  Result<bool> NotEq(const std::shared_ptr<Bound>&, const Literal&) override {
    // Like Java, keep negative membership predicates inclusive.
    return true;
  }

  Result<bool> In(const std::shared_ptr<Bound>& expr,
                  const BoundSetPredicate::LiteralSet& values) override {
    const auto metrics = GetMetrics(expr);
    if (ContainsNullsOnly(metrics)) {
      return false;
    }
    if (!metrics.lower_bound || !metrics.upper_bound ||
        values.size() > kInPredicateLimit) {
      return true;
    }
    for (const auto& value : values) {
      if (!ComparableLiteral(value) ||
          !(value < *metrics.lower_bound || value > *metrics.upper_bound)) {
        return true;
      }
    }
    return false;
  }

  Result<bool> NotIn(const std::shared_ptr<Bound>&,
                     const BoundSetPredicate::LiteralSet&) override {
    return true;
  }

  Result<bool> StartsWith(const std::shared_ptr<Bound>& expr,
                          const Literal& value) override {
    const auto metrics = GetMetrics(expr);
    if (ContainsNullsOnly(metrics)) {
      return false;
    }
    if (!metrics.lower_bound || !metrics.upper_bound || !ComparableLiteral(value) ||
        metrics.lower_bound->type()->type_id() != TypeId::kString) {
      return true;
    }
    const auto& prefix = std::get<std::string>(value.value());
    const auto& lower = std::get<std::string>(metrics.lower_bound->value());
    const auto& upper = std::get<std::string>(metrics.upper_bound->value());
    return !(lower.substr(0, prefix.size()) > prefix ||
             upper.substr(0, prefix.size()) < prefix);
  }

  Result<bool> NotStartsWith(const std::shared_ptr<Bound>& expr,
                             const Literal& value) override {
    const auto metrics = GetMetrics(expr);
    if (MayContainNull(metrics) || !metrics.lower_bound || !metrics.upper_bound ||
        !ComparableLiteral(value) ||
        metrics.lower_bound->type()->type_id() != TypeId::kString) {
      return true;
    }
    const auto& prefix = std::get<std::string>(value.value());
    const auto& lower = std::get<std::string>(metrics.lower_bound->value());
    const auto& upper = std::get<std::string>(metrics.upper_bound->value());
    return !lower.starts_with(prefix) || !upper.starts_with(prefix);
  }

 private:
  static bool ContainsNullsOnly(const FieldMetrics& metrics) {
    return metrics.null_value_count >= 0 &&
           metrics.null_value_count == metrics.value_count;
  }

  static bool MayContainNull(const FieldMetrics& metrics) {
    return metrics.null_value_count != 0;
  }

  static bool ComparableLiteral(const Literal& value) {
    return !value.IsNaN() && !value.IsNull() && !value.IsAboveMax() &&
           !value.IsBelowMin();
  }

  FieldMetrics GetMetrics(const std::shared_ptr<Bound>& expr,
                          bool read_bounds = true) const {
    FieldMetrics metrics;
    auto ref = std::dynamic_pointer_cast<BoundReference>(expr);
    if (!ref) {
      return metrics;
    }
    metrics.field_id = ref->field_id();
    auto column = columns_.find(ref->field_id());
    // Missing columns can have initial defaults, so do not assume all nulls.
    if (column == columns_.end()) {
      return metrics;
    }
    const auto& descriptor = *schema_.Column(column->second);
    const auto& type = static_cast<const PrimitiveType&>(*ref->type());
    auto chunk = row_group_.ColumnChunk(column->second);
    auto stats = chunk->statistics();
    if (!stats) {
      return metrics;
    }
    metrics.value_count = chunk->num_values();
    if (stats->HasNullCount()) {
      metrics.null_value_count = stats->null_count();
    }
    if (!read_bounds || ContainsNullsOnly(metrics) || !stats->HasMinMax()) {
      return metrics;
    }
    auto lower_result =
        ParquetMetrics::StatsValueToLiteral(descriptor, type, *stats, true);
    auto upper_result =
        ParquetMetrics::StatsValueToLiteral(descriptor, type, *stats, false);
    if (!lower_result || !upper_result) {
      return metrics;
    }
    auto lower = std::move(*lower_result);
    auto upper = std::move(*upper_result);
    if (lower.IsNaN() || upper.IsNaN()) {
      return metrics;
    }
    if (type.type_id() == TypeId::kFloat) {
      if (std::get<float>(lower.value()) == 0) {
        lower = Literal::Float(-0.0F);
      }
      if (std::get<float>(upper.value()) == 0) {
        upper = Literal::Float(0.0F);
      }
    } else if (type.type_id() == TypeId::kDouble) {
      if (std::get<double>(lower.value()) == 0) {
        lower = Literal::Double(-0.0);
      }
      if (std::get<double>(upper.value()) == 0) {
        upper = Literal::Double(0.0);
      }
    }
    if (lower > upper) {
      return metrics;
    }
    metrics.lower_bound = std::move(lower);
    metrics.upper_bound = std::move(upper);
    return metrics;
  }

  const ::parquet::SchemaDescriptor& schema_;
  const ::parquet::RowGroupMetaData& row_group_;
  std::unordered_map<int32_t, int> columns_;
};

}  // namespace

Result<std::unique_ptr<ParquetMetricsRowGroupFilter>> ParquetMetricsRowGroupFilter::Make(
    const Schema& schema, const std::shared_ptr<Expression>& filter,
    const ::parquet::arrow::SchemaManifest& manifest, bool case_sensitive) {
  auto result =
      std::unique_ptr<ParquetMetricsRowGroupFilter>(new ParquetMetricsRowGroupFilter());
  result->bound_ = True::Instance();
  if (!filter) {
    return result;
  }
  // Eliminate NOT before unsupported predicates are weakened to true.
  ICEBERG_ASSIGN_OR_RAISE(auto rewritten, RewriteNot::Visit(filter));
  BindFilter binder(schema, case_sensitive, manifest);
  ICEBERG_ASSIGN_OR_RAISE(result->bound_,
                          Visit<std::shared_ptr<Expression>>(rewritten, binder));
  return result;
}

Result<bool> ParquetMetricsRowGroupFilter::ShouldRead(
    const ::parquet::SchemaDescriptor& file_schema,
    const ::parquet::RowGroupMetaData& row_group) const {
  if (row_group.num_rows() <= 0) {
    return false;
  }
  try {
    MetricsVisitor visitor(file_schema, row_group);
    return Visit<bool>(bound_, visitor);
  } catch (const ::parquet::ParquetException&) {
    // Unusable optional statistics must never turn into false negatives.
    return true;
  }
}

}  // namespace iceberg::parquet
