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

#include "iceberg/parquet/parquet_metrics.h"

#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>

#include <parquet/column_reader.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include "iceberg/expression/literal.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/conversions.h"
#include "iceberg/util/truncate_util.h"
#include "iceberg/util/visit_type.h"

namespace iceberg::parquet {

namespace {

/// \brief Get the Iceberg field ID from a Parquet column descriptor.
/// \return The field ID, or nullopt if no field ID is set.
std::optional<int32_t> GetFieldId(const ::parquet::ColumnDescriptor& column) {
  const auto& node = column.schema_node();
  if (node == nullptr || !node->is_primitive()) {
    return std::nullopt;
  }
  if (node->field_id() < 0) {
    return std::nullopt;
  }
  return node->field_id();
}

/// \brief Find the column index for a field in the Parquet schema.
std::optional<int32_t> FindColumnIndex(const ::parquet::SchemaDescriptor& parquet_schema,
                                       int32_t field_id) {
  auto columns = std::views::iota(0, parquet_schema.num_columns());
  auto it = std::ranges::find_if(columns, [&](int i) {
    auto column_field_id = GetFieldId(*parquet_schema.Column(i));
    return column_field_id.has_value() && column_field_id.value() == field_id;
  });
  return it != columns.end() ? std::optional(*it) : std::nullopt;
}

/// \brief Collect counts (value count and null count) from footer statistics.
/// \param field_id The Iceberg field ID.
/// \param metadata The Parquet file metadata.
/// \param column_idx The column index in the Parquet schema.
/// \return A pair of (value_count, null_count), or nullopt if stats are not available.
std::optional<FieldMetrics> CollectCounts(int32_t field_id,
                                          const ::parquet::FileMetaData& metadata,
                                          int32_t column_idx) {
  int64_t value_count = 0;
  int64_t null_count = 0;

  for (int rg = 0; rg < metadata.num_row_groups(); ++rg) {
    auto row_group = metadata.RowGroup(rg);
    auto column_chunk = row_group->ColumnChunk(column_idx);
    auto stats = column_chunk->statistics();
    if (stats == nullptr || !stats->HasNullCount()) {
      return std::nullopt;
    }

    null_count += stats->null_count();
    value_count += column_chunk->num_values();
  }

  return FieldMetrics{
      .field_id = field_id, .value_count = value_count, .null_value_count = null_count};
}

/// \brief Collect bounds (lower and upper) from footer statistics.
/// \param field_id The Iceberg field ID.
/// \param iceberg_type The Iceberg primitive type for deserializing values.
/// \param metadata The Parquet file metadata.
/// \param column_idx The column index in the Parquet schema.
/// \param truncate_length The length to truncate strings/binary values.
/// \return FieldMetrics with counts and bounds, or nullopt if stats are not available.
Result<std::optional<FieldMetrics>> CollectBounds(
    int32_t field_id, std::shared_ptr<PrimitiveType> iceberg_type,
    const ::parquet::FileMetaData& metadata, int32_t column_idx,
    int32_t truncate_length) {
  int64_t null_count = 0;
  int64_t value_count = 0;
  std::optional<Literal> lower_bound;
  std::optional<Literal> upper_bound;

  for (int32_t rg = 0; rg < metadata.num_row_groups(); ++rg) {
    auto row_group = metadata.RowGroup(rg);
    auto column_chunk = row_group->ColumnChunk(column_idx);
    auto stats = column_chunk->statistics();
    if (stats == nullptr || !stats->HasNullCount()) {
      return std::nullopt;
    }

    null_count += stats->null_count();
    value_count += column_chunk->num_values();

    if (stats->HasMinMax()) {
      auto min_bytes = stats->EncodeMin();
      auto min_span = std::span<const uint8_t>(
          reinterpret_cast<const uint8_t*>(min_bytes.data()), min_bytes.size());
      ICEBERG_ASSIGN_OR_RAISE(auto min_value,
                              Conversions::FromBytes(iceberg_type, min_span));
      if (!lower_bound.has_value() || min_value < lower_bound.value()) {
        lower_bound = std::move(min_value);
      }

      auto max_bytes = stats->EncodeMax();
      auto max_span = std::span<const uint8_t>(
          reinterpret_cast<const uint8_t*>(max_bytes.data()), max_bytes.size());
      ICEBERG_ASSIGN_OR_RAISE(auto max_value,
                              Conversions::FromBytes(iceberg_type, max_span));
      if (!upper_bound.has_value() || max_value > upper_bound.value()) {
        upper_bound = std::move(max_value);
      }
    }
  }

  if (!lower_bound.has_value() || !upper_bound.has_value() || lower_bound->IsNaN() ||
      upper_bound->IsNaN()) {
    return FieldMetrics{
        .field_id = field_id,
        .value_count = value_count,
        .null_value_count = null_count,
    };
  }

  ICEBERG_ASSIGN_OR_RAISE(auto truncated_lower,
                          TruncateUtils::TruncateLowerBound(
                              *iceberg_type, lower_bound.value(), truncate_length));
  ICEBERG_ASSIGN_OR_RAISE(auto truncated_upper,
                          TruncateUtils::TruncateUpperBound(
                              *iceberg_type, upper_bound.value(), truncate_length));

  return FieldMetrics{
      .field_id = field_id,
      .value_count = value_count,
      .null_value_count = null_count,
      .lower_bound = std::move(truncated_lower),
      .upper_bound = std::move(truncated_upper),
  };
}

/// \brief Process pre-computed field metrics, applying truncation if needed.
/// \param field_id The field ID to look up.
/// \param field_metrics The map of pre-computed field metrics.
/// \param primitive_type The primitive type for truncation.
/// \param truncate_length The truncation length (0 means no bounds).
/// \return Processed FieldMetrics with truncated bounds if applicable.
Result<std::optional<FieldMetrics>> MetricsFromFieldMetrics(
    int32_t field_id, const std::unordered_map<int32_t, FieldMetrics>& field_metrics,
    std::shared_ptr<PrimitiveType> primitive_type, int32_t truncate_length) {
  auto it = field_metrics.find(field_id);
  if (it == field_metrics.end()) {
    return std::nullopt;
  }

  const auto& fm = it->second;
  FieldMetrics result{.field_id = fm.field_id,
                      .value_count = fm.value_count,
                      .null_value_count = fm.null_value_count,
                      .nan_value_count = fm.nan_value_count};

  if (truncate_length > 0) {
    if (fm.lower_bound.has_value()) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto lower, TruncateUtils::TruncateLowerBound(
                          *primitive_type, fm.lower_bound.value(), truncate_length));
      result.lower_bound = std::move(lower);
    }
    if (fm.upper_bound.has_value()) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto upper, TruncateUtils::TruncateUpperBound(
                          *primitive_type, fm.upper_bound.value(), truncate_length));
      result.upper_bound = std::move(upper);
    }
  }

  return result;
}

/// \brief Collect metrics for a single primitive field from footer statistics.
Result<std::optional<FieldMetrics>> MetricsFromFooter(
    int32_t field_id, std::shared_ptr<PrimitiveType> iceberg_type,
    const ::parquet::SchemaDescriptor& parquet_schema,
    const ::parquet::FileMetaData& metadata, int32_t truncate_length) {
  auto column_idx = FindColumnIndex(parquet_schema, field_id);
  if (!column_idx.has_value()) {
    return std::nullopt;
  }

  auto column_desc = parquet_schema.Column(column_idx.value());
  if (column_desc->physical_type() == ::parquet::Type::INT96) {
    return std::nullopt;
  }

  if (truncate_length <= 0) {
    return CollectCounts(field_id, metadata, column_idx.value());
  }

  return CollectBounds(field_id, iceberg_type, metadata, column_idx.value(),
                       truncate_length);
}

/// \brief Visitor for collecting metrics from all primitive fields in a schema.
class CollectMetricsVisitor {
 public:
  CollectMetricsVisitor(const ::parquet::SchemaDescriptor& parquet_schema,
                        const MetricsConfig& metrics_config,
                        const ::parquet::FileMetaData& metadata,
                        const std::unordered_map<int32_t, FieldMetrics>& field_metrics,
                        Metrics& metrics)
      : parquet_schema_(parquet_schema),
        metrics_config_(metrics_config),
        metadata_(metadata),
        field_metrics_(field_metrics),
        metrics_(metrics) {}

  Status VisitStruct(const StructType& type, const std::string& prefix) {
    for (const auto& field : type.fields()) {
      std::string full_name = prefix.empty() ? std::string(field.name())
                                             : prefix + "." + std::string(field.name());
      ICEBERG_RETURN_UNEXPECTED(VisitField(field, full_name));
    }
    return {};
  }

  Status VisitList(const ListType& /*type*/, const std::string& /*prefix*/) { return {}; }

  Status VisitMap(const MapType& /*type*/, const std::string& /*prefix*/) { return {}; }

  Status VisitPrimitive(const PrimitiveType& /*type*/, const std::string& /*prefix*/) {
    return {};
  }

 private:
  Status VisitField(const SchemaField& field, const std::string& full_name) {
    if (field.type()->is_primitive()) {
      return ProcessPrimitiveField(field, full_name);
    } else if (field.type()->is_nested()) {
      return VisitTypeCategory(*field.type(), this, full_name);
    }
    return {};
  }

  Status ProcessPrimitiveField(const SchemaField& field, const std::string& full_name) {
    int32_t field_id = field.field_id();
    MetricsMode mode = metrics_config_.ColumnMode(full_name);
    if (mode.kind == MetricsMode::Kind::kNone) {
      return {};
    }

    int32_t truncate_length = mode.TruncateLength();
    const auto& primitive_type =
        internal::checked_pointer_cast<PrimitiveType>(field.type());

    ICEBERG_ASSIGN_OR_RAISE(auto field_metrics,
                            MetricsFromFieldMetrics(field_id, field_metrics_,
                                                    primitive_type, truncate_length));
    if (field_metrics.has_value()) {
      ApplyFieldMetrics(field_id, std::move(field_metrics.value()));
      return {};
    }

    ICEBERG_ASSIGN_OR_RAISE(auto footer_metrics,
                            MetricsFromFooter(field_id, primitive_type, parquet_schema_,
                                              metadata_, truncate_length));
    if (footer_metrics.has_value()) {
      ApplyFieldMetrics(field_id, std::move(footer_metrics.value()));
    }
    return {};
  }

  void ApplyFieldMetrics(int32_t field_id, FieldMetrics&& fm) {
    if (fm.value_count >= 0) {
      metrics_.value_counts[field_id] = fm.value_count;
    }
    if (fm.null_value_count >= 0) {
      metrics_.null_value_counts[field_id] = fm.null_value_count;
    }
    if (fm.nan_value_count >= 0) {
      metrics_.nan_value_counts[field_id] = fm.nan_value_count;
    }
    if (fm.lower_bound.has_value()) {
      metrics_.lower_bounds.emplace(field_id, std::move(fm.lower_bound.value()));
    }
    if (fm.upper_bound.has_value()) {
      metrics_.upper_bounds.emplace(field_id, std::move(fm.upper_bound.value()));
    }
  }

  const ::parquet::SchemaDescriptor& parquet_schema_;
  const MetricsConfig& metrics_config_;
  const ::parquet::FileMetaData& metadata_;
  const std::unordered_map<int32_t, FieldMetrics>& field_metrics_;
  Metrics& metrics_;
};

}  // namespace

Result<Metrics> ParquetMetrics::GetMetrics(
    const Schema& schema, const ::parquet::SchemaDescriptor& parquet_schema,
    const MetricsConfig& metrics_config, const ::parquet::FileMetaData& metadata,
    const std::unordered_map<int32_t, FieldMetrics>& field_metrics) {
  Metrics metrics;

  // Collect row count and column sizes
  int64_t row_count = 0;
  for (int rg = 0; rg < metadata.num_row_groups(); ++rg) {
    auto row_group = metadata.RowGroup(rg);
    row_count += row_group->num_rows();
    for (int col = 0; col < row_group->num_columns(); ++col) {
      auto column_chunk = row_group->ColumnChunk(col);
      auto field_id_opt = GetFieldId(*parquet_schema.Column(col));
      if (!field_id_opt.has_value()) {
        continue;
      }
      int32_t field_id = field_id_opt.value();

      ICEBERG_ASSIGN_OR_RAISE(auto field_name, schema.FindColumnNameById(field_id));
      if (!field_name.has_value()) {
        continue;
      }

      MetricsMode mode = metrics_config.ColumnMode(field_name.value());
      if (mode.kind != MetricsMode::Kind::kNone) {
        metrics.column_sizes[field_id] =
            metrics.column_sizes.contains(field_id)
                ? metrics.column_sizes[field_id] + column_chunk->total_compressed_size()
                : column_chunk->total_compressed_size();
      }
    }
  }
  metrics.row_count = row_count;

  // Collect metrics for all primitive fields
  CollectMetricsVisitor visitor(parquet_schema, metrics_config, metadata, field_metrics,
                                metrics);
  ICEBERG_RETURN_UNEXPECTED(visitor.VisitStruct(schema, ""));

  return metrics;
}

}  // namespace iceberg::parquet
