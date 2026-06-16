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

#include <cstdint>
#include <limits>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <parquet/column_reader.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include "iceberg/expression/literal.h"
#include "iceberg/parquet/parquet_metrics_internal.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/decimal.h"
#include "iceberg/util/truncate_util.h"
#include "iceberg/util/uuid.h"
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

int64_t CollectColumnSize(const ::parquet::FileMetaData& metadata, int32_t column_idx) {
  int64_t size = 0;
  for (int rg = 0; rg < metadata.num_row_groups(); ++rg) {
    size += metadata.RowGroup(rg)->ColumnChunk(column_idx)->total_compressed_size();
  }
  return size;
}

template <typename StatsType, typename Converter>
Result<Literal> TypedStatsLiteral(const ::parquet::Statistics& stats, bool is_min,
                                  Converter&& converter) {
  const auto& typed_stats = internal::checked_cast<const StatsType&>(stats);
  return converter(is_min ? typed_stats.min() : typed_stats.max());
}

std::vector<uint8_t> BytesFromByteArray(const ::parquet::ByteArray& value) {
  return std::vector<uint8_t>{value.ptr, value.ptr + value.len};
}

std::vector<uint8_t> BytesFromFLBA(const ::parquet::FixedLenByteArray& value,
                                   int32_t length) {
  return std::vector<uint8_t>{value.ptr, value.ptr + length};
}

Literal DecimalLiteral(int128_t value, const PrimitiveType& iceberg_type) {
  const auto& decimal_type = internal::checked_cast<const DecimalType&>(iceberg_type);
  return Literal::Decimal(value, decimal_type.precision(), decimal_type.scale());
}

Result<Literal> DecimalLiteralFromBytes(std::span<const uint8_t> bytes,
                                        const PrimitiveType& iceberg_type) {
  ICEBERG_ASSIGN_OR_RAISE(auto decimal,
                          Decimal::FromBigEndian(bytes.data(), bytes.size()));
  return DecimalLiteral(decimal.value(), iceberg_type);
}

Result<Literal> BinaryStatsLiteral(std::vector<uint8_t> bytes,
                                   const PrimitiveType& iceberg_type) {
  switch (iceberg_type.type_id()) {
    case TypeId::kString:
      return Literal::String(std::string(bytes.begin(), bytes.end()));
    case TypeId::kBinary:
      return Literal::Binary(std::move(bytes));
    case TypeId::kFixed:
      return Literal::Fixed(std::move(bytes));
    case TypeId::kUuid: {
      ICEBERG_ASSIGN_OR_RAISE(auto uuid, Uuid::FromBytes(bytes));
      return Literal::UUID(std::move(uuid));
    }
    case TypeId::kDecimal:
      return DecimalLiteralFromBytes(bytes, iceberg_type);
    default:
      return InvalidArgument(
          "Cannot convert Parquet binary statistics to Iceberg type {}",
          iceberg_type.ToString());
  }
}

Result<Literal> Int32StatsLiteral(int32_t value, const PrimitiveType& iceberg_type) {
  switch (iceberg_type.type_id()) {
    case TypeId::kInt:
      return Literal::Int(value);
    case TypeId::kLong:
      return Literal::Long(value);
    case TypeId::kDate:
      return Literal::Date(value);
    case TypeId::kDecimal:
      return DecimalLiteral(value, iceberg_type);
    default:
      return InvalidArgument("Cannot convert Parquet INT32 statistics to Iceberg type {}",
                             iceberg_type.ToString());
  }
}

Result<Literal> Int64StatsLiteral(int64_t value, const PrimitiveType& iceberg_type) {
  switch (iceberg_type.type_id()) {
    case TypeId::kLong:
      return Literal::Long(value);
    case TypeId::kTime:
      return Literal::Time(value);
    case TypeId::kTimestamp:
      return Literal::Timestamp(value);
    case TypeId::kTimestampTz:
      return Literal::TimestampTz(value);
    case TypeId::kTimestampNs:
      return Literal::TimestampNs(value);
    case TypeId::kTimestampTzNs:
      return Literal::TimestampTzNs(value);
    case TypeId::kDecimal:
      return DecimalLiteral(value, iceberg_type);
    default:
      return InvalidArgument("Cannot convert Parquet INT64 statistics to Iceberg type {}",
                             iceberg_type.ToString());
  }
}

Result<Literal> FloatStatsLiteral(float value, const PrimitiveType& iceberg_type) {
  switch (iceberg_type.type_id()) {
    case TypeId::kFloat:
      return Literal::Float(value);
    case TypeId::kDouble:
      return Literal::Double(value);
    default:
      return InvalidArgument("Cannot convert Parquet FLOAT statistics to Iceberg type {}",
                             iceberg_type.ToString());
  }
}

bool IsFloatingType(const PrimitiveType& type) {
  return type.type_id() == TypeId::kFloat || type.type_id() == TypeId::kDouble;
}

bool NeedsBoundTruncation(const PrimitiveType& type) {
  return type.type_id() == TypeId::kString || type.type_id() == TypeId::kBinary;
}

Result<Literal> StatsValueToLiteral(const ::parquet::ColumnDescriptor& column,
                                    const PrimitiveType& iceberg_type,
                                    const ::parquet::Statistics& stats, bool is_min) {
  switch (column.physical_type()) {
    case ::parquet::Type::BOOLEAN:
      return TypedStatsLiteral<::parquet::BoolStatistics>(
          stats, is_min, [](bool value) { return Literal::Boolean(value); });
    case ::parquet::Type::INT32:
      return TypedStatsLiteral<::parquet::Int32Statistics>(
          stats, is_min,
          [&](int32_t value) { return Int32StatsLiteral(value, iceberg_type); });
    case ::parquet::Type::INT64:
      return TypedStatsLiteral<::parquet::Int64Statistics>(
          stats, is_min,
          [&](int64_t value) { return Int64StatsLiteral(value, iceberg_type); });
    case ::parquet::Type::FLOAT:
      return TypedStatsLiteral<::parquet::FloatStatistics>(
          stats, is_min,
          [&](float value) { return FloatStatsLiteral(value, iceberg_type); });
    case ::parquet::Type::DOUBLE:
      return TypedStatsLiteral<::parquet::DoubleStatistics>(
          stats, is_min, [](double value) { return Literal::Double(value); });
    case ::parquet::Type::BYTE_ARRAY:
      return TypedStatsLiteral<::parquet::ByteArrayStatistics>(
          stats, is_min, [&](const ::parquet::ByteArray& value) {
            return BinaryStatsLiteral(BytesFromByteArray(value), iceberg_type);
          });
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return TypedStatsLiteral<::parquet::FLBAStatistics>(
          stats, is_min, [&](const ::parquet::FixedLenByteArray& value) {
            return BinaryStatsLiteral(BytesFromFLBA(value, column.type_length()),
                                      iceberg_type);
          });
    case ::parquet::Type::INT96:
    case ::parquet::Type::UNDEFINED:
      return NotSupported("Cannot convert Parquet statistics for physical type {}",
                          static_cast<int>(column.physical_type()));
  }
  std::unreachable();
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
  auto column_desc = metadata.schema()->Column(column_idx);
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
      ICEBERG_ASSIGN_OR_RAISE(auto min_value,
                              StatsValueToLiteral(*column_desc, *iceberg_type, *stats,
                                                  /*is_min=*/true));
      if (!lower_bound.has_value() || min_value < lower_bound.value()) {
        lower_bound = std::move(min_value);
      }

      ICEBERG_ASSIGN_OR_RAISE(auto max_value,
                              StatsValueToLiteral(*column_desc, *iceberg_type, *stats,
                                                  /*is_min=*/false));
      if (!upper_bound.has_value() || max_value > upper_bound.value()) {
        upper_bound = std::move(max_value);
      }
    }
  }

  if (!lower_bound.has_value() || !upper_bound.has_value() ||
      (IsFloatingType(*iceberg_type) && (lower_bound->IsNaN() || upper_bound->IsNaN()))) {
    return FieldMetrics{
        .field_id = field_id,
        .value_count = value_count,
        .null_value_count = null_count,
    };
  }

  if (NeedsBoundTruncation(*iceberg_type)) {
    ICEBERG_ASSIGN_OR_RAISE(
        lower_bound, TruncateUtils::TruncateLowerBound(*iceberg_type, lower_bound.value(),
                                                       truncate_length));
    ICEBERG_ASSIGN_OR_RAISE(
        upper_bound, TruncateUtils::TruncateUpperBound(*iceberg_type, upper_bound.value(),
                                                       truncate_length));
  }

  return FieldMetrics{
      .field_id = field_id,
      .value_count = value_count,
      .null_value_count = null_count,
      .lower_bound = std::move(lower_bound),
      .upper_bound = std::move(upper_bound),
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

  if (truncate_length <= 0) {
    return result;
  }

  if (!NeedsBoundTruncation(*primitive_type)) {
    result.lower_bound = fm.lower_bound;
    result.upper_bound = fm.upper_bound;
    return result;
  }

  if (fm.lower_bound.has_value()) {
    ICEBERG_ASSIGN_OR_RAISE(
        result.lower_bound,
        TruncateUtils::TruncateLowerBound(*primitive_type, fm.lower_bound.value(),
                                          truncate_length));
  }
  if (fm.upper_bound.has_value()) {
    ICEBERG_ASSIGN_OR_RAISE(
        result.upper_bound,
        TruncateUtils::TruncateUpperBound(*primitive_type, fm.upper_bound.value(),
                                          truncate_length));
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

  Status VisitVariant(const VariantType& /*type*/, const std::string& /*prefix*/) {
    return {};
  }

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
    auto column_idx = FindColumnIndex(parquet_schema_, field_id);
    if (column_idx.has_value()) {
      metrics_.column_sizes[field_id] = CollectColumnSize(metadata_, column_idx.value());
    }

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

  metrics.row_count = metadata.num_rows();

  // Apply MetricsConfig while visiting schema fields, then collect footer metrics only
  // for fields whose mode is not `none`.
  CollectMetricsVisitor visitor(parquet_schema, metrics_config, metadata, field_metrics,
                                metrics);
  ICEBERG_RETURN_UNEXPECTED(visitor.VisitStruct(schema, ""));

  return metrics;
}

}  // namespace iceberg::parquet
