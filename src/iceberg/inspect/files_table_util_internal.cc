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

#include "iceberg/inspect/files_table_util_internal.h"

#include <algorithm>
#include <cstdint>
#include <format>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/file_format.h"
#include "iceberg/inspect/metadata_table_util_internal.h"
#include "iceberg/nanoarrow_status_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/conversions.h"
#include "iceberg/util/macros.h"

namespace iceberg::internal {
namespace {

template <typename T>
Status AppendOptionalInt(ArrowArray* array, const std::optional<T>& value) {
  if (!value.has_value()) {
    return AppendNull(array);
  }
  return AppendInt(array, static_cast<int64_t>(*value));
}

constexpr std::string_view MetadataFileFormat(FileFormatType format) {
  switch (format) {
    case FileFormatType::kParquet:
      return "PARQUET";
    case FileFormatType::kAvro:
      return "AVRO";
    case FileFormatType::kOrc:
      return "ORC";
    case FileFormatType::kPuffin:
      return "PUFFIN";
  }
  std::unreachable();
}

void CollectPrimitiveFields(
    const NestedType& type,
    std::vector<std::reference_wrapper<const SchemaField>>& primitive_fields) {
  for (const auto& field : type.fields()) {
    if (field.type()->is_primitive()) {
      primitive_fields.emplace_back(field);
    } else if (field.type()->is_nested()) {
      CollectPrimitiveFields(*std::static_pointer_cast<NestedType>(field.type()),
                             primitive_fields);
    }
  }
}

Result<SchemaField> ReadableMetricsField(const Schema& table_schema,
                                         int32_t highest_metadata_field_id) {
  std::vector<std::reference_wrapper<const SchemaField>> primitive_fields;
  CollectPrimitiveFields(table_schema, primitive_fields);

  int32_t next_id = highest_metadata_field_id;
  std::vector<SchemaField> column_metrics;
  column_metrics.reserve(primitive_fields.size());
  for (const auto& field_ref : primitive_fields) {
    const auto& field = field_ref.get();
    ICEBERG_ASSIGN_OR_RAISE(auto column_name,
                            table_schema.FindColumnNameById(field.field_id()));
    ICEBERG_PRECHECK(column_name.has_value(), "Cannot find name for field {}",
                     field.field_id());

    const int32_t column_metrics_id = ++next_id;
    std::vector<SchemaField> metrics{
        SchemaField::MakeOptional(++next_id, "column_size", int64(),
                                  "Total size on disk"),
        SchemaField::MakeOptional(++next_id, "value_count", int64(),
                                  "Total count, including null and NaN"),
        SchemaField::MakeOptional(++next_id, "null_value_count", int64(),
                                  "Null value count"),
        SchemaField::MakeOptional(++next_id, "nan_value_count", int64(),
                                  "NaN value count"),
        SchemaField::MakeOptional(++next_id, "lower_bound", field.type(), "Lower bound"),
        SchemaField::MakeOptional(++next_id, "upper_bound", field.type(), "Upper bound"),
    };
    column_metrics.emplace_back(
        column_metrics_id, *column_name, struct_(std::move(metrics)),
        /*optional=*/true, std::format("Metrics for column {}", *column_name));
  }

  std::ranges::sort(column_metrics, {},
                    [](const SchemaField& field) { return field.name(); });
  return SchemaField::MakeOptional(++next_id, "readable_metrics",
                                   struct_(std::move(column_metrics)),
                                   "Column metrics in readable form");
}

Status AppendMetric(ArrowArray* array, const std::map<int32_t, int64_t>& metrics,
                    int32_t field_id) {
  auto metric = metrics.find(field_id);
  return metric == metrics.end() ? AppendNull(array) : AppendInt(array, metric->second);
}

Status AppendBound(ArrowArray* array,
                   const std::map<int32_t, std::vector<uint8_t>>& bounds,
                   const SchemaField& field) {
  auto bound = bounds.find(field.field_id());
  if (bound == bounds.end()) {
    return AppendNull(array);
  }
  auto primitive = checked_pointer_cast<PrimitiveType>(field.type());
  // Four-byte bounds under a promoted timestamp still contain date values in days.
  if (bound->second.size() == sizeof(int32_t) &&
      (primitive->type_id() == TypeId::kTimestamp ||
       primitive->type_id() == TypeId::kTimestampNs)) {
    ICEBERG_ASSIGN_OR_RAISE(auto date_value,
                            Conversions::FromBytes(date(), bound->second));
    const int64_t days = std::get<int32_t>(date_value.value());
    const int64_t units_per_day = primitive->type_id() == TypeId::kTimestampNs
                                      ? 86'400'000'000'000LL
                                      : 86'400'000'000LL;
    ICEBERG_PRECHECK(days >= std::numeric_limits<int64_t>::min() / units_per_day &&
                         days <= std::numeric_limits<int64_t>::max() / units_per_day,
                     "Date bound {} is outside the {} range", days,
                     primitive->ToString());
    return AppendInt(array, days * units_per_day);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto literal,
                          Conversions::FromBytes(std::move(primitive), bound->second));
  return AppendLiteral(array, literal);
}

Status AppendReadableMetrics(ArrowArray* array, const StructType& readable_type,
                             const Schema& table_schema, const DataFile& file) {
  ICEBERG_PRECHECK(array != nullptr, "Readable metrics Arrow array cannot be null");
  ICEBERG_PRECHECK(
      array->n_children == static_cast<int64_t>(readable_type.fields().size()),
      "Readable metrics Arrow array has {} fields but schema has {}", array->n_children,
      readable_type.fields().size());
  for (int64_t index = 0; index < array->n_children; ++index) {
    auto* column_metrics = array->children[index];
    ICEBERG_PRECHECK(column_metrics != nullptr && column_metrics->n_children == 6,
                     "Readable column metrics must contain six fields");
    const auto readable_name = readable_type.fields()[index].name();
    ICEBERG_ASSIGN_OR_RAISE(auto source_field,
                            table_schema.FindFieldByName(readable_name));
    ICEBERG_PRECHECK(source_field.has_value(),
                     "Cannot find readable metrics source field '{}'", readable_name);
    const auto& field = source_field->get();
    ICEBERG_PRECHECK(field.type()->is_primitive(),
                     "Readable metrics source field '{}' must be primitive",
                     readable_name);

    ICEBERG_RETURN_UNEXPECTED(
        AppendMetric(column_metrics->children[0], file.column_sizes, field.field_id()));
    ICEBERG_RETURN_UNEXPECTED(
        AppendMetric(column_metrics->children[1], file.value_counts, field.field_id()));
    ICEBERG_RETURN_UNEXPECTED(AppendMetric(column_metrics->children[2],
                                           file.null_value_counts, field.field_id()));
    ICEBERG_RETURN_UNEXPECTED(AppendMetric(column_metrics->children[3],
                                           file.nan_value_counts, field.field_id()));
    ICEBERG_RETURN_UNEXPECTED(
        AppendBound(column_metrics->children[4], file.lower_bounds, field));
    ICEBERG_RETURN_UNEXPECTED(
        AppendBound(column_metrics->children[5], file.upper_bounds, field));
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(column_metrics));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

}  // namespace

Result<std::shared_ptr<Schema>> FilesTableSchema(
    const Schema& table_schema, const std::shared_ptr<StructType>& partition_type) {
  ICEBERG_PRECHECK(partition_type != nullptr, "Partition type cannot be null");
  auto data_file_type = DataFile::Type(partition_type);
  std::vector<SchemaField> fields;
  fields.reserve(data_file_type->fields().size() + 1);
  for (const auto& field : data_file_type->fields()) {
    fields.push_back(field);
    if (field.field_id() == DataFile::kFileFormatFieldId) {
      fields.push_back(DataFile::kSpecId);
    }
  }

  if (partition_type->fields().empty()) {
    std::erase_if(fields, [](const SchemaField& field) {
      return field.field_id() == DataFile::kPartitionFieldId;
    });
  }
  auto file_schema = std::make_shared<Schema>(std::move(fields));
  ICEBERG_ASSIGN_OR_RAISE(auto highest_field_id, file_schema->HighestFieldId());
  ICEBERG_ASSIGN_OR_RAISE(auto readable_metrics,
                          ReadableMetricsField(table_schema, highest_field_id));
  fields = std::vector<SchemaField>(file_schema->fields().begin(),
                                    file_schema->fields().end());
  fields.push_back(std::move(readable_metrics));
  return std::make_shared<Schema>(std::move(fields));
}

Status AppendDataFile(ArrowRowBuilder& builder, const Schema& schema,
                      const Schema& table_schema, const StructType& partition_type,
                      int32_t readable_metrics_id, const LiveFile& live_file) {
  ICEBERG_PRECHECK(live_file.file != nullptr, "Data file cannot be null");
  ICEBERG_PRECHECK(live_file.spec != nullptr, "Partition spec cannot be null");
  const auto& file = *live_file.file;

  for (size_t index = 0; index < schema.fields().size(); ++index) {
    const auto& field = schema.fields()[index];
    auto* array = builder.column(index);
    switch (field.field_id()) {
      case DataFile::kContentFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, static_cast<int64_t>(file.content)));
        break;
      case DataFile::kFilePathFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendString(array, file.file_path));
        break;
      case DataFile::kFileFormatFieldId:
        ICEBERG_RETURN_UNEXPECTED(
            AppendString(array, MetadataFileFormat(file.file_format)));
        break;
      case DataFile::kSpecIdFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, live_file.spec->spec_id()));
        break;
      case DataFile::kPartitionFieldId: {
        ICEBERG_ASSIGN_OR_RAISE(
            auto projected,
            ProjectPartitionValues(partition_type, *live_file.spec, file.partition));
        ICEBERG_RETURN_UNEXPECTED(
            AppendPartitionValues(array, partition_type, projected));
        break;
      }
      case DataFile::kRecordCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, file.record_count));
        break;
      case DataFile::kFileSizeFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, file.file_size_in_bytes));
        break;
      case DataFile::kColumnSizesFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendIntMap(array, file.column_sizes));
        break;
      case DataFile::kValueCountsFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendIntMap(array, file.value_counts));
        break;
      case DataFile::kNullValueCountsFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendIntMap(array, file.null_value_counts));
        break;
      case DataFile::kNanValueCountsFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendIntMap(array, file.nan_value_counts));
        break;
      case DataFile::kLowerBoundsFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendBinaryMap(array, file.lower_bounds));
        break;
      case DataFile::kUpperBoundsFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendBinaryMap(array, file.upper_bounds));
        break;
      case DataFile::kKeyMetadataFieldId:
        if (file.key_metadata.empty()) {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendBytes(array, file.key_metadata));
        }
        break;
      case DataFile::kSplitOffsetsFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendIntList(array, file.split_offsets));
        break;
      case DataFile::kEqualityIdsFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendIntList(array, file.equality_ids));
        break;
      case DataFile::kSortOrderIdFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendOptionalInt(array, file.sort_order_id));
        break;
      case DataFile::kFirstRowIdFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendOptionalInt(array, file.first_row_id));
        break;
      case DataFile::kReferencedDataFileFieldId:
        if (file.referenced_data_file.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendString(array, *file.referenced_data_file));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      case DataFile::kContentOffsetFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendOptionalInt(array, file.content_offset));
        break;
      case DataFile::kContentSizeFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendOptionalInt(array, file.content_size_in_bytes));
        break;
      default:
        if (field.field_id() == readable_metrics_id) {
          auto readable_type = checked_pointer_cast<StructType>(field.type());
          ICEBERG_RETURN_UNEXPECTED(
              AppendReadableMetrics(array, *readable_type, table_schema, file));
        } else {
          return InvalidSchema("Unsupported files metadata field {}", field.field_id());
        }
    }
  }
  return builder.FinishRow();
}

}  // namespace iceberg::internal
