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

#include "iceberg/inspect/metadata_table_util_internal.h"

#include <algorithm>
#include <cstdint>
#include <format>
#include <functional>
#include <map>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/constants.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/conversions.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"

namespace iceberg::internal {
namespace {

Result<std::shared_ptr<Snapshot>> SnapshotAtRef(const Table& table,
                                                std::string_view ref_name) {
  const auto& metadata = table.metadata();
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");

  if (ref_name.empty() || ref_name == SnapshotRef::kMainBranch) {
    if (metadata->current_snapshot_id == kInvalidSnapshotId) {
      return std::shared_ptr<Snapshot>{nullptr};
    }
    return metadata->SnapshotById(metadata->current_snapshot_id);
  }

  auto ref = metadata->refs.find(std::string(ref_name));
  ICEBERG_CHECK(ref != metadata->refs.end(), "Cannot find snapshot reference '{}'",
                ref_name);
  ICEBERG_PRECHECK(ref->second != nullptr, "Snapshot reference '{}' is null", ref_name);
  return metadata->SnapshotById(ref->second->snapshot_id);
}

Result<bool> IsAncestorOf(const Table& table, int64_t ancestor_id,
                          const std::shared_ptr<Snapshot>& head) {
  std::unordered_set<int64_t> visited;
  auto current = head;
  while (current != nullptr) {
    if (!visited.insert(current->snapshot_id).second) {
      return Invalid("Cycle detected in snapshot ancestry at {}", current->snapshot_id);
    }
    if (current->snapshot_id == ancestor_id) {
      return true;
    }
    if (!current->parent_snapshot_id.has_value()) {
      break;
    }
    auto parent = table.SnapshotById(*current->parent_snapshot_id);
    if (!parent.has_value()) {
      if (parent.error().kind == ErrorKind::kNotFound) {
        break;
      }
      return std::unexpected<Error>(parent.error());
    }
    current = std::move(parent).value();
  }
  return false;
}

Status AppendLiteral(ArrowArray* array, const Literal& literal) {
  if (literal.IsNull()) {
    return AppendNull(array);
  }
  if (literal.IsAboveMax() || literal.IsBelowMin()) {
    return InvalidArgument("Cannot append non-value partition literal {}",
                           literal.ToString());
  }

  switch (literal.type()->type_id()) {
    case TypeId::kBoolean:
      return AppendBoolean(array, std::get<bool>(literal.value()));
    case TypeId::kInt:
    case TypeId::kDate:
      return AppendInt(array, std::get<int32_t>(literal.value()));
    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
    case TypeId::kTimestampNs:
    case TypeId::kTimestampTzNs:
      return AppendInt(array, std::get<int64_t>(literal.value()));
    case TypeId::kFloat:
      return AppendDouble(array, std::get<float>(literal.value()));
    case TypeId::kDouble:
      return AppendDouble(array, std::get<double>(literal.value()));
    case TypeId::kString:
      return AppendString(array, std::get<std::string>(literal.value()));
    case TypeId::kBinary:
    case TypeId::kFixed:
      return AppendBytes(array, std::get<std::vector<uint8_t>>(literal.value()));
    case TypeId::kDecimal:
      return AppendBytes(array, std::get<Decimal>(literal.value()).ToBytes());
    case TypeId::kUuid:
      return AppendBytes(array, std::get<Uuid>(literal.value()).bytes());
    case TypeId::kUnknown:
    case TypeId::kStruct:
    case TypeId::kList:
    case TypeId::kMap:
    case TypeId::kVariant:
    case TypeId::kGeometry:
    case TypeId::kGeography:
      return NotSupported("Cannot append partition literal of type {}",
                          literal.type()->ToString());
  }
  std::unreachable();
}

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

Result<std::shared_ptr<Snapshot>> ResolveMetadataTableSnapshot(
    const Table& table, const SnapshotSelection& selection) {
  ICEBERG_ASSIGN_OR_RAISE(auto head, SnapshotAtRef(table, selection.ref_name));

  if (std::holds_alternative<std::monostate>(selection.snapshot)) {
    return head;
  }

  if (const auto* snapshot_id = std::get_if<int64_t>(&selection.snapshot)) {
    ICEBERG_ASSIGN_OR_RAISE(auto selected, table.SnapshotById(*snapshot_id));
    if (!selection.ref_name.empty()) {
      ICEBERG_ASSIGN_OR_RAISE(auto is_ancestor, IsAncestorOf(table, *snapshot_id, head));
      ICEBERG_CHECK(is_ancestor, "Snapshot {} is not reachable from reference '{}'",
                    *snapshot_id, selection.ref_name);
    }
    return selected;
  }

  const auto timestamp = std::get<TimePointMs>(selection.snapshot);
  if (selection.ref_name.empty() || selection.ref_name == SnapshotRef::kMainBranch) {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id,
                            SnapshotUtil::SnapshotIdAsOfTime(table, timestamp));
    return table.SnapshotById(snapshot_id);
  }

  std::shared_ptr<Snapshot> selected;
  std::unordered_set<int64_t> visited;
  auto current = head;
  while (current != nullptr) {
    if (!visited.insert(current->snapshot_id).second) {
      return Invalid("Cycle detected in snapshot ancestry at {}", current->snapshot_id);
    }
    if (current->timestamp_ms <= timestamp &&
        (selected == nullptr || current->timestamp_ms > selected->timestamp_ms)) {
      selected = current;
    }
    if (!current->parent_snapshot_id.has_value()) {
      break;
    }
    auto parent = table.SnapshotById(*current->parent_snapshot_id);
    if (!parent.has_value()) {
      if (parent.error().kind == ErrorKind::kNotFound) {
        break;
      }
      return std::unexpected<Error>(parent.error());
    }
    current = std::move(parent).value();
  }
  ICEBERG_CHECK(selected != nullptr, "Cannot find a snapshot at or before the timestamp");
  return selected;
}

Result<std::shared_ptr<StructType>> UnifiedPartitionType(const Table& table) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, table.schema());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_ref, table.specs());

  std::vector<std::shared_ptr<PartitionSpec>> specs;
  specs.reserve(specs_ref.get().size());
  for (const auto& [_, spec] : specs_ref.get()) {
    ICEBERG_PRECHECK(spec != nullptr, "Partition spec cannot be null");
    specs.push_back(spec);
  }
  std::ranges::sort(specs, std::greater{}, &PartitionSpec::spec_id);

  std::unordered_set<int32_t> active_field_ids;
  for (const auto& spec : specs) {
    for (const auto& field : spec->fields()) {
      ICEBERG_PRECHECK(field.transform() != nullptr,
                       "Partition field {} has a null transform", field.field_id());
      ICEBERG_CHECK(field.transform()->transform_type() != TransformType::kUnknown,
                    "Cannot build table partition type with unknown transform '{}'",
                    field.transform()->ToString());
      ICEBERG_ASSIGN_OR_RAISE(auto source_field,
                              schema->FindFieldById(field.source_id()));
      if (source_field.has_value()) {
        active_field_ids.insert(field.field_id());
      }
    }
  }

  struct ProjectedField {
    const PartitionField* definition;
    std::string name;
    std::shared_ptr<Type> type;
  };
  std::map<int32_t, ProjectedField> fields_by_id;
  for (const auto& spec : specs) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec_type, spec->PartitionType(*schema));
    ICEBERG_PRECHECK(spec_type->fields().size() == spec->fields().size(),
                     "Partition spec {} has mismatched field and type counts",
                     spec->spec_id());
    for (size_t index = 0; index < spec->fields().size(); ++index) {
      const auto& partition_field = spec->fields()[index];
      if (!active_field_ids.contains(partition_field.field_id())) {
        continue;
      }

      const auto& spec_field = spec_type->fields()[index];
      auto [iter, inserted] =
          fields_by_id.try_emplace(partition_field.field_id(),
                                   ProjectedField{.definition = &partition_field,
                                                  .name = std::string(spec_field.name()),
                                                  .type = spec_field.type()});
      if (!inserted) {
        const auto& existing = *iter->second.definition;
        const auto current_transform = partition_field.transform()->transform_type();
        const auto existing_transform = existing.transform()->transform_type();
        const bool compatible_transform =
            *partition_field.transform() == *existing.transform() ||
            current_transform == TransformType::kVoid ||
            existing_transform == TransformType::kVoid;
        ICEBERG_CHECK(
            partition_field.source_id() == existing.source_id() && compatible_transform,
            "Conflicting partition fields with ID {}: '{}' and '{}'",
            partition_field.field_id(), partition_field.ToString(), existing.ToString());

        if (existing_transform == TransformType::kVoid &&
            current_transform != TransformType::kVoid) {
          iter->second.definition = &partition_field;
          iter->second.type = spec_field.type();
        }
      }
    }
  }

  std::vector<SchemaField> fields;
  fields.reserve(fields_by_id.size());
  for (auto& [_, field] : fields_by_id) {
    fields.emplace_back(field.definition->field_id(), std::move(field.name),
                        std::move(field.type), /*optional=*/true);
  }
  return std::make_shared<StructType>(std::move(fields));
}

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

Result<PartitionValues> ProjectPartitionValues(const StructType& partition_type,
                                               const PartitionSpec& spec,
                                               const PartitionValues& values) {
  ICEBERG_PRECHECK(values.num_fields() == spec.fields().size(),
                   "Partition has {} values but spec {} has {} fields",
                   values.num_fields(), spec.spec_id(), spec.fields().size());

  std::unordered_map<int32_t, size_t> positions;
  positions.reserve(spec.fields().size());
  for (size_t index = 0; index < spec.fields().size(); ++index) {
    positions.emplace(spec.fields()[index].field_id(), index);
  }

  std::vector<Literal> projected;
  projected.reserve(partition_type.fields().size());
  for (const auto& field : partition_type.fields()) {
    auto target_type = checked_pointer_cast<PrimitiveType>(field.type());
    auto position = positions.find(field.field_id());
    if (position == positions.end()) {
      projected.push_back(Literal::Null(std::move(target_type)));
      continue;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto value, values.ValueAt(position->second));
    if (value.get().IsNull()) {
      projected.push_back(Literal::Null(std::move(target_type)));
    } else if (value.get().type()->type_id() == target_type->type_id()) {
      projected.push_back(value.get());
    } else {
      ICEBERG_ASSIGN_OR_RAISE(auto coerced, value.get().CastTo(target_type));
      projected.push_back(std::move(coerced));
    }
  }
  return PartitionValues(std::move(projected));
}

Status AppendPartitionValues(ArrowArray* array, const StructType& partition_type,
                             const PartitionValues& values) {
  ICEBERG_PRECHECK(array != nullptr, "Partition Arrow array cannot be null");
  ICEBERG_PRECHECK(
      array->n_children == static_cast<int64_t>(partition_type.fields().size()),
      "Partition Arrow array has {} fields but schema has {}", array->n_children,
      partition_type.fields().size());
  ICEBERG_PRECHECK(values.num_fields() == partition_type.fields().size(),
                   "Partition has {} values but schema has {} fields",
                   values.num_fields(), partition_type.fields().size());

  for (size_t index = 0; index < values.num_fields(); ++index) {
    ICEBERG_ASSIGN_OR_RAISE(auto value, values.ValueAt(index));
    ICEBERG_RETURN_UNEXPECTED(AppendLiteral(array->children[index], value.get()));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

Status AppendDataFile(ArrowRowBuilder& builder, const Schema& schema,
                      const Schema& table_schema, const StructType& partition_type,
                      const LiveFile& live_file) {
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
        if (field.name() == "readable_metrics") {
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

Result<std::vector<LiveFile>> LoadLiveFiles(const Table& table,
                                            const std::shared_ptr<Snapshot>& snapshot) {
  if (snapshot == nullptr) {
    return std::vector<LiveFile>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto schema, table.schema());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_ref, table.specs());
  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot_cache.Manifests(table.io()));

  std::vector<LiveFile> files;
  for (const auto& manifest : manifests) {
    auto spec = specs_ref.get().find(manifest.partition_spec_id);
    ICEBERG_CHECK(spec != specs_ref.get().end(),
                  "Cannot find partition spec {} for manifest '{}'",
                  manifest.partition_spec_id, manifest.manifest_path);
    ICEBERG_PRECHECK(spec->second != nullptr, "Partition spec {} is null",
                     manifest.partition_spec_id);

    ICEBERG_ASSIGN_OR_RAISE(
        auto reader, ManifestReader::Make(manifest, table.io(), schema, specs_ref.get()));
    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());
    files.reserve(files.size() + entries.size());
    for (auto& entry : entries) {
      ICEBERG_PRECHECK(entry.data_file != nullptr,
                       "Manifest '{}' contains an entry with no data file",
                       manifest.manifest_path);
      files.push_back(LiveFile{.file = std::move(entry.data_file),
                               .spec = spec->second,
                               .snapshot_id = entry.snapshot_id});
    }
  }
  return files;
}

}  // namespace iceberg::internal
