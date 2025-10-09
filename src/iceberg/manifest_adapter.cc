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

#include "iceberg/manifest_adapter.h"

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_error_transform_internal.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

#define NANOARROW_RETURN_IF_FAILED(status)                  \
  if (status != NANOARROW_OK) [[unlikely]] {                \
    return InvalidArrowData("nanoarrow error: {}", status); \
  }

namespace {
static constexpr int64_t kBlockSizeInBytesV1 = 64 * 1024 * 1024L;
}

namespace iceberg {

Status ManifestAdapter::StartAppending() {
  if (size_ > 0) {
    return InvalidArgument("Adapter buffer not empty, cannot start appending.");
  }
  array_ = {};
  size_ = 0;
  ArrowError error;
  ICEBERG_NANOARROW_RETURN_IF_NOT_OK(ArrowArrayInitFromSchema(&array_, &schema_, &error),
                                     error);
  NANOARROW_RETURN_IF_FAILED(ArrowArrayStartAppending(&array_));
  return {};
}

Result<ArrowArray*> ManifestAdapter::FinishAppending() {
  ArrowError error;
  ICEBERG_NANOARROW_RETURN_IF_NOT_OK(ArrowArrayFinishBuildingDefault(&array_, &error),
                                     error);
  return &array_;
}

Status ManifestAdapter::AppendField(ArrowArray* arrowArray, int64_t value) {
  NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendInt(arrowArray, value));
  return {};
}

Status ManifestAdapter::AppendField(ArrowArray* arrowArray, uint64_t value) {
  NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendUInt(arrowArray, value));
  return {};
}

Status ManifestAdapter::AppendField(ArrowArray* arrowArray, double value) {
  NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendDouble(arrowArray, value));
  return {};
}

Status ManifestAdapter::AppendField(ArrowArray* arrowArray, std::string_view value) {
  ArrowStringView view(value.data(), value.size());
  NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendString(arrowArray, view));
  return {};
}

Status ManifestAdapter::AppendField(ArrowArray* arrowArray,
                                    const std::span<const uint8_t>& value) {
  ArrowBufferViewData data;
  data.as_char = reinterpret_cast<const char*>(value.data());
  ArrowBufferView view(data, value.size());
  NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendBytes(arrowArray, view));
  return {};
}

ManifestEntryAdapter::~ManifestEntryAdapter() {
  if (array_.release != nullptr) {
    ArrowArrayRelease(&array_);
  }
  if (schema_.release != nullptr) {
    ArrowSchemaRelease(&schema_);
  }
}

Result<std::shared_ptr<StructType>> ManifestEntryAdapter::GetManifestEntryStructType() {
  if (partition_spec_ == nullptr) {
    return ManifestEntry::TypeFromPartitionType(nullptr);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto partition_schema, partition_spec_->GetPartitionSchema());
  return ManifestEntry::TypeFromPartitionType(std::move(partition_schema));
}

Status ManifestEntryAdapter::AppendPartition(
    ArrowArray* arrow_array, const std::shared_ptr<StructType>& partition_type,
    const std::vector<Literal>& partitions) {
  if (arrow_array->n_children != partition_type->fields().size()) [[unlikely]] {
    return InvalidManifest("Arrow array of partition does not match partition type.");
  }
  if (partitions.size() != partition_type->fields().size()) [[unlikely]] {
    return InvalidManifest("Literal list of partition does not match partition type.");
  }
  auto fields = partition_type->fields();

  for (int32_t i = 0; i < fields.size(); i++) {
    const auto& partition = partitions[i];
    const auto& field = fields[i];
    auto array = arrow_array->children[i];
    if (partition.IsNull()) {
      NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
      continue;
    }
    switch (field.type()->type_id()) {
      case TypeId::kBoolean:
        ICEBERG_RETURN_UNEXPECTED(AppendField(
            array,
            static_cast<uint64_t>(std::get<bool>(partition.value()) == true ? 1L : 0L)));
        break;
      case TypeId::kInt:
        ICEBERG_RETURN_UNEXPECTED(AppendField(
            array, static_cast<int64_t>(std::get<int32_t>(partition.value()))));
        break;
      case TypeId::kLong:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(array, std::get<int64_t>(partition.value())));
        break;
      case TypeId::kFloat:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(array, static_cast<double>(std::get<float>(partition.value()))));
        break;
      case TypeId::kDouble:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(array, std::get<double>(partition.value())));
        break;
      case TypeId::kString:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(array, std::get<std::string>(partition.value())));
        break;
      case TypeId::kFixed:
      case TypeId::kBinary:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(array, std::get<std::vector<uint8_t>>(partition.value())));
        break;
      case TypeId::kDate:
        ICEBERG_RETURN_UNEXPECTED(AppendField(
            array, static_cast<int64_t>(std::get<int32_t>(partition.value()))));
        break;
      case TypeId::kTime:
      case TypeId::kTimestamp:
      case TypeId::kTimestampTz:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(array, std::get<int64_t>(partition.value())));
        break;
      case TypeId::kDecimal:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(array, std::get<std::array<uint8_t, 16>>(partition.value())));
        break;
      case TypeId::kUuid:
      case TypeId::kStruct:
      case TypeId::kList:
      case TypeId::kMap:
        // TODO(xiao.dong) currently literal does not support those types
      default:
        return InvalidManifest("Unsupported partition type: {}", field.ToString());
    }
  }
  NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(arrow_array));
  return {};
}

Status ManifestEntryAdapter::AppendList(ArrowArray* arrow_array,
                                        const std::vector<int32_t>& list_value) {
  auto list_array = arrow_array->children[0];
  for (const auto& value : list_value) {
    NANOARROW_RETURN_IF_FAILED(
        ArrowArrayAppendInt(list_array, static_cast<int64_t>(value)));
  }
  NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(arrow_array));
  return {};
}

Status ManifestEntryAdapter::AppendList(ArrowArray* arrow_array,
                                        const std::vector<int64_t>& list_value) {
  auto list_array = arrow_array->children[0];
  for (const auto& value : list_value) {
    NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendInt(list_array, value));
  }
  NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(arrow_array));
  return {};
}

Status ManifestEntryAdapter::AppendMap(ArrowArray* arrow_array,
                                       const std::map<int32_t, int64_t>& map_value) {
  auto map_array = arrow_array->children[0];
  if (map_array->n_children != 2) {
    return InvalidManifest("Invalid map array.");
  }
  for (const auto& [key, value] : map_value) {
    auto key_array = map_array->children[0];
    auto value_array = map_array->children[1];
    ICEBERG_RETURN_UNEXPECTED(AppendField(key_array, static_cast<int64_t>(key)));
    ICEBERG_RETURN_UNEXPECTED(AppendField(value_array, value));
    NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(map_array));
  }
  NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(arrow_array));
  return {};
}

Status ManifestEntryAdapter::AppendMap(
    ArrowArray* arrow_array, const std::map<int32_t, std::vector<uint8_t>>& map_value) {
  auto map_array = arrow_array->children[0];
  if (map_array->n_children != 2) {
    return InvalidManifest("Invalid map array.");
  }
  for (const auto& [key, value] : map_value) {
    auto key_array = map_array->children[0];
    auto value_array = map_array->children[1];
    ICEBERG_RETURN_UNEXPECTED(AppendField(key_array, static_cast<int64_t>(key)));
    ICEBERG_RETURN_UNEXPECTED(AppendField(value_array, value));
    NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(map_array));
  }
  NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(arrow_array));
  return {};
}

Status ManifestEntryAdapter::AppendDataFile(
    ArrowArray* arrow_array, const std::shared_ptr<StructType>& data_file_type,
    const std::shared_ptr<DataFile>& file) {
  auto fields = data_file_type->fields();
  for (int32_t i = 0; i < fields.size(); i++) {
    const auto& field = fields[i];
    auto array = arrow_array->children[i];

    switch (field.field_id()) {
      case 134:  // content (optional int32)
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(array, static_cast<int64_t>(file->content)));
        break;
      case 100:  // file_path (required string)
        ICEBERG_RETURN_UNEXPECTED(AppendField(array, file->file_path));
        break;
      case 101:  // file_format (required string)
        ICEBERG_RETURN_UNEXPECTED(AppendField(array, ToString(file->file_format)));
        break;
      case 102:  // partition (required struct)
      {
        auto partition_type = internal::checked_pointer_cast<StructType>(field.type());
        ICEBERG_RETURN_UNEXPECTED(
            AppendPartition(array, partition_type, file->partition));
      } break;
      case 103:  // record_count (required int64)
        ICEBERG_RETURN_UNEXPECTED(AppendField(array, file->record_count));
        break;
      case 104:  // file_size_in_bytes (required int64)
        ICEBERG_RETURN_UNEXPECTED(AppendField(array, file->file_size_in_bytes));
        break;
      case 105:  // block_size_in_bytes (compatible in v1)
        // always 64MB for v1
        ICEBERG_RETURN_UNEXPECTED(AppendField(array, kBlockSizeInBytesV1));
        break;
      case 108:  // column_sizes (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(array, file->column_sizes));
        break;
      case 109:  // value_counts (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(array, file->value_counts));
        break;
      case 110:  // null_value_counts (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(array, file->null_value_counts));
        break;
      case 137:  // nan_value_counts (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(array, file->nan_value_counts));
        break;
      case 125:  // lower_bounds (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(array, file->lower_bounds));
        break;
      case 128:  // upper_bounds (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(array, file->upper_bounds));
        break;
      case 131:  // key_metadata (optional binary)
        if (!file->key_metadata.empty()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, file->key_metadata));
        } else {
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 132:  // split_offsets (optional list)
        ICEBERG_RETURN_UNEXPECTED(AppendList(array, file->split_offsets));
        break;
      case 135:  // equality_ids (optional list)
        ICEBERG_RETURN_UNEXPECTED(AppendList(array, file->equality_ids));
        break;
      case 140:  // sort_order_id (optional int32)
        if (file->sort_order_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(array, static_cast<int64_t>(file->sort_order_id.value())));
        } else {
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 142:  // first_row_id (optional int64)
        if (file->first_row_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, file->first_row_id.value()));
        } else {
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 143:  // referenced_data_file (optional string)
      {
        ICEBERG_ASSIGN_OR_RAISE(auto referenced_data_file,
                                GetWrappedReferenceDataFile(file));
        if (referenced_data_file.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, referenced_data_file.value()));
        } else {
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      }
      case 144:  // content_offset (optional int64)
        if (file->content_offset.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, file->content_offset.value()));
        } else {
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 145:  // content_size_in_bytes (optional int64)
        if (file->content_size_in_bytes.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(array, file->content_size_in_bytes.value()));
        } else {
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      default:
        return InvalidManifest("Unknown data file field id: {} ", field.field_id());
    }
  }
  NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(arrow_array));
  return {};
}

Result<std::optional<int64_t>> ManifestEntryAdapter::GetSequenceNumber(
    const ManifestEntry& entry) {
  return entry.sequence_number;
}

Result<std::optional<std::string>> ManifestEntryAdapter::GetWrappedReferenceDataFile(
    const std::shared_ptr<DataFile>& file) {
  return file->referenced_data_file;
}

Result<std::optional<int64_t>> ManifestEntryAdapter::GetWrappedFirstRowId(
    const std::shared_ptr<DataFile>& file) {
  return file->first_row_id;
}

Result<std::optional<int64_t>> ManifestEntryAdapter::GetWrappedContentOffset(
    const std::shared_ptr<DataFile>& file) {
  return file->content_offset;
}

Result<std::optional<int64_t>> ManifestEntryAdapter::GetWrappedContentSizeInBytes(
    const std::shared_ptr<DataFile>& file) {
  return file->content_size_in_bytes;
}

Status ManifestEntryAdapter::AppendInternal(const ManifestEntry& entry) {
  const auto& fields = manifest_schema_->fields();
  for (int32_t i = 0; i < fields.size(); i++) {
    const auto& field = fields[i];
    auto array = array_.children[i];

    switch (field.field_id()) {
      case 0:  // status (required int32)
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(array, static_cast<int64_t>(static_cast<int32_t>(entry.status))));
        break;
      case 1:  // snapshot_id (optional int64)
        if (entry.snapshot_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, entry.snapshot_id.value()));
        } else {
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 2:  // data_file (required struct)
        if (entry.data_file) {
          // Get the data file type from the field
          auto data_file_type = internal::checked_pointer_cast<StructType>(field.type());
          ICEBERG_RETURN_UNEXPECTED(
              AppendDataFile(array, data_file_type, entry.data_file));
        } else {
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 3:  // sequence_number (optional int64)
      {
        ICEBERG_ASSIGN_OR_RAISE(auto sequence_num, GetSequenceNumber(entry));
        if (sequence_num.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, sequence_num.value()));
        } else {
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      }
      case 4:  // file_sequence_number (optional int64)
        if (entry.file_sequence_number.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(array, entry.file_sequence_number.value()));
        } else {
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      default:
        return InvalidManifest("Unknown manifest entry field id: {}", field.field_id());
    }
  }

  NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(&array_));
  size_++;
  return {};
}

Status ManifestEntryAdapter::InitSchema(const std::unordered_set<int32_t>& fields_ids) {
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_entry_schema, GetManifestEntryStructType())
  auto fields_span = manifest_entry_schema->fields();
  std::vector<SchemaField> fields;
  // TODO(xiao.dong) make this a common function to recursive handle
  // all nested fields in schema
  for (const auto& field : fields_span) {
    if (field.field_id() == 2) {
      // handle data_file field
      auto data_file_struct = internal::checked_pointer_cast<StructType>(field.type());
      std::vector<SchemaField> data_file_fields;
      for (const auto& data_file_field : data_file_struct->fields()) {
        if (fields_ids.contains(data_file_field.field_id())) {
          data_file_fields.emplace_back(data_file_field);
        }
      }
      auto type = std::make_shared<StructType>(data_file_fields);
      auto data_file_field = SchemaField::MakeRequired(
          field.field_id(), std::string(field.name()), std::move(type));
      fields.emplace_back(std::move(data_file_field));
    } else {
      if (fields_ids.contains(field.field_id())) {
        fields.emplace_back(field);
      }
    }
  }
  manifest_schema_ = std::make_shared<Schema>(fields);
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*manifest_schema_, &schema_));
  return {};
}

ManifestFileAdapter::~ManifestFileAdapter() {
  if (array_.release != nullptr) {
    ArrowArrayRelease(&array_);
  }
  if (schema_.release != nullptr) {
    ArrowSchemaRelease(&schema_);
  }
}

Status ManifestFileAdapter::AppendPartitions(
    ArrowArray* arrow_array, const std::shared_ptr<ListType>& partition_type,
    const std::vector<PartitionFieldSummary>& partitions) {
  auto& array = arrow_array->children[0];
  if (array->n_children != 4) {
    return InvalidManifestList("Invalid partition array.");
  }
  auto partition_struct =
      internal::checked_pointer_cast<StructType>(partition_type->fields()[0].type());
  auto fields = partition_struct->fields();
  for (const auto& partition : partitions) {
    for (const auto& field : fields) {
      switch (field.field_id()) {
        case 509:  // contains_null (required bool)
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(array->children[0],
                          static_cast<uint64_t>(partition.contains_null ? 1 : 0)));
          break;
        case 518:  // contains_nan (optional bool)
        {
          auto field_array = array->children[1];
          if (partition.contains_nan.has_value()) {
            ICEBERG_RETURN_UNEXPECTED(AppendField(
                field_array,
                static_cast<uint64_t>(partition.contains_nan.value() ? 1 : 0)));
          } else {
            NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(field_array, 1));
          }
          break;
        }
        case 510:  // lower_bound (optional binary)
        {
          auto field_array = array->children[2];
          if (partition.lower_bound.has_value() && !partition.lower_bound->empty()) {
            ICEBERG_RETURN_UNEXPECTED(
                AppendField(field_array, partition.lower_bound.value()));
          } else {
            NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(field_array, 1));
          }
          break;
        }
        case 511:  // upper_bound (optional binary)
        {
          auto field_array = array->children[3];
          if (partition.upper_bound.has_value() && !partition.upper_bound->empty()) {
            ICEBERG_RETURN_UNEXPECTED(
                AppendField(field_array, partition.upper_bound.value()));
          } else {
            NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(field_array, 1));
          }
          break;
        }
        default:
          return InvalidManifestList("Unknown field id: {}", field.field_id());
      }
    }
    NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(array));
  }

  NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(arrow_array));
  return {};
}

Result<int64_t> ManifestFileAdapter::GetSequenceNumber(const ManifestFile& file) {
  return file.sequence_number;
}

Result<int64_t> ManifestFileAdapter::GetWrappedMinSequenceNumber(
    const ManifestFile& file) {
  return file.min_sequence_number;
}

Result<std::optional<int64_t>> ManifestFileAdapter::GetWrappedFirstRowId(
    const ManifestFile& file) {
  return file.first_row_id;
}

Status ManifestFileAdapter::AppendInternal(const ManifestFile& file) {
  const auto& fields = manifest_list_schema_->fields();
  for (int32_t i = 0; i < fields.size(); i++) {
    const auto& field = fields[i];
    auto array = array_.children[i];
    switch (field.field_id()) {
      case 500:  // manifest_path
        ICEBERG_RETURN_UNEXPECTED(AppendField(array, file.manifest_path));
        break;
      case 501:  // manifest_length
        ICEBERG_RETURN_UNEXPECTED(AppendField(array, file.manifest_length));
        break;
      case 502:  // partition_spec_id
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(array, static_cast<int64_t>(file.partition_spec_id)));
        break;
      case 517:  // content
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(array, static_cast<int64_t>(static_cast<int32_t>(file.content))));
        break;
      case 515:  // sequence_number
      {
        ICEBERG_ASSIGN_OR_RAISE(auto sequence_num, GetSequenceNumber(file));
        ICEBERG_RETURN_UNEXPECTED(AppendField(array, sequence_num));
        break;
      }
      case 516:  // min_sequence_number
      {
        ICEBERG_ASSIGN_OR_RAISE(auto min_sequence_num, GetWrappedMinSequenceNumber(file));
        ICEBERG_RETURN_UNEXPECTED(AppendField(array, min_sequence_num));
        break;
      }
      case 503:  // added_snapshot_id
        ICEBERG_RETURN_UNEXPECTED(AppendField(array, file.added_snapshot_id));
        break;
      case 504:  // added_files_count
        if (file.added_files_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(array, static_cast<int64_t>(file.added_files_count.value())));
        } else {
          // Append null for optional field
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 505:  // existing_files_count
        if (file.existing_files_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(
              array, static_cast<int64_t>(file.existing_files_count.value())));
        } else {
          // Append null for optional field
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 506:  // deleted_files_count
        if (file.deleted_files_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(array, static_cast<int64_t>(file.deleted_files_count.value())));
        } else {
          // Append null for optional field
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 512:  // added_rows_count
        if (file.added_rows_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, file.added_rows_count.value()));
        } else {
          // Append null for optional field
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 513:  // existing_rows_count
        if (file.existing_rows_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, file.existing_rows_count.value()));
        } else {
          // Append null for optional field
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 514:  // deleted_rows_count
        if (file.deleted_rows_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, file.deleted_rows_count.value()));
        } else {
          // Append null for optional field
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 507:  // partitions
        ICEBERG_RETURN_UNEXPECTED(AppendPartitions(
            array, internal::checked_pointer_cast<ListType>(field.type()),
            file.partitions));
        break;
      case 519:  // key_metadata
        if (!file.key_metadata.empty()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, file.key_metadata));
        } else {
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 520:  // first_row_id
      {
        ICEBERG_ASSIGN_OR_RAISE(auto first_row_id, GetWrappedFirstRowId(file));
        if (first_row_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, first_row_id.value()));
        } else {
          // Append null for optional field
          NANOARROW_RETURN_IF_FAILED(ArrowArrayAppendNull(array, 1));
        }
        break;
      }
      default:
        return InvalidManifestList("Unknown field id: {}", field.field_id());
    }
  }
  NANOARROW_RETURN_IF_FAILED(ArrowArrayFinishElement(&array_));
  size_++;
  return {};
}

Status ManifestFileAdapter::InitSchema(const std::unordered_set<int32_t>& fields_ids) {
  std::vector<SchemaField> fields;
  for (const auto& field : ManifestFile::Type().fields()) {
    if (fields_ids.contains(field.field_id())) {
      fields.emplace_back(field);
    }
  }
  manifest_list_schema_ = std::make_shared<Schema>(fields);
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*manifest_list_schema_, &schema_));
  return {};
}

}  // namespace iceberg
