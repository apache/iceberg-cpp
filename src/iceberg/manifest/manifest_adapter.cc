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

#include <memory>
#include <utility>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/manifest/manifest_adapter_internal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

constexpr int64_t kBlockSizeInBytesV1 = 64 * 1024 * 1024L;

Status AppendField(ArrowArray* array, int64_t value) {
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendInt(array, value));
  return {};
}

Status AppendField(ArrowArray* array, uint64_t value) {
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendUInt(array, value));
  return {};
}

Status AppendField(ArrowArray* array, double value) {
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendDouble(array, value));
  return {};
}

Status AppendField(ArrowArray* array, std::string_view value) {
  ArrowStringView view(value.data(), value.size());
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendString(array, view));
  return {};
}

Status AppendField(ArrowArray* array, std::span<const uint8_t> value) {
  ArrowBufferViewData data;
  data.as_char = reinterpret_cast<const char*>(value.data());
  ArrowBufferView view(data, value.size());
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendBytes(array, view));
  return {};
}

Status AppendList(ArrowArray* array, const std::vector<int32_t>& list_value) {
  auto list_array = array->children[0];
  for (const auto& value : list_value) {
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(
        ArrowArrayAppendInt(list_array, static_cast<int64_t>(value)));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

Status AppendList(ArrowArray* array, const std::vector<int64_t>& list_value) {
  auto list_array = array->children[0];
  for (const auto& value : list_value) {
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendInt(list_array, value));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

Status AppendMap(ArrowArray* array, const std::map<int32_t, int64_t>& map_value) {
  auto map_array = array->children[0];
  if (map_array->n_children != 2) {
    return InvalidArrowData("Map array must have exactly 2 children.");
  }
  for (const auto& [key, value] : map_value) {
    auto key_array = map_array->children[0];
    auto value_array = map_array->children[1];
    ICEBERG_RETURN_UNEXPECTED(AppendField(key_array, static_cast<int64_t>(key)));
    ICEBERG_RETURN_UNEXPECTED(AppendField(value_array, value));
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(map_array));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

Status AppendMap(ArrowArray* array,
                 const std::map<int32_t, std::vector<uint8_t>>& map_value) {
  auto map_array = array->children[0];
  if (map_array->n_children != 2) {
    return InvalidArrowData("Map array must have exactly 2 children.");
  }
  for (const auto& [key, value] : map_value) {
    auto key_array = map_array->children[0];
    auto value_array = map_array->children[1];
    ICEBERG_RETURN_UNEXPECTED(AppendField(key_array, static_cast<int64_t>(key)));
    ICEBERG_RETURN_UNEXPECTED(AppendField(value_array, value));
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(map_array));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

}  // namespace

Status ManifestAdapter::StartAppending() {
  if (size_ > 0) {
    return InvalidArgument("Adapter buffer not empty, cannot start appending.");
  }
  array_ = {};
  size_ = 0;
  ArrowError error;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayInitFromSchema(&array_, &schema_, &error), error);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayStartAppending(&array_));
  return {};
}

Result<ArrowArray*> ManifestAdapter::FinishAppending() {
  ArrowError error;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayFinishBuildingDefault(&array_, &error), error);
  return &array_;
}

ManifestEntryAdapter::ManifestEntryAdapter(std::optional<int64_t> snapshot_id_,
                                           std::shared_ptr<PartitionSpec> partition_spec,
                                           std::shared_ptr<Schema> current_schema,
                                           ManifestContent content)
    : snapshot_id_(snapshot_id_),
      partition_spec_(std::move(partition_spec)),
      current_schema_(std::move(current_schema)),
      content_(content) {
  if (!partition_spec_) {
    partition_spec_ = PartitionSpec::Unpartitioned();
  }
}

ManifestEntryAdapter::~ManifestEntryAdapter() {
  if (array_.release != nullptr) {
    ArrowArrayRelease(&array_);
  }
  if (schema_.release != nullptr) {
    ArrowSchemaRelease(&schema_);
  }
}

Status ManifestEntryAdapter::AppendPartitionValues(
    ArrowArray* array, const std::shared_ptr<StructType>& partition_type,
    const PartitionValues& partition_values) {
  if (array->n_children != partition_type->fields().size()) [[unlikely]] {
    return InvalidArrowData("Arrow array of partition does not match partition type.");
  }
  if (partition_values.num_fields() != partition_type->fields().size()) [[unlikely]] {
    return InvalidArrowData("Literal list of partition does not match partition type.");
  }
  auto fields = partition_type->fields();

  for (size_t i = 0; i < fields.size(); i++) {
    const auto& partition_value = partition_values.ValueAt(i)->get();
    const auto& partition_field = fields[i];
    auto child_array = array->children[i];
    if (partition_value.IsNull()) {
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(child_array, 1));
      continue;
    }
    switch (partition_field.type()->type_id()) {
      case TypeId::kBoolean:
        ICEBERG_RETURN_UNEXPECTED(AppendField(
            child_array, static_cast<uint64_t>(
                             std::get<bool>(partition_value.value()) == true ? 1L : 0L)));
        break;
      case TypeId::kInt:
        ICEBERG_RETURN_UNEXPECTED(AppendField(
            child_array,
            static_cast<int64_t>(std::get<int32_t>(partition_value.value()))));
        break;
      case TypeId::kLong:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(child_array, std::get<int64_t>(partition_value.value())));
        break;
      case TypeId::kFloat:
        ICEBERG_RETURN_UNEXPECTED(AppendField(
            child_array, static_cast<double>(std::get<float>(partition_value.value()))));
        break;
      case TypeId::kDouble:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(child_array, std::get<double>(partition_value.value())));
        break;
      case TypeId::kString:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(child_array, std::get<std::string>(partition_value.value())));
        break;
      case TypeId::kFixed:
      case TypeId::kBinary:
        ICEBERG_RETURN_UNEXPECTED(AppendField(
            child_array, std::get<std::vector<uint8_t>>(partition_value.value())));
        break;
      case TypeId::kDate:
        ICEBERG_RETURN_UNEXPECTED(AppendField(
            child_array,
            static_cast<int64_t>(std::get<int32_t>(partition_value.value()))));
        break;
      case TypeId::kTime:
      case TypeId::kTimestamp:
      case TypeId::kTimestampTz:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(child_array, std::get<int64_t>(partition_value.value())));
        break;
      case TypeId::kDecimal:
        ICEBERG_RETURN_UNEXPECTED(AppendField(
            child_array, std::get<Decimal>(partition_value.value()).ToBytes()));
        break;
      case TypeId::kUuid:
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(child_array, std::get<Uuid>(partition_value.value()).bytes()));
        break;
      case TypeId::kStruct:
      case TypeId::kList:
      case TypeId::kMap:
        // TODO(xiao.dong) Literals do not currently support these types
      default:
        return InvalidManifest("Unsupported partition type: {}",
                               partition_field.ToString());
    }
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

Status ManifestEntryAdapter::AppendDataFile(
    ArrowArray* array, const std::shared_ptr<StructType>& data_file_type,
    const DataFile& file) {
  auto fields = data_file_type->fields();
  for (size_t i = 0; i < fields.size(); i++) {
    const auto& field = fields[i];
    auto child_array = array->children[i];

    switch (field.field_id()) {
      case 134:  // content (optional int32)
        ICEBERG_RETURN_UNEXPECTED(
            AppendField(child_array, static_cast<int64_t>(file.content)));
        break;
      case 100:  // file_path (required string)
        ICEBERG_RETURN_UNEXPECTED(AppendField(child_array, file.file_path));
        break;
      case 101:  // file_format (required string)
        ICEBERG_RETURN_UNEXPECTED(AppendField(child_array, ToString(file.file_format)));
        break;
      case 102: {
        // partition (required struct)
        auto partition_type = internal::checked_pointer_cast<StructType>(field.type());
        ICEBERG_RETURN_UNEXPECTED(
            AppendPartitionValues(child_array, partition_type, file.partition));
      } break;
      case 103:  // record_count (required int64)
        ICEBERG_RETURN_UNEXPECTED(AppendField(child_array, file.record_count));
        break;
      case 104:  // file_size_in_bytes (required int64)
        ICEBERG_RETURN_UNEXPECTED(AppendField(child_array, file.file_size_in_bytes));
        break;
      case 105:  // block_size_in_bytes (compatible in v1)
        // always 64MB for v1
        ICEBERG_RETURN_UNEXPECTED(AppendField(child_array, kBlockSizeInBytesV1));
        break;
      case 108:  // column_sizes (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(child_array, file.column_sizes));
        break;
      case 109:  // value_counts (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(child_array, file.value_counts));
        break;
      case 110:  // null_value_counts (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(child_array, file.null_value_counts));
        break;
      case 137:  // nan_value_counts (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(child_array, file.nan_value_counts));
        break;
      case 125:  // lower_bounds (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(child_array, file.lower_bounds));
        break;
      case 128:  // upper_bounds (optional map)
        ICEBERG_RETURN_UNEXPECTED(AppendMap(child_array, file.upper_bounds));
        break;
      case 131:  // key_metadata (optional binary)
        if (!file.key_metadata.empty()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(child_array, file.key_metadata));
        } else {
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(child_array, 1));
        }
        break;
      case 132:  // split_offsets (optional list)
        ICEBERG_RETURN_UNEXPECTED(AppendList(child_array, file.split_offsets));
        break;
      case 135:  // equality_ids (optional list)
        ICEBERG_RETURN_UNEXPECTED(AppendList(child_array, file.equality_ids));
        break;
      case 140:  // sort_order_id (optional int32)
        if (file.sort_order_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(child_array, static_cast<int64_t>(file.sort_order_id.value())));
        } else {
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(child_array, 1));
        }
        break;
      case 142:  // first_row_id (optional int64)
        if (file.first_row_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(child_array, file.first_row_id.value()));
        } else {
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(child_array, 1));
        }
        break;
      case 143: {
        // referenced_data_file (optional string)
        ICEBERG_ASSIGN_OR_RAISE(auto referenced_data_file, GetReferenceDataFile(file));
        if (referenced_data_file.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(child_array, referenced_data_file.value()));
        } else {
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(child_array, 1));
        }
        break;
      }
      case 144:  // content_offset (optional int64)
        if (file.content_offset.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(child_array, file.content_offset.value()));
        } else {
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(child_array, 1));
        }
        break;
      case 145:  // content_size_in_bytes (optional int64)
        if (file.content_size_in_bytes.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(child_array, file.content_size_in_bytes.value()));
        } else {
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(child_array, 1));
        }
        break;
      default:
        return InvalidManifest("Unknown data file field id: {} ", field.field_id());
    }
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

Result<std::optional<int64_t>> ManifestEntryAdapter::GetSequenceNumber(
    const ManifestEntry& entry) const {
  return entry.sequence_number;
}

Result<std::optional<std::string>> ManifestEntryAdapter::GetReferenceDataFile(
    const DataFile& file) const {
  return file.referenced_data_file;
}

Result<std::optional<int64_t>> ManifestEntryAdapter::GetFirstRowId(
    const DataFile& file) const {
  return file.first_row_id;
}

Result<std::optional<int64_t>> ManifestEntryAdapter::GetContentOffset(
    const DataFile& file) const {
  return file.content_offset;
}

Result<std::optional<int64_t>> ManifestEntryAdapter::GetContentSizeInBytes(
    const DataFile& file) const {
  return file.content_size_in_bytes;
}

Status ManifestEntryAdapter::AppendInternal(const ManifestEntry& entry) {
  if (entry.data_file == nullptr) [[unlikely]] {
    return InvalidManifest("Missing required data_file field from manifest entry.");
  }

  const auto& fields = manifest_schema_->fields();
  for (size_t i = 0; i < fields.size(); i++) {
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
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 2:  // data_file (required struct)
        if (entry.data_file) {
          // Get the data file type from the field
          auto data_file_type = internal::checked_pointer_cast<StructType>(field.type());
          ICEBERG_RETURN_UNEXPECTED(
              AppendDataFile(array, data_file_type, *entry.data_file));
        } else {
          return InvalidManifest("Missing required data_file field from manifest entry.");
        }
        break;
      case 3: {
        // sequence_number (optional int64)
        ICEBERG_ASSIGN_OR_RAISE(auto sequence_num, GetSequenceNumber(entry));
        if (sequence_num.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, sequence_num.value()));
        } else {
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
        }
        break;
      }
      case 4:  // file_sequence_number (optional int64)
        if (entry.file_sequence_number.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(array, entry.file_sequence_number.value()));
        } else {
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
        }
        break;
      default:
        return InvalidManifest("Unknown manifest entry field id: {}", field.field_id());
    }
  }

  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(&array_));
  size_++;
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

Status ManifestFileAdapter::AppendPartitionSummary(
    ArrowArray* array, const std::shared_ptr<ListType>& summary_type,
    const std::vector<PartitionFieldSummary>& summaries) {
  auto& summary_array = array->children[0];
  if (summary_array->n_children != 4) {
    return InvalidManifestList("Invalid partition summary array.");
  }
  auto summary_struct =
      internal::checked_pointer_cast<StructType>(summary_type->fields()[0].type());
  auto summary_fields = summary_struct->fields();
  for (const auto& summary : summaries) {
    for (const auto& summary_field : summary_fields) {
      switch (summary_field.field_id()) {
        case 509:  // contains_null (required bool)
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(summary_array->children[0],
                          static_cast<uint64_t>(summary.contains_null ? 1 : 0)));
          break;
        case 518: {
          // contains_nan (optional bool)
          auto field_array = summary_array->children[1];
          if (summary.contains_nan.has_value()) {
            ICEBERG_RETURN_UNEXPECTED(
                AppendField(field_array,
                            static_cast<uint64_t>(summary.contains_nan.value() ? 1 : 0)));
          } else {
            ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(field_array, 1));
          }
          break;
        }
        case 510: {
          // lower_bound (optional binary)
          auto field_array = summary_array->children[2];
          if (summary.lower_bound.has_value() && !summary.lower_bound->empty()) {
            ICEBERG_RETURN_UNEXPECTED(
                AppendField(field_array, summary.lower_bound.value()));
          } else {
            ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(field_array, 1));
          }
          break;
        }
        case 511: {
          // upper_bound (optional binary)
          auto field_array = summary_array->children[3];
          if (summary.upper_bound.has_value() && !summary.upper_bound->empty()) {
            ICEBERG_RETURN_UNEXPECTED(
                AppendField(field_array, summary.upper_bound.value()));
          } else {
            ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(field_array, 1));
          }
          break;
        }
        default:
          return InvalidManifestList("Unknown field id: {}", summary_field.field_id());
      }
    }
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(summary_array));
  }

  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

Result<int64_t> ManifestFileAdapter::GetSequenceNumber(const ManifestFile& file) const {
  return file.sequence_number;
}

Result<int64_t> ManifestFileAdapter::GetMinSequenceNumber(
    const ManifestFile& file) const {
  return file.min_sequence_number;
}

Result<std::optional<int64_t>> ManifestFileAdapter::GetFirstRowId(
    const ManifestFile& file) const {
  return file.first_row_id;
}

Status ManifestFileAdapter::AppendInternal(const ManifestFile& file) {
  const auto& fields = manifest_list_schema_->fields();
  for (size_t i = 0; i < fields.size(); i++) {
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
      case 515: {
        // sequence_number
        ICEBERG_ASSIGN_OR_RAISE(auto sequence_num, GetSequenceNumber(file));
        ICEBERG_RETURN_UNEXPECTED(AppendField(array, sequence_num));
        break;
      }
      case 516: {
        // min_sequence_number
        ICEBERG_ASSIGN_OR_RAISE(auto min_sequence_num, GetMinSequenceNumber(file));
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
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 505:  // existing_files_count
        if (file.existing_files_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(
              array, static_cast<int64_t>(file.existing_files_count.value())));
        } else {
          // Append null for optional field
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 506:  // deleted_files_count
        if (file.deleted_files_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendField(array, static_cast<int64_t>(file.deleted_files_count.value())));
        } else {
          // Append null for optional field
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 512:  // added_rows_count
        if (file.added_rows_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, file.added_rows_count.value()));
        } else {
          // Append null for optional field
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 513:  // existing_rows_count
        if (file.existing_rows_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, file.existing_rows_count.value()));
        } else {
          // Append null for optional field
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 514:  // deleted_rows_count
        if (file.deleted_rows_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, file.deleted_rows_count.value()));
        } else {
          // Append null for optional field
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 507:  // partitions
        ICEBERG_RETURN_UNEXPECTED(AppendPartitionSummary(
            array, internal::checked_pointer_cast<ListType>(field.type()),
            file.partitions));
        break;
      case 519:  // key_metadata
        if (!file.key_metadata.empty()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, file.key_metadata));
        } else {
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
        }
        break;
      case 520: {
        // first_row_id
        ICEBERG_ASSIGN_OR_RAISE(auto first_row_id, GetFirstRowId(file));
        if (first_row_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendField(array, first_row_id.value()));
        } else {
          // Append null for optional field
          ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
        }
        break;
      }
      default:
        return InvalidManifestList("Unknown field id: {}", field.field_id());
    }
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(&array_));
  size_++;
  return {};
}

}  // namespace iceberg
