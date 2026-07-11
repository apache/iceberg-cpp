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
#include "iceberg/arrow_row_builder_internal.h"
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
constexpr int32_t kBlockSizeInBytesFieldId = 105;

}  // namespace

Status ManifestAdapter::StartAppending() {
  if (size_ > 0) {
    return InvalidArgument("Adapter buffer not empty, cannot start appending.");
  }
  array_ = {};
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
  size_ = 0;
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
      ICEBERG_RETURN_UNEXPECTED(AppendNull(child_array));
      continue;
    }
    switch (partition_field.type()->type_id()) {
      case TypeId::kBoolean:
        ICEBERG_RETURN_UNEXPECTED(
            AppendBoolean(child_array, std::get<bool>(partition_value.value())));
        break;
      case TypeId::kInt:
        ICEBERG_RETURN_UNEXPECTED(
            AppendInt(child_array,
                      static_cast<int64_t>(std::get<int32_t>(partition_value.value()))));
        break;
      case TypeId::kLong:
        ICEBERG_RETURN_UNEXPECTED(
            AppendInt(child_array, std::get<int64_t>(partition_value.value())));
        break;
      case TypeId::kFloat:
        ICEBERG_RETURN_UNEXPECTED(AppendDouble(
            child_array, static_cast<double>(std::get<float>(partition_value.value()))));
        break;
      case TypeId::kDouble:
        ICEBERG_RETURN_UNEXPECTED(
            AppendDouble(child_array, std::get<double>(partition_value.value())));
        break;
      case TypeId::kString:
        ICEBERG_RETURN_UNEXPECTED(
            AppendString(child_array, std::get<std::string>(partition_value.value())));
        break;
      case TypeId::kFixed:
      case TypeId::kBinary:
        ICEBERG_RETURN_UNEXPECTED(AppendBytes(
            child_array, std::get<std::vector<uint8_t>>(partition_value.value())));
        break;
      case TypeId::kDate:
        ICEBERG_RETURN_UNEXPECTED(
            AppendInt(child_array,
                      static_cast<int64_t>(std::get<int32_t>(partition_value.value()))));
        break;
      case TypeId::kTime:
      case TypeId::kTimestamp:
      case TypeId::kTimestampTz:
      case TypeId::kTimestampNs:
      case TypeId::kTimestampTzNs:
        ICEBERG_RETURN_UNEXPECTED(
            AppendInt(child_array, std::get<int64_t>(partition_value.value())));
        break;
      case TypeId::kDecimal:
        ICEBERG_RETURN_UNEXPECTED(AppendBytes(
            child_array, std::get<Decimal>(partition_value.value()).ToBytes()));
        break;
      case TypeId::kUuid:
        ICEBERG_RETURN_UNEXPECTED(
            AppendBytes(child_array, std::get<Uuid>(partition_value.value()).bytes()));
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
      case DataFile::kContentFieldId:  // optional int32
        ICEBERG_RETURN_UNEXPECTED(
            AppendInt(child_array, static_cast<int64_t>(file.content)));
        break;
      case DataFile::kFilePathFieldId:  // required string
        ICEBERG_RETURN_UNEXPECTED(AppendString(child_array, file.file_path));
        break;
      case DataFile::kFileFormatFieldId:  // required string
        ICEBERG_RETURN_UNEXPECTED(AppendString(child_array, ToString(file.file_format)));
        break;
      case DataFile::kPartitionFieldId: {  // required struct
        auto partition_type = internal::checked_pointer_cast<StructType>(field.type());
        ICEBERG_RETURN_UNEXPECTED(
            AppendPartitionValues(child_array, partition_type, file.partition));
      } break;
      case DataFile::kRecordCountFieldId:  // required int64
        ICEBERG_RETURN_UNEXPECTED(AppendInt(child_array, file.record_count));
        break;
      case DataFile::kFileSizeFieldId:  // required int64
        ICEBERG_RETURN_UNEXPECTED(AppendInt(child_array, file.file_size_in_bytes));
        break;
      case kBlockSizeInBytesFieldId:  // compatible with v1
        // always 64MB for v1
        ICEBERG_RETURN_UNEXPECTED(AppendInt(child_array, kBlockSizeInBytesV1));
        break;
      case DataFile::kColumnSizesFieldId:  // optional map
        ICEBERG_RETURN_UNEXPECTED(AppendIntMap(child_array, file.column_sizes));
        break;
      case DataFile::kValueCountsFieldId:  // optional map
        ICEBERG_RETURN_UNEXPECTED(AppendIntMap(child_array, file.value_counts));
        break;
      case DataFile::kNullValueCountsFieldId:  // optional map
        ICEBERG_RETURN_UNEXPECTED(AppendIntMap(child_array, file.null_value_counts));
        break;
      case DataFile::kNanValueCountsFieldId:  // optional map
        ICEBERG_RETURN_UNEXPECTED(AppendIntMap(child_array, file.nan_value_counts));
        break;
      case DataFile::kLowerBoundsFieldId:  // optional map
        ICEBERG_RETURN_UNEXPECTED(AppendBinaryMap(child_array, file.lower_bounds));
        break;
      case DataFile::kUpperBoundsFieldId:  // optional map
        ICEBERG_RETURN_UNEXPECTED(AppendBinaryMap(child_array, file.upper_bounds));
        break;
      case DataFile::kKeyMetadataFieldId:  // optional binary
        if (!file.key_metadata.empty()) {
          ICEBERG_RETURN_UNEXPECTED(AppendBytes(child_array, file.key_metadata));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(child_array));
        }
        break;
      case DataFile::kSplitOffsetsFieldId:  // optional list
        ICEBERG_RETURN_UNEXPECTED(AppendIntList(child_array, file.split_offsets));
        break;
      case DataFile::kEqualityIdsFieldId:  // optional list
        ICEBERG_RETURN_UNEXPECTED(AppendIntList(child_array, file.equality_ids));
        break;
      case DataFile::kSortOrderIdFieldId:  // optional int32
        if (file.sort_order_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendInt(child_array, static_cast<int64_t>(file.sort_order_id.value())));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(child_array));
        }
        break;
      case DataFile::kFirstRowIdFieldId:  // optional int64
        if (file.first_row_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendInt(child_array, file.first_row_id.value()));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(child_array));
        }
        break;
      case DataFile::kReferencedDataFileFieldId: {  // optional string
        ICEBERG_ASSIGN_OR_RAISE(auto referenced_data_file, GetReferenceDataFile(file));
        if (referenced_data_file.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendString(child_array, referenced_data_file.value()));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(child_array));
        }
        break;
      }
      case DataFile::kContentOffsetFieldId:  // optional int64
        if (file.content_offset.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendInt(child_array, file.content_offset.value()));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(child_array));
        }
        break;
      case DataFile::kContentSizeFieldId:  // optional int64
        if (file.content_size_in_bytes.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendInt(child_array, file.content_size_in_bytes.value()));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(child_array));
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
      case ManifestEntry::kStatusFieldId:  // required int32
        ICEBERG_RETURN_UNEXPECTED(
            AppendInt(array, static_cast<int64_t>(static_cast<int32_t>(entry.status))));
        break;
      case ManifestEntry::kSnapshotIdFieldId:  // optional int64
        if (entry.snapshot_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendInt(array, entry.snapshot_id.value()));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      case ManifestEntry::kDataFileFieldId:  // required struct
        if (entry.data_file) {
          // Get the data file type from the field
          auto data_file_type = internal::checked_pointer_cast<StructType>(field.type());
          ICEBERG_RETURN_UNEXPECTED(
              AppendDataFile(array, data_file_type, *entry.data_file));
        } else {
          return InvalidManifest("Missing required data_file field from manifest entry.");
        }
        break;
      case ManifestEntry::kSequenceNumberFieldId: {  // optional int64
        ICEBERG_ASSIGN_OR_RAISE(auto sequence_num, GetSequenceNumber(entry));
        if (sequence_num.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendInt(array, sequence_num.value()));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      }
      case ManifestEntry::kFileSequenceNumberFieldId:  // optional int64
        if (entry.file_sequence_number.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendInt(array, entry.file_sequence_number.value()));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
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
              AppendBoolean(summary_array->children[0], summary.contains_null));
          break;
        case 518: {
          // contains_nan (optional bool)
          auto field_array = summary_array->children[1];
          if (summary.contains_nan.has_value()) {
            ICEBERG_RETURN_UNEXPECTED(
                AppendBoolean(field_array, summary.contains_nan.value()));
          } else {
            ICEBERG_RETURN_UNEXPECTED(AppendNull(field_array));
          }
          break;
        }
        case 510: {
          // lower_bound (optional binary)
          auto field_array = summary_array->children[2];
          if (summary.lower_bound.has_value() && !summary.lower_bound->empty()) {
            ICEBERG_RETURN_UNEXPECTED(
                AppendBytes(field_array, summary.lower_bound.value()));
          } else {
            ICEBERG_RETURN_UNEXPECTED(AppendNull(field_array));
          }
          break;
        }
        case 511: {
          // upper_bound (optional binary)
          auto field_array = summary_array->children[3];
          if (summary.upper_bound.has_value() && !summary.upper_bound->empty()) {
            ICEBERG_RETURN_UNEXPECTED(
                AppendBytes(field_array, summary.upper_bound.value()));
          } else {
            ICEBERG_RETURN_UNEXPECTED(AppendNull(field_array));
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
      case ManifestFile::kManifestPathFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendString(array, file.manifest_path));
        break;
      case ManifestFile::kManifestLengthFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, file.manifest_length));
        break;
      case ManifestFile::kPartitionSpecIdFieldId:
        ICEBERG_RETURN_UNEXPECTED(
            AppendInt(array, static_cast<int64_t>(file.partition_spec_id)));
        break;
      case ManifestFile::kContentFieldId:
        ICEBERG_RETURN_UNEXPECTED(
            AppendInt(array, static_cast<int64_t>(static_cast<int32_t>(file.content))));
        break;
      case ManifestFile::kSequenceNumberFieldId: {
        ICEBERG_ASSIGN_OR_RAISE(auto sequence_num, GetSequenceNumber(file));
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, sequence_num));
        break;
      }
      case ManifestFile::kMinSequenceNumberFieldId: {
        ICEBERG_ASSIGN_OR_RAISE(auto min_sequence_num, GetMinSequenceNumber(file));
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, min_sequence_num));
        break;
      }
      case ManifestFile::kAddedSnapshotIdFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, file.added_snapshot_id));
        break;
      case ManifestFile::kAddedFilesCountFieldId:
        if (file.added_files_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendInt(array, static_cast<int64_t>(file.added_files_count.value())));
        } else {
          // Append null for optional field
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      case ManifestFile::kExistingFilesCountFieldId:
        if (file.existing_files_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendInt(array, static_cast<int64_t>(file.existing_files_count.value())));
        } else {
          // Append null for optional field
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      case ManifestFile::kDeletedFilesCountFieldId:
        if (file.deleted_files_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendInt(array, static_cast<int64_t>(file.deleted_files_count.value())));
        } else {
          // Append null for optional field
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      case ManifestFile::kAddedRowsCountFieldId:
        if (file.added_rows_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendInt(array, file.added_rows_count.value()));
        } else {
          // Append null for optional field
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      case ManifestFile::kExistingRowsCountFieldId:
        if (file.existing_rows_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendInt(array, file.existing_rows_count.value()));
        } else {
          // Append null for optional field
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      case ManifestFile::kDeletedRowsCountFieldId:
        if (file.deleted_rows_count.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendInt(array, file.deleted_rows_count.value()));
        } else {
          // Append null for optional field
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      case ManifestFile::kPartitionSummaryFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendPartitionSummary(
            array, internal::checked_pointer_cast<ListType>(field.type()),
            file.partitions));
        break;
      case ManifestFile::kKeyMetadataFieldId:
        if (!file.key_metadata.empty()) {
          ICEBERG_RETURN_UNEXPECTED(AppendBytes(array, file.key_metadata));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      case ManifestFile::kFirstRowIdFieldId: {
        ICEBERG_ASSIGN_OR_RAISE(auto first_row_id, GetFirstRowId(file));
        if (first_row_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(AppendInt(array, first_row_id.value()));
        } else {
          // Append null for optional field
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
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
