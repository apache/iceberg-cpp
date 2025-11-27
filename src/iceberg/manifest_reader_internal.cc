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

#include "manifest_reader_internal.h"

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

#define PARSE_PRIMITIVE_FIELD(item, array_view, type)                                   \
  for (int64_t row_idx = 0; row_idx < array_view->length; row_idx++) {                  \
    if (!ArrowArrayViewIsNull(array_view, row_idx)) {                                   \
      auto value = ArrowArrayViewGetIntUnsafe(array_view, row_idx);                     \
      item = static_cast<type>(value);                                                  \
    } else if (required) {                                                              \
      return InvalidManifestList("Field {} is required but null at row {}", field_name, \
                                 row_idx);                                              \
    }                                                                                   \
  }

#define PARSE_STRING_FIELD(item, array_view)                                            \
  for (int64_t row_idx = 0; row_idx < array_view->length; row_idx++) {                  \
    if (!ArrowArrayViewIsNull(array_view, row_idx)) {                                   \
      auto value = ArrowArrayViewGetStringUnsafe(array_view, row_idx);                  \
      item = std::string(value.data, value.size_bytes);                                 \
    } else if (required) {                                                              \
      return InvalidManifestList("Field {} is required but null at row {}", field_name, \
                                 row_idx);                                              \
    }                                                                                   \
  }

#define PARSE_BINARY_FIELD(item, array_view)                                            \
  for (int64_t row_idx = 0; row_idx < array_view->length; row_idx++) {                  \
    if (!ArrowArrayViewIsNull(view_of_column, row_idx)) {                               \
      item = ArrowArrayViewGetInt8Vector(array_view, row_idx);                          \
    } else if (required) {                                                              \
      return InvalidManifestList("Field {} is required but null at row {}", field_name, \
                                 row_idx);                                              \
    }                                                                                   \
  }

#define PARSE_INTEGER_VECTOR_FIELD(item, count, array_view, type)                   \
  for (int64_t manifest_idx = 0; manifest_idx < count; manifest_idx++) {            \
    auto offset = ArrowArrayViewListChildOffset(array_view, manifest_idx);          \
    auto next_offset = ArrowArrayViewListChildOffset(array_view, manifest_idx + 1); \
    for (int64_t offset_idx = offset; offset_idx < next_offset; offset_idx++) {     \
      item.emplace_back(static_cast<type>(                                          \
          ArrowArrayViewGetIntUnsafe(array_view->children[0], offset_idx)));        \
    }                                                                               \
  }

#define PARSE_MAP_FIELD(item, count, array_view, key_type, value_type, assignment)   \
  do {                                                                               \
    if (array_view->storage_type != ArrowType::NANOARROW_TYPE_MAP) {                 \
      return InvalidManifest("Field:{} should be a map.", field_name);               \
    }                                                                                \
    auto view_of_map = array_view->children[0];                                      \
    ASSERT_VIEW_TYPE_AND_CHILDREN(view_of_map, ArrowType::NANOARROW_TYPE_STRUCT, 2); \
    auto view_of_map_key = view_of_map->children[0];                                 \
    ASSERT_VIEW_TYPE(view_of_map_key, key_type);                                     \
    auto view_of_map_value = view_of_map->children[1];                               \
    ASSERT_VIEW_TYPE(view_of_map_value, value_type);                                 \
    for (int64_t row_idx = 0; row_idx < count; row_idx++) {                          \
      auto offset = array_view->buffer_views[1].data.as_int32[row_idx];              \
      auto next_offset = array_view->buffer_views[1].data.as_int32[row_idx + 1];     \
      for (int32_t offset_idx = offset; offset_idx < next_offset; offset_idx++) {    \
        auto key = ArrowArrayViewGetIntUnsafe(view_of_map_key, offset_idx);          \
        item[key] = assignment;                                                      \
      }                                                                              \
    }                                                                                \
  } while (0)

#define PARSE_INT_LONG_MAP_FIELD(item, count, array_view)                   \
  PARSE_MAP_FIELD(item, count, array_view, ArrowType::NANOARROW_TYPE_INT32, \
                  ArrowType::NANOARROW_TYPE_INT64,                          \
                  ArrowArrayViewGetIntUnsafe(view_of_map_value, offset_idx));

#define PARSE_INT_BINARY_MAP_FIELD(item, count, array_view)                 \
  PARSE_MAP_FIELD(item, count, array_view, ArrowType::NANOARROW_TYPE_INT32, \
                  ArrowType::NANOARROW_TYPE_BINARY,                         \
                  ArrowArrayViewGetInt8Vector(view_of_map_value, offset_idx));

#define ASSERT_VIEW_TYPE(view, type)                                           \
  if (view->storage_type != type) {                                            \
    return InvalidManifest("Sub Field:{} should be a {}.", field_name, #type); \
  }

#define ASSERT_VIEW_TYPE_AND_CHILDREN(view, type, n_child)                       \
  if (view->storage_type != type) {                                              \
    return InvalidManifest("Sub Field:{} should be a {}.", field_name, #type);   \
  }                                                                              \
  if (view->n_children != n_child) {                                             \
    return InvalidManifest("Sub Field for:{} should have key&value:{} columns.", \
                           field_name, n_child);                                 \
  }

std::vector<uint8_t> ArrowArrayViewGetInt8Vector(const ArrowArrayView* view,
                                                 int32_t offset_idx) {
  auto buffer = ArrowArrayViewGetBytesUnsafe(view, offset_idx);
  return {buffer.data.as_char, buffer.data.as_char + buffer.size_bytes};
}

Status ParsePartitionFieldSummaryList(ArrowArrayView* view_of_column,
                                      std::vector<ManifestFile>& manifest_files) {
  auto manifest_count = view_of_column->length;
  // view_of_column is list<struct<PartitionFieldSummary>>
  if (view_of_column->storage_type != ArrowType::NANOARROW_TYPE_LIST) {
    return InvalidManifestList("partitions field should be a list.");
  }
  auto view_of_list_iterm = view_of_column->children[0];
  // view_of_list_iterm is struct<PartitionFieldSummary>
  if (view_of_list_iterm->storage_type != ArrowType::NANOARROW_TYPE_STRUCT) {
    return InvalidManifestList("partitions list field should be a list.");
  }
  if (view_of_list_iterm->n_children != 4) {
    return InvalidManifestList("PartitionFieldSummary should have 4 fields.");
  }
  if (view_of_list_iterm->children[0]->storage_type != ArrowType::NANOARROW_TYPE_BOOL) {
    return InvalidManifestList("contains_null should have be bool type column.");
  }
  auto contains_null = view_of_list_iterm->children[0];
  if (view_of_list_iterm->children[1]->storage_type != ArrowType::NANOARROW_TYPE_BOOL) {
    return InvalidManifestList("contains_nan should have be bool type column.");
  }
  auto contains_nan = view_of_list_iterm->children[1];
  if (view_of_list_iterm->children[2]->storage_type != ArrowType::NANOARROW_TYPE_BINARY) {
    return InvalidManifestList("lower_bound should have be binary type column.");
  }
  auto lower_bound_list = view_of_list_iterm->children[2];
  if (view_of_list_iterm->children[3]->storage_type != ArrowType::NANOARROW_TYPE_BINARY) {
    return InvalidManifestList("upper_bound should have be binary type column.");
  }
  auto upper_bound_list = view_of_list_iterm->children[3];
  for (int64_t manifest_idx = 0; manifest_idx < manifest_count; manifest_idx++) {
    auto offset = ArrowArrayViewListChildOffset(view_of_column, manifest_idx);
    auto next_offset = ArrowArrayViewListChildOffset(view_of_column, manifest_idx + 1);
    // partitions from offset to next_offset belongs to manifest_idx
    auto& manifest_file = manifest_files[manifest_idx];
    for (int64_t partition_idx = offset; partition_idx < next_offset; partition_idx++) {
      PartitionFieldSummary partition_field_summary;
      if (!ArrowArrayViewIsNull(contains_null, partition_idx)) {
        partition_field_summary.contains_null =
            ArrowArrayViewGetIntUnsafe(contains_null, partition_idx);
      } else {
        return InvalidManifestList("contains_null is null at row {}", partition_idx);
      }
      if (!ArrowArrayViewIsNull(contains_nan, partition_idx)) {
        partition_field_summary.contains_nan =
            ArrowArrayViewGetIntUnsafe(contains_nan, partition_idx);
      }
      if (!ArrowArrayViewIsNull(lower_bound_list, partition_idx)) {
        partition_field_summary.lower_bound =
            ArrowArrayViewGetInt8Vector(lower_bound_list, partition_idx);
      }
      if (!ArrowArrayViewIsNull(upper_bound_list, partition_idx)) {
        partition_field_summary.upper_bound =
            ArrowArrayViewGetInt8Vector(upper_bound_list, partition_idx);
      }

      manifest_file.partitions.emplace_back(partition_field_summary);
    }
  }
  return {};
}

Result<std::vector<ManifestFile>> ParseManifestList(ArrowSchema* schema,
                                                    ArrowArray* array_in,
                                                    const Schema& iceberg_schema) {
  if (schema->n_children != array_in->n_children) {
    return InvalidManifestList("Columns size not match between schema:{} and array:{}",
                               schema->n_children, array_in->n_children);
  }
  if (iceberg_schema.fields().size() != array_in->n_children) {
    return InvalidManifestList("Columns size not match between schema:{} and array:{}",
                               iceberg_schema.fields().size(), array_in->n_children);
  }

  ArrowError error;
  ArrowArrayView array_view;
  auto status = ArrowArrayViewInitFromSchema(&array_view, schema, &error);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(status, error);
  internal::ArrowArrayViewGuard view_guard(&array_view);
  status = ArrowArrayViewSetArray(&array_view, array_in, &error);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(status, error);
  status = ArrowArrayViewValidate(&array_view, NANOARROW_VALIDATION_LEVEL_FULL, &error);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(status, error);

  std::vector<ManifestFile> manifest_files;
  manifest_files.resize(array_in->length);

  for (int64_t idx = 0; idx < array_in->n_children; idx++) {
    const auto& field = iceberg_schema.GetFieldByIndex(idx);
    if (!field.has_value()) {
      return InvalidSchema("Field index {} is not found in schema", idx);
    }
    auto field_name = field.value()->get().name();
    bool required = !field.value()->get().optional();
    auto view_of_column = array_view.children[idx];
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_file_field, ManifestFileFieldFromIndex(idx));
    switch (manifest_file_field) {
      case ManifestFileField::kManifestPath:
        PARSE_STRING_FIELD(manifest_files[row_idx].manifest_path, view_of_column);
        break;
      case ManifestFileField::kManifestLength:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].manifest_length, view_of_column,
                              int64_t);
        break;
      case ManifestFileField::kPartitionSpecId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].partition_spec_id, view_of_column,
                              int32_t);
        break;
      case ManifestFileField::kContent:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].content, view_of_column,
                              ManifestContent);
        break;
      case ManifestFileField::kSequenceNumber:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].sequence_number, view_of_column,
                              int64_t);
        break;
      case ManifestFileField::kMinSequenceNumber:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].min_sequence_number, view_of_column,
                              int64_t);
        break;
      case ManifestFileField::kAddedSnapshotId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].added_snapshot_id, view_of_column,
                              int64_t);
        break;
      case ManifestFileField::kAddedFilesCount:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].added_files_count, view_of_column,
                              int32_t);
        break;
      case ManifestFileField::kExistingFilesCount:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].existing_files_count,
                              view_of_column, int32_t);
        break;
      case ManifestFileField::kDeletedFilesCount:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].deleted_files_count, view_of_column,
                              int32_t);
        break;
      case ManifestFileField::kAddedRowsCount:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].added_rows_count, view_of_column,
                              int64_t);
        break;
      case ManifestFileField::kExistingRowsCount:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].existing_rows_count, view_of_column,
                              int64_t);
        break;
      case ManifestFileField::kDeletedRowsCount:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].deleted_rows_count, view_of_column,
                              int64_t);
        break;
      case ManifestFileField::kPartitionFieldSummary:
        ICEBERG_RETURN_UNEXPECTED(
            ParsePartitionFieldSummaryList(view_of_column, manifest_files));
        break;
      case ManifestFileField::kKeyMetadata:
        PARSE_BINARY_FIELD(manifest_files[row_idx].key_metadata, view_of_column);
        break;
      case ManifestFileField::kFirstRowId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].first_row_id, view_of_column,
                              int64_t);
        break;
      default:
        return InvalidManifestList("Unsupported field: {} in manifest file.", field_name);
    }
  }
  return manifest_files;
}

Status ParseLiteral(ArrowArrayView* view_of_partition, int64_t row_idx,
                    std::vector<ManifestEntry>& manifest_entries) {
  if (view_of_partition->storage_type == ArrowType::NANOARROW_TYPE_BOOL) {
    auto value = ArrowArrayViewGetUIntUnsafe(view_of_partition, row_idx);
    manifest_entries[row_idx].data_file->partition.AddValue(Literal::Boolean(value != 0));
  } else if (view_of_partition->storage_type == ArrowType::NANOARROW_TYPE_INT32) {
    auto value = ArrowArrayViewGetIntUnsafe(view_of_partition, row_idx);
    manifest_entries[row_idx].data_file->partition.AddValue(Literal::Int(value));
  } else if (view_of_partition->storage_type == ArrowType::NANOARROW_TYPE_INT64) {
    auto value = ArrowArrayViewGetIntUnsafe(view_of_partition, row_idx);
    manifest_entries[row_idx].data_file->partition.AddValue(Literal::Long(value));
  } else if (view_of_partition->storage_type == ArrowType::NANOARROW_TYPE_FLOAT) {
    auto value = ArrowArrayViewGetDoubleUnsafe(view_of_partition, row_idx);
    manifest_entries[row_idx].data_file->partition.AddValue(Literal::Float(value));
  } else if (view_of_partition->storage_type == ArrowType::NANOARROW_TYPE_DOUBLE) {
    auto value = ArrowArrayViewGetDoubleUnsafe(view_of_partition, row_idx);
    manifest_entries[row_idx].data_file->partition.AddValue(Literal::Double(value));
  } else if (view_of_partition->storage_type == ArrowType::NANOARROW_TYPE_STRING) {
    auto value = ArrowArrayViewGetStringUnsafe(view_of_partition, row_idx);
    manifest_entries[row_idx].data_file->partition.AddValue(
        Literal::String(std::string(value.data, value.size_bytes)));
  } else if (view_of_partition->storage_type == ArrowType::NANOARROW_TYPE_BINARY) {
    auto buffer = ArrowArrayViewGetBytesUnsafe(view_of_partition, row_idx);
    manifest_entries[row_idx].data_file->partition.AddValue(
        Literal::Binary(std::vector<uint8_t>(buffer.data.as_char,
                                             buffer.data.as_char + buffer.size_bytes)));
  } else {
    return InvalidManifest("Unsupported field type: {} in data file partition.",
                           static_cast<int32_t>(view_of_partition->storage_type));
  }
  return {};
}

Status ParseDataFile(const std::shared_ptr<StructType>& data_file_schema,
                     ArrowArrayView* view_of_column, std::optional<int64_t>& first_row_id,
                     std::vector<ManifestEntry>& manifest_entries) {
  if (view_of_column->storage_type != ArrowType::NANOARROW_TYPE_STRUCT) {
    return InvalidManifest("DataFile field should be a struct.");
  }
  if (view_of_column->n_children != data_file_schema->fields().size()) {
    return InvalidManifest("DataFile schema size:{} not match with ArrayArray columns:{}",
                           data_file_schema->fields().size(), view_of_column->n_children);
  }
  for (int64_t col_idx = 0; col_idx < view_of_column->n_children; ++col_idx) {
    auto field_name = data_file_schema->GetFieldByIndex(col_idx).value()->get().name();
    auto required = !data_file_schema->GetFieldByIndex(col_idx).value()->get().optional();
    auto view_of_file_field = view_of_column->children[col_idx];
    auto manifest_entry_count = view_of_file_field->length;

    switch (col_idx) {
      case 0:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->content,
                              view_of_file_field, DataFile::Content);
        break;
      case 1:
        PARSE_STRING_FIELD(manifest_entries[row_idx].data_file->file_path,
                           view_of_file_field);
        break;
      case 2:
        for (int64_t row_idx = 0; row_idx < view_of_file_field->length; row_idx++) {
          if (!ArrowArrayViewIsNull(view_of_file_field, row_idx)) {
            auto value = ArrowArrayViewGetStringUnsafe(view_of_file_field, row_idx);
            std::string_view path_str(value.data, value.size_bytes);
            ICEBERG_ASSIGN_OR_RAISE(manifest_entries[row_idx].data_file->file_format,
                                    FileFormatTypeFromString(path_str));
          }
        }
        break;
      case 3: {
        if (view_of_file_field->storage_type != ArrowType::NANOARROW_TYPE_STRUCT) {
          return InvalidManifest("Field:{} should be a struct.", field_name);
        }
        if (view_of_file_field->n_children > 0) {
          for (int64_t partition_idx = 0; partition_idx < view_of_file_field->n_children;
               partition_idx++) {
            auto view_of_partition = view_of_file_field->children[partition_idx];
            for (int64_t row_idx = 0; row_idx < view_of_partition->length; row_idx++) {
              if (ArrowArrayViewIsNull(view_of_partition, row_idx)) {
                break;
              }
              ICEBERG_RETURN_UNEXPECTED(
                  ParseLiteral(view_of_partition, row_idx, manifest_entries));
            }
          }
        }
      } break;
      case 4:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->record_count,
                              view_of_file_field, int64_t);
        break;
      case 5:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->file_size_in_bytes,
                              view_of_file_field, int64_t);
        break;
      case 6:
        // key&value should have the same offset
        // HACK(xiao.dong) workaround for arrow bug:
        // ArrowArrayViewListChildOffset can not get the correct offset for map
        PARSE_INT_LONG_MAP_FIELD(manifest_entries[row_idx].data_file->column_sizes,
                                 manifest_entry_count, view_of_file_field);
        break;
      case 7:
        PARSE_INT_LONG_MAP_FIELD(manifest_entries[row_idx].data_file->value_counts,
                                 manifest_entry_count, view_of_file_field);
        break;
      case 8:
        PARSE_INT_LONG_MAP_FIELD(manifest_entries[row_idx].data_file->null_value_counts,
                                 manifest_entry_count, view_of_file_field);
        break;
      case 9:
        PARSE_INT_LONG_MAP_FIELD(manifest_entries[row_idx].data_file->nan_value_counts,
                                 manifest_entry_count, view_of_file_field);
        break;
      case 10:
        PARSE_INT_BINARY_MAP_FIELD(manifest_entries[row_idx].data_file->lower_bounds,
                                   manifest_entry_count, view_of_file_field);
        break;
      case 11:
        PARSE_INT_BINARY_MAP_FIELD(manifest_entries[row_idx].data_file->upper_bounds,
                                   manifest_entry_count, view_of_file_field);
        break;
      case 12:
        PARSE_BINARY_FIELD(manifest_entries[row_idx].data_file->key_metadata,
                           view_of_file_field);
        break;
      case 13:
        PARSE_INTEGER_VECTOR_FIELD(
            manifest_entries[manifest_idx].data_file->split_offsets, manifest_entry_count,
            view_of_file_field, int64_t);
        break;
      case 14:
        PARSE_INTEGER_VECTOR_FIELD(manifest_entries[manifest_idx].data_file->equality_ids,
                                   manifest_entry_count, view_of_file_field, int32_t);
        break;
      case 15:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->sort_order_id,
                              view_of_file_field, int32_t);
        break;
      case 16: {
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->first_row_id,
                              view_of_file_field, int64_t);
        if (first_row_id.has_value()) {
          std::ranges::for_each(manifest_entries, [&first_row_id](ManifestEntry& entry) {
            if (entry.status != ManifestStatus::kDeleted &&
                !entry.data_file->first_row_id.has_value()) {
              entry.data_file->first_row_id = first_row_id.value();
              first_row_id = first_row_id.value() + entry.data_file->record_count;
            }
          });
        } else {
          // data file's first_row_id is null when the manifest's first_row_id is null
          std::ranges::for_each(manifest_entries, [](ManifestEntry& entry) {
            entry.data_file->first_row_id = std::nullopt;
          });
        }
        break;
      }
      case 17:
        PARSE_STRING_FIELD(manifest_entries[row_idx].data_file->referenced_data_file,
                           view_of_file_field);
        break;
      case 18:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->content_offset,
                              view_of_file_field, int64_t);
        break;
      case 19:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->content_size_in_bytes,
                              view_of_file_field, int64_t);
        break;
      default:
        return InvalidManifest("Unsupported field: {} in data file.", field_name);
    }
  }
  return {};
}

Result<std::vector<ManifestEntry>> ParseManifestEntry(
    ArrowSchema* schema, ArrowArray* array_in, const Schema& iceberg_schema,
    std::optional<int64_t>& first_row_id) {
  if (schema->n_children != array_in->n_children) {
    return InvalidManifest("Columns size not match between schema:{} and array:{}",
                           schema->n_children, array_in->n_children);
  }
  if (iceberg_schema.fields().size() != array_in->n_children) {
    return InvalidManifest("Columns size not match between schema:{} and array:{}",
                           iceberg_schema.fields().size(), array_in->n_children);
  }

  ArrowError error;
  ArrowArrayView array_view;
  auto status = ArrowArrayViewInitFromSchema(&array_view, schema, &error);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(status, error);
  internal::ArrowArrayViewGuard view_guard(&array_view);
  status = ArrowArrayViewSetArray(&array_view, array_in, &error);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(status, error);
  status = ArrowArrayViewValidate(&array_view, NANOARROW_VALIDATION_LEVEL_FULL, &error);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(status, error);

  std::vector<ManifestEntry> manifest_entries;
  manifest_entries.resize(array_in->length);
  for (int64_t i = 0; i < array_in->length; i++) {
    manifest_entries[i].data_file = std::make_shared<DataFile>();
  }

  for (int64_t idx = 0; idx < array_in->n_children; idx++) {
    const auto& field = iceberg_schema.GetFieldByIndex(idx);
    if (!field.has_value()) {
      return InvalidManifest("Field not found in schema: {}", idx);
    }
    auto field_name = field.value()->get().name();
    bool required = !field.value()->get().optional();
    auto view_of_column = array_view.children[idx];

    switch (idx) {
      case 0:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].status, view_of_column,
                              ManifestStatus);
        break;
      case 1:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].snapshot_id, view_of_column,
                              int64_t);
        break;
      case 2:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].sequence_number, view_of_column,
                              int64_t);
        break;
      case 3:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].file_sequence_number,
                              view_of_column, int64_t);
        break;
      case 4: {
        auto data_file_schema =
            internal::checked_pointer_cast<StructType>(field.value()->get().type());
        ICEBERG_RETURN_UNEXPECTED(ParseDataFile(data_file_schema, view_of_column,
                                                first_row_id, manifest_entries));
        break;
      }
      default:
        return InvalidManifest("Unsupported field: {} in manifest entry.", field_name);
    }
  }
  return manifest_entries;
}

Result<std::vector<ManifestEntry>> ManifestReaderImpl::Entries() const {
  std::vector<ManifestEntry> manifest_entries;
  ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, reader_->Schema());
  internal::ArrowSchemaGuard schema_guard(&arrow_schema);
  while (true) {
    ICEBERG_ASSIGN_OR_RAISE(auto result, reader_->Next());
    if (result.has_value()) {
      internal::ArrowArrayGuard array_guard(&result.value());
      ICEBERG_ASSIGN_OR_RAISE(
          auto parse_result,
          ParseManifestEntry(&arrow_schema, &result.value(), *schema_, first_row_id_));
      manifest_entries.insert(manifest_entries.end(),
                              std::make_move_iterator(parse_result.begin()),
                              std::make_move_iterator(parse_result.end()));
    } else {
      // eof
      break;
    }
  }

  // Apply inheritance to all entries
  for (auto& entry : manifest_entries) {
    ICEBERG_RETURN_UNEXPECTED(inheritable_metadata_->Apply(entry));
  }

  return manifest_entries;
}

Result<std::unordered_map<std::string, std::string>> ManifestReaderImpl::Metadata()
    const {
  return reader_->Metadata();
}

Result<std::vector<ManifestFile>> ManifestListReaderImpl::Files() const {
  std::vector<ManifestFile> manifest_files;
  ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, reader_->Schema());
  internal::ArrowSchemaGuard schema_guard(&arrow_schema);
  while (true) {
    ICEBERG_ASSIGN_OR_RAISE(auto result, reader_->Next());
    if (result.has_value()) {
      internal::ArrowArrayGuard array_guard(&result.value());
      ICEBERG_ASSIGN_OR_RAISE(
          auto parse_result, ParseManifestList(&arrow_schema, &result.value(), *schema_));
      manifest_files.insert(manifest_files.end(),
                            std::make_move_iterator(parse_result.begin()),
                            std::make_move_iterator(parse_result.end()));
    } else {
      // eof
      break;
    }
  }
  return manifest_files;
}

Result<std::unordered_map<std::string, std::string>> ManifestListReaderImpl::Metadata()
    const {
  return reader_->Metadata();
}

Result<ManifestFileField> ManifestFileFieldFromIndex(int32_t index) {
  if (index >= 0 && index < static_cast<int32_t>(ManifestFileField::kNextUnusedId)) {
    return static_cast<ManifestFileField>(index);
  }
  return InvalidArgument("Invalid manifest file field index: {}", index);
}

}  // namespace iceberg
