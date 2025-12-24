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

#include "iceberg/manifest/manifest_reader.h"

#include <algorithm>
#include <memory>
#include <unordered_set>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/expression/expression.h"
#include "iceberg/expression/projections.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader_internal.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

// TODO(gangwu): refactor these macros with template functions.
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
  auto view_of_list_item = view_of_column->children[0];
  // view_of_list_item is struct<PartitionFieldSummary>
  if (view_of_list_item->storage_type != ArrowType::NANOARROW_TYPE_STRUCT) {
    return InvalidManifestList("partitions list item should be a struct.");
  }
  if (view_of_list_item->n_children != 4) {
    return InvalidManifestList("PartitionFieldSummary should have 4 fields.");
  }
  if (view_of_list_item->children[0]->storage_type != ArrowType::NANOARROW_TYPE_BOOL) {
    return InvalidManifestList("contains_null should have be bool type column.");
  }
  auto contains_null = view_of_list_item->children[0];
  if (view_of_list_item->children[1]->storage_type != ArrowType::NANOARROW_TYPE_BOOL) {
    return InvalidManifestList("contains_nan should have be bool type column.");
  }
  auto contains_nan = view_of_list_item->children[1];
  if (view_of_list_item->children[2]->storage_type != ArrowType::NANOARROW_TYPE_BINARY) {
    return InvalidManifestList("lower_bound should have be binary type column.");
  }
  auto lower_bound_list = view_of_list_item->children[2];
  if (view_of_list_item->children[3]->storage_type != ArrowType::NANOARROW_TYPE_BINARY) {
    return InvalidManifestList("upper_bound should have be binary type column.");
  }
  auto upper_bound_list = view_of_list_item->children[3];
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
    ICEBERG_ASSIGN_OR_RAISE(auto field, iceberg_schema.GetFieldByIndex(idx));
    ICEBERG_CHECK(field.has_value(), "Invalid index {} for data file schema", idx);
    auto field_name = field->get().name();
    auto field_id = field->get().field_id();
    auto required = !field->get().optional();
    auto view_of_column = array_view.children[idx];
    switch (field_id) {
      case ManifestFile::kManifestPathFieldId:
        PARSE_STRING_FIELD(manifest_files[row_idx].manifest_path, view_of_column);
        break;
      case ManifestFile::kManifestLengthFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].manifest_length, view_of_column,
                              int64_t);
        break;
      case ManifestFile::kPartitionSpecIdFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].partition_spec_id, view_of_column,
                              int32_t);
        break;
      case ManifestFile::kContentFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].content, view_of_column,
                              ManifestContent);
        break;
      case ManifestFile::kSequenceNumberFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].sequence_number, view_of_column,
                              int64_t);
        break;
      case ManifestFile::kMinSequenceNumberFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].min_sequence_number, view_of_column,
                              int64_t);
        break;
      case ManifestFile::kAddedSnapshotIdFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].added_snapshot_id, view_of_column,
                              int64_t);
        break;
      case ManifestFile::kAddedFilesCountFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].added_files_count, view_of_column,
                              int32_t);
        break;
      case ManifestFile::kExistingFilesCountFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].existing_files_count,
                              view_of_column, int32_t);
        break;
      case ManifestFile::kDeletedFilesCountFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].deleted_files_count, view_of_column,
                              int32_t);
        break;
      case ManifestFile::kAddedRowsCountFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].added_rows_count, view_of_column,
                              int64_t);
        break;
      case ManifestFile::kExistingRowsCountFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].existing_rows_count, view_of_column,
                              int64_t);
        break;
      case ManifestFile::kDeletedRowsCountFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].deleted_rows_count, view_of_column,
                              int64_t);
        break;
      case ManifestFile::kPartitionSummaryFieldId:
        ICEBERG_RETURN_UNEXPECTED(
            ParsePartitionFieldSummaryList(view_of_column, manifest_files));
        break;
      case ManifestFile::kKeyMetadataFieldId:
        PARSE_BINARY_FIELD(manifest_files[row_idx].key_metadata, view_of_column);
        break;
      case ManifestFile::kFirstRowIdFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_files[row_idx].first_row_id, view_of_column,
                              int64_t);
        break;
      default:
        return InvalidManifestList("Unsupported field: {} in manifest file.", field_name);
    }
  }
  return manifest_files;
}

Status ParsePartitionValues(ArrowArrayView* view_of_partition, int64_t row_idx,
                            std::vector<ManifestEntry>& manifest_entries) {
  switch (view_of_partition->storage_type) {
    case ArrowType::NANOARROW_TYPE_BOOL: {
      auto value = ArrowArrayViewGetUIntUnsafe(view_of_partition, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(
          Literal::Boolean(value != 0));
    } break;
    case ArrowType::NANOARROW_TYPE_INT32: {
      auto value = ArrowArrayViewGetIntUnsafe(view_of_partition, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(Literal::Int(value));
    } break;
    case ArrowType::NANOARROW_TYPE_INT64: {
      auto value = ArrowArrayViewGetIntUnsafe(view_of_partition, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(Literal::Long(value));
    } break;
    case ArrowType::NANOARROW_TYPE_FLOAT: {
      auto value = ArrowArrayViewGetDoubleUnsafe(view_of_partition, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(Literal::Float(value));
    } break;
    case ArrowType::NANOARROW_TYPE_DOUBLE: {
      auto value = ArrowArrayViewGetDoubleUnsafe(view_of_partition, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(Literal::Double(value));
    } break;
    case ArrowType::NANOARROW_TYPE_STRING: {
      auto value = ArrowArrayViewGetStringUnsafe(view_of_partition, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(
          Literal::String(std::string(value.data, value.size_bytes)));
    } break;
    case ArrowType::NANOARROW_TYPE_BINARY: {
      auto buffer = ArrowArrayViewGetBytesUnsafe(view_of_partition, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(
          Literal::Binary(std::vector<uint8_t>(buffer.data.as_char,
                                               buffer.data.as_char + buffer.size_bytes)));
    } break;
    default:
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
    ICEBERG_ASSIGN_OR_RAISE(auto field, data_file_schema->GetFieldByIndex(col_idx));
    ICEBERG_CHECK(field.has_value(), "Invalid index {} for data file schema", col_idx);
    auto field_name = field->get().name();
    auto field_id = field->get().field_id();
    auto required = !field->get().optional();
    auto array_view = view_of_column->children[col_idx];
    auto manifest_entry_count = array_view->length;

    switch (field_id) {
      case DataFile::kContentFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->content, array_view,
                              DataFile::Content);
        break;
      case DataFile::kFilePathFieldId:
        PARSE_STRING_FIELD(manifest_entries[row_idx].data_file->file_path, array_view);
        break;
      case DataFile::kFileFormatFieldId:
        for (int64_t row_idx = 0; row_idx < array_view->length; row_idx++) {
          if (!ArrowArrayViewIsNull(array_view, row_idx)) {
            auto value = ArrowArrayViewGetStringUnsafe(array_view, row_idx);
            std::string_view path_str(value.data, value.size_bytes);
            ICEBERG_ASSIGN_OR_RAISE(manifest_entries[row_idx].data_file->file_format,
                                    FileFormatTypeFromString(path_str));
          }
        }
        break;
      case DataFile::kPartitionFieldId: {
        if (array_view->storage_type != ArrowType::NANOARROW_TYPE_STRUCT) {
          return InvalidManifest("Field:{} should be a struct.", field_name);
        }
        if (array_view->n_children > 0) {
          for (int64_t partition_idx = 0; partition_idx < array_view->n_children;
               partition_idx++) {
            auto view_of_partition = array_view->children[partition_idx];
            for (int64_t row_idx = 0; row_idx < view_of_partition->length; row_idx++) {
              if (ArrowArrayViewIsNull(view_of_partition, row_idx)) {
                break;
              }
              ICEBERG_RETURN_UNEXPECTED(
                  ParsePartitionValues(view_of_partition, row_idx, manifest_entries));
            }
          }
        }
      } break;
      case DataFile::kRecordCountFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->record_count,
                              array_view, int64_t);
        break;
      case DataFile::kFileSizeFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->file_size_in_bytes,
                              array_view, int64_t);
        break;
      case DataFile::kColumnSizesFieldId:
        // key&value should have the same offset
        // HACK(xiao.dong) workaround for arrow bug:
        // ArrowArrayViewListChildOffset can not get the correct offset for map
        PARSE_INT_LONG_MAP_FIELD(manifest_entries[row_idx].data_file->column_sizes,
                                 manifest_entry_count, array_view);
        break;
      case DataFile::kValueCountsFieldId:
        PARSE_INT_LONG_MAP_FIELD(manifest_entries[row_idx].data_file->value_counts,
                                 manifest_entry_count, array_view);
        break;
      case DataFile::kNullValueCountsFieldId:
        PARSE_INT_LONG_MAP_FIELD(manifest_entries[row_idx].data_file->null_value_counts,
                                 manifest_entry_count, array_view);
        break;
      case DataFile::kNanValueCountsFieldId:
        PARSE_INT_LONG_MAP_FIELD(manifest_entries[row_idx].data_file->nan_value_counts,
                                 manifest_entry_count, array_view);
        break;
      case DataFile::kLowerBoundsFieldId:
        PARSE_INT_BINARY_MAP_FIELD(manifest_entries[row_idx].data_file->lower_bounds,
                                   manifest_entry_count, array_view);
        break;
      case DataFile::kUpperBoundsFieldId:
        PARSE_INT_BINARY_MAP_FIELD(manifest_entries[row_idx].data_file->upper_bounds,
                                   manifest_entry_count, array_view);
        break;
      case DataFile::kKeyMetadataFieldId:
        PARSE_BINARY_FIELD(manifest_entries[row_idx].data_file->key_metadata, array_view);
        break;
      case DataFile::kSplitOffsetsFieldId:
        PARSE_INTEGER_VECTOR_FIELD(
            manifest_entries[manifest_idx].data_file->split_offsets, manifest_entry_count,
            array_view, int64_t);
        break;
      case DataFile::kEqualityIdsFieldId:
        PARSE_INTEGER_VECTOR_FIELD(manifest_entries[manifest_idx].data_file->equality_ids,
                                   manifest_entry_count, array_view, int32_t);
        break;
      case DataFile::kSortOrderIdFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->sort_order_id,
                              array_view, int32_t);
        break;
      case DataFile::kFirstRowIdFieldId: {
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->first_row_id,
                              array_view, int64_t);
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
      case DataFile::kReferencedDataFileFieldId:
        PARSE_STRING_FIELD(manifest_entries[row_idx].data_file->referenced_data_file,
                           array_view);
        break;
      case DataFile::kContentOffsetFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->content_offset,
                              array_view, int64_t);
        break;
      case DataFile::kContentSizeFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].data_file->content_size_in_bytes,
                              array_view, int64_t);
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
    ICEBERG_ASSIGN_OR_RAISE(auto field, iceberg_schema.GetFieldByIndex(idx));
    ICEBERG_CHECK(field.has_value(), "Invalid index {} for manifest entry schema", idx);
    auto field_name = field->get().name();
    auto field_id = field->get().field_id();
    auto required = !field->get().optional();
    auto view_of_column = array_view.children[idx];

    switch (field_id) {
      case ManifestEntry::kStatusFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].status, view_of_column,
                              ManifestStatus);
        break;
      case ManifestEntry::kSnapshotIdFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].snapshot_id, view_of_column,
                              int64_t);
        break;
      case ManifestEntry::kSequenceNumberFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].sequence_number, view_of_column,
                              int64_t);
        break;
      case ManifestEntry::kFileSequenceNumberFieldId:
        PARSE_PRIMITIVE_FIELD(manifest_entries[row_idx].file_sequence_number,
                              view_of_column, int64_t);
        break;
      case ManifestEntry::kDataFileFieldId: {
        auto data_file_schema =
            internal::checked_pointer_cast<StructType>(field->get().type());
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

const std::vector<std::string> kStatsColumns = {"value_counts",     "null_value_counts",
                                                "nan_value_counts", "lower_bounds",
                                                "upper_bounds",     "record_count"};

bool RequireStatsProjection(const std::shared_ptr<Expression>& row_filter,
                            const std::vector<std::string>& columns) {
  if (!row_filter || row_filter->op() == Expression::Operation::kTrue) {
    return false;
  }
  if (columns.empty()) {
    return false;
  }
  const std::unordered_set<std::string_view> selected(columns.cbegin(), columns.cend());
  if (selected.contains(ManifestReader::kAllColumns)) {
    return false;
  }
  if (std::ranges::all_of(kStatsColumns, [&selected](const std::string& col) {
        return selected.contains(col);
      })) {
    return false;
  }
  return true;
}

Result<std::shared_ptr<Schema>> ProjectSchema(std::shared_ptr<Schema> schema,
                                              const std::vector<std::string>& columns,
                                              bool case_sensitive) {
  if (!columns.empty()) {
    return schema->Select(columns, case_sensitive);
  }
  return schema;
}

}  // namespace

std::vector<std::string> ManifestReader::WithStatsColumns(
    const std::vector<std::string>& columns) {
  if (std::ranges::contains(columns, ManifestReader::kAllColumns)) {
    return columns;
  } else {
    std::vector<std::string> updated_columns{columns};
    updated_columns.insert(updated_columns.end(), kStatsColumns.begin(),
                           kStatsColumns.end());
    return updated_columns;
  }
}

// ManifestReaderImpl constructor
ManifestReaderImpl::ManifestReaderImpl(
    std::string manifest_path, std::optional<int64_t> manifest_length,
    std::shared_ptr<FileIO> file_io, std::shared_ptr<Schema> schema,
    std::shared_ptr<PartitionSpec> spec,
    std::unique_ptr<InheritableMetadata> inheritable_metadata,
    std::optional<int64_t> first_row_id)
    : manifest_path_(std::move(manifest_path)),
      manifest_length_(manifest_length),
      file_io_(std::move(file_io)),
      schema_(std::move(schema)),
      spec_(std::move(spec)),
      inheritable_metadata_(std::move(inheritable_metadata)),
      first_row_id_(first_row_id) {}

ManifestReader& ManifestReaderImpl::Select(const std::vector<std::string>& columns) {
  columns_ = columns;
  return *this;
}

ManifestReader& ManifestReaderImpl::FilterPartitions(std::shared_ptr<Expression> expr) {
  part_filter_ = expr ? std::move(expr) : True::Instance();
  return *this;
}

ManifestReader& ManifestReaderImpl::FilterPartitions(
    std::shared_ptr<PartitionSet> partition_set) {
  partition_set_ = std::move(partition_set);
  return *this;
}

ManifestReader& ManifestReaderImpl::FilterRows(std::shared_ptr<Expression> expr) {
  row_filter_ = expr ? std::move(expr) : True::Instance();
  return *this;
}

ManifestReader& ManifestReaderImpl::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  return *this;
}

bool ManifestReaderImpl::HasPartitionFilter() const {
  ICEBERG_DCHECK(part_filter_, "Partition filter is not set");
  return part_filter_->op() != Expression::Operation::kTrue;
}

bool ManifestReaderImpl::HasRowFilter() const {
  ICEBERG_DCHECK(row_filter_, "Row filter is not set");
  return row_filter_->op() != Expression::Operation::kTrue;
}

Result<Evaluator*> ManifestReaderImpl::GetEvaluator() {
  if (!evaluator_) {
    auto projection_evaluator = Projections::Inclusive(*spec_, *schema_, case_sensitive_);
    ICEBERG_ASSIGN_OR_RAISE(auto projected, projection_evaluator->Project(row_filter_));
    ICEBERG_ASSIGN_OR_RAISE(auto final_part_filter,
                            And::Make(std::move(projected), part_filter_));

    ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec_->PartitionType(*schema_));
    auto partition_schema = partition_type->ToSchema();
    ICEBERG_ASSIGN_OR_RAISE(
        evaluator_, Evaluator::Make(*partition_schema, std::move(final_part_filter),
                                    case_sensitive_));
  }
  return evaluator_.get();
}

Result<InclusiveMetricsEvaluator*> ManifestReaderImpl::GetMetricsEvaluator() {
  if (!metrics_evaluator_) {
    ICEBERG_ASSIGN_OR_RAISE(
        metrics_evaluator_,
        InclusiveMetricsEvaluator::Make(row_filter_, *schema_, case_sensitive_));
  }
  return metrics_evaluator_.get();
}

bool ManifestReaderImpl::InPartitionSet(const DataFile& file) const {
  if (!partition_set_) {
    return true;
  }
  return partition_set_->contains(file.partition_spec_id, file.partition);
}

Status ManifestReaderImpl::OpenReader(std::shared_ptr<Schema> projection) {
  ICEBERG_PRECHECK(projection != nullptr, "Projection schema cannot be null");

  // Ensure required fields are present in the projected data file schema.
  std::vector<SchemaField> fields(projection->fields().begin(),
                                  projection->fields().end());
  ICEBERG_ASSIGN_OR_RAISE(auto record_count_field,
                          projection->FindFieldById(DataFile::kRecordCount.field_id()));
  ICEBERG_ASSIGN_OR_RAISE(auto first_row_id_field,
                          projection->FindFieldById(DataFile::kFirstRowId.field_id()));
  if (!record_count_field.has_value()) {
    fields.push_back(DataFile::kRecordCount);
  }
  if (!first_row_id_field.has_value()) {
    fields.push_back(DataFile::kFirstRowId);
  }
  /// FIXME: this is not supported yet.
  // fields.push_back(MetadataColumns::kRowPosition);
  auto data_file_type = std::make_shared<StructType>(std::move(fields));

  // Wrap the projected data file schema into manifest entry schema
  file_schema_ =
      ManifestEntry::TypeFromDataFileType(std::move(data_file_type))->ToSchema();

  ReaderOptions options;
  options.path = manifest_path_;
  options.io = file_io_;
  options.projection = file_schema_;
  if (manifest_length_.has_value()) {
    options.length = manifest_length_;
  }

  ICEBERG_ASSIGN_OR_RAISE(file_reader_,
                          ReaderFactoryRegistry::Open(FileFormatType::kAvro, options));

  return {};
}

Result<std::vector<ManifestEntry>> ManifestReaderImpl::Entries() {
  return ReadEntries(/*only_live=*/false);
}

Result<std::vector<ManifestEntry>> ManifestReaderImpl::LiveEntries() {
  return ReadEntries(/*only_live=*/true);
}

Result<std::vector<ManifestEntry>> ManifestReaderImpl::ReadEntries(bool only_live) {
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec_->PartitionType(*schema_));
  auto data_file_schema = DataFile::Type(std::move(partition_type))->ToSchema();

  std::shared_ptr<Schema> projected_data_file_schema;
  bool needs_filtering =
      HasRowFilter() || HasPartitionFilter() || partition_set_ != nullptr;
  if (needs_filtering) {
    // ensure stats columns are present for metrics evaluation
    bool requires_stats_projection = RequireStatsProjection(row_filter_, columns_);
    ICEBERG_ASSIGN_OR_RAISE(
        projected_data_file_schema,
        ProjectSchema(
            std::move(data_file_schema),
            (requires_stats_projection ? ManifestReader::WithStatsColumns(columns_)
                                       : columns_),
            case_sensitive_));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(
        projected_data_file_schema,
        ProjectSchema(std::move(data_file_schema), columns_, case_sensitive_));
  }

  ICEBERG_RETURN_UNEXPECTED(OpenReader(std::move(projected_data_file_schema)));
  ICEBERG_DCHECK(file_reader_ != nullptr, "File reader should be initialized");

  std::vector<ManifestEntry> manifest_entries;
  ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, file_reader_->Schema());
  internal::ArrowSchemaGuard schema_guard(&arrow_schema);

  // Get evaluators if needed
  Evaluator* evaluator = nullptr;
  InclusiveMetricsEvaluator* metrics_evaluator = nullptr;
  if (HasPartitionFilter() || HasRowFilter()) {
    ICEBERG_ASSIGN_OR_RAISE(evaluator, GetEvaluator());
  }
  if (HasRowFilter()) {
    ICEBERG_ASSIGN_OR_RAISE(metrics_evaluator, GetMetricsEvaluator());
  }

  while (true) {
    ICEBERG_ASSIGN_OR_RAISE(auto result, file_reader_->Next());
    if (!result.has_value()) {
      break;  // EOF
    }

    internal::ArrowArrayGuard array_guard(&result.value());
    ICEBERG_ASSIGN_OR_RAISE(
        auto entries,
        ParseManifestEntry(&arrow_schema, &result.value(), *file_schema_, first_row_id_));

    for (auto& entry : entries) {
      ICEBERG_RETURN_UNEXPECTED(inheritable_metadata_->Apply(entry));

      if (only_live && !entry.IsAlive()) {
        continue;
      }

      if (needs_filtering) {
        ICEBERG_DCHECK(entry.data_file != nullptr, "Data file cannot be null");
        if (evaluator) {
          ICEBERG_ASSIGN_OR_RAISE(bool partition_match,
                                  evaluator->Evaluate(entry.data_file->partition));
          if (!partition_match) {
            continue;
          }
        }
        if (metrics_evaluator) {
          ICEBERG_ASSIGN_OR_RAISE(bool metrics_match,
                                  metrics_evaluator->Evaluate(*entry.data_file));
          if (!metrics_match) {
            continue;
          }
        }
        if (!InPartitionSet(*entry.data_file)) {
          continue;
        }
      }

      manifest_entries.push_back(std::move(entry));
    }
  }

  return manifest_entries;
}

Result<std::vector<ManifestFile>> ManifestListReaderImpl::Files() const {
  std::vector<ManifestFile> manifest_files;
  ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, reader_->Schema());
  internal::ArrowSchemaGuard schema_guard(&arrow_schema);
  while (true) {
    ICEBERG_ASSIGN_OR_RAISE(auto result, reader_->Next());
    if (!result.has_value()) {
      // eof
      break;
    }
    internal::ArrowArrayGuard array_guard(&result.value());
    ICEBERG_ASSIGN_OR_RAISE(auto parse_result,
                            ParseManifestList(&arrow_schema, &result.value(), *schema_));
    manifest_files.insert(manifest_files.end(),
                          std::make_move_iterator(parse_result.begin()),
                          std::make_move_iterator(parse_result.end()));
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

Result<std::unique_ptr<ManifestReader>> ManifestReader::Make(
    const ManifestFile& manifest, std::shared_ptr<FileIO> file_io,
    std::shared_ptr<Schema> schema, std::shared_ptr<PartitionSpec> spec) {
  if (file_io == nullptr || schema == nullptr || spec == nullptr) {
    return InvalidArgument(
        "FileIO, Schema, and PartitionSpec cannot be null to create ManifestReader");
  }

  // Create inheritable metadata for this manifest
  ICEBERG_ASSIGN_OR_RAISE(auto inheritable_metadata,
                          InheritableMetadataFactory::FromManifest(manifest));
  return std::make_unique<ManifestReaderImpl>(
      manifest.manifest_path, manifest.manifest_length, std::move(file_io),
      std::move(schema), std::move(spec), std::move(inheritable_metadata),
      manifest.first_row_id);
}

Result<std::unique_ptr<ManifestReader>> ManifestReader::Make(
    std::string_view manifest_location, std::shared_ptr<FileIO> file_io,
    std::shared_ptr<Schema> schema, std::shared_ptr<PartitionSpec> spec) {
  if (file_io == nullptr || schema == nullptr || spec == nullptr) {
    return InvalidArgument(
        "FileIO, Schema, and PartitionSpec cannot be null to create ManifestReader");
  }

  // No metadata to inherit in this case.
  ICEBERG_ASSIGN_OR_RAISE(auto inheritable_metadata, InheritableMetadataFactory::Empty());
  return std::make_unique<ManifestReaderImpl>(
      std::string(manifest_location), std::nullopt, std::move(file_io), std::move(schema),
      std::move(spec), std::move(inheritable_metadata), std::nullopt);
}

Result<std::unique_ptr<ManifestListReader>> ManifestListReader::Make(
    std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io) {
  std::shared_ptr<Schema> schema = ManifestFile::Type()->ToSchema();
  ICEBERG_ASSIGN_OR_RAISE(
      auto reader,
      ReaderFactoryRegistry::Open(FileFormatType::kAvro,
                                  {
                                      .path = std::string(manifest_list_location),
                                      .io = std::move(file_io),
                                      .projection = schema,
                                  }));
  return std::make_unique<ManifestListReaderImpl>(std::move(reader), std::move(schema));
}

}  // namespace iceberg
