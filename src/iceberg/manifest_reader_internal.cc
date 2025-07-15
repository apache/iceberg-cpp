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

#include <array>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

#define NANOARROW_RETURN_IF_NOT_OK(status, error)                  \
  if (status != NANOARROW_OK) [[unlikely]] {                       \
    return InvalidArrowData("Nanoarrow error: {}", error.message); \
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
      }
      if (!ArrowArrayViewIsNull(contains_nan, partition_idx)) {
        partition_field_summary.contains_nan =
            ArrowArrayViewGetIntUnsafe(contains_nan, partition_idx);
      }
      if (!ArrowArrayViewIsNull(lower_bound_list, partition_idx)) {
        auto buffer = ArrowArrayViewGetBytesUnsafe(lower_bound_list, partition_idx);
        partition_field_summary.lower_bound = std::vector<uint8_t>(
            buffer.data.as_char, buffer.data.as_char + buffer.size_bytes);
      }
      if (!ArrowArrayViewIsNull(upper_bound_list, partition_idx)) {
        auto buffer = ArrowArrayViewGetBytesUnsafe(upper_bound_list, partition_idx);
        partition_field_summary.upper_bound = std::vector<uint8_t>(
            buffer.data.as_char, buffer.data.as_char + buffer.size_bytes);
      }

      manifest_file.partitions.emplace_back(partition_field_summary);
    }
  }
  return {};
}

Result<std::vector<ManifestFile>> ParseManifestListEntry(ArrowSchema* schema,
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
  NANOARROW_RETURN_IF_NOT_OK(status, error);
  internal::ArrowArrayViewGuard view_guard(&array_view);
  status = ArrowArrayViewSetArray(&array_view, array_in, &error);
  NANOARROW_RETURN_IF_NOT_OK(status, error);
  status = ArrowArrayViewValidate(&array_view, NANOARROW_VALIDATION_LEVEL_FULL, &error);
  NANOARROW_RETURN_IF_NOT_OK(status, error);

  std::vector<ManifestFile> manifest_files;
  manifest_files.resize(array_in->length);

  for (int64_t idx = 0; idx < array_in->n_children; idx++) {
    const auto& field = iceberg_schema.GetFieldByIndex(idx);
    if (!field.has_value()) {
      return InvalidSchema("Field index {} is not found in schema", idx);
    }
    auto field_name = field.value().get().name();
    bool required = !field.value().get().optional();
    auto view_of_column = array_view.children[idx];

#define PARSE_PRIMITIVE_FIELD(item, type)                                               \
  for (size_t row_idx = 0; row_idx < view_of_column->length; row_idx++) {               \
    if (!ArrowArrayViewIsNull(view_of_column, row_idx)) {                               \
      auto value = ArrowArrayViewGetIntUnsafe(view_of_column, row_idx);                 \
      manifest_files[row_idx].item = static_cast<type>(value);                          \
    } else if (required) {                                                              \
      return InvalidManifestList("Field {} is required but null at row {}", field_name, \
                                 row_idx);                                              \
    }                                                                                   \
  }

    switch (idx) {
      case 0:
        for (size_t row_idx = 0; row_idx < view_of_column->length; row_idx++) {
          if (!ArrowArrayViewIsNull(view_of_column, row_idx)) {
            auto value = ArrowArrayViewGetStringUnsafe(view_of_column, row_idx);
            std::string path_str(value.data, value.size_bytes);
            manifest_files[row_idx].manifest_path = path_str;
          }
        }
        break;
      case 1:
        PARSE_PRIMITIVE_FIELD(manifest_length, int64_t);
        break;
      case 2:
        PARSE_PRIMITIVE_FIELD(partition_spec_id, int32_t);
        break;
      case 3:
        for (size_t row_idx = 0; row_idx < view_of_column->length; row_idx++) {
          if (!ArrowArrayViewIsNull(view_of_column, row_idx)) {
            auto value = ArrowArrayViewGetIntUnsafe(view_of_column, row_idx);
            manifest_files[row_idx].content = static_cast<ManifestFile::Content>(value);
          }
        }
        break;
      case 4:
        PARSE_PRIMITIVE_FIELD(sequence_number, int64_t);
        break;
      case 5:
        PARSE_PRIMITIVE_FIELD(min_sequence_number, int64_t);
        break;
      case 6:
        PARSE_PRIMITIVE_FIELD(added_snapshot_id, int64_t);
        break;
      case 7:
        PARSE_PRIMITIVE_FIELD(added_files_count, int32_t);
        break;
      case 8:
        PARSE_PRIMITIVE_FIELD(existing_files_count, int32_t);
        break;
      case 9:
        PARSE_PRIMITIVE_FIELD(deleted_files_count, int32_t);
        break;
      case 10:
        PARSE_PRIMITIVE_FIELD(added_rows_count, int64_t);
        break;
      case 11:
        PARSE_PRIMITIVE_FIELD(existing_rows_count, int64_t);
        break;
      case 12:
        PARSE_PRIMITIVE_FIELD(deleted_rows_count, int64_t);
        break;
      case 13:
        ICEBERG_RETURN_UNEXPECTED(
            ParsePartitionFieldSummaryList(view_of_column, manifest_files));
        break;
      case 14:
        for (size_t row_idx = 0; row_idx < view_of_column->length; row_idx++) {
          if (!ArrowArrayViewIsNull(view_of_column, row_idx)) {
            auto buffer = ArrowArrayViewGetBytesUnsafe(view_of_column, row_idx);
            manifest_files[row_idx].key_metadata = std::vector<uint8_t>(
                buffer.data.as_char, buffer.data.as_char + buffer.size_bytes);
          }
        }
        break;
      case 15:
        PARSE_PRIMITIVE_FIELD(first_row_id, int64_t);
        break;
      default:
        return InvalidManifestList("Unsupported type: {}", field_name);
    }
  }
  return manifest_files;
}

Result<std::vector<ManifestEntry>> ManifestReaderImpl::Entries() const { return {}; }

Result<std::vector<ManifestFile>> ManifestListReaderImpl::Files() const {
  std::vector<ManifestFile> manifest_files;
  ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, reader_->Schema());
  internal::ArrowSchemaGuard schema_guard(&arrow_schema);
  while (true) {
    auto result = reader_->Next();
    if (!result.has_value()) {
      return InvalidManifestList("Failed to read manifest list entry:{}",
                                 result.error().message);
    }
    if (result.value().has_value()) {
      internal::ArrowArrayGuard array_guard(&result.value().value());
      ICEBERG_ASSIGN_OR_RAISE(
          auto entries,
          ParseManifestListEntry(&arrow_schema, &result.value().value(), *schema_));
      manifest_files.insert(manifest_files.end(),
                            std::make_move_iterator(entries.begin()),
                            std::make_move_iterator(entries.end()));
    } else {
      // eof
      break;
    }
  }
  return manifest_files;
}

}  // namespace iceberg
