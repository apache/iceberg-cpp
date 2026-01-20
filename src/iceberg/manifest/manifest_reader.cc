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
#include <ranges>
#include <type_traits>
#include <unordered_set>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/expression/expression.h"
#include "iceberg/expression/projections.h"
#include "iceberg/file_format.h"
#include "iceberg/inheritable_metadata.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/macros.h"
#include "nanoarrow/common/inline_types.h"

namespace iceberg {

namespace {

[[nodiscard]] Status MakeNullError(std::string_view field_name, int64_t row_idx) {
  return InvalidArgument("Required field '{}' is null at row {}", field_name, row_idx);
}

[[nodiscard]] Status AssertViewType(const ArrowArrayView* view, ArrowType expected_type,
                                    std::string_view field_name) {
  if (view->storage_type != expected_type) {
    return InvalidArgument("Field '{}' should be of type {}, got {}", field_name,
                           ArrowTypeString(expected_type),
                           ArrowTypeString(view->storage_type));
  }
  return {};
}

[[nodiscard]] Status AssertViewTypeAndChildren(const ArrowArrayView* view,
                                               ArrowType expected_type,
                                               int64_t expected_children,
                                               std::string_view field_name) {
  if (view->storage_type != expected_type) {
    return InvalidArgument("Field '{}' should be of type {}, got {}", field_name,
                           ArrowTypeString(expected_type),
                           ArrowTypeString(view->storage_type));
  }
  if (view->n_children != expected_children) {
    return InvalidArgument("Field '{}' should have {} children, got {}", field_name,
                           expected_children, view->n_children);
  }
  return {};
}

std::vector<uint8_t> ParseInt8VectorField(const ArrowArrayView* view,
                                          int64_t offset_idx) {
  auto buffer = ArrowArrayViewGetBytesUnsafe(view, offset_idx);
  return {buffer.data.as_char, buffer.data.as_char + buffer.size_bytes};
}

template <typename Container, typename Accessor, typename Transfer>
  requires std::ranges::forward_range<Container> &&
           requires(Accessor& a, Container& c, Transfer& t, const ArrowArrayView* v,
                    int64_t i) {
             std::invoke(a, *std::ranges::begin(c)) = std::invoke(t, v, i);
           }
Status ParseField(Transfer transfer, const ArrowArrayView* array_view,
                  Container& container, Accessor accessor, std::string_view field_name,
                  bool required) {
  auto iter = std::ranges::begin(container);
  for (int64_t row_idx = 0; row_idx < array_view->length; row_idx++, iter++) {
    if (!ArrowArrayViewIsNull(array_view, row_idx)) {
      std::invoke(accessor, *iter) = std::invoke(transfer, array_view, row_idx);
    } else if (required) {
      return MakeNullError(field_name, row_idx);
    }
  }
  return {};
}

template <typename T>
struct unwrap_optional {
  using type = T;
};
template <typename U>
struct unwrap_optional<std::optional<U>> {
  using type = U;
};
template <typename T>
using unwrap_optional_t = typename unwrap_optional<T>::type;

template <typename Container, typename Accessor, typename... Args>
  requires std::ranges::forward_range<Container>
Status ParseIntegerField(const ArrowArrayView* array_view, Container& container,
                         Accessor accessor, Args&&... args) {
  using T = unwrap_optional_t<std::remove_cvref_t<
      std::invoke_result_t<Accessor&, std::ranges::range_reference_t<Container>>>>;
  return ParseField(
      [](const ArrowArrayView* view, int64_t row_idx) {
        return static_cast<T>(ArrowArrayViewGetIntUnsafe(view, row_idx));
      },
      array_view, container, accessor, std::forward<Args>(args)...);
}

template <typename... Args>
Status ParseStringField(Args&&... args) {
  return ParseField(
      [](const ArrowArrayView* view, int64_t row_idx) {
        auto value = ArrowArrayViewGetStringUnsafe(view, row_idx);
        return std::string(value.data, value.size_bytes);
      },
      std::forward<Args>(args)...);
}

template <typename... Args>
Status ParseBinaryField(Args&&... args) {
  return ParseField(ParseInt8VectorField, std::forward<Args>(args)...);
}

template <typename Container, typename Accessor, typename Transfer>
  requires std::ranges::forward_range<Container> && requires(Accessor& a, Container& c,
                                                             Transfer& t,
                                                             const ArrowArrayView* v,
                                                             int64_t offset) {
    std::invoke(a, *std::ranges::begin(c)).emplace_back(std::invoke(t, v, offset));
  }
void ParseVectorField(Transfer transfer, const ArrowArrayView* view, int64_t length,
                      Container& container, Accessor&& accessor) {
  auto iter = std::ranges::begin(container);
  for (int64_t row_idx = 0; row_idx < length; row_idx++, iter++) {
    auto begin_offset = ArrowArrayViewListChildOffset(view, row_idx);
    auto end_offset = ArrowArrayViewListChildOffset(view, row_idx + 1);
    for (int64_t offset = begin_offset; offset < end_offset; offset++) {
      std::invoke(accessor, *iter)
          .emplace_back(std::invoke(transfer, view->children[0], offset));
    }
  }
}

template <typename Container, typename Accessor>
  requires std::ranges::forward_range<Container> &&
           std::ranges::range<std::remove_cvref_t<std::invoke_result_t<
               Accessor&, std::ranges::range_reference_t<Container>>>>
void ParseIntegerVectorField(const ArrowArrayView* view, int64_t length,
                             Container& container, Accessor accessor) {
  using T = unwrap_optional_t<std::ranges::range_value_t<std::remove_cvref_t<
      std::invoke_result_t<Accessor&, std::ranges::range_reference_t<Container>>>>>;
  return ParseVectorField(
      [](const ArrowArrayView* v, int64_t offset) {
        return static_cast<T>(ArrowArrayViewGetIntUnsafe(v, offset));
      },
      view, length, container, accessor);
}

template <typename Container, typename Accessor, typename Transfer>
  requires std::ranges::forward_range<Container> &&
           requires(Accessor& a, Container& c, Transfer& t, const ArrowArrayView* v,
                    int64_t key, int64_t offset) {
             std::invoke(a, *std::ranges::begin(c))[key] = std::invoke(t, v, offset);
           }
Status ParseIntMapField(Transfer transfer, ArrowType value_type,
                        const ArrowArrayView* view, int64_t length, Container& container,
                        Accessor&& accessor, std::string_view field_name) {
  ICEBERG_RETURN_UNEXPECTED(
      AssertViewType(view, ArrowType::NANOARROW_TYPE_MAP, field_name));

  auto map_view = view->children[0];
  ICEBERG_RETURN_UNEXPECTED(AssertViewTypeAndChildren(
      map_view, ArrowType::NANOARROW_TYPE_STRUCT, 2, field_name));

  auto key_view = map_view->children[0];
  auto value_view = map_view->children[1];
  ICEBERG_RETURN_UNEXPECTED(
      AssertViewType(key_view, ArrowType::NANOARROW_TYPE_INT32, field_name));
  ICEBERG_RETURN_UNEXPECTED(AssertViewType(value_view, value_type, field_name));

  auto iter = std::ranges::begin(container);
  for (int64_t row_idx = 0; row_idx < length; row_idx++, iter++) {
    auto begin_offset = view->buffer_views[1].data.as_int32[row_idx];
    auto end_offset = view->buffer_views[1].data.as_int32[row_idx + 1];
    for (int32_t offset = begin_offset; offset < end_offset; offset++) {
      auto key = ArrowArrayViewGetIntUnsafe(key_view, offset);
      std::invoke(accessor, *iter)[key] = std::invoke(transfer, value_view, offset);
    }
  }
  return {};
}

template <typename... Args>
Status ParseIntLongMapField(Args&&... args) {
  return ParseIntMapField(ArrowArrayViewGetIntUnsafe, ArrowType::NANOARROW_TYPE_INT64,
                          std::forward<Args>(args)...);
}

template <typename... Args>
Status ParseIntBinaryMapField(Args&&... args) {
  return ParseIntMapField(ParseInt8VectorField, ArrowType::NANOARROW_TYPE_BINARY,
                          std::forward<Args>(args)...);
}

Status ParsePartitionFieldSummaryList(ArrowArrayView* view,
                                      std::vector<ManifestFile>& manifest_files) {
  // schema of view is list<struct<PartitionFieldSummary>>
  ICEBERG_RETURN_UNEXPECTED(
      AssertViewType(view, ArrowType::NANOARROW_TYPE_LIST, "partitions"));

  // schema of elem_view is struct<PartitionFieldSummary>
  auto elem_view = view->children[0];
  ICEBERG_RETURN_UNEXPECTED(AssertViewTypeAndChildren(
      elem_view, ArrowType::NANOARROW_TYPE_STRUCT, 4, "partitions"));

  auto contains_null = elem_view->children[0];
  auto contains_nan = elem_view->children[1];
  auto lower_bounds = elem_view->children[2];
  auto upper_bounds = elem_view->children[3];
  ICEBERG_RETURN_UNEXPECTED(
      AssertViewType(contains_null, ArrowType::NANOARROW_TYPE_BOOL, "contains_null"));
  ICEBERG_RETURN_UNEXPECTED(
      AssertViewType(contains_nan, ArrowType::NANOARROW_TYPE_BOOL, "contains_nan"));
  ICEBERG_RETURN_UNEXPECTED(
      AssertViewType(lower_bounds, ArrowType::NANOARROW_TYPE_BINARY, "lower_bounds"));
  ICEBERG_RETURN_UNEXPECTED(
      AssertViewType(upper_bounds, ArrowType::NANOARROW_TYPE_BINARY, "upper_bounds"));

  for (int64_t row_idx = 0; row_idx < view->length; row_idx++) {
    auto begin_offset = ArrowArrayViewListChildOffset(view, row_idx);
    auto end_offset = ArrowArrayViewListChildOffset(view, row_idx + 1);
    auto& manifest_file = manifest_files[row_idx];
    for (int64_t offset = begin_offset; offset < end_offset; offset++) {
      auto& summary = manifest_file.partitions.emplace_back();
      if (!ArrowArrayViewIsNull(contains_null, offset)) {
        summary.contains_null = ArrowArrayViewGetIntUnsafe(contains_null, offset);
      } else {
        return MakeNullError("contains_null", offset);
      }
      if (!ArrowArrayViewIsNull(contains_nan, offset)) {
        summary.contains_nan = ArrowArrayViewGetIntUnsafe(contains_nan, offset);
      }
      if (!ArrowArrayViewIsNull(lower_bounds, offset)) {
        summary.lower_bound = ParseInt8VectorField(lower_bounds, offset);
      }
      if (!ArrowArrayViewIsNull(upper_bounds, offset)) {
        summary.upper_bound = ParseInt8VectorField(upper_bounds, offset);
      }
    }
  }
  return {};
}

Result<std::vector<ManifestFile>> ParseManifestList(ArrowSchema* arrow_schema,
                                                    ArrowArray* array,
                                                    const Schema& schema) {
  ArrowError error;
  ArrowArrayView view;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayViewInitFromSchema(&view, arrow_schema, &error), error);
  internal::ArrowArrayViewGuard view_guard(&view);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayViewSetArray(&view, array, &error), error);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayViewValidate(&view, NANOARROW_VALIDATION_LEVEL_FULL, &error), error);

  ICEBERG_RETURN_UNEXPECTED(AssertViewTypeAndChildren(
      &view, ArrowType::NANOARROW_TYPE_STRUCT, schema.fields().size(), "manifest_list"));

  std::vector<ManifestFile> manifest_files;
  manifest_files.resize(array->length);

  for (int64_t idx = 0; idx < array->n_children; idx++) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, schema.GetFieldByIndex(idx));
    ICEBERG_CHECK(field.has_value(), "Invalid index {} for data file schema", idx);

    auto field_name = field->get().name();
    auto field_id = field->get().field_id();
    auto required = !field->get().optional();
    auto field_view = view.children[idx];
    switch (field_id) {
      case ManifestFile::kManifestPathFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseStringField(field_view, manifest_files,
                                                   &ManifestFile::manifest_path,
                                                   field_name, required));
        break;
      case ManifestFile::kManifestLengthFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::manifest_length,
                                                    field_name, required));
        break;
      case ManifestFile::kPartitionSpecIdFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::partition_spec_id,
                                                    field_name, required));
        break;
      case ManifestFile::kContentFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(
            field_view, manifest_files, &ManifestFile::content, field_name, required));
        break;
      case ManifestFile::kSequenceNumberFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::sequence_number,
                                                    field_name, required));
        break;
      case ManifestFile::kMinSequenceNumberFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::min_sequence_number,
                                                    field_name, required));
        break;
      case ManifestFile::kAddedSnapshotIdFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::added_snapshot_id,
                                                    field_name, required));
        break;
      case ManifestFile::kAddedFilesCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::added_files_count,
                                                    field_name, required));
        break;
      case ManifestFile::kExistingFilesCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::existing_files_count,
                                                    field_name, required));
        break;
      case ManifestFile::kDeletedFilesCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::deleted_files_count,
                                                    field_name, required));
        break;
      case ManifestFile::kAddedRowsCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::added_rows_count,
                                                    field_name, required));
        break;
      case ManifestFile::kExistingRowsCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::existing_rows_count,
                                                    field_name, required));
        break;
      case ManifestFile::kDeletedRowsCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::deleted_rows_count,
                                                    field_name, required));
        break;
      case ManifestFile::kPartitionSummaryFieldId:
        ICEBERG_RETURN_UNEXPECTED(
            ParsePartitionFieldSummaryList(field_view, manifest_files));
        break;
      case ManifestFile::kKeyMetadataFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseBinaryField(field_view, manifest_files,
                                                   &ManifestFile::key_metadata,
                                                   field_name, required));
        break;
      case ManifestFile::kFirstRowIdFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_files,
                                                    &ManifestFile::first_row_id,
                                                    field_name, required));
        break;
      default:
        return InvalidManifestList("Unsupported field: {} in manifest file.", field_name);
    }
  }
  return manifest_files;
}

Status ParsePartitionValues(ArrowArrayView* view, int64_t row_idx,
                            std::vector<ManifestEntry>& manifest_entries) {
  switch (view->storage_type) {
    case ArrowType::NANOARROW_TYPE_BOOL: {
      auto value = ArrowArrayViewGetUIntUnsafe(view, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(
          Literal::Boolean(value != 0));
    } break;
    case ArrowType::NANOARROW_TYPE_INT32: {
      auto value = ArrowArrayViewGetIntUnsafe(view, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(Literal::Int(value));
    } break;
    case ArrowType::NANOARROW_TYPE_INT64: {
      auto value = ArrowArrayViewGetIntUnsafe(view, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(Literal::Long(value));
    } break;
    case ArrowType::NANOARROW_TYPE_FLOAT: {
      auto value = ArrowArrayViewGetDoubleUnsafe(view, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(Literal::Float(value));
    } break;
    case ArrowType::NANOARROW_TYPE_DOUBLE: {
      auto value = ArrowArrayViewGetDoubleUnsafe(view, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(Literal::Double(value));
    } break;
    case ArrowType::NANOARROW_TYPE_STRING: {
      auto value = ArrowArrayViewGetStringUnsafe(view, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(
          Literal::String(std::string(value.data, value.size_bytes)));
    } break;
    case ArrowType::NANOARROW_TYPE_BINARY: {
      auto buffer = ArrowArrayViewGetBytesUnsafe(view, row_idx);
      manifest_entries[row_idx].data_file->partition.AddValue(
          Literal::Binary(std::vector<uint8_t>(buffer.data.as_char,
                                               buffer.data.as_char + buffer.size_bytes)));
    } break;
    default:
      return InvalidManifest("Unsupported type {} for partition values",
                             ArrowTypeString(view->storage_type));
  }
  return {};
}

Status ParseDataFile(const std::shared_ptr<StructType>& data_file_schema,
                     ArrowArrayView* view, std::optional<int64_t>& first_row_id,
                     std::vector<ManifestEntry>& manifest_entries) {
  ICEBERG_RETURN_UNEXPECTED(
      AssertViewTypeAndChildren(view, ArrowType::NANOARROW_TYPE_STRUCT,
                                data_file_schema->fields().size(), "data_file"));

  for (int64_t col_idx = 0; col_idx < view->n_children; ++col_idx) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, data_file_schema->GetFieldByIndex(col_idx));
    ICEBERG_CHECK(field.has_value(), "Invalid index {} for data file schema", col_idx);
    auto field_name = field->get().name();
    auto field_id = field->get().field_id();
    auto required = !field->get().optional();
    auto field_view = view->children[col_idx];

    constexpr auto proj_data_file = [](auto proj) {
      return [proj](ManifestEntry& c) -> auto& { return std::invoke(proj, c.data_file); };
    };

    switch (field_id) {
      case DataFile::kContentFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_entries,
                                                    proj_data_file(&DataFile::content),
                                                    field_name, required));
        break;
      case DataFile::kFilePathFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseStringField(field_view, manifest_entries,
                                                   proj_data_file(&DataFile::file_path),
                                                   field_name, required));
        break;
      case DataFile::kFileFormatFieldId:
        for (int64_t row_idx = 0; row_idx < field_view->length; row_idx++) {
          if (!ArrowArrayViewIsNull(field_view, row_idx)) {
            auto value = ArrowArrayViewGetStringUnsafe(field_view, row_idx);
            std::string_view path_str(value.data, value.size_bytes);
            ICEBERG_ASSIGN_OR_RAISE(manifest_entries[row_idx].data_file->file_format,
                                    FileFormatTypeFromString(path_str));
          }
        }
        break;
      case DataFile::kPartitionFieldId: {
        ICEBERG_RETURN_UNEXPECTED(
            AssertViewType(field_view, ArrowType::NANOARROW_TYPE_STRUCT, field_name));
        for (int64_t part_idx = 0; part_idx < field_view->n_children; part_idx++) {
          auto part_view = field_view->children[part_idx];
          for (int64_t row_idx = 0; row_idx < part_view->length; row_idx++) {
            if (ArrowArrayViewIsNull(part_view, row_idx)) {
              break;
            }
            ICEBERG_RETURN_UNEXPECTED(
                ParsePartitionValues(part_view, row_idx, manifest_entries));
          }
        }
      } break;
      case DataFile::kRecordCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(
            field_view, manifest_entries, proj_data_file(&DataFile::record_count),
            field_name, required));
        break;
      case DataFile::kFileSizeFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(
            field_view, manifest_entries, proj_data_file(&DataFile::file_size_in_bytes),
            field_name, required));
        break;
      case DataFile::kColumnSizesFieldId:
        // XXX: map key and value should have the same offset but
        // ArrowArrayViewListChildOffset cannot get the correct offset for map
        ICEBERG_RETURN_UNEXPECTED(
            ParseIntLongMapField(field_view, field_view->length, manifest_entries,
                                 proj_data_file(&DataFile::column_sizes), field_name));
        break;
      case DataFile::kValueCountsFieldId:
        ICEBERG_RETURN_UNEXPECTED(
            ParseIntLongMapField(field_view, field_view->length, manifest_entries,
                                 proj_data_file(&DataFile::value_counts), field_name));
        break;
      case DataFile::kNullValueCountsFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntLongMapField(
            field_view, field_view->length, manifest_entries,
            proj_data_file(&DataFile::null_value_counts), field_name));
        break;
      case DataFile::kNanValueCountsFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntLongMapField(
            field_view, field_view->length, manifest_entries,
            proj_data_file(&DataFile::nan_value_counts), field_name));
        break;
      case DataFile::kLowerBoundsFieldId:
        ICEBERG_RETURN_UNEXPECTED(
            ParseIntBinaryMapField(field_view, field_view->length, manifest_entries,
                                   proj_data_file(&DataFile::lower_bounds), field_name));
        break;
      case DataFile::kUpperBoundsFieldId:
        ICEBERG_RETURN_UNEXPECTED(
            ParseIntBinaryMapField(field_view, field_view->length, manifest_entries,
                                   proj_data_file(&DataFile::upper_bounds), field_name));
        break;
      case DataFile::kKeyMetadataFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseBinaryField(
            field_view, manifest_entries, proj_data_file(&DataFile::key_metadata),
            field_name, required));
        break;
      case DataFile::kSplitOffsetsFieldId:
        ParseIntegerVectorField(field_view, field_view->length, manifest_entries,
                                proj_data_file(&DataFile::split_offsets));
        break;
      case DataFile::kEqualityIdsFieldId:
        ParseIntegerVectorField(field_view, field_view->length, manifest_entries,
                                proj_data_file(&DataFile::equality_ids));
        break;
      case DataFile::kSortOrderIdFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(
            field_view, manifest_entries, proj_data_file(&DataFile::sort_order_id),
            field_name, required));
        break;
      case DataFile::kFirstRowIdFieldId: {
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(
            field_view, manifest_entries, proj_data_file(&DataFile::first_row_id),
            field_name, required));
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
          std::ranges::for_each(
              manifest_entries, [](auto& first_row_id) { first_row_id = std::nullopt; },
              proj_data_file(&DataFile::first_row_id));
        }
        break;
      }
      case DataFile::kReferencedDataFileFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseStringField(
            field_view, manifest_entries, proj_data_file(&DataFile::referenced_data_file),
            field_name, required));
        break;
      case DataFile::kContentOffsetFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(
            field_view, manifest_entries, proj_data_file(&DataFile::content_offset),
            field_name, required));
        break;
      case DataFile::kContentSizeFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(
            field_view, manifest_entries,
            proj_data_file(&DataFile::content_size_in_bytes), field_name, required));
        break;
      default:
        return InvalidManifest("Unsupported field '{}' in the data file schema",
                               field_name);
    }
  }
  return {};
}

Result<std::vector<ManifestEntry>> ParseManifestEntry(
    ArrowSchema* arrow_schema, ArrowArray* array, const Schema& schema,
    std::optional<int64_t>& first_row_id) {
  ArrowError error;
  ArrowArrayView view;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayViewInitFromSchema(&view, arrow_schema, &error), error);
  internal::ArrowArrayViewGuard view_guard(&view);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayViewSetArray(&view, array, &error), error);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayViewValidate(&view, NANOARROW_VALIDATION_LEVEL_FULL, &error), error);

  ICEBERG_RETURN_UNEXPECTED(AssertViewTypeAndChildren(
      &view, ArrowType::NANOARROW_TYPE_STRUCT, schema.fields().size(), "manifest_entry"));

  std::vector<ManifestEntry> manifest_entries;
  manifest_entries.resize(array->length);
  for (int64_t i = 0; i < array->length; i++) {
    manifest_entries[i].data_file = std::make_shared<DataFile>();
  }

  for (int64_t col_idx = 0; col_idx < array->n_children; col_idx++) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, schema.GetFieldByIndex(col_idx));
    ICEBERG_CHECK(field.has_value(), "Invalid column index {} for manifest entry schema",
                  col_idx);

    auto field_name = field->get().name();
    auto field_id = field->get().field_id();
    auto required = !field->get().optional();
    auto field_view = view.children[col_idx];

    switch (field_id) {
      case ManifestEntry::kStatusFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(
            field_view, manifest_entries, &ManifestEntry::status, field_name, required));
        break;
      case ManifestEntry::kSnapshotIdFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_entries,
                                                    &ManifestEntry::snapshot_id,
                                                    field_name, required));
        break;
      case ManifestEntry::kSequenceNumberFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_entries,
                                                    &ManifestEntry::sequence_number,
                                                    field_name, required));
        break;
      case ManifestEntry::kFileSequenceNumberFieldId:
        ICEBERG_RETURN_UNEXPECTED(ParseIntegerField(field_view, manifest_entries,
                                                    &ManifestEntry::file_sequence_number,
                                                    field_name, required));
        break;
      case ManifestEntry::kDataFileFieldId: {
        auto data_file_schema =
            internal::checked_pointer_cast<StructType>(field->get().type());
        ICEBERG_RETURN_UNEXPECTED(
            ParseDataFile(data_file_schema, field_view, first_row_id, manifest_entries));
        break;
      }
      default:
        return InvalidManifest("Unsupported field: {} in manifest entry.", field_name);
    }
  }
  return manifest_entries;
}

const std::vector<std::string> kStatsColumns = {
    "value_counts", "null_value_counts", "nan_value_counts", "lower_bounds",
    "upper_bounds", "column_sizes",      "record_count"};

bool RequireStatsProjection(const std::shared_ptr<Expression>& row_filter,
                            const std::vector<std::string>& columns) {
  if (!row_filter || row_filter->op() == Expression::Operation::kTrue) {
    return false;
  }
  if (columns.empty()) {
    return false;
  }
  const std::unordered_set<std::string_view> selected(columns.cbegin(), columns.cend());
  if (selected.contains(Schema::kAllColumns)) {
    return false;
  }

  return !std::ranges::all_of(kStatsColumns, [&selected](const std::string& col) {
    return selected.contains(col);
  });
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

bool ManifestReader::ShouldDropStats(const std::vector<std::string>& columns) {
  // Make sure we only drop all stats if we had projected all stats.
  // We do not drop stats even if we had partially added some stats columns, except for
  // record_count column.
  // Since we don't want to keep stats map which could be huge in size just because we
  // select record_count, which is a primitive type.
  if (!columns.empty()) {
    const std::unordered_set<std::string_view> selected(columns.cbegin(), columns.cend());
    if (selected.contains(Schema::kAllColumns)) {
      return false;
    }
    std::unordered_set<std::string_view> intersection;
    for (const auto& col : kStatsColumns) {
      if (selected.contains(col)) {
        intersection.insert(col);
      }
    }
    return intersection.empty() ||
           (intersection.size() == 1 && intersection.contains("record_count"));
  }
  return false;
}

std::vector<std::string> ManifestReader::WithStatsColumns(
    const std::vector<std::string>& columns) {
  if (std::ranges::contains(columns, Schema::kAllColumns)) {
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

ManifestReader& ManifestReaderImpl::TryDropStats() {
  drop_stats_ = true;
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

Result<bool> ManifestReaderImpl::InPartitionSet(const DataFile& file) const {
  if (!partition_set_) {
    return true;
  }
  ICEBERG_PRECHECK(file.partition_spec_id.has_value(),
                   "Missing partition spec id from data file {}", file.file_path);
  return partition_set_->contains(file.partition_spec_id.value(), file.partition);
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

  bool drop_stats = drop_stats_ && ShouldDropStats(columns_);

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
        ICEBERG_ASSIGN_OR_RAISE(bool in_partition_set, InPartitionSet(*entry.data_file));
        if (!in_partition_set) {
          continue;
        }
      }

      if (drop_stats) {
        ContentFileUtil::DropAllStats(*entry.data_file);
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
    std::string_view manifest_location, std::optional<int64_t> manifest_length,
    std::shared_ptr<FileIO> file_io, std::shared_ptr<Schema> schema,
    std::shared_ptr<PartitionSpec> spec,
    std::unique_ptr<InheritableMetadata> inheritable_metadata,
    std::optional<int64_t> first_row_id) {
  ICEBERG_PRECHECK(file_io != nullptr, "FileIO cannot be null to read manifest");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null to read manifest");
  ICEBERG_PRECHECK(spec != nullptr, "PartitionSpec cannot be null to read manifest");

  if (inheritable_metadata == nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(inheritable_metadata, InheritableMetadataFactory::Empty());
  }

  return std::make_unique<ManifestReaderImpl>(
      std::string(manifest_location), manifest_length, std::move(file_io),
      std::move(schema), std::move(spec), std::move(inheritable_metadata), first_row_id);
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
