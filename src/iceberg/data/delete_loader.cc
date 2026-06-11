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

#include "iceberg/data/delete_loader.h"

#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/deletes/position_delete_range_consumer.h"
#include "iceberg/file_io.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/result.h"
#include "iceberg/row/arrow_array_wrapper.h"
#include "iceberg/schema.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/struct_like_set.h"

namespace iceberg {

namespace {

/// \brief Build the projection schema for reading position delete files.
std::shared_ptr<Schema> PosDeleteSchema() {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      MetadataColumns::kDeleteFilePath,
      MetadataColumns::kDeleteFilePos,
  });
}

/// \brief Open a delete file with the given projection schema.
Result<std::unique_ptr<Reader>> OpenDeleteFile(const DataFile& file,
                                               std::shared_ptr<Schema> projection,
                                               const std::shared_ptr<FileIO>& io) {
  ReaderOptions options{
      .path = file.file_path,
      .length = static_cast<size_t>(file.file_size_in_bytes),
      .io = io,
      .projection = std::move(projection),
  };
  return ReaderFactoryRegistry::Open(file.file_format, options);
}

/// Raw `int64` values buffer (offset-adjusted). Skips the validity bitmap:
/// `kDeleteFilePos` is required by the V2 spec.
const int64_t* Int64ValuesBuffer(const ArrowArrayView* view) {
  return view->buffer_views[1].data.as_int64 + view->offset;
}

/// String-equals at `row_idx` via nanoarrow's unsafe direct-buffer access.
/// Skips the validity bitmap: `kDeleteFilePath` is required by the V2 spec.
bool StringEquals(const ArrowArrayView* view, int64_t row_idx, std::string_view target) {
  ArrowStringView sv = ArrowArrayViewGetStringUnsafe(view, row_idx);
  if (static_cast<size_t>(sv.size_bytes) != target.size()) {
    return false;
  }
  if (target.empty()) {
    return true;
  }
  return sv.data != nullptr && std::memcmp(sv.data, target.data(), target.size()) == 0;
}

}  // namespace

DeleteLoader::DeleteLoader(std::shared_ptr<FileIO> io) : io_(std::move(io)) {}

DeleteLoader::~DeleteLoader() = default;

Status DeleteLoader::LoadPositionDelete(const DataFile& file, PositionDeleteIndex& index,
                                        std::string_view data_file_path) const {
  // TODO(gangwu): push down path filter to open the file.
  ICEBERG_ASSIGN_OR_RAISE(auto reader, OpenDeleteFile(file, PosDeleteSchema(), io_));

  ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, reader->Schema());
  internal::ArrowSchemaGuard schema_guard(&arrow_schema);

  // Reused across batches; reads child buffers directly to avoid the
  // per-row `Scalar` dispatch in `ArrowArrayStructLike`.
  ArrowArrayView array_view;
  internal::ArrowArrayViewGuard view_guard(&array_view);
  ArrowError error;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayViewInitFromSchema(&array_view, &arrow_schema, &error), error);

  // Fast path when the writer's `referenced_data_file` hint matches our
  // target: skip the path column, hand `pos_data` straight to
  // `ForEachPositionDelete`. Trusts the hint -- spec-compliant writers
  // only set it when all rows share one data file.
  const bool use_referenced_data_file_fast_path =
      file.referenced_data_file.has_value() &&
      file.referenced_data_file.value() == data_file_path;

  // Filter-path staging buffer; reused across batches via `clear()`.
  std::vector<int64_t> positions;
  // Scratch buffer for `ForEachPositionDelete`'s bulk dispatch path;
  // reused across batches and across both routing branches.
  std::vector<uint32_t> bulk_scratch;

  while (true) {
    ICEBERG_ASSIGN_OR_RAISE(auto batch_opt, reader->Next());
    if (!batch_opt.has_value()) break;

    auto& batch = batch_opt.value();
    internal::ArrowArrayGuard batch_guard(&batch);

    ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
        ArrowArrayViewSetArray(&array_view, &batch, &error), error);

    const int64_t length = batch.length;
    if (length <= 0) {
      continue;
    }

    // Child indices must match `PosDeleteSchema()`: 0 = file_path, 1 = pos.
    const ArrowArrayView* path_view = array_view.children[0];
    const ArrowArrayView* pos_view = array_view.children[1];

    // V2 spec marks pos and file_path as required (NOT NULL). The direct
    // buffer access below skips the validity bitmap, so a non-compliant
    // batch would silently corrupt the index. Fail fast instead.
    if (ArrowArrayViewComputeNullCount(pos_view) != 0 ||
        ArrowArrayViewComputeNullCount(path_view) != 0) {
      return InvalidArrowData(
          "position delete file has null values in required pos/file_path columns");
    }

    const int64_t* pos_data = Int64ValuesBuffer(pos_view);

    if (use_referenced_data_file_fast_path) {
      ForEachPositionDelete(std::span<const int64_t>(pos_data, length), index,
                            bulk_scratch);
      continue;
    }

    positions.clear();
    if (positions.capacity() < static_cast<size_t>(length)) {
      positions.reserve(static_cast<size_t>(length));
    }
    for (int64_t i = 0; i < length; ++i) {
      if (StringEquals(path_view, i, data_file_path)) {
        positions.push_back(pos_data[i]);
      }
    }
    ForEachPositionDelete(positions, index, bulk_scratch);
  }

  return reader->Close();
}

Status DeleteLoader::LoadDV(const DataFile& file, PositionDeleteIndex& index) const {
  // A deletion vector must reference exactly one data file; without it the
  // caller cannot know which data file the positions apply to.
  ICEBERG_PRECHECK(file.referenced_data_file.has_value(),
                   "Deletion vector requires referenced_data_file: {}", file.file_path);

  // For deletion vectors, content_offset and content_size_in_bytes point directly
  // at the DV blob bytes within the Puffin file and are required by the spec.
  ICEBERG_PRECHECK(
      file.content_offset.has_value() && file.content_size_in_bytes.has_value(),
      "Deletion vector requires content_offset and content_size_in_bytes: {}",
      file.file_path);

  const int64_t offset = file.content_offset.value();
  const int64_t length = file.content_size_in_bytes.value();
  ICEBERG_PRECHECK(offset >= 0 && length >= 0,
                   "Invalid deletion vector offset/length: offset={}, length={}", offset,
                   length);
  ICEBERG_PRECHECK(length <= std::numeric_limits<int32_t>::max(),
                   "Cannot read deletion vector larger than 2GB: {}", length);

  ICEBERG_ASSIGN_OR_RAISE(auto input_file, io_->NewInputFile(file.file_path));
  ICEBERG_ASSIGN_OR_RAISE(auto stream, input_file->Open());

  std::vector<std::byte> bytes(static_cast<size_t>(length));
  ICEBERG_RETURN_UNEXPECTED(stream->ReadFully(offset, bytes));
  ICEBERG_RETURN_UNEXPECTED(stream->Close());

  std::span<const uint8_t> blob(reinterpret_cast<const uint8_t*>(bytes.data()),
                                bytes.size());
  ICEBERG_ASSIGN_OR_RAISE(auto dv, PositionDeleteIndex::Deserialize(blob));

  // The bitmap cardinality must match the record count recorded in metadata.
  ICEBERG_PRECHECK(std::cmp_equal(dv.Cardinality(), file.record_count),
                   "Deletion vector cardinality {} does not match record count {}: {}",
                   dv.Cardinality(), file.record_count, file.file_path);

  index.Merge(dv);
  return {};
}

Result<PositionDeleteIndex> DeleteLoader::LoadPositionDeletes(
    std::span<const std::shared_ptr<DataFile>> delete_files,
    std::string_view data_file_path) const {
  PositionDeleteIndex index;

  for (const auto& file : delete_files) {
    if (file->referenced_data_file.has_value() &&
        file->referenced_data_file.value() != data_file_path) {
      continue;
    }

    if (file->IsDeletionVector()) {
      ICEBERG_RETURN_UNEXPECTED(LoadDV(*file, index));
      continue;
    }

    ICEBERG_PRECHECK(file->content == DataFile::Content::kPositionDeletes,
                     "Expected position delete file but got content type {}",
                     ToString(file->content));

    ICEBERG_RETURN_UNEXPECTED(LoadPositionDelete(*file, index, data_file_path));
  }

  return index;
}

Result<std::unique_ptr<UncheckedStructLikeSet>> DeleteLoader::LoadEqualityDeletes(
    std::span<const std::shared_ptr<DataFile>> delete_files,
    const StructType& equality_type) const {
  auto eq_set = std::make_unique<UncheckedStructLikeSet>(equality_type);

  std::shared_ptr<Schema> projection = equality_type.ToSchema();

  for (const auto& file : delete_files) {
    ICEBERG_PRECHECK(file->content == DataFile::Content::kEqualityDeletes,
                     "Expected equality delete file but got content type {}",
                     static_cast<int>(file->content));

    ICEBERG_ASSIGN_OR_RAISE(auto reader, OpenDeleteFile(*file, projection, io_));
    ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, reader->Schema());
    internal::ArrowSchemaGuard schema_guard(&arrow_schema);

    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto batch_opt, reader->Next());
      if (!batch_opt.has_value()) break;

      auto& batch = batch_opt.value();
      internal::ArrowArrayGuard batch_guard(&batch);

      ICEBERG_ASSIGN_OR_RAISE(
          auto row, ArrowArrayStructLike::Make(arrow_schema, batch, /*row_index=*/0));

      for (int64_t i = 0; i < batch.length; ++i) {
        if (i > 0) {
          ICEBERG_RETURN_UNEXPECTED(row->Reset(i));
        }
        ICEBERG_RETURN_UNEXPECTED(eq_set->Insert(*row));
      }
    }

    ICEBERG_RETURN_UNEXPECTED(reader->Close());
  }

  return eq_set;
}

}  // namespace iceberg
