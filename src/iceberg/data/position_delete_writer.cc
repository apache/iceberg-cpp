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

#include "iceberg/data/position_delete_writer.h"

#include <map>
#include <set>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/file_writer.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class PositionDeleteWriter::Impl {
 public:
  static Result<std::unique_ptr<Impl>> Make(PositionDeleteWriterOptions options) {
    // Build the position delete schema with file_path and pos columns
    std::vector<SchemaField> fields;
    fields.push_back(MetadataColumns::kDeleteFilePath);
    fields.push_back(MetadataColumns::kDeleteFilePos);

    auto delete_schema = std::make_shared<Schema>(std::move(fields));

    WriterOptions writer_options{
        .path = options.path,
        .schema = delete_schema,
        .io = options.io,
        .properties = WriterProperties::FromMap(options.properties),
    };

    ICEBERG_ASSIGN_OR_RAISE(auto writer,
                            WriterFactoryRegistry::Open(options.format, writer_options));

    return std::unique_ptr<Impl>(
        new Impl(std::move(options), std::move(delete_schema), std::move(writer)));
  }

  Status Write(ArrowArray* data) {
    ICEBERG_DCHECK(writer_, "Writer not initialized");
    return writer_->Write(data);
  }

  Status WriteDelete(std::string_view file_path, int64_t pos) {
    ICEBERG_DCHECK(writer_, "Writer not initialized");
    buffered_paths_.emplace_back(file_path);
    buffered_positions_.push_back(pos);
    referenced_paths_.emplace(file_path);

    if (static_cast<int64_t>(buffered_paths_.size()) >= kFlushThreshold) {
      return FlushBuffer();
    }
    return {};
  }

  Result<int64_t> Length() const {
    ICEBERG_DCHECK(writer_, "Writer not initialized");
    return writer_->length();
  }

  Status Close() {
    ICEBERG_DCHECK(writer_, "Writer not initialized");
    if (closed_) {
      return {};
    }
    if (!buffered_paths_.empty()) {
      ICEBERG_RETURN_UNEXPECTED(FlushBuffer());
    }
    ICEBERG_RETURN_UNEXPECTED(writer_->Close());
    closed_ = true;
    return {};
  }

  Result<FileWriter::WriteResult> Metadata() {
    ICEBERG_CHECK(closed_, "Cannot get metadata before closing the writer");

    ICEBERG_ASSIGN_OR_RAISE(auto metrics, writer_->metrics());
    ICEBERG_ASSIGN_OR_RAISE(auto length, writer_->length());
    auto split_offsets = writer_->split_offsets();

    // Serialize literal bounds to binary format
    std::map<int32_t, std::vector<uint8_t>> lower_bounds_map;
    for (const auto& [col_id, literal] : metrics.lower_bounds) {
      ICEBERG_ASSIGN_OR_RAISE(auto serialized, literal.Serialize());
      lower_bounds_map[col_id] = std::move(serialized);
    }
    std::map<int32_t, std::vector<uint8_t>> upper_bounds_map;
    for (const auto& [col_id, literal] : metrics.upper_bounds) {
      ICEBERG_ASSIGN_OR_RAISE(auto serialized, literal.Serialize());
      upper_bounds_map[col_id] = std::move(serialized);
    }

    // Set referenced_data_file if all deletes reference the same data file
    std::optional<std::string> referenced_data_file;
    if (referenced_paths_.size() == 1) {
      referenced_data_file = *referenced_paths_.begin();
    }

    auto data_file = std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kPositionDeletes,
        .file_path = options_.path,
        .file_format = options_.format,
        .partition = options_.partition,
        .record_count = metrics.row_count.value_or(-1),
        .file_size_in_bytes = length,
        .column_sizes = {metrics.column_sizes.begin(), metrics.column_sizes.end()},
        .value_counts = {metrics.value_counts.begin(), metrics.value_counts.end()},
        .null_value_counts = {metrics.null_value_counts.begin(),
                              metrics.null_value_counts.end()},
        .nan_value_counts = {metrics.nan_value_counts.begin(),
                             metrics.nan_value_counts.end()},
        .lower_bounds = std::move(lower_bounds_map),
        .upper_bounds = std::move(upper_bounds_map),
        .split_offsets = std::move(split_offsets),
        .sort_order_id = std::nullopt,
        .referenced_data_file = std::move(referenced_data_file),
    });

    FileWriter::WriteResult result;
    result.data_files.push_back(std::move(data_file));
    return result;
  }

 private:
  static constexpr int64_t kFlushThreshold = 1000;

  Impl(PositionDeleteWriterOptions options, std::shared_ptr<Schema> delete_schema,
       std::unique_ptr<Writer> writer)
      : options_(std::move(options)),
        delete_schema_(std::move(delete_schema)),
        writer_(std::move(writer)) {}

  Status FlushBuffer() {
    ArrowSchema arrow_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*delete_schema_, &arrow_schema));

    ArrowArray array;
    ArrowError error;
    ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
        ArrowArrayInitFromSchema(&array, &arrow_schema, &error), error);
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayStartAppending(&array));

    for (size_t i = 0; i < buffered_paths_.size(); ++i) {
      ArrowStringView path_view(buffered_paths_[i].data(),
                                static_cast<int64_t>(buffered_paths_[i].size()));
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(
          ArrowArrayAppendString(array.children[0], path_view));
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(
          ArrowArrayAppendInt(array.children[1], buffered_positions_[i]));
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(&array));
    }

    ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
        ArrowArrayFinishBuildingDefault(&array, &error), error);

    ICEBERG_RETURN_UNEXPECTED(writer_->Write(&array));

    buffered_paths_.clear();
    buffered_positions_.clear();
    arrow_schema.release(&arrow_schema);
    return {};
  }

  PositionDeleteWriterOptions options_;
  std::shared_ptr<Schema> delete_schema_;
  std::unique_ptr<Writer> writer_;
  bool closed_ = false;
  std::vector<std::string> buffered_paths_;
  std::vector<int64_t> buffered_positions_;
  std::set<std::string> referenced_paths_;
};

PositionDeleteWriter::PositionDeleteWriter(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

PositionDeleteWriter::~PositionDeleteWriter() = default;

Result<std::unique_ptr<PositionDeleteWriter>> PositionDeleteWriter::Make(
    const PositionDeleteWriterOptions& options) {
  ICEBERG_ASSIGN_OR_RAISE(auto impl, Impl::Make(options));
  return std::unique_ptr<PositionDeleteWriter>(new PositionDeleteWriter(std::move(impl)));
}

Status PositionDeleteWriter::Write(ArrowArray* data) { return impl_->Write(data); }

Status PositionDeleteWriter::WriteDelete(std::string_view file_path, int64_t pos) {
  return impl_->WriteDelete(file_path, pos);
}

Result<int64_t> PositionDeleteWriter::Length() const { return impl_->Length(); }

Status PositionDeleteWriter::Close() { return impl_->Close(); }

Result<FileWriter::WriteResult> PositionDeleteWriter::Metadata() {
  return impl_->Metadata();
}

}  // namespace iceberg
