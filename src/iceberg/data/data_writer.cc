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

#include "iceberg/data/data_writer.h"

#include "iceberg/file_writer.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class DataWriter::Impl {
 public:
  explicit Impl(DataWriterOptions options) : options_(std::move(options)) {}

  Status Initialize() {
    WriterOptions writer_options;
    writer_options.path = options_.path;
    writer_options.schema = options_.schema;
    writer_options.io = options_.io;
    writer_options.properties = WriterProperties::FromMap(options_.properties);

    ICEBERG_ASSIGN_OR_RAISE(writer_,
                            WriterFactoryRegistry::Open(options_.format, writer_options));
    return {};
  }

  Status Write(ArrowArray* data) {
    if (!writer_) {
      return InvalidArgument("Writer not initialized");
    }
    return writer_->Write(data);
  }

  Result<int64_t> Length() const {
    if (!writer_) {
      return InvalidArgument("Writer not initialized");
    }
    return writer_->length();
  }

  Status Close() {
    if (!writer_) {
      return InvalidArgument("Writer not initialized");
    }
    if (closed_) {
      return InvalidArgument("Writer already closed");
    }
    ICEBERG_RETURN_UNEXPECTED(writer_->Close());
    closed_ = true;
    return {};
  }

  Result<FileWriter::WriteResult> Metadata() {
    if (!closed_) {
      return InvalidArgument("Cannot get metadata before closing the writer");
    }

    ICEBERG_ASSIGN_OR_RAISE(auto metrics, writer_->metrics());
    ICEBERG_ASSIGN_OR_RAISE(auto length, writer_->length());
    auto split_offsets = writer_->split_offsets();

    auto data_file = std::make_shared<DataFile>();
    data_file->content = DataFile::Content::kData;
    data_file->file_path = options_.path;
    data_file->file_format = options_.format;
    data_file->partition = options_.partition;
    data_file->record_count = metrics.row_count.value_or(0);
    data_file->file_size_in_bytes = length;
    data_file->sort_order_id = options_.sort_order_id;
    data_file->split_offsets = std::move(split_offsets);

    // Convert metrics maps from unordered_map to map
    for (const auto& [col_id, size] : metrics.column_sizes) {
      data_file->column_sizes[col_id] = size;
    }
    for (const auto& [col_id, count] : metrics.value_counts) {
      data_file->value_counts[col_id] = count;
    }
    for (const auto& [col_id, count] : metrics.null_value_counts) {
      data_file->null_value_counts[col_id] = count;
    }
    for (const auto& [col_id, count] : metrics.nan_value_counts) {
      data_file->nan_value_counts[col_id] = count;
    }

    // Serialize literal bounds to binary format
    for (const auto& [col_id, literal] : metrics.lower_bounds) {
      ICEBERG_ASSIGN_OR_RAISE(auto serialized, literal.Serialize());
      data_file->lower_bounds[col_id] = std::move(serialized);
    }
    for (const auto& [col_id, literal] : metrics.upper_bounds) {
      ICEBERG_ASSIGN_OR_RAISE(auto serialized, literal.Serialize());
      data_file->upper_bounds[col_id] = std::move(serialized);
    }

    FileWriter::WriteResult result;
    result.data_files.push_back(std::move(data_file));
    return result;
  }

 private:
  DataWriterOptions options_;
  std::unique_ptr<Writer> writer_;
  bool closed_ = false;
};

DataWriter::DataWriter(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

DataWriter::~DataWriter() = default;

Result<std::unique_ptr<DataWriter>> DataWriter::Make(const DataWriterOptions& options) {
  auto impl = std::make_unique<Impl>(options);
  ICEBERG_RETURN_UNEXPECTED(impl->Initialize());
  return std::unique_ptr<DataWriter>(new DataWriter(std::move(impl)));
}

Status DataWriter::Write(ArrowArray* data) { return impl_->Write(data); }

Result<int64_t> DataWriter::Length() const { return impl_->Length(); }

Status DataWriter::Close() { return impl_->Close(); }

Result<FileWriter::WriteResult> DataWriter::Metadata() { return impl_->Metadata(); }

}  // namespace iceberg
