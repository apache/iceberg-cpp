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

#include <map>

#include "iceberg/file_writer.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class DataWriter::Impl {
 public:
  static Result<std::unique_ptr<Impl>> Make(DataWriterOptions options) {
    WriterOptions writer_options{
        .path = options.path,
        .schema = options.schema,
        .io = options.io,
        .properties = WriterProperties::FromMap(options.properties),
    };

    ICEBERG_ASSIGN_OR_RAISE(auto writer,
                            WriterFactoryRegistry::Open(options.format, writer_options));

    return std::unique_ptr<Impl>(new Impl(std::move(options), std::move(writer)));
  }

  Status Write(ArrowArray* data) {
    ICEBERG_DCHECK(writer_, "Writer not initialized");
    return writer_->Write(data);
  }

  Result<int64_t> Length() const {
    ICEBERG_DCHECK(writer_, "Writer not initialized");
    return writer_->length();
  }

  Status Close() {
    ICEBERG_DCHECK(writer_, "Writer not initialized");
    if (closed_) {
      // Idempotent: no-op if already closed
      return {};
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

    auto data_file = std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kData,
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
        .sort_order_id = options_.sort_order_id,
    });

    FileWriter::WriteResult result;
    result.data_files.push_back(std::move(data_file));
    return result;
  }

 private:
  Impl(DataWriterOptions options, std::unique_ptr<Writer> writer)
      : options_(std::move(options)), writer_(std::move(writer)) {}

  DataWriterOptions options_;
  std::unique_ptr<Writer> writer_;
  bool closed_ = false;
};

DataWriter::DataWriter(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

DataWriter::~DataWriter() = default;

Result<std::unique_ptr<DataWriter>> DataWriter::Make(const DataWriterOptions& options) {
  ICEBERG_ASSIGN_OR_RAISE(auto impl, Impl::Make(options));
  return std::unique_ptr<DataWriter>(new DataWriter(std::move(impl)));
}

Status DataWriter::Write(ArrowArray* data) { return impl_->Write(data); }

Result<int64_t> DataWriter::Length() const { return impl_->Length(); }

Status DataWriter::Close() { return impl_->Close(); }

Result<FileWriter::WriteResult> DataWriter::Metadata() { return impl_->Metadata(); }

}  // namespace iceberg
