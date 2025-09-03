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

#include "iceberg/parquet/parquet_writer.h"

#include <memory>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>

#include "iceberg/arrow/arrow_error_transform_internal.h"
#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/schema_internal.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::parquet {

namespace {

Result<std::shared_ptr<::arrow::io::OutputStream>> OpenOutputStream(
    const WriterOptions& options) {
  auto io = internal::checked_pointer_cast<arrow::ArrowFileSystemFileIO>(options.io);
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto output, io->fs()->OpenOutputStream(options.path));
  return output;
}

}  // namespace

class ParquetWriter::Impl {
 public:
  Status Open(const WriterOptions& options) {
    auto writer_properties =
        ::parquet::WriterProperties::Builder().memory_pool(pool_)->build();
    auto arrow_writer_properties = ::parquet::default_arrow_writer_properties();

    ArrowSchema c_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*options.schema, &c_schema));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(arrow_schema_, ::arrow::ImportSchema(&c_schema));

    std::shared_ptr<::parquet::SchemaDescriptor> schema_descriptor;
    ICEBERG_ARROW_RETURN_NOT_OK(
        ::parquet::arrow::ToParquetSchema(arrow_schema_.get(), *writer_properties,
                                          *arrow_writer_properties, &schema_descriptor));
    auto schema_node = std::static_pointer_cast<::parquet::schema::GroupNode>(
        schema_descriptor->schema_root());

    ICEBERG_ASSIGN_OR_RAISE(output_stream_, OpenOutputStream(options));
    auto file_writer = ::parquet::ParquetFileWriter::Open(
        output_stream_, std::move(schema_node), std::move(writer_properties));
    ICEBERG_ARROW_RETURN_NOT_OK(
        ::parquet::arrow::FileWriter::Make(pool_, std::move(file_writer), arrow_schema_,
                                           std::move(arrow_writer_properties), &writer_));

    return {};
  }

  Status Write(ArrowArray array) {
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto batch,
                                   ::arrow::ImportRecordBatch(&array, arrow_schema_));

    ICEBERG_ARROW_RETURN_NOT_OK(writer_->WriteRecordBatch(*batch));

    return {};
  }

  // Close the writer and release resources
  Status Close() {
    if (writer_ == nullptr) {
      return {};  // Already closed
    }

    ICEBERG_ARROW_RETURN_NOT_OK(writer_->Close());
    auto& metadata = writer_->metadata();
    split_offsets_.reserve(metadata->num_row_groups());
    for (int i = 0; i < metadata->num_row_groups(); ++i) {
      split_offsets_.push_back(metadata->RowGroup(i)->file_offset());
    }
    writer_.reset();

    ICEBERG_ARROW_ASSIGN_OR_RETURN(total_bytes_, output_stream_->Tell());
    ICEBERG_ARROW_RETURN_NOT_OK(output_stream_->Close());
    return {};
  }

  bool Closed() const { return writer_ == nullptr; }

  int64_t length() const { return total_bytes_; }

  std::vector<int64_t> split_offsets() const { return split_offsets_; }

 private:
  // TODO(gangwu): make memory pool configurable
  ::arrow::MemoryPool* pool_ = ::arrow::default_memory_pool();
  // Schema to write from the Parquet file.
  std::shared_ptr<::arrow::Schema> arrow_schema_;
  // The output stream to write Parquet file.
  std::shared_ptr<::arrow::io::OutputStream> output_stream_;
  // Parquet file writer to write ArrowArray.
  std::unique_ptr<::parquet::arrow::FileWriter> writer_;
  // Total length of the written Parquet file.
  int64_t total_bytes_{0};
  // Row group start offsets in the Parquet file.
  std::vector<int64_t> split_offsets_;
};

ParquetWriter::~ParquetWriter() = default;

Status ParquetWriter::Open(const WriterOptions& options) {
  impl_ = std::make_unique<Impl>();
  return impl_->Open(options);
}

Status ParquetWriter::Write(ArrowArray array) { return impl_->Write(array); }

Status ParquetWriter::Close() { return impl_->Close(); }

std::optional<Metrics> ParquetWriter::metrics() {
  if (!impl_->Closed()) {
    return std::nullopt;
  }
  return {};
}

std::optional<int64_t> ParquetWriter::length() {
  if (!impl_->Closed()) {
    return std::nullopt;
  }
  return impl_->length();
}

std::vector<int64_t> ParquetWriter::split_offsets() {
  if (!impl_->Closed()) {
    return {};
  }
  return impl_->split_offsets();
}

void RegisterWriter() {
  static WriterFactoryRegistry parquet_writer_register(
      FileFormatType::kParquet, []() -> Result<std::unique_ptr<Writer>> {
        return std::make_unique<ParquetWriter>();
      });
}

}  // namespace iceberg::parquet
