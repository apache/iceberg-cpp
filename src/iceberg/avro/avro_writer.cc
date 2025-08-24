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

#include "iceberg/avro/avro_writer.h"

#include <memory>

#include <arrow/array/builder_base.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <avro/DataFile.hh>
#include <avro/GenericDatum.hh>

#include "iceberg/arrow/arrow_error_transform_internal.h"
#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/avro/avro_stream_internal.h"
#include "iceberg/schema.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::avro {

namespace {

Result<std::unique_ptr<AvroOutputStream>> CreateOutputStream(const WriterOptions& options,
                                                             int64_t buffer_size) {
  auto io = internal::checked_pointer_cast<arrow::ArrowFileSystemFileIO>(options.io);
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto output, io->fs()->OpenOutputStream(options.path));
  return std::make_unique<AvroOutputStream>(output, buffer_size);
}

}  // namespace

// A stateful context to keep track of the writing progress.
struct WriteContext {};

class AvroWriter::Impl {
 public:
  Status Open(const WriterOptions& options) {
    write_schema_ = options.schema;

    ::avro::NodePtr root;
    ICEBERG_RETURN_UNEXPECTED(ToAvroNodeVisitor{}.Visit(*write_schema_, &root));

    avro_schema_ = std::make_shared<::avro::ValidSchema>(root);

    // Open the output stream and adapt to the avro interface.
    constexpr int64_t kDefaultBufferSize = 1024 * 1024;
    ICEBERG_ASSIGN_OR_RAISE(auto output_stream,
                            CreateOutputStream(options, kDefaultBufferSize));

    writer_ = std::make_unique<::avro::DataFileWriter<::avro::GenericDatum>>(
        std::move(output_stream), *avro_schema_);
    return {};
  }

  Status Write(ArrowArray /*data*/) {
    // TODO(xiao.dong) convert data and write to avro
    // total_bytes_+= written_bytes;
    return {};
  }

  Status Close() {
    if (writer_ != nullptr) {
      writer_->close();
      writer_.reset();
    }
    return {};
  }

  bool Closed() const { return writer_ == nullptr; }

  int64_t length() { return total_bytes_; }

 private:
  int64_t total_bytes_ = 0;
  // The schema to write.
  std::shared_ptr<::iceberg::Schema> write_schema_;
  // The avro schema to write.
  std::shared_ptr<::avro::ValidSchema> avro_schema_;
  // The avro writer to write the data into a datum.
  std::unique_ptr<::avro::DataFileWriter<::avro::GenericDatum>> writer_;
};

AvroWriter::~AvroWriter() = default;

Status AvroWriter::Write(ArrowArray data) { return impl_->Write(data); }

Status AvroWriter::Open(const WriterOptions& options) {
  impl_ = std::make_unique<Impl>();
  return impl_->Open(options);
}

Status AvroWriter::Close() {
  if (!impl_->Closed()) {
    return impl_->Close();
  }
  return {};
}

std::optional<Metrics> AvroWriter::metrics() {
  if (impl_->Closed()) {
    // TODO(xiao.dong) implement metrics
    return {};
  }
  return std::nullopt;
}

std::optional<int64_t> AvroWriter::length() {
  if (impl_->Closed()) {
    return impl_->length();
  }
  return std::nullopt;
}

std::vector<int64_t> AvroWriter::split_offsets() { return {}; }

void RegisterWriter() {
  static WriterFactoryRegistry avro_writer_register(
      FileFormatType::kAvro,
      []() -> Result<std::unique_ptr<Writer>> { return std::make_unique<AvroWriter>(); });
}

}  // namespace iceberg::avro
