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
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/avro/avro_data_util_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/avro/avro_stream_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
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

class AvroWriter::Impl {
 public:
  ~Impl() {
    if (arrow_schema_.release != nullptr) {
      ArrowSchemaRelease(&arrow_schema_);
    }
  }

  Status Open(const WriterOptions& options) {
    write_schema_ = options.schema;

    ::avro::NodePtr root;
    ICEBERG_RETURN_UNEXPECTED(ToAvroNodeVisitor{}.Visit(*write_schema_, &root));
    if (const auto& schema_name =
            options.properties->Get(WriterProperties::kAvroSchemaName);
        !schema_name.empty()) {
      root->setName(::avro::Name(schema_name));
    }

    avro_schema_ = std::make_shared<::avro::ValidSchema>(root);

    // Open the output stream and adapt to the avro interface.
    ICEBERG_ASSIGN_OR_RAISE(
        auto output_stream,
        CreateOutputStream(options,
                           options.properties->Get(WriterProperties::kAvroBufferSize)));
    arrow_output_stream_ = output_stream->arrow_output_stream();
    std::map<std::string, std::vector<uint8_t>> metadata;
    for (const auto& [key, value] : options.metadata) {
      std::vector<uint8_t> vec;
      vec.reserve(value.size());
      vec.assign(value.begin(), value.end());
      metadata.emplace(key, std::move(vec));
    }
    writer_ = std::make_unique<::avro::DataFileWriter<::avro::GenericDatum>>(
        std::move(output_stream), *avro_schema_,
        options.properties->Get(WriterProperties::kAvroSyncInterval),
        ::avro::NULL_CODEC /*codec*/, metadata);
    datum_ = std::make_unique<::avro::GenericDatum>(*avro_schema_);
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*write_schema_, &arrow_schema_));
    return {};
  }

  Status Write(ArrowArray* data) {
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto result,
                                   ::arrow::ImportArray(data, &arrow_schema_));

    for (int64_t i = 0; i < result->length(); i++) {
      ICEBERG_RETURN_UNEXPECTED(ExtractDatumFromArray(*result, i, datum_.get()));
      writer_->write(*datum_);
    }

    return {};
  }

  Status Close() {
    if (writer_ != nullptr) {
      writer_->close();
      writer_.reset();
      ICEBERG_ARROW_ASSIGN_OR_RETURN(total_bytes_, arrow_output_stream_->Tell());
      ICEBERG_ARROW_RETURN_NOT_OK(arrow_output_stream_->Close());
    }
    return {};
  }

  bool Closed() const { return writer_ == nullptr; }

  int64_t length() { return total_bytes_; }

 private:
  // The schema to write.
  std::shared_ptr<::iceberg::Schema> write_schema_;
  // The avro schema to write.
  std::shared_ptr<::avro::ValidSchema> avro_schema_;
  // Arrow output stream of the Avro file to write
  std::shared_ptr<::arrow::io::OutputStream> arrow_output_stream_;
  // The avro writer to write the data into a datum.
  std::unique_ptr<::avro::DataFileWriter<::avro::GenericDatum>> writer_;
  // Reusable Avro datum for writing individual records.
  std::unique_ptr<::avro::GenericDatum> datum_;
  // Arrow schema to write data.
  ArrowSchema arrow_schema_;
  int64_t total_bytes_ = 0;
};

AvroWriter::~AvroWriter() = default;

Status AvroWriter::Write(ArrowArray* data) { return impl_->Write(data); }

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
