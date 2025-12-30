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
#include "iceberg/avro/avro_direct_encoder_internal.h"
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
    use_direct_encoder_ = options.properties->Get(WriterProperties::kAvroSkipDatum);

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

    if (use_direct_encoder_) {
      // Skip avro::GenericDatum by using encoder provided by DataFileWriterBase.
      writer_base_ = std::make_unique<::avro::DataFileWriterBase>(
          std::move(output_stream), *avro_schema_,
          options.properties->Get(WriterProperties::kAvroSyncInterval),
          ::avro::NULL_CODEC /*codec*/, metadata);
      avro_root_node_ = avro_schema_->root();
    } else {
      // Everything via avro::GenericDatum.
      writer_datum_ = std::make_unique<::avro::DataFileWriter<::avro::GenericDatum>>(
          std::move(output_stream), *avro_schema_,
          options.properties->Get(WriterProperties::kAvroSyncInterval),
          ::avro::NULL_CODEC /*codec*/, metadata);
      datum_ = std::make_unique<::avro::GenericDatum>(*avro_schema_);
    }

    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*write_schema_, &arrow_schema_));
    return {};
  }

  Status Write(ArrowArray* data) {
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto result,
                                   ::arrow::ImportArray(data, &arrow_schema_));

    if (use_direct_encoder_) {
      for (int64_t i = 0; i < result->length(); i++) {
        ICEBERG_RETURN_UNEXPECTED(
            EncodeArrowToAvro(avro_root_node_, writer_base_->encoder(), *write_schema_,
                              *result, i, encode_ctx_));
        writer_base_->incr();
      }
    } else {
      for (int64_t i = 0; i < result->length(); i++) {
        ICEBERG_RETURN_UNEXPECTED(ExtractDatumFromArray(*result, i, datum_.get()));
        writer_datum_->write(*datum_);
      }
    }

    return {};
  }

  Status Close() {
    if (use_direct_encoder_) {
      if (writer_base_ != nullptr) {
        writer_base_->close();
        writer_base_.reset();
        ICEBERG_ARROW_ASSIGN_OR_RETURN(total_bytes_, arrow_output_stream_->Tell());
        ICEBERG_ARROW_RETURN_NOT_OK(arrow_output_stream_->Close());
      }
    } else {
      if (writer_datum_ != nullptr) {
        writer_datum_->close();
        writer_datum_.reset();
        ICEBERG_ARROW_ASSIGN_OR_RETURN(total_bytes_, arrow_output_stream_->Tell());
        ICEBERG_ARROW_RETURN_NOT_OK(arrow_output_stream_->Close());
      }
    }
    return {};
  }

  bool Closed() const {
    return use_direct_encoder_ ? writer_base_ == nullptr : writer_datum_ == nullptr;
  }

  Result<int64_t> length() {
    if (Closed()) {
      return total_bytes_;
    }
    // Return current flushed length when writer is still open
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto current_pos, arrow_output_stream_->Tell());
    return current_pos;
  }

 private:
  // The schema to write.
  std::shared_ptr<::iceberg::Schema> write_schema_;
  // The avro schema to write.
  std::shared_ptr<::avro::ValidSchema> avro_schema_;
  // Arrow output stream of the Avro file to write
  std::shared_ptr<::arrow::io::OutputStream> arrow_output_stream_;
  // Arrow schema to write data.
  ArrowSchema arrow_schema_;
  // Total length of the written Avro file.
  int64_t total_bytes_ = 0;

  // Flag to determine which encoder to use
  bool use_direct_encoder_ = true;

  // [Encoder path] Root node of the Avro schema
  ::avro::NodePtr avro_root_node_;
  // [Encoder path] The avro writer using direct encoder
  std::unique_ptr<::avro::DataFileWriterBase> writer_base_;
  // [Encoder path] Encode context for reusing scratch buffers
  EncodeContext encode_ctx_;

  // [GenericDatum path] The avro writer to write the data into a datum
  std::unique_ptr<::avro::DataFileWriter<::avro::GenericDatum>> writer_datum_;
  // [GenericDatum path] Reusable Avro datum for writing individual records
  std::unique_ptr<::avro::GenericDatum> datum_;
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

Result<Metrics> AvroWriter::metrics() {
  if (impl_->Closed()) {
    // TODO(xiao.dong) implement metrics
    return {};
  }
  return Invalid("AvroWriter is not closed");
}

Result<int64_t> AvroWriter::length() { return impl_->length(); }

std::vector<int64_t> AvroWriter::split_offsets() { return {}; }

void RegisterWriter() {
  static WriterFactoryRegistry avro_writer_register(
      FileFormatType::kAvro,
      []() -> Result<std::unique_ptr<Writer>> { return std::make_unique<AvroWriter>(); });
}

}  // namespace iceberg::avro
