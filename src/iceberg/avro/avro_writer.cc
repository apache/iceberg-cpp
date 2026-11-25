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

#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/avro/avro_data_util_internal.h"
#include "iceberg/avro/avro_direct_encoder_internal.h"
#include "iceberg/avro/avro_metrics.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/avro/avro_stream_internal.h"
#include "iceberg/file_io.h"
#include "iceberg/metrics_config.h"
#include "iceberg/nanoarrow_status_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::avro {

namespace {

Result<std::shared_ptr<PositionOutputStream>> CreateOutputStream(
    const WriterOptions& options) {
  ICEBERG_PRECHECK(options.io != nullptr, "FileIO cannot be null");
  ICEBERG_ASSIGN_OR_RAISE(auto output_file, options.io->NewOutputFile(options.path));
  ICEBERG_ASSIGN_OR_RAISE(auto output, output_file->CreateOrOverwrite());
  return std::shared_ptr<PositionOutputStream>(std::move(output));
}

Result<::avro::Codec> ParseCodec(const WriterProperties& properties) {
  const auto& codec_name = properties.Get(WriterProperties::kAvroCompression);
  ::avro::Codec codec;
  if (codec_name == "uncompressed") {
    codec = ::avro::NULL_CODEC;
  } else if (codec_name == "gzip") {
    codec = ::avro::DEFLATE_CODEC;
  } else if (codec_name == "snappy") {
    codec = ::avro::SNAPPY_CODEC;
  } else if (codec_name == "zstd") {
    codec = ::avro::ZSTD_CODEC;
  } else {
    return InvalidArgument("Unsupported Avro codec: {}", codec_name);
  }
  ICEBERG_PRECHECK(::avro::isCodecAvailable(codec),
                   "Avro codec {} is not available in the current build", codec_name);
  return codec;
}

Result<std::optional<int32_t>> ParseCodecLevel(const WriterProperties& properties) {
  auto level_str = properties.Get(WriterProperties::kAvroCompressionLevel);
  if (level_str.empty()) {
    return std::nullopt;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto level, StringUtils::ParseNumber<int32_t>(level_str));
  return level;
}

// Abstract base class for Avro write backends.
class AvroWriteBackend {
 public:
  virtual ~AvroWriteBackend() = default;
  virtual Status Init(std::unique_ptr<AvroOutputStream> output_stream,
                      const ::avro::ValidSchema& avro_schema, int64_t sync_interval,
                      ::avro::Codec codec, std::optional<int32_t> compression_level,
                      const std::map<std::string, std::vector<uint8_t>>& metadata) = 0;
  virtual Status WriteRow(const Schema& write_schema, const ArrowArrayView& array_view,
                          int64_t row_index) = 0;
  virtual void Close() = 0;
  virtual bool Closed() const = 0;
};

// Backend implementation using direct Avro encoder.
class DirectEncoderBackend : public AvroWriteBackend {
 public:
  Status Init(std::unique_ptr<AvroOutputStream> output_stream,
              const ::avro::ValidSchema& avro_schema, int64_t sync_interval,
              ::avro::Codec codec, std::optional<int32_t> compression_level,
              const std::map<std::string, std::vector<uint8_t>>& metadata) override {
    writer_ = std::make_unique<::avro::DataFileWriterBase>(
        std::move(output_stream), avro_schema, sync_interval, codec, metadata,
        compression_level);
    avro_root_node_ = avro_schema.root();
    return {};
  }

  Status WriteRow(const Schema& write_schema, const ArrowArrayView& array_view,
                  int64_t row_index) override {
    writer_->syncIfNeeded();
    ICEBERG_RETURN_UNEXPECTED(EncodeArrowToAvro(avro_root_node_, writer_->encoder(),
                                                write_schema, array_view, row_index,
                                                encode_ctx_));
    writer_->incr();
    return {};
  }

  void Close() override {
    if (writer_) {
      writer_->close();
      writer_.reset();
    }
  }

  bool Closed() const override { return writer_ == nullptr; }

 private:
  // Root node of the Avro schema
  ::avro::NodePtr avro_root_node_;
  // The avro writer using direct encoder
  std::unique_ptr<::avro::DataFileWriterBase> writer_;
  // Encode context for reusing scratch buffers
  EncodeContext encode_ctx_;
};

// Backend implementation using avro::GenericDatum as the intermediate representation.
class GenericDatumBackend : public AvroWriteBackend {
 public:
  Status Init(std::unique_ptr<AvroOutputStream> output_stream,
              const ::avro::ValidSchema& avro_schema, int64_t sync_interval,
              ::avro::Codec codec, std::optional<int32_t> compression_level,
              const std::map<std::string, std::vector<uint8_t>>& metadata) override {
    writer_ = std::make_unique<::avro::DataFileWriter<::avro::GenericDatum>>(
        std::move(output_stream), avro_schema, sync_interval, codec, metadata,
        compression_level);
    datum_ = std::make_unique<::avro::GenericDatum>(avro_schema);
    return {};
  }

  Status WriteRow(const Schema& /*write_schema*/, const ArrowArrayView& array_view,
                  int64_t row_index) override {
    ICEBERG_RETURN_UNEXPECTED(ExtractDatumFromArray(array_view, row_index, datum_.get()));
    writer_->write(*datum_);
    return {};
  }

  void Close() override {
    if (writer_) {
      writer_->close();
      writer_.reset();
    }
  }

  bool Closed() const override { return writer_ == nullptr; }

 private:
  // The avro writer to write the data into a datum
  std::unique_ptr<::avro::DataFileWriter<::avro::GenericDatum>> writer_;
  // Reusable Avro datum for writing individual records
  std::unique_ptr<::avro::GenericDatum> datum_;
};

}  // namespace

class AvroWriter::Impl {
 public:
  ~Impl() { ArrowArrayViewReset(&array_view_); }

  Status Open(const WriterOptions& options) {
    write_schema_ = options.schema;

    ::avro::NodePtr root;
    ICEBERG_RETURN_UNEXPECTED(ToAvroNodeVisitor{}.Visit(*write_schema_, &root));
    if (const auto& schema_name =
            options.properties.Get(WriterProperties::kAvroSchemaName);
        !schema_name.empty()) {
      root->setName(::avro::Name(schema_name));
    }

    avro_schema_ = std::make_shared<::avro::ValidSchema>(root);

    // Open the output stream and adapt to the avro interface.
    ICEBERG_ASSIGN_OR_RAISE(output_stream_, CreateOutputStream(options));
    auto output_stream = std::make_unique<AvroOutputStream>(
        output_stream_, options.properties.Get(WriterProperties::kAvroBufferSize));

    std::map<std::string, std::vector<uint8_t>> metadata;
    for (const auto& [key, value] : options.metadata) {
      std::vector<uint8_t> vec;
      vec.reserve(value.size());
      vec.assign(value.begin(), value.end());
      metadata.emplace(key, std::move(vec));
    }

    // Create the appropriate backend based on configuration
    if (options.properties.Get(WriterProperties::kAvroSkipDatum)) {
      backend_ = std::make_unique<DirectEncoderBackend>();
    } else {
      backend_ = std::make_unique<GenericDatumBackend>();
    }

    ICEBERG_ASSIGN_OR_RAISE(auto codec, ParseCodec(options.properties));
    ICEBERG_ASSIGN_OR_RAISE(auto compression_level, ParseCodecLevel(options.properties));

    ICEBERG_RETURN_UNEXPECTED(
        backend_->Init(std::move(output_stream), *avro_schema_,
                       options.properties.Get(WriterProperties::kAvroSyncInterval), codec,
                       compression_level, metadata));

    ArrowSchema c_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*write_schema_, &c_schema));
    internal::ArrowSchemaGuard schema_guard(&c_schema);
    ArrowError error;
    ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
        ArrowArrayViewInitFromSchema(&array_view_, &c_schema, &error), error);
    return {};
  }

  Status Write(ArrowArray* data) {
    // The writer takes ownership of the batch and releases it once written.
    internal::ArrowArrayGuard data_guard(data);
    ArrowError error;
    ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
        ArrowArrayViewSetArray(&array_view_, data, &error), error);

    for (int64_t i = 0; i < array_view_.length; i++) {
      ICEBERG_RETURN_UNEXPECTED(backend_->WriteRow(*write_schema_, array_view_, i));
    }

    num_records_ += array_view_.length;
    return {};
  }

  Status Close() {
    if (!backend_->Closed()) {
      backend_->Close();
      ICEBERG_ASSIGN_OR_RAISE(total_bytes_, output_stream_->Position());
      ICEBERG_RETURN_UNEXPECTED(output_stream_->Close());
    }
    return {};
  }

  bool Closed() const { return backend_->Closed(); }

  Result<int64_t> length() {
    if (Closed()) {
      return total_bytes_;
    }
    // Return current flushed length when writer is still open
    return output_stream_->Position();
  }

  Result<Metrics> metrics() const {
    if (!Closed()) {
      return Invalid("AvroWriter is not closed");
    }
    return AvroMetrics::GetMetrics(*write_schema_, num_records_,
                                   *MetricsConfig::Default());
  }

 private:
  // The schema to write.
  std::shared_ptr<::iceberg::Schema> write_schema_;
  // The avro schema to write.
  std::shared_ptr<::avro::ValidSchema> avro_schema_;
  // Output stream of the Avro file to write
  std::shared_ptr<PositionOutputStream> output_stream_;
  // View over the batch being written, typed by the write schema.
  ArrowArrayView array_view_{};
  // Total length of the written Avro file.
  int64_t total_bytes_ = 0;
  // Number of records written.
  int64_t num_records_ = 0;
  // The write backend to write data.
  std::unique_ptr<AvroWriteBackend> backend_;
};

AvroWriter::AvroWriter() noexcept = default;

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

Result<Metrics> AvroWriter::metrics() { return impl_->metrics(); }

Result<int64_t> AvroWriter::length() { return impl_->length(); }

std::vector<int64_t> AvroWriter::split_offsets() { return {}; }

void RegisterWriter() {
  static WriterFactoryRegistry avro_writer_register(
      FileFormatType::kAvro,
      []() -> Result<std::unique_ptr<Writer>> { return std::make_unique<AvroWriter>(); });
}

}  // namespace iceberg::avro
