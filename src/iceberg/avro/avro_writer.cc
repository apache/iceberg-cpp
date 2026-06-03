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
#include <optional>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/builder_base.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/avro/avro_data_util_internal.h"
#include "iceberg/avro/avro_direct_encoder_internal.h"
#include "iceberg/avro/avro_metrics.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/avro/avro_stream_internal.h"
#include "iceberg/metrics_config.h"
#include "iceberg/parquet/parquet_data_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/schema_util.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::avro {

namespace {

Result<std::unique_ptr<AvroOutputStream>> CreateOutputStream(const WriterOptions& options,
                                                             int64_t buffer_size) {
  ICEBERG_ASSIGN_OR_RAISE(auto output,
                          arrow::OpenArrowOutputStream(options.io, options.path));
  return std::make_unique<AvroOutputStream>(output, buffer_size);
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

enum class FieldContext {
  kTopLevel,
  kStruct,
  kListElement,
  kMapKey,
  kMapValue,
};

Result<std::optional<SchemaField>> PruneUnknownField(const SchemaField& field,
                                                     FieldContext context) {
  if (field.type()->type_id() == TypeId::kUnknown) {
    ICEBERG_PRECHECK(context != FieldContext::kMapKey,
                     "Cannot write map key '{}' of unknown type because it has no "
                     "physical Avro representation",
                     field.name());
    ICEBERG_PRECHECK(field.optional(), "Unknown type field '{}' must be optional",
                     field.name());
    if (context == FieldContext::kListElement || context == FieldContext::kMapValue) {
      return field;
    }
    return std::nullopt;
  }

  switch (field.type()->type_id()) {
    case TypeId::kStruct: {
      const auto& struct_type = internal::checked_cast<const StructType&>(*field.type());
      std::vector<SchemaField> pruned_fields;
      pruned_fields.reserve(struct_type.fields().size());
      bool changed = false;
      for (const auto& child : struct_type.fields()) {
        ICEBERG_ASSIGN_OR_RAISE(auto pruned_child,
                                PruneUnknownField(child, FieldContext::kStruct));
        if (pruned_child.has_value()) {
          if (!(pruned_child.value() == child)) {
            changed = true;
          }
          pruned_fields.push_back(std::move(pruned_child.value()));
        } else {
          changed = true;
        }
      }

      if (!changed) {
        return field;
      }

      ICEBERG_PRECHECK(
          !pruned_fields.empty(),
          "Cannot write struct field '{}' because all child fields are unknown and "
          "would be omitted from Avro",
          field.name());

      return SchemaField(field.field_id(), field.name(),
                         std::make_shared<StructType>(std::move(pruned_fields)),
                         field.optional(), field.doc());
    }
    case TypeId::kList: {
      const auto& list_type = internal::checked_cast<const ListType&>(*field.type());
      const auto& element = list_type.element();
      ICEBERG_ASSIGN_OR_RAISE(auto pruned_element,
                              PruneUnknownField(element, FieldContext::kListElement));
      ICEBERG_PRECHECK(pruned_element.has_value(),
                       "Cannot write list field '{}' because its element has no "
                       "physical Avro representation",
                       field.name());
      if (pruned_element.value() == element) {
        return field;
      }
      return SchemaField(field.field_id(), field.name(),
                         std::make_shared<ListType>(std::move(pruned_element.value())),
                         field.optional(), field.doc());
    }
    case TypeId::kMap: {
      const auto& map_type = internal::checked_cast<const MapType&>(*field.type());
      ICEBERG_ASSIGN_OR_RAISE(auto pruned_key,
                              PruneUnknownField(map_type.key(), FieldContext::kMapKey));
      ICEBERG_ASSIGN_OR_RAISE(
          auto pruned_value,
          PruneUnknownField(map_type.value(), FieldContext::kMapValue));
      ICEBERG_PRECHECK(pruned_key.has_value(),
                       "Cannot write map field '{}' because its key has no physical "
                       "Avro representation",
                       field.name());
      ICEBERG_PRECHECK(pruned_value.has_value(),
                       "Cannot write map field '{}' because its value has no physical "
                       "Avro representation",
                       field.name());
      if (pruned_key.value() == map_type.key() &&
          pruned_value.value() == map_type.value()) {
        return field;
      }
      return SchemaField(field.field_id(), field.name(),
                         std::make_shared<MapType>(std::move(pruned_key.value()),
                                                   std::move(pruned_value.value())),
                         field.optional(), field.doc());
    }
    default:
      return field;
  }
}

Result<std::shared_ptr<Schema>> PhysicalWriteSchema(const Schema& schema) {
  std::vector<SchemaField> pruned_fields;
  pruned_fields.reserve(schema.fields().size());
  for (const auto& field : schema.fields()) {
    ICEBERG_ASSIGN_OR_RAISE(auto pruned_field,
                            PruneUnknownField(field, FieldContext::kTopLevel));
    if (pruned_field.has_value()) {
      pruned_fields.push_back(std::move(pruned_field.value()));
    }
  }

  ICEBERG_PRECHECK(
      !pruned_fields.empty(),
      "Cannot write schema because all fields are unknown and would be omitted from "
      "Avro");

  return std::make_shared<Schema>(std::move(pruned_fields), schema.schema_id());
}

// Abstract base class for Avro write backends.
class AvroWriteBackend {
 public:
  virtual ~AvroWriteBackend() = default;
  virtual Status Init(std::unique_ptr<AvroOutputStream> output_stream,
                      const ::avro::ValidSchema& avro_schema, int64_t sync_interval,
                      ::avro::Codec codec, std::optional<int32_t> compression_level,
                      const std::map<std::string, std::vector<uint8_t>>& metadata) = 0;
  virtual Status WriteRow(const Schema& write_schema, const ::arrow::Array& array,
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

  Status WriteRow(const Schema& write_schema, const ::arrow::Array& array,
                  int64_t row_index) override {
    writer_->syncIfNeeded();
    ICEBERG_RETURN_UNEXPECTED(EncodeArrowToAvro(avro_root_node_, writer_->encoder(),
                                                write_schema, array, row_index,
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

  Status WriteRow(const Schema& /*write_schema*/, const ::arrow::Array& array,
                  int64_t row_index) override {
    ICEBERG_RETURN_UNEXPECTED(ExtractDatumFromArray(array, row_index, datum_.get()));
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
  Status Open(const WriterOptions& options) {
    schema_ = options.schema;
    ICEBERG_ASSIGN_OR_RAISE(physical_schema_, PhysicalWriteSchema(*schema_));
    ICEBERG_ASSIGN_OR_RAISE(projection_, iceberg::Project(*physical_schema_, *schema_,
                                                          /*prune_source=*/false));

    ::avro::NodePtr root;
    ICEBERG_RETURN_UNEXPECTED(ToAvroNodeVisitor{}.Visit(*physical_schema_, &root));
    if (const auto& schema_name =
            options.properties.Get(WriterProperties::kAvroSchemaName);
        !schema_name.empty()) {
      root->setName(::avro::Name(schema_name));
    }

    avro_schema_ = std::make_shared<::avro::ValidSchema>(root);

    // Open the output stream and adapt to the avro interface.
    ICEBERG_ASSIGN_OR_RAISE(
        auto output_stream,
        CreateOutputStream(options,
                           options.properties.Get(WriterProperties::kAvroBufferSize)));
    arrow_output_stream_ = output_stream->arrow_output_stream();

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

    ArrowSchema input_arrow_c_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema_, &input_arrow_c_schema));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto input_type,
                                   ::arrow::ImportType(&input_arrow_c_schema));
    input_arrow_type_ = internal::checked_pointer_cast<::arrow::StructType>(input_type);
    input_arrow_schema_ = ::arrow::schema(input_arrow_type_->fields());

    ArrowSchema physical_arrow_c_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*physical_schema_, &physical_arrow_c_schema));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto physical_type,
                                   ::arrow::ImportType(&physical_arrow_c_schema));
    write_arrow_type_ =
        internal::checked_pointer_cast<::arrow::StructType>(physical_type);
    write_arrow_schema_ = ::arrow::schema(write_arrow_type_->fields());
    return {};
  }

  Status Write(ArrowArray* data) {
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto result,
                                   ::arrow::ImportArray(data, input_arrow_type_));
    auto input_struct_array =
        internal::checked_pointer_cast<::arrow::StructArray>(result);
    auto batch = ::arrow::RecordBatch::Make(input_arrow_schema_, result->length(),
                                            input_struct_array->fields());
    ICEBERG_ASSIGN_OR_RAISE(
        batch, iceberg::parquet::ProjectRecordBatch(
                   std::move(batch), write_arrow_schema_, *physical_schema_, projection_,
                   arrow::MetadataColumnContext{}, ::arrow::default_memory_pool()));

    auto write_array = std::make_shared<::arrow::StructArray>(
        write_arrow_type_, batch->num_rows(), batch->columns());

    for (int64_t i = 0; i < write_array->length(); i++) {
      ICEBERG_RETURN_UNEXPECTED(backend_->WriteRow(*physical_schema_, *write_array, i));
    }

    num_records_ += write_array->length();
    return {};
  }

  Status Close() {
    if (!backend_->Closed()) {
      backend_->Close();
      ICEBERG_ARROW_ASSIGN_OR_RETURN(total_bytes_, arrow_output_stream_->Tell());
      ICEBERG_ARROW_RETURN_NOT_OK(arrow_output_stream_->Close());
    }
    return {};
  }

  bool Closed() const { return backend_->Closed(); }

  Result<int64_t> length() {
    if (Closed()) {
      return total_bytes_;
    }
    // Return current flushed length when writer is still open
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto current_pos, arrow_output_stream_->Tell());
    return current_pos;
  }

  Result<Metrics> metrics() const {
    if (!Closed()) {
      return Invalid("AvroWriter is not closed");
    }
    return AvroMetrics::GetMetrics(*schema_, num_records_, *MetricsConfig::Default());
  }

 private:
  // Schema supplied by the caller.
  std::shared_ptr<::iceberg::Schema> schema_;
  // Schema used to write physical Avro fields after pruning unknown fields.
  std::shared_ptr<::iceberg::Schema> physical_schema_;
  // Arrow type used to import caller-provided ArrowArray data.
  std::shared_ptr<::arrow::StructType> input_arrow_type_;
  // Arrow schema used to project caller-provided data.
  std::shared_ptr<::arrow::Schema> input_arrow_schema_;
  // Arrow type used by the Avro writer backends.
  std::shared_ptr<::arrow::StructType> write_arrow_type_;
  // Arrow schema used to write physical Avro fields.
  std::shared_ptr<::arrow::Schema> write_arrow_schema_;
  // Projection from the logical Iceberg schema to the physical write schema.
  SchemaProjection projection_;
  // The avro schema to write.
  std::shared_ptr<::avro::ValidSchema> avro_schema_;
  // Arrow output stream of the Avro file to write
  std::shared_ptr<::arrow::io::OutputStream> arrow_output_stream_;
  // Total length of the written Avro file.
  int64_t total_bytes_ = 0;
  // Number of records written.
  int64_t num_records_ = 0;
  // The write backend to write data.
  std::unique_ptr<AvroWriteBackend> backend_;
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

Result<Metrics> AvroWriter::metrics() { return impl_->metrics(); }

Result<int64_t> AvroWriter::length() { return impl_->length(); }

std::vector<int64_t> AvroWriter::split_offsets() { return {}; }

void RegisterWriter() {
  static WriterFactoryRegistry avro_writer_register(
      FileFormatType::kAvro,
      []() -> Result<std::unique_ptr<Writer>> { return std::make_unique<AvroWriter>(); });
}

}  // namespace iceberg::avro
