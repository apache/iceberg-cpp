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

#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/util/compression.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/parquet/parquet_metrics_internal.h"
#include "iceberg/schema_internal.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/visit_type.h"

namespace iceberg::parquet {

namespace {

Result<std::shared_ptr<::arrow::io::OutputStream>> OpenOutputStream(
    const WriterOptions& options) {
  return arrow::OpenArrowOutputStream(options.io, options.path);
}

Result<::arrow::Compression::type> ParseCompression(const WriterProperties& properties) {
  const auto& compression_name = properties.Get(WriterProperties::kParquetCompression);
  if (compression_name == "uncompressed") {
    return ::arrow::Compression::UNCOMPRESSED;
  } else if (compression_name == "snappy") {
    return ::arrow::Compression::SNAPPY;
  } else if (compression_name == "gzip") {
    return ::arrow::Compression::GZIP;
  } else if (compression_name == "brotli") {
    return ::arrow::Compression::BROTLI;
  } else if (compression_name == "lz4") {
    return ::arrow::Compression::LZ4;
  } else if (compression_name == "zstd") {
    return ::arrow::Compression::ZSTD;
  } else {
    return InvalidArgument("Unsupported Parquet compression codec: {}", compression_name);
  }
}

Status CheckCompressionAvailable(std::string_view compression_name,
                                 ::arrow::Compression::type compression) {
  ICEBERG_PRECHECK(::arrow::util::Codec::IsAvailable(compression),
                   "Parquet compression codec {} is not available in the current build",
                   compression_name);
  return {};
}

template <typename ArrowArrayType, typename ValueType>
Status UpdateFloatingFieldMetrics(int32_t field_id, const ::arrow::Array& arrow_array,
                                  const std::vector<uint8_t>* valid_rows,
                                  std::unordered_map<int32_t, FieldMetrics>& metrics) {
  constexpr auto expected_type_id =
      std::is_same_v<ValueType, float> ? ::arrow::Type::FLOAT : ::arrow::Type::DOUBLE;
  ICEBERG_PRECHECK(arrow_array.type_id() == expected_type_id,
                   "Expected Arrow floating-point array for field metrics collection");
  const auto& array = static_cast<const ArrowArrayType&>(arrow_array);
  auto& field_metrics = metrics[field_id];
  field_metrics.field_id = field_id;
  if (field_metrics.value_count < 0) {
    field_metrics.value_count = 0;
  }
  if (field_metrics.null_value_count < 0) {
    field_metrics.null_value_count = 0;
  }
  if (field_metrics.nan_value_count < 0) {
    field_metrics.nan_value_count = 0;
  }

  field_metrics.value_count += array.length();

  for (int64_t i = 0; i < array.length(); ++i) {
    if ((valid_rows != nullptr && (*valid_rows)[i] == 0) || array.IsNull(i)) {
      ++field_metrics.null_value_count;
      continue;
    }

    ValueType value = array.Value(i);
    if (std::isnan(value)) {
      ++field_metrics.nan_value_count;
      continue;
    }

    auto literal = [&]() {
      if constexpr (std::is_same_v<ValueType, float>) {
        return Literal::Float(value);
      } else {
        return Literal::Double(value);
      }
    }();
    if (!field_metrics.lower_bound.has_value() ||
        literal < field_metrics.lower_bound.value()) {
      field_metrics.lower_bound = literal;
    }
    if (!field_metrics.upper_bound.has_value() ||
        literal > field_metrics.upper_bound.value()) {
      field_metrics.upper_bound = std::move(literal);
    }
  }

  return {};
}

std::optional<std::vector<uint8_t>> BuildValidRows(const ::arrow::Array& array,
                                                   const std::vector<uint8_t>* parent) {
  if (parent == nullptr && array.null_count() == 0) {
    return std::nullopt;
  }

  std::vector<uint8_t> valid_rows(array.length(), 1);
  for (int64_t i = 0; i < array.length(); ++i) {
    if ((parent != nullptr && (*parent)[i] == 0) || array.IsNull(i)) {
      valid_rows[i] = 0;
    }
  }
  return valid_rows;
}

class FieldMetricsCollector {
 public:
  FieldMetricsCollector(std::unordered_map<int32_t, FieldMetrics>& metrics,
                        const MetricsConfig& metrics_config, const Schema& schema)
      : metrics_(metrics), metrics_config_(metrics_config), schema_(schema) {}

  Status VisitStruct(const StructType& type, const ::arrow::Array& array) {
    ICEBERG_PRECHECK(array.type_id() == ::arrow::Type::STRUCT,
                     "Expected Arrow struct array for Iceberg struct metrics collection");
    const auto& struct_array = static_cast<const ::arrow::StructArray&>(array);
    ICEBERG_PRECHECK(
        struct_array.num_fields() == type.fields().size(),
        "Arrow struct field count does not match Iceberg struct field count");

    for (int i = 0; i < struct_array.num_fields(); ++i) {
      ICEBERG_RETURN_UNEXPECTED(VisitField(type.fields()[i], *struct_array.field(i)));
    }
    return {};
  }

  Status VisitList(const ListType& /*type*/, const ::arrow::Array& /*array*/) {
    return {};
  }

  Status VisitMap(const MapType& /*type*/, const ::arrow::Array& /*array*/) { return {}; }

  Status VisitPrimitive(const PrimitiveType& type, const ::arrow::Array& array) {
    switch (type.type_id()) {
      case TypeId::kFloat:
        return UpdateFloatingFieldMetrics<::arrow::FloatArray, float>(
            field_id_, array, valid_rows_, metrics_);
      case TypeId::kDouble:
        return UpdateFloatingFieldMetrics<::arrow::DoubleArray, double>(
            field_id_, array, valid_rows_, metrics_);
      default:
        return {};
    }
  }

 private:
  Status VisitField(const SchemaField& field, const ::arrow::Array& array) {
    // Skip metrics collection for fields whose mode is kNone in MetricsConfig.
    ICEBERG_ASSIGN_OR_RAISE(auto column_name,
                            schema_.FindColumnNameById(field.field_id()));
    if (column_name.has_value() && metrics_config_.ColumnMode(column_name.value()).kind ==
                                       MetricsMode::Kind::kNone) {
      return {};
    }

    auto previous_valid_rows = valid_rows_;
    auto field_valid_rows = BuildValidRows(array, previous_valid_rows);
    if (field_valid_rows.has_value()) {
      valid_rows_ = &field_valid_rows.value();
    }

    field_id_ = field.field_id();
    auto status = VisitTypeCategory(*field.type(), this, array);
    valid_rows_ = previous_valid_rows;
    return status;
  }

  std::unordered_map<int32_t, FieldMetrics>& metrics_;
  const MetricsConfig& metrics_config_;
  const Schema& schema_;
  const std::vector<uint8_t>* valid_rows_ = nullptr;
  int32_t field_id_ = -1;
};

Result<std::optional<int32_t>> ParseCodecLevel(const WriterProperties& properties) {
  auto level_str = properties.Get(WriterProperties::kParquetCompressionLevel);
  if (level_str.empty()) {
    return std::nullopt;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto level, StringUtils::ParseNumber<int32_t>(level_str));
  return level;
}

}  // namespace

class ParquetWriter::Impl {
 public:
  Status Open(const WriterOptions& options) {
    schema_ = options.schema;

    ICEBERG_ASSIGN_OR_RAISE(auto compression, ParseCompression(options.properties));
    ICEBERG_ASSIGN_OR_RAISE(auto compression_level, ParseCodecLevel(options.properties));

    auto properties_builder = ::parquet::WriterProperties::Builder();
    properties_builder.compression(compression);
    auto max_row_group_rows =
        options.properties.Get(WriterProperties::kParquetMaxRowGroupRows);
    ICEBERG_PRECHECK(max_row_group_rows > 0,
                     "Parquet max row group rows must be greater than 0");
    properties_builder.max_row_group_length(max_row_group_rows);
    if (compression_level.has_value()) {
      properties_builder.compression_level(compression_level.value());
    }
    auto writer_properties = properties_builder.memory_pool(pool_)->build();
    auto arrow_writer_properties = ::parquet::default_arrow_writer_properties();

    ArrowSchema c_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema_, &c_schema));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(arrow_schema_, ::arrow::ImportSchema(&c_schema));

    ICEBERG_ARROW_RETURN_NOT_OK(
        ::parquet::arrow::ToParquetSchema(arrow_schema_.get(), *writer_properties,
                                          *arrow_writer_properties, &parquet_schema_));
    auto schema_node = std::static_pointer_cast<::parquet::schema::GroupNode>(
        parquet_schema_->schema_root());

    ICEBERG_RETURN_UNEXPECTED(CheckCompressionAvailable(
        options.properties.Get(WriterProperties::kParquetCompression), compression));

    ICEBERG_ASSIGN_OR_RAISE(output_stream_, OpenOutputStream(options));
    auto file_writer = ::parquet::ParquetFileWriter::Open(
        output_stream_, std::move(schema_node), std::move(writer_properties),
        std::make_shared<::arrow::KeyValueMetadata>(options.metadata));
    ICEBERG_ARROW_RETURN_NOT_OK(
        ::parquet::arrow::FileWriter::Make(pool_, std::move(file_writer), arrow_schema_,
                                           std::move(arrow_writer_properties), &writer_));

    metrics_config_ = options.metrics_config;

    return {};
  }

  Status Write(ArrowArray* array) {
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto batch,
                                   ::arrow::ImportRecordBatch(array, arrow_schema_));

    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto struct_array, batch->ToStructArray());
    FieldMetricsCollector field_metrics_collector(field_metrics_, *metrics_config_,
                                                  *schema_);
    ICEBERG_RETURN_UNEXPECTED(
        field_metrics_collector.VisitStruct(*schema_, *struct_array));

    ICEBERG_ARROW_RETURN_NOT_OK(writer_->WriteRecordBatch(*batch));

    return {};
  }

  // Close the writer and release resources
  Status Close() {
    if (writer_ == nullptr) {
      return {};  // Already closed
    }

    ICEBERG_ARROW_RETURN_NOT_OK(writer_->Close());
    metadata_ = writer_->metadata();
    split_offsets_.reserve(metadata_->num_row_groups());
    for (int i = 0; i < metadata_->num_row_groups(); ++i) {
      split_offsets_.push_back(metadata_->RowGroup(i)->file_offset());
    }
    writer_.reset();

    ICEBERG_ARROW_ASSIGN_OR_RETURN(total_bytes_, output_stream_->Tell());
    ICEBERG_ARROW_RETURN_NOT_OK(output_stream_->Close());
    return {};
  }

  bool Closed() const { return writer_ == nullptr; }

  Result<int64_t> length() {
    if (Closed()) {
      return total_bytes_;
    }
    // Return current flushed length when writer is still open.
    // It would be good if we could get the number of buffered bytes
    // from the internal RowGroupWriter.
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto current_pos, output_stream_->Tell());
    return current_pos;
  }

  std::vector<int64_t> split_offsets() const { return split_offsets_; }

  Result<Metrics> metrics() {
    ICEBERG_PRECHECK(writer_ == nullptr, "Cannot return metrics for unclosed writer");
    ICEBERG_PRECHECK(metadata_ != nullptr,
                     "Cannot return metrics because Parquet metadata is not available");
    return ParquetMetrics::GetMetrics(*schema_, *parquet_schema_, *metrics_config_,
                                      *metadata_, field_metrics_);
  }

 private:
  // TODO(gangwu): make memory pool configurable
  ::arrow::MemoryPool* pool_ = ::arrow::default_memory_pool();
  // Schema to write from the Iceberg table.
  std::shared_ptr<Schema> schema_;
  // Schema to write from the Parquet file.
  std::shared_ptr<::arrow::Schema> arrow_schema_;
  // Parquet schema descriptor generated from the Arrow schema.
  std::shared_ptr<::parquet::SchemaDescriptor> parquet_schema_;
  // Metrics config for collecting metrics during write.
  std::shared_ptr<MetricsConfig> metrics_config_;
  // The output stream to write Parquet file.
  std::shared_ptr<::arrow::io::OutputStream> output_stream_;
  // Parquet file writer to write ArrowArray.
  std::unique_ptr<::parquet::arrow::FileWriter> writer_;
  // Store the metadata if writer has been closed.
  std::shared_ptr<::parquet::FileMetaData> metadata_;
  // Total length of the written Parquet file.
  int64_t total_bytes_{0};
  // Row group start offsets in the Parquet file.
  std::vector<int64_t> split_offsets_;
  // Write-side metrics for fields whose Parquet footer metrics are incomplete.
  std::unordered_map<int32_t, FieldMetrics> field_metrics_;
};

ParquetWriter::~ParquetWriter() = default;

Status ParquetWriter::Open(const WriterOptions& options) {
  impl_ = std::make_unique<Impl>();
  return impl_->Open(options);
}

Status ParquetWriter::Write(ArrowArray* array) { return impl_->Write(array); }

Status ParquetWriter::Close() { return impl_->Close(); }

Result<Metrics> ParquetWriter::metrics() { return impl_->metrics(); }

Result<int64_t> ParquetWriter::length() { return impl_->length(); }

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
