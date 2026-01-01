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

#include "iceberg/avro/avro_reader.h"

#include <memory>

#include <arrow/array/builder_base.h>
#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/metadata_column_util_internal.h"
#include "iceberg/avro/avro_data_util_internal.h"
#include "iceberg/avro/avro_direct_decoder_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/avro/avro_stream_internal.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/name_mapping.h"
#include "iceberg/schema_internal.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::avro {

namespace {

Result<std::unique_ptr<AvroInputStream>> CreateInputStream(const ReaderOptions& options,
                                                           int64_t buffer_size) {
  ::arrow::fs::FileInfo file_info(options.path, ::arrow::fs::FileType::File);
  if (options.length) {
    file_info.set_size(options.length.value());
  }

  auto io = internal::checked_pointer_cast<arrow::ArrowFileSystemFileIO>(options.io);
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto file, io->fs()->OpenInputFile(file_info));
  return std::make_unique<AvroInputStream>(file, buffer_size);
}

// Check if the row position metadata column is in the read schema
bool HasRowPositionColumn(const Schema& schema) {
  for (const auto& field : schema.fields()) {
    if (field.field_id() == MetadataColumns::kFilePositionColumnId) {
      return true;
    }
  }
  return false;
}

// Abstract base class for Avro read backends.
class AvroReadBackend {
 public:
  virtual ~AvroReadBackend() = default;
  virtual Result<::avro::ValidSchema> Init(
      std::unique_ptr<AvroInputStream> input_stream) = 0;
  virtual Status InitWithSchema(const ::avro::ValidSchema& file_schema,
                                const std::optional<Split>& split) = 0;
  virtual void InitReadContext(const ::avro::ValidSchema& reader_schema) = 0;
  virtual bool HasMore() = 0;
  virtual Status DecodeNext(const SchemaProjection& projection, const Schema& read_schema,
                            const arrow::MetadataColumnContext& metadata_context,
                            ::arrow::ArrayBuilder* builder) = 0;
  virtual bool IsPastSync(int64_t split_end) const = 0;
  virtual const ::avro::Metadata& GetMetadata() const = 0;
  virtual const ::avro::ValidSchema& GetReaderSchema() const = 0;
  virtual void Close() = 0;
  virtual bool Closed() const = 0;
};

// Backend implementation using direct Avro decoder.
class DirectDecoderBackend : public AvroReadBackend {
 public:
  Result<::avro::ValidSchema> Init(
      std::unique_ptr<AvroInputStream> input_stream) override {
    reader_ = std::make_unique<::avro::DataFileReaderBase>(std::move(input_stream));
    return reader_->dataSchema();
  }

  Status InitWithSchema(const ::avro::ValidSchema& file_schema,
                        const std::optional<Split>& split) override {
    reader_->init(file_schema);
    if (split) {
      reader_->sync(split->offset);
    }
    return {};
  }

  void InitReadContext(const ::avro::ValidSchema&) override {}

  bool HasMore() override { return reader_->hasMore(); }

  Status DecodeNext(const SchemaProjection& projection, const Schema& read_schema,
                    const arrow::MetadataColumnContext& metadata_context,
                    ::arrow::ArrayBuilder* builder) override {
    reader_->decr();
    return DecodeAvroToBuilder(GetReaderSchema().root(), reader_->decoder(), projection,
                               read_schema, metadata_context, builder, decode_context_);
  }

  bool IsPastSync(int64_t split_end) const override {
    return reader_->pastSync(split_end);
  }

  const ::avro::Metadata& GetMetadata() const override { return reader_->metadata(); }

  const ::avro::ValidSchema& GetReaderSchema() const override {
    return reader_->readerSchema();
  }

  void Close() override {
    if (reader_) {
      reader_->close();
      reader_.reset();
    }
  }

  bool Closed() const override { return reader_ == nullptr; }

 private:
  std::unique_ptr<::avro::DataFileReaderBase> reader_;
  // Decode context for reusing scratch buffers
  DecodeContext decode_context_;
};

// Backend implementation using avro::GenericDatum.
class GenericDatumBackend : public AvroReadBackend {
 public:
  Result<::avro::ValidSchema> Init(
      std::unique_ptr<AvroInputStream> input_stream) override {
    reader_ = std::make_unique<::avro::DataFileReader<::avro::GenericDatum>>(
        std::move(input_stream));
    return reader_->dataSchema();
  }

  Status InitWithSchema(const ::avro::ValidSchema& /*file_schema*/,
                        const std::optional<Split>& split) override {
    if (split) {
      reader_->sync(split->offset);
    }
    return {};
  }

  void InitReadContext(const ::avro::ValidSchema& reader_schema) override {
    datum_ = std::make_unique<::avro::GenericDatum>(reader_schema);
  }

  bool HasMore() override {
    has_more_ = reader_->read(*datum_);
    return has_more_;
  }

  Status DecodeNext(const SchemaProjection& projection, const Schema& read_schema,
                    const arrow::MetadataColumnContext& metadata_context,
                    ::arrow::ArrayBuilder* builder) override {
    return AppendDatumToBuilder(GetReaderSchema().root(), *datum_, projection,
                                read_schema, metadata_context, builder);
  }

  bool IsPastSync(int64_t split_end) const override {
    return reader_->pastSync(split_end);
  }

  const ::avro::Metadata& GetMetadata() const override { return reader_->metadata(); }

  const ::avro::ValidSchema& GetReaderSchema() const override {
    return reader_->readerSchema();
  }

  void Close() override {
    if (reader_) {
      reader_->close();
      reader_.reset();
    }
  }

  bool Closed() const override { return reader_ == nullptr; }

 private:
  std::unique_ptr<::avro::DataFileReader<::avro::GenericDatum>> reader_;
  // Reusable GenericDatum for reading records
  std::unique_ptr<::avro::GenericDatum> datum_;
  // Cached result from HasMore()
  bool has_more_ = false;
};

// A stateful context to keep track of the reading progress.
struct ReadContext {
  // The arrow schema to build the record batch.
  std::shared_ptr<::arrow::Schema> arrow_schema_;
  // The builder to build the record batch.
  std::shared_ptr<::arrow::ArrayBuilder> builder_;
};

}  // namespace

// TODO(gang.wu): collect basic reader metrics
class AvroReader::Impl {
 public:
  Status Open(const ReaderOptions& options) {
    // TODO(gangwu): perhaps adding a ReaderOptions::Validate() method
    if (options.projection == nullptr) {
      return InvalidArgument("Projected schema is required by Avro reader");
    }

    batch_size_ = options.properties->Get(ReaderProperties::kBatchSize);
    read_schema_ = options.projection;

    // Open the input stream and adapt to the avro interface.
    ICEBERG_ASSIGN_OR_RAISE(
        auto input_stream,
        CreateInputStream(options,
                          options.properties->Get(ReaderProperties::kAvroBufferSize)));

    // Create the appropriate backend based on configuration
    if (options.properties->Get(ReaderProperties::kAvroSkipDatum)) {
      backend_ = std::make_unique<DirectDecoderBackend>();
    } else {
      backend_ = std::make_unique<GenericDatumBackend>();
    }

    ICEBERG_ASSIGN_OR_RAISE(auto file_schema, backend_->Init(std::move(input_stream)));

    // Validate field ids in the file schema.
    HasIdVisitor has_id_visitor;
    ICEBERG_RETURN_UNEXPECTED(has_id_visitor.Visit(file_schema));

    if (has_id_visitor.HasNoIds()) {
      // Apply field IDs based on name mapping if available
      if (options.name_mapping) {
        ICEBERG_ASSIGN_OR_RAISE(
            auto new_root_node,
            MakeAvroNodeWithFieldIds(file_schema.root(), *options.name_mapping));

        // Update the file schema to use the new schema with field IDs
        file_schema = ::avro::ValidSchema(new_root_node);
      } else {
        return InvalidSchema(
            "Avro file schema has no field IDs and no name mapping provided");
      }
    } else if (!has_id_visitor.AllHaveIds()) {
      return InvalidSchema("Not all fields in the Avro file schema have field IDs");
    }

    // Project the read schema on top of the file schema.
    ICEBERG_ASSIGN_OR_RAISE(projection_, Project(*read_schema_, file_schema.root(),
                                                 /*prune_source=*/false));

    ICEBERG_RETURN_UNEXPECTED(backend_->InitWithSchema(file_schema, options.split));

    if (options.split) {
      split_end_ = options.split->offset + options.split->length;

      if (options.split->offset != 0 && HasRowPositionColumn(*read_schema_)) {
        return NotSupported(
            "Reading '_pos' metadata column with split is not supported for Avro files.");
      }
    }

    metadata_context_ = {.file_path = options.path, .next_file_pos = 0};

    return {};
  }

  Result<std::optional<ArrowArray>> Next() {
    if (!context_) {
      ICEBERG_RETURN_UNEXPECTED(InitReadContext());
    }

    while (context_->builder_->length() < batch_size_) {
      if (IsPastSync()) {
        break;
      }
      if (!backend_->HasMore()) {
        break;
      }
      ICEBERG_RETURN_UNEXPECTED(backend_->DecodeNext(
          projection_, *read_schema_, metadata_context_, context_->builder_.get()));
      metadata_context_.next_file_pos++;
    }

    return ConvertBuilderToArrowArray();
  }

  Status Close() {
    backend_->Close();
    context_.reset();
    return {};
  }

  Result<ArrowSchema> Schema() {
    if (!context_) {
      ICEBERG_RETURN_UNEXPECTED(InitReadContext());
    }
    ArrowSchema arrow_schema;
    auto export_result = ::arrow::ExportSchema(*context_->arrow_schema_, &arrow_schema);
    if (!export_result.ok()) {
      return InvalidSchema("Failed to export the arrow schema: {}",
                           export_result.message());
    }
    return arrow_schema;
  }

  Result<std::unordered_map<std::string, std::string>> Metadata() {
    if (backend_->Closed()) {
      return Invalid("Reader is not opened");
    }

    const auto& metadata = backend_->GetMetadata();
    std::unordered_map<std::string, std::string> metadata_map;
    metadata_map.reserve(metadata.size());

    for (const auto& pair : metadata) {
      metadata_map.insert_or_assign(pair.first,
                                    std::string(pair.second.begin(), pair.second.end()));
    }

    return metadata_map;
  }

 private:
  Status InitReadContext() {
    context_ = std::make_unique<ReadContext>();

    ArrowSchema arrow_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*read_schema_, &arrow_schema));
    auto import_result = ::arrow::ImportSchema(&arrow_schema);
    if (!import_result.ok()) {
      return InvalidSchema("Failed to import the arrow schema: {}",
                           import_result.status().message());
    }
    context_->arrow_schema_ = import_result.MoveValueUnsafe();

    auto arrow_struct_type =
        std::make_shared<::arrow::StructType>(context_->arrow_schema_->fields());
    auto builder_result = ::arrow::MakeBuilder(arrow_struct_type);
    if (!builder_result.ok()) {
      return InvalidSchema("Failed to make the arrow builder: {}",
                           builder_result.status().message());
    }
    context_->builder_ = builder_result.MoveValueUnsafe();
    backend_->InitReadContext(backend_->GetReaderSchema());

    return {};
  }

  Result<std::optional<ArrowArray>> ConvertBuilderToArrowArray() {
    if (context_->builder_->length() == 0) {
      return std::nullopt;
    }

    auto builder_result = context_->builder_->Finish();
    if (!builder_result.ok()) {
      return InvalidArrowData("Failed to finish the arrow array builder: {}",
                              builder_result.status().message());
    }

    auto array = builder_result.MoveValueUnsafe();
    ArrowArray arrow_array;
    auto export_result = ::arrow::ExportArray(*array, &arrow_array);
    if (!export_result.ok()) {
      return InvalidArrowData("Failed to export the arrow array: {}",
                              export_result.message());
    }
    return arrow_array;
  }

  bool IsPastSync() const {
    if (!split_end_) {
      return false;
    }
    return backend_->IsPastSync(split_end_.value());
  }

 private:
  // Max number of rows in the record batch to read.
  int64_t batch_size_{};
  // The end of the split to read and used to terminate the reading.
  std::optional<int64_t> split_end_;
  // The schema to read.
  std::shared_ptr<::iceberg::Schema> read_schema_;
  // The projection result to apply to the read schema.
  SchemaProjection projection_;
  // The metadata column context for populating _file and _pos columns.
  arrow::MetadataColumnContext metadata_context_;
  // The read backend to read data into Arrow.
  std::unique_ptr<AvroReadBackend> backend_;
  // The context to keep track of the reading progress.
  std::unique_ptr<ReadContext> context_;
};

AvroReader::~AvroReader() = default;

Result<std::optional<ArrowArray>> AvroReader::Next() { return impl_->Next(); }

Result<ArrowSchema> AvroReader::Schema() { return impl_->Schema(); }

Result<std::unordered_map<std::string, std::string>> AvroReader::Metadata() {
  return impl_->Metadata();
}

Status AvroReader::Open(const ReaderOptions& options) {
  impl_ = std::make_unique<Impl>();
  return impl_->Open(options);
}

Status AvroReader::Close() { return impl_->Close(); }

void RegisterReader() {
  static ReaderFactoryRegistry avro_reader_register(
      FileFormatType::kAvro,
      []() -> Result<std::unique_ptr<Reader>> { return std::make_unique<AvroReader>(); });
}

}  // namespace iceberg::avro
