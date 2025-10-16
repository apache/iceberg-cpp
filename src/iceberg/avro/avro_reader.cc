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
#include "iceberg/avro/avro_data_util_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/avro/avro_stream_internal.h"
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

}  // namespace

// A stateful context to keep track of the reading progress.
struct ReadContext {
  // The datum to reuse for reading the data.
  std::unique_ptr<::avro::GenericDatum> datum_;
  // The arrow schema to build the record batch.
  std::shared_ptr<::arrow::Schema> arrow_schema_;
  // The builder to build the record batch.
  std::shared_ptr<::arrow::ArrayBuilder> builder_;
};

// TODO(gang.wu): there are a lot to do to make this reader work.
// 1. prune the reader schema based on the projection
// 2. read key-value metadata from the avro file
// 3. collect basic reader metrics
class AvroReader::Impl {
 public:
  Status Open(const ReaderOptions& options) {
    // TODO(gangwu): perhaps adding a ReaderOptions::Validate() method
    if (options.projection == nullptr) {
      return InvalidArgument("Projected schema is required by Avro reader");
    }

    batch_size_ = options.batch_size;
    read_schema_ = options.projection;

    // Open the input stream and adapt to the avro interface.
    // TODO(gangwu): make this configurable
    constexpr int64_t kDefaultBufferSize = 1024 * 1024;
    ICEBERG_ASSIGN_OR_RAISE(auto input_stream,
                            CreateInputStream(options, kDefaultBufferSize));

    // Create a base reader without setting reader schema to enable projection.
    auto base_reader =
        std::make_unique<::avro::DataFileReaderBase>(std::move(input_stream));
    ::avro::ValidSchema file_schema = base_reader->dataSchema();

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
    // TODO(gangwu): support pruning source fields
    ICEBERG_ASSIGN_OR_RAISE(projection_, Project(*read_schema_, file_schema.root(),
                                                 /*prune_source=*/false));
    reader_ = std::make_unique<::avro::DataFileReader<::avro::GenericDatum>>(
        std::move(base_reader), file_schema);

    if (options.split) {
      reader_->sync(options.split->offset);
      split_end_ = options.split->offset + options.split->length;
    }
    return {};
  }

  Result<std::optional<ArrowArray>> Next() {
    if (!context_) {
      ICEBERG_RETURN_UNEXPECTED(InitReadContext());
    }

    while (context_->builder_->length() < batch_size_) {
      if (split_end_ && reader_->pastSync(split_end_.value())) {
        break;
      }
      if (!reader_->read(*context_->datum_)) {
        break;
      }
      ICEBERG_RETURN_UNEXPECTED(
          AppendDatumToBuilder(reader_->readerSchema().root(), *context_->datum_,
                               projection_, *read_schema_, context_->builder_.get()));
    }

    return ConvertBuilderToArrowArray();
  }

  Status Close() {
    if (reader_ != nullptr) {
      reader_->close();
      reader_.reset();
    }
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
    if (reader_ == nullptr) {
      return Invalid("Reader is not opened");
    }

    const auto& metadata = reader_->metadata();

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
    context_->datum_ = std::make_unique<::avro::GenericDatum>(reader_->readerSchema());

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

 private:
  // Max number of rows in the record batch to read.
  int64_t batch_size_{};
  // The end of the split to read and used to terminate the reading.
  std::optional<int64_t> split_end_;
  // The schema to read.
  std::shared_ptr<::iceberg::Schema> read_schema_;
  // The projection result to apply to the read schema.
  SchemaProjection projection_;
  // The avro reader to read the data into a datum.
  std::unique_ptr<::avro::DataFileReader<::avro::GenericDatum>> reader_;
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
