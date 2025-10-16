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

#include "iceberg/parquet/parquet_reader.h"

#include <numeric>

#include <arrow/c/bridge.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/parquet/parquet_data_util_internal.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/parquet/parquet_schema_util_internal.h"
#include "iceberg/result.h"
#include "iceberg/schema_internal.h"
#include "iceberg/schema_util.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::parquet {

namespace {

Result<std::shared_ptr<::arrow::io::RandomAccessFile>> OpenInputStream(
    const ReaderOptions& options) {
  ::arrow::fs::FileInfo file_info(options.path, ::arrow::fs::FileType::File);
  if (options.length) {
    file_info.set_size(options.length.value());
  }

  auto io = internal::checked_pointer_cast<arrow::ArrowFileSystemFileIO>(options.io);
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto input, io->fs()->OpenInputFile(file_info));
  return input;
}

Result<SchemaProjection> BuildProjection(::parquet::arrow::FileReader* reader,
                                         const Schema& read_schema) {
  auto metadata = reader->parquet_reader()->metadata();

  if (!HasFieldIds(metadata->schema()->schema_root())) {
    // TODO(gangwu): apply name mapping to Parquet schema
    return NotImplemented("Applying name mapping to Parquet schema is not implemented");
  }

  ::parquet::arrow::SchemaManifest schema_manifest;
  ICEBERG_ARROW_RETURN_NOT_OK(::parquet::arrow::SchemaManifest::Make(
      metadata->schema(), metadata->key_value_metadata(), reader->properties(),
      &schema_manifest));

  // Leverage SchemaManifest to project the schema
  ICEBERG_ASSIGN_OR_RAISE(auto projection, Project(read_schema, schema_manifest));
  return projection;
}

class EmptyRecordBatchReader : public ::arrow::RecordBatchReader {
 public:
  EmptyRecordBatchReader() = default;
  ~EmptyRecordBatchReader() override = default;

  std::shared_ptr<::arrow::Schema> schema() const override { return nullptr; }

  ::arrow::Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* batch) override {
    batch = nullptr;
    return ::arrow::Status::OK();
  }
};

}  // namespace

// A stateful context to keep track of the reading progress.
struct ReadContext {
  // The arrow schema to output record batches. It may be different with
  // the schema of record batches returned by `record_batch_reader_`
  // when there is any schema evolution.
  std::shared_ptr<::arrow::Schema> output_arrow_schema_;
  // The reader to read record batches from the Parquet file.
  std::unique_ptr<::arrow::RecordBatchReader> record_batch_reader_;
};

// TODO(gangwu): list of work items
// 1. Make the memory pool configurable
// 2. Catch ParquetException and convert to Status/Result
// 3. Add utility to convert Arrow Status/Result to Iceberg Status/Result
// 4. Check field ids and apply name mapping if needed
class ParquetReader::Impl {
 public:
  // Open the Parquet reader with the given options
  Status Open(const ReaderOptions& options) {
    if (options.projection == nullptr) {
      return InvalidArgument("Projected schema is required by Parquet reader");
    }

    split_ = options.split;
    read_schema_ = options.projection;

    // Prepare reader properties
    ::parquet::ReaderProperties reader_properties(pool_);
    ::parquet::ArrowReaderProperties arrow_reader_properties;
    arrow_reader_properties.set_batch_size(options.batch_size);
    arrow_reader_properties.set_arrow_extensions_enabled(true);

    // Open the Parquet file reader
    ICEBERG_ASSIGN_OR_RAISE(input_stream_, OpenInputStream(options));
    auto file_reader =
        ::parquet::ParquetFileReader::Open(input_stream_, reader_properties);
    ICEBERG_ARROW_RETURN_NOT_OK(::parquet::arrow::FileReader::Make(
        pool_, std::move(file_reader), arrow_reader_properties, &reader_));

    // Project read schema onto the Parquet file schema
    ICEBERG_ASSIGN_OR_RAISE(projection_, BuildProjection(reader_.get(), *read_schema_));

    return {};
  }

  // Read the next batch of data
  Result<std::optional<ArrowArray>> Next() {
    if (!context_) {
      ICEBERG_RETURN_UNEXPECTED(InitReadContext());
    }

    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto batch, context_->record_batch_reader_->Next());
    if (!batch) {
      return std::nullopt;
    }

    ICEBERG_ASSIGN_OR_RAISE(
        batch, ProjectRecordBatch(std::move(batch), context_->output_arrow_schema_,
                                  *read_schema_, projection_, pool_));

    ArrowArray arrow_array;
    ICEBERG_ARROW_RETURN_NOT_OK(::arrow::ExportRecordBatch(*batch, &arrow_array));
    return arrow_array;
  }

  // Close the reader and release resources
  Status Close() {
    if (reader_ == nullptr) {
      return {};  // Already closed
    }

    if (context_ != nullptr) {
      ICEBERG_ARROW_RETURN_NOT_OK(context_->record_batch_reader_->Close());
      context_.reset();
    }

    reader_.reset();
    ICEBERG_ARROW_RETURN_NOT_OK(input_stream_->Close());
    return {};
  }

  // Get the schema of the data
  Result<ArrowSchema> Schema() {
    if (!context_) {
      ICEBERG_RETURN_UNEXPECTED(InitReadContext());
    }

    ArrowSchema arrow_schema;
    ICEBERG_ARROW_RETURN_NOT_OK(
        ::arrow::ExportSchema(*context_->output_arrow_schema_, &arrow_schema));
    return arrow_schema;
  }

  Result<std::unordered_map<std::string, std::string>> Metadata() {
    if (reader_ == nullptr) {
      return Invalid("Reader is not opened");
    }

    auto metadata = reader_->parquet_reader()->metadata();
    if (!metadata) {
      return Invalid("Failed to get Parquet file metadata");
    }

    const auto& kv_metadata = metadata->key_value_metadata();
    if (!kv_metadata) {
      return std::unordered_map<std::string, std::string>{};
    }

    std::unordered_map<std::string, std::string> metadata_map;
    kv_metadata->ToUnorderedMap(&metadata_map);

    return metadata_map;
  }

 private:
  Status InitReadContext() {
    context_ = std::make_unique<ReadContext>();

    // Build the output Arrow schema
    ArrowSchema arrow_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*read_schema_, &arrow_schema));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(context_->output_arrow_schema_,
                                   ::arrow::ImportSchema(&arrow_schema));

    // Row group pruning based on the split
    // TODO(gangwu): add row group filtering based on zone map, bloom filter, etc.
    std::vector<int> row_group_indices;
    if (split_.has_value()) {
      auto metadata = reader_->parquet_reader()->metadata();
      for (int i = 0; i < metadata->num_row_groups(); ++i) {
        auto row_group_offset = metadata->RowGroup(i)->file_offset();
        if (row_group_offset >= split_->offset &&
            row_group_offset < split_->offset + split_->length) {
          row_group_indices.push_back(i);
        } else if (row_group_offset >= split_->offset + split_->length) {
          break;
        }
      }
    } else {
      row_group_indices.resize(reader_->parquet_reader()->metadata()->num_row_groups());
      std::iota(row_group_indices.begin(), row_group_indices.end(), 0);  // NOLINT
    }

    // Create the record batch reader
    if (row_group_indices.empty()) {
      // None of the row groups are selected, return an empty record batch reader
      context_->record_batch_reader_ = std::make_unique<EmptyRecordBatchReader>();
    } else {
      auto column_indices = SelectedColumnIndices(projection_);
      ICEBERG_ARROW_ASSIGN_OR_RETURN(
          context_->record_batch_reader_,
          reader_->GetRecordBatchReader(row_group_indices, column_indices));
    }

    return {};
  }

 private:
  // TODO(gangwu): make memory pool configurable
  ::arrow::MemoryPool* pool_ = ::arrow::default_memory_pool();
  // The split to read from the Parquet file.
  std::optional<Split> split_;
  // Schema to read from the Parquet file.
  std::shared_ptr<::iceberg::Schema> read_schema_;
  // The projection result to apply to the read schema.
  SchemaProjection projection_;
  // The input stream to read Parquet file.
  std::shared_ptr<::arrow::io::RandomAccessFile> input_stream_;
  // Parquet file reader to create RecordBatchReader.
  std::unique_ptr<::parquet::arrow::FileReader> reader_;
  // The context to keep track of the reading progress.
  std::unique_ptr<ReadContext> context_;
};

ParquetReader::~ParquetReader() = default;

Result<std::optional<ArrowArray>> ParquetReader::Next() { return impl_->Next(); }

Result<ArrowSchema> ParquetReader::Schema() { return impl_->Schema(); }

Result<std::unordered_map<std::string, std::string>> ParquetReader::Metadata() {
  return impl_->Metadata();
}

Status ParquetReader::Open(const ReaderOptions& options) {
  impl_ = std::make_unique<Impl>();
  return impl_->Open(options);
}

Status ParquetReader::Close() { return impl_->Close(); }

void RegisterReader() {
  static ReaderFactoryRegistry parquet_reader_register(
      FileFormatType::kParquet, []() -> Result<std::unique_ptr<Reader>> {
        return std::make_unique<ParquetReader>();
      });
}

}  // namespace iceberg::parquet
