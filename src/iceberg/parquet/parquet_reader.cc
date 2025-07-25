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

#include <memory>

#include <arrow/c/bridge.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <iceberg/result.h>
#include <iceberg/schema_util.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>

#include "iceberg/arrow/arrow_fs_file_io.h"
#include "iceberg/parquet/parquet_data_util_internal.h"
#include "iceberg/parquet/parquet_schema_util_internal.h"
#include "iceberg/schema_internal.h"
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
  auto result = io->fs()->OpenInputFile(file_info);
  if (!result.ok()) {
    return IOError("Failed to open file {} for reading: {}", options.path,
                   result.status().message());
  }

  return result.MoveValueUnsafe();
}

Result<SchemaProjection> BuildProjection(::parquet::arrow::FileReader* reader,
                                         const Schema& read_schema) {
  auto metadata = reader->parquet_reader()->metadata();

  ICEBERG_ASSIGN_OR_RAISE(auto has_field_ids,
                          HasFieldIds(metadata->schema()->schema_root()));
  if (!has_field_ids) {
    // TODO(gangwu): apply name mapping to Parquet schema
    return NotImplemented("Applying name mapping to Parquet schema is not implemented");
  }

  ::parquet::arrow::SchemaManifest schema_manifest;
  auto schema_manifest_result = ::parquet::arrow::SchemaManifest::Make(
      metadata->schema(), metadata->key_value_metadata(), reader->properties(),
      &schema_manifest);
  if (!schema_manifest_result.ok()) {
    return ParquetError("Failed to make schema manifest: {}",
                        schema_manifest_result.message());
  }

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

    // TODO(gangwu): make memory pool configurable
    ::arrow::MemoryPool* pool = ::arrow::default_memory_pool();

    // Prepare reader properties
    ::parquet::ReaderProperties reader_properties(pool);
    ::parquet::ArrowReaderProperties arrow_reader_properties;
    arrow_reader_properties.set_batch_size(options.batch_size);
    arrow_reader_properties.set_arrow_extensions_enabled(true);

    // Open the Parquet file reader
    ICEBERG_ASSIGN_OR_RAISE(auto input_stream, OpenInputStream(options));
    auto file_reader =
        ::parquet::ParquetFileReader::Open(std::move(input_stream), reader_properties);
    auto make_reader_result = ::parquet::arrow::FileReader::Make(
        pool, std::move(file_reader), arrow_reader_properties, &reader_);
    if (!make_reader_result.ok()) {
      return ParquetError("Failed to make file reader: {}", make_reader_result.message());
    }

    // Project read schema onto the Parquet file schema
    ICEBERG_ASSIGN_OR_RAISE(projection_, BuildProjection(reader_.get(), *read_schema_));

    return {};
  }

  // Read the next batch of data
  Result<std::optional<ArrowArray>> Next() {
    if (!context_) {
      ICEBERG_RETURN_UNEXPECTED(InitReadContext());
    }

    auto next_result = context_->record_batch_reader_->Next();
    if (!next_result.ok()) {
      return ParquetError("Failed to read next batch: {}",
                          next_result.status().message());
    }

    auto batch = next_result.MoveValueUnsafe();
    if (!batch) {
      return std::nullopt;
    }

    ICEBERG_ASSIGN_OR_RAISE(
        batch, ConvertRecordBatch(std::move(batch), context_->output_arrow_schema_,
                                  *read_schema_, projection_));

    ArrowArray arrow_array;
    auto export_result = ::arrow::ExportRecordBatch(*batch, &arrow_array);
    if (!export_result.ok()) {
      return ParquetError("Failed to export the Arrow record batch: {}",
                          export_result.message());
    }
    return arrow_array;
  }

  // Close the reader and release resources
  Status Close() {
    if (reader_ == nullptr) {
      return {};  // Already closed
    }

    if (context_ != nullptr) {
      auto close_result = context_->record_batch_reader_->Close();
      if (!close_result.ok()) {
        return ParquetError("Failed to close record batch reader: {}",
                            close_result.message());
      }
      context_.reset();
    }

    reader_.reset();
    return {};
  }

  // Get the schema of the data
  Result<ArrowSchema> Schema() {
    if (!context_) {
      ICEBERG_RETURN_UNEXPECTED(InitReadContext());
    }

    ArrowSchema arrow_schema;
    auto export_result =
        ::arrow::ExportSchema(*context_->output_arrow_schema_, &arrow_schema);
    if (!export_result.ok()) {
      return ParquetError("Failed to export Arrow schema: {}", export_result.message());
    }
    return arrow_schema;
  }

 private:
  Status InitReadContext() {
    context_ = std::make_unique<ReadContext>();

    // Build the output Arrow schema
    ArrowSchema arrow_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*read_schema_, &arrow_schema));
    auto import_result = ::arrow::ImportSchema(&arrow_schema);
    if (!import_result.ok()) {
      return ParquetError("Failed to import Arrow schema: {}",
                          import_result.status().message());
    }
    context_->output_arrow_schema_ = import_result.MoveValueUnsafe();

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
      if (row_group_indices.empty()) {
        // None of the row groups are selected, return an empty record batch reader
        context_->record_batch_reader_ = std::make_unique<EmptyRecordBatchReader>();
        return {};
      }
    }

    // Create the record batch reader
    ICEBERG_ASSIGN_OR_RAISE(auto column_indices, SelectedColumnIndices(projection_));
    auto reader_result = reader_->GetRecordBatchReader(row_group_indices, column_indices);
    if (!reader_result.ok()) {
      return ParquetError("Failed to get record batch reader: {}",
                          reader_result.status().message());
    }
    context_->record_batch_reader_ = std::move(reader_result).MoveValueUnsafe();

    return {};
  }

 private:
  // The split to read from the Parquet file.
  std::optional<Split> split_;
  // Schema to read from the Parquet file.
  std::shared_ptr<::iceberg::Schema> read_schema_;
  // The projection result to apply to the read schema.
  SchemaProjection projection_;
  // Parquet file reader to create RecordBatchReader.
  std::unique_ptr<::parquet::arrow::FileReader> reader_;
  // The context to keep track of the reading progress.
  std::unique_ptr<ReadContext> context_;
};

ParquetReader::~ParquetReader() = default;

Result<std::optional<ArrowArray>> ParquetReader::Next() { return impl_->Next(); }

Result<ArrowSchema> ParquetReader::Schema() { return impl_->Schema(); }

Status ParquetReader::Open(const ReaderOptions& options) {
  impl_ = std::make_unique<Impl>();
  return impl_->Open(options);
}

Status ParquetReader::Close() { return impl_->Close(); }

void ParquetReader::Register() {
  static ReaderFactoryRegistry parquet_reader_register(
      FileFormatType::kParquet, []() -> Result<std::unique_ptr<Reader>> {
        return std::make_unique<ParquetReader>();
      });
}

}  // namespace iceberg::parquet
