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

#include <limits>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
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

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/metadata_column_util_internal.h"
#include "iceberg/expression/binder.h"
#include "iceberg/parquet/parquet_data_util_internal.h"
#include "iceberg/parquet/parquet_dictionary_row_group_filter_internal.h"
#include "iceberg/parquet/parquet_metrics_row_group_filter_internal.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/parquet/parquet_schema_util_internal.h"
#include "iceberg/parquet/row_number_reader_internal.h"
#include "iceberg/result.h"
#include "iceberg/schema_internal.h"
#include "iceberg/schema_util.h"
#include "iceberg/util/macros.h"

namespace iceberg::parquet {

namespace {

Result<std::shared_ptr<::arrow::io::RandomAccessFile>> OpenInputStream(
    const ReaderOptions& options) {
  return arrow::OpenArrowInputStream(options.io, options.path, options.length);
}

Result<SchemaProjection> BuildProjection(::parquet::arrow::FileReader* reader,
                                         const Schema& read_schema,
                                         const NameMapping* name_mapping) {
  auto metadata = reader->parquet_reader()->metadata();

  if (!HasFieldIds(metadata->schema()->schema_root())) {
    if (name_mapping == nullptr) {
      return InvalidSchema(
          "Parquet file schema has no field IDs and no name mapping provided");
    }
    ::parquet::arrow::SchemaManifest schema_manifest;
    ICEBERG_ARROW_RETURN_NOT_OK(::parquet::arrow::SchemaManifest::Make(
        metadata->schema(), metadata->key_value_metadata(), reader->properties(),
        &schema_manifest));
    ApplyNameMapping(*name_mapping, &schema_manifest);
    return Project(read_schema, schema_manifest);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto projection, Project(read_schema, reader->manifest()));
  return projection;
}

// Stored ARROW:schema metadata can override ArrowReaderProperties::set_list_type().
// Reject a conflicting output layout before any arrays are interpreted or emitted.
Status ValidateListTypes(const ::arrow::Field& field, bool use_large_list,
                         const std::string& path) {
  const auto& type = *field.type();
  const auto expected = use_large_list ? ::arrow::Type::LARGE_LIST : ::arrow::Type::LIST;
  if ((type.id() == ::arrow::Type::LIST || type.id() == ::arrow::Type::LARGE_LIST) &&
      type.id() != expected) {
    return NotSupported(
        "Parquet reader cannot honor read.arrow.use-large-list={} for field '{}': "
        "expected {}, but the stored Arrow schema produces {}. "
        "Converting list offset widths is not supported",
        use_large_list, path, use_large_list ? "large_list" : "list", type.name());
  }
  for (const auto& child : type.fields()) {
    ICEBERG_RETURN_UNEXPECTED(
        ValidateListTypes(*child, use_large_list, path + "." + child->name()));
  }
  return {};
}

}  // namespace

// A stateful context to keep track of the reading progress.
struct ReadContext {
  // The arrow schema to output record batches. It may be different with
  // the schema of record batches returned by `record_batch_reader_`
  // when there is any schema evolution.
  std::shared_ptr<::arrow::Schema> output_arrow_schema_;
  // The reader to read record batches from the Parquet file.
  std::unique_ptr<::arrow::RecordBatchReader> record_batch_reader_;
  // Produces file row positions in lockstep with `record_batch_reader_`. Only set when
  // the projection needs row positions (_pos or inherited _row_id).
  // TODO(advancedxy): use Arrow C++'s row number virtual column once available.
  std::unique_ptr<RowNumberReader> row_number_reader_;
};

// TODO(gangwu): list of work items
// 1. Make the memory pool configurable
// 2. Catch ParquetException and convert to Status/Result
// 3. Add utility to convert Arrow Status/Result to Iceberg Status/Result
class ParquetReader::Impl {
 public:
  // Open the Parquet reader with the given options
  Status Open(const ReaderOptions& options) {
    if (options.projection == nullptr) {
      return InvalidArgument("Projected schema is required by Parquet reader");
    }

    read_schema_ = options.projection;
    stats_filter_.reset();
    dictionary_filter_.reset();

    // Prepare reader properties
    ::parquet::ReaderProperties reader_properties(pool_);
    ::parquet::ArrowReaderProperties arrow_reader_properties;
    arrow_reader_properties.set_batch_size(
        options.properties.Get(ReaderProperties::kBatchSize));
    arrow_reader_properties.set_arrow_extensions_enabled(true);
    use_large_list_ = options.properties.Get(ReaderProperties::kArrowUseLargeList);
    arrow_reader_properties.set_list_type(use_large_list_ ? ::arrow::Type::LARGE_LIST
                                                          : ::arrow::Type::LIST);

    // Open the Parquet file reader
    ICEBERG_ASSIGN_OR_RAISE(input_stream_, OpenInputStream(options));
    auto file_reader =
        ::parquet::ParquetFileReader::Open(input_stream_, reader_properties);
    ICEBERG_ARROW_ASSIGN_OR_RETURN(
        reader_, ::parquet::arrow::FileReader::Make(pool_, std::move(file_reader),
                                                    arrow_reader_properties));

    // Project read schema onto the Parquet file schema
    ICEBERG_ASSIGN_OR_RAISE(projection_, BuildProjection(reader_.get(), *read_schema_,
                                                         options.name_mapping.get()));
    if (options.filter &&
        options.properties.Get(ReaderProperties::kParquetRowGroupFilter)) {
      auto filter = options.filter;
      ICEBERG_ASSIGN_OR_RAISE(auto is_bound, IsBoundVisitor::IsBound(filter));
      if (!is_bound) {
        ICEBERG_ASSIGN_OR_RAISE(
            filter, Binder::Bind(*options.projection, filter, options.case_sensitive));
      }
      ICEBERG_ASSIGN_OR_RAISE(stats_filter_, ParquetMetricsRowGroupFilter::Make(
                                                 filter, *reader_->manifest().descr));
      ICEBERG_ASSIGN_OR_RAISE(
          dictionary_filter_,
          ParquetDictionaryRowGroupFilter::Make(filter, *reader_->manifest().descr));
    }

    ICEBERG_RETURN_UNEXPECTED(PlanRowGroups(options, reader_properties));

    metadata_context_ = {.file_path = options.path,
                         .first_row_id = options.first_row_id,
                         .data_sequence_number = options.data_sequence_number};

    return InitReadContext();
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

    std::shared_ptr<::arrow::Int64Array> row_positions;
    if (context_->row_number_reader_) {
      ICEBERG_ASSIGN_OR_RAISE(row_positions,
                              context_->row_number_reader_->NextBatch(batch->num_rows()));
      if (row_positions->length() != batch->num_rows()) {
        return Invalid("Parquet batch has {} rows, but row number reader returned {}",
                       batch->num_rows(), row_positions->length());
      }
    }

    ICEBERG_ASSIGN_OR_RAISE(
        batch, ProjectRecordBatch(std::move(batch), context_->output_arrow_schema_,
                                  *read_schema_, projection_, metadata_context_,
                                  row_positions, pool_));

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
  Status PlanRowGroups(const ReaderOptions& options,
                       const ::parquet::ReaderProperties& reader_properties) {
    auto metadata = reader_->parquet_reader()->metadata();
    if (options.split.has_value()) {
      const auto& split = *options.split;
      ICEBERG_ARROW_ASSIGN_OR_RETURN(auto file_size, input_stream_->GetSize());
      int64_t row_count = 0;
      for (int i = 0; i < metadata->num_row_groups(); ++i) {
        auto row_group = metadata->RowGroup(i);
        // Empty row groups have no pages to assign to a split.
        if (row_group->num_rows() == 0) {
          continue;
        }
        // Like Java Iceberg, a split reads the row groups whose midpoints it contains.
        auto midpoint = RowGroupMidpoint(*row_group, file_size);
        if (!midpoint.has_value()) {
          return Invalid(
              "Cannot read Parquet split {} (start={}, length={}), row "
              "group {}: {}",
              options.path, split.offset, split.length, i, midpoint.error().message);
        }
        // Subtraction avoids overflow for a caller-supplied unbounded split length.
        if (std::cmp_greater_equal(*midpoint, split.offset) &&
            static_cast<uint64_t>(*midpoint) - split.offset < split.length) {
          row_group_indices_.push_back(i);
        }
        if (row_group->num_rows() < 0 ||
            row_group->num_rows() > std::numeric_limits<int64_t>::max() - row_count) {
          return Invalid("Invalid Parquet row count in file {}, row group {}",
                         options.path, i);
        }
        row_count += row_group->num_rows();
      }
    } else if (metadata->num_row_groups() > 0) {
      row_group_indices_.resize(metadata->num_row_groups());
      std::iota(row_group_indices_.begin(), row_group_indices_.end(), 0);  // NOLINT
    }

    if (stats_filter_) {
      size_t selected_count = 0;
      for (int row_group_index : row_group_indices_) {
        auto row_group = metadata->RowGroup(row_group_index);
        ICEBERG_ASSIGN_OR_RAISE(
            auto should_read, stats_filter_->ShouldRead(reader_->manifest(), *row_group));
        if (should_read && dictionary_filter_) {
          ICEBERG_ASSIGN_OR_RAISE(
              should_read, dictionary_filter_->ShouldRead(reader_->manifest(), *metadata,
                                                          row_group_index, input_stream_,
                                                          reader_properties));
        }
        if (should_read) {
          row_group_indices_[selected_count++] = row_group_index;
        }
      }
      row_group_indices_.resize(selected_count);
    }
    return {};
  }

  Status InitReadContext() {
    // Build locally so initialization can be retried without consuming any rows.
    auto context = std::make_unique<ReadContext>();
    // Create the record batch reader, including for empty splits to preserve the schema.
    ICEBERG_ARROW_ASSIGN_OR_RETURN(
        context->record_batch_reader_,
        reader_->GetRecordBatchReader(row_group_indices_,
                                      SelectedColumnIndices(projection_)));

    if (NeedsRowPositions(*read_schema_, metadata_context_)) {
      auto metadata = reader_->parquet_reader()->metadata();
      // Share the batch reader's selection so positions remain file-relative even
      // when a batch spans nonconsecutive row groups.
      ICEBERG_ASSIGN_OR_RAISE(
          context->row_number_reader_,
          RowNumberReader::Make(*metadata, row_group_indices_, pool_));
    }

    // Build the output Arrow schema from the projected Iceberg schema. This schema is the
    // target of ProjectRecordBatch, so it must describe the projected schema rather than
    // the schema of the file.
    ArrowSchema arrow_schema;
    ICEBERG_RETURN_UNEXPECTED(
        ToArrowSchema(*read_schema_, &arrow_schema, use_large_list_));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(context->output_arrow_schema_,
                                   ::arrow::ImportSchema(&arrow_schema));

    // Validate only the fields selected from the file. Missing fields are built with
    // the configured type by ProjectRecordBatch using output_arrow_schema_.
    for (const auto& field : context->record_batch_reader_->schema()->fields()) {
      ICEBERG_RETURN_UNEXPECTED(
          ValidateListTypes(*field, use_large_list_, field->name()));
    }
    context_ = std::move(context);
    return {};
  }

 private:
  // TODO(gangwu): make memory pool configurable
  ::arrow::MemoryPool* pool_ = ::arrow::default_memory_pool();
  // Row groups in footer order, shared by the batch and row-number readers.
  std::vector<int> row_group_indices_;
  // Whether to read list columns as large_list (64-bit offsets).
  bool use_large_list_ = false;
  // Schema to read from the Parquet file.
  std::shared_ptr<::iceberg::Schema> read_schema_;
  // Footer-statistics filter for row group pruning, if enabled.
  std::unique_ptr<ParquetMetricsRowGroupFilter> stats_filter_;
  std::unique_ptr<ParquetDictionaryRowGroupFilter> dictionary_filter_;
  // The projection result to apply to the read schema.
  SchemaProjection projection_;
  // The input stream to read Parquet file.
  std::shared_ptr<::arrow::io::RandomAccessFile> input_stream_;
  // Parquet file reader to create RecordBatchReader.
  std::unique_ptr<::parquet::arrow::FileReader> reader_;
  // File-level constants for metadata and row lineage columns.
  arrow::MetadataColumnContext metadata_context_;
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
