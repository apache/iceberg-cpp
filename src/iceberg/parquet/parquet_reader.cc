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

#include <algorithm>
#include <numeric>
#include <variant>
#include <vector>

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
  return arrow::OpenArrowInputStream(options.io, options.path, options.length);
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
    *batch = nullptr;
    return ::arrow::Status::OK();
  }
};

// forward declaration to unblock cycle dependence.
std::shared_ptr<::arrow::Field> UseLargeListField(
    const std::shared_ptr<::arrow::Field>& field);

// Rebuild a data type with all nested list types replaced by large_list.
std::shared_ptr<::arrow::DataType> UseLargeListType(
    const std::shared_ptr<::arrow::DataType>& type) {
  switch (type->id()) {
    case ::arrow::Type::LIST: {
      const auto& list_type = internal::checked_cast<const ::arrow::ListType&>(*type);
      return ::arrow::large_list(UseLargeListField(list_type.value_field()));
    }
    case ::arrow::Type::STRUCT: {
      ::arrow::FieldVector fields;
      fields.reserve(type->num_fields());
      for (const auto& field : type->fields()) {
        fields.push_back(UseLargeListField(field));
      }
      return ::arrow::struct_(std::move(fields));
    }
    case ::arrow::Type::MAP: {
      const auto& map_type = internal::checked_cast<const ::arrow::MapType&>(*type);
      return std::make_shared<::arrow::MapType>(UseLargeListField(map_type.key_field()),
                                                UseLargeListField(map_type.item_field()),
                                                map_type.keys_sorted());
    }
    default:
      return type;
  }
}

std::shared_ptr<::arrow::Field> UseLargeListField(
    const std::shared_ptr<::arrow::Field>& field) {
  return field->WithType(UseLargeListType(field->type()));
}

// Rebuild a type so its nested lists match the list type (list vs large_list) of the
// arrays the reader produces, correlating struct fields to the reader by field id via the
// projection rather than by name, so a renamed column is still matched to the array it is
// read from. `projections` are the child projections of the field whose type this is.
std::shared_ptr<::arrow::DataType> AlignTypeToReader(
    const std::shared_ptr<::arrow::DataType>& output_type,
    const std::shared_ptr<::arrow::DataType>& reader_type,
    const std::vector<FieldProjection>& projections, bool use_large_list_default);

// Rewrite the fields of a struct level (including the top level) so their list types
// match the reader's. `projections[i].from` gives the reader field for output field `i`,
// matching how ProjectStructArray reads the arrays. A field not projected from the source
// (null, default, constant or metadata) is filled with an array of the output type, so it
// takes the configured preference instead.
::arrow::FieldVector AlignFieldsToReader(const ::arrow::FieldVector& output_fields,
                                         const ::arrow::FieldVector& reader_fields,
                                         const std::vector<FieldProjection>& projections,
                                         bool use_large_list_default) {
  ::arrow::FieldVector aligned;
  aligned.reserve(output_fields.size());

  for (size_t i = 0; i < output_fields.size(); ++i) {
    const auto& output_field = output_fields[i];
    // Defensive: the projection carries one entry per output field. If it does not line
    // up, leave the field untouched rather than risk an out-of-bounds access.
    if (i >= projections.size()) {
      aligned.push_back(output_field);
      continue;
    }

    const auto& projection = projections[i];
    if (projection.kind == FieldProjection::Kind::kProjected) {
      auto reader_index = std::get<size_t>(projection.from);
      if (reader_index >= reader_fields.size()) {
        aligned.push_back(output_field);
        continue;
      }
      aligned.push_back(output_field->WithType(
          AlignTypeToReader(output_field->type(), reader_fields[reader_index]->type(),
                            projection.children, use_large_list_default)));
    } else {
      aligned.push_back(use_large_list_default ? UseLargeListField(output_field)
                                               : output_field);
    }
  }

  return aligned;
}

std::shared_ptr<::arrow::DataType> AlignTypeToReader(
    const std::shared_ptr<::arrow::DataType>& output_type,
    const std::shared_ptr<::arrow::DataType>& reader_type,
    const std::vector<FieldProjection>& projections, bool use_large_list_default) {
  switch (output_type->id()) {
    case ::arrow::Type::STRUCT: {
      if (reader_type->id() != ::arrow::Type::STRUCT) {
        return output_type;
      }
      const auto& output_struct =
          internal::checked_cast<const ::arrow::StructType&>(*output_type);
      const auto& reader_struct =
          internal::checked_cast<const ::arrow::StructType&>(*reader_type);
      return ::arrow::struct_(AlignFieldsToReader(output_struct.fields(),
                                                  reader_struct.fields(), projections,
                                                  use_large_list_default));
    }
    case ::arrow::Type::LIST: {
      // A list carries exactly one child projection, its element, matched positionally.
      if (projections.size() != 1) {
        return output_type;
      }
      const auto& output_list =
          internal::checked_cast<const ::arrow::ListType&>(*output_type);
      const auto& element = projections.front().children;
      if (reader_type->id() == ::arrow::Type::LARGE_LIST) {
        const auto& reader_list =
            internal::checked_cast<const ::arrow::LargeListType&>(*reader_type);
        return ::arrow::large_list(output_list.value_field()->WithType(AlignTypeToReader(
            output_list.value_field()->type(), reader_list.value_field()->type(), element,
            use_large_list_default)));
      }
      if (reader_type->id() == ::arrow::Type::LIST) {
        const auto& reader_list =
            internal::checked_cast<const ::arrow::ListType&>(*reader_type);
        return ::arrow::list(output_list.value_field()->WithType(AlignTypeToReader(
            output_list.value_field()->type(), reader_list.value_field()->type(), element,
            use_large_list_default)));
      }
      return output_type;
    }
    case ::arrow::Type::MAP: {
      // A map carries two child projections, its key and its value, matched positionally.
      if (reader_type->id() != ::arrow::Type::MAP || projections.size() != 2) {
        return output_type;
      }
      const auto& output_map =
          internal::checked_cast<const ::arrow::MapType&>(*output_type);
      const auto& reader_map =
          internal::checked_cast<const ::arrow::MapType&>(*reader_type);
      return std::make_shared<::arrow::MapType>(
          output_map.key_field()->WithType(AlignTypeToReader(
              output_map.key_field()->type(), reader_map.key_field()->type(),
              projections[0].children, use_large_list_default)),
          output_map.item_field()->WithType(AlignTypeToReader(
              output_map.item_field()->type(), reader_map.item_field()->type(),
              projections[1].children, use_large_list_default)),
          output_map.keys_sorted());
    }
    default:
      return output_type;
  }
}

// Align the output schema to the arrays the reader actually produces. Arrow honors the
// requested large_list type only when it derives the schema from the Parquet schema; a
// file that carries serialized ARROW:schema metadata keeps its stored list types, so the
// reader may produce list, large_list, or a mix of the two. The output schema, built from
// the Iceberg projection and always using plain list, is rewritten per field to match the
// reader so that ProjectRecordBatch casts each array to the type it actually is. Fields
// are correlated to the reader through the projection (by field id), never by name.
std::shared_ptr<::arrow::Schema> AlignOutputSchemaToReaderSchema(
    const std::shared_ptr<::arrow::Schema>& output_schema,
    const std::shared_ptr<::arrow::Schema>& reader_schema,
    const SchemaProjection& projection, bool use_large_list_default) {
  if (reader_schema == nullptr || output_schema == nullptr) {
    return output_schema;
  }

  return ::arrow::schema(
      AlignFieldsToReader(output_schema->fields(), reader_schema->fields(),
                          projection.fields, use_large_list_default),
      output_schema->metadata());
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
    arrow_reader_properties.set_batch_size(
        options.properties.Get(ReaderProperties::kBatchSize));
    arrow_reader_properties.set_arrow_extensions_enabled(true);
    use_large_list_ = options.properties.Get(ReaderProperties::kArrowUseLargeList);
    if (use_large_list_) {
      arrow_reader_properties.set_list_type(::arrow::Type::LARGE_LIST);
    }

    // Open the Parquet file reader
    ICEBERG_ASSIGN_OR_RAISE(input_stream_, OpenInputStream(options));
    auto file_reader =
        ::parquet::ParquetFileReader::Open(input_stream_, reader_properties);
    ICEBERG_ARROW_ASSIGN_OR_RETURN(
        reader_, ::parquet::arrow::FileReader::Make(pool_, std::move(file_reader),
                                                    arrow_reader_properties));

    // Project read schema onto the Parquet file schema
    ICEBERG_ASSIGN_OR_RAISE(projection_, BuildProjection(reader_.get(), *read_schema_));
    metadata_context_ = {.file_path = options.path,
                         .next_file_pos = 0,
                         .first_row_id = options.first_row_id,
                         .data_sequence_number = options.data_sequence_number};

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
                                  *read_schema_, projection_, metadata_context_, pool_));

    metadata_context_.next_file_pos += batch->num_rows();

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
        } else {
          metadata_context_.next_file_pos += metadata->RowGroup(i)->num_rows();
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

    // Build the output Arrow schema from the projected Iceberg schema. This schema is the
    // target of ProjectRecordBatch, so it must describe the projected schema rather than
    // the schema of the file.
    ArrowSchema arrow_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*read_schema_, &arrow_schema));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(context_->output_arrow_schema_,
                                   ::arrow::ImportSchema(&arrow_schema));

    // Align the output schema with the arrays the reader actually produces. The reader's
    // schema determines the actual list types (list vs large_list) for each field, which
    // may differ from the desired output due to:
    //   1. The reader's requested list type (set via set_list_type)
    //   2. ARROW:schema metadata in the file that overrides the Parquet schema type
    //   3. Mixed list and large_list types in files with stored schemas
    // For each projected field, we use the reader's actual type. For missing fields
    // (columns not in the file), we apply the configured use_large_list preference.
    context_->output_arrow_schema_ = AlignOutputSchemaToReaderSchema(
        context_->output_arrow_schema_, context_->record_batch_reader_->schema(),
        projection_, use_large_list_);

    return {};
  }

 private:
  // TODO(gangwu): make memory pool configurable
  ::arrow::MemoryPool* pool_ = ::arrow::default_memory_pool();
  // The split to read from the Parquet file.
  std::optional<Split> split_;
  // Whether to read list columns as large_list (64-bit offsets).
  bool use_large_list_ = false;
  // Schema to read from the Parquet file.
  std::shared_ptr<::iceberg::Schema> read_schema_;
  // The projection result to apply to the read schema.
  SchemaProjection projection_;
  // The input stream to read Parquet file.
  std::shared_ptr<::arrow::io::RandomAccessFile> input_stream_;
  // Parquet file reader to create RecordBatchReader.
  std::unique_ptr<::parquet::arrow::FileReader> reader_;
  // Metadata column context for populating _file and _pos columns.
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
