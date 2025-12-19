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

#include "iceberg/table_scan.h"

#include <cstring>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {
/// \brief Private data structure to hold the Reader and error state
struct ReaderStreamPrivateData {
  std::unique_ptr<Reader> reader;
  std::string last_error;

  explicit ReaderStreamPrivateData(std::unique_ptr<Reader> reader_ptr)
      : reader(std::move(reader_ptr)) {}

  ~ReaderStreamPrivateData() {
    if (reader) {
      std::ignore = reader->Close();
    }
  }
};

/// \brief Callback to get the stream schema
static int GetSchema(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
  if (!stream || !stream->private_data) {
    return EINVAL;
  }
  auto* private_data = static_cast<ReaderStreamPrivateData*>(stream->private_data);
  // Get schema from reader
  auto schema_result = private_data->reader->Schema();
  if (!schema_result.has_value()) {
    private_data->last_error = schema_result.error().message;
    std::memset(out, 0, sizeof(ArrowSchema));
    return EIO;
  }

  *out = std::move(schema_result.value());
  return 0;
}

/// \brief Callback to get the next array from the stream
static int GetNext(struct ArrowArrayStream* stream, struct ArrowArray* out) {
  if (!stream || !stream->private_data) {
    return EINVAL;
  }

  auto* private_data = static_cast<ReaderStreamPrivateData*>(stream->private_data);

  auto next_result = private_data->reader->Next();
  if (!next_result.has_value()) {
    private_data->last_error = next_result.error().message;
    std::memset(out, 0, sizeof(ArrowArray));
    return EIO;
  }

  auto& optional_array = next_result.value();
  if (optional_array.has_value()) {
    *out = std::move(optional_array.value());
  } else {
    // End of stream - set release to nullptr to signal end
    std::memset(out, 0, sizeof(ArrowArray));
    out->release = nullptr;
  }

  return 0;
}

/// \brief Callback to get the last error message
static const char* GetLastError(struct ArrowArrayStream* stream) {
  if (!stream || !stream->private_data) {
    return nullptr;
  }

  auto* private_data = static_cast<ReaderStreamPrivateData*>(stream->private_data);
  return private_data->last_error.empty() ? nullptr : private_data->last_error.c_str();
}

/// \brief Callback to release the stream resources
static void Release(struct ArrowArrayStream* stream) {
  if (!stream || !stream->private_data) {
    return;
  }

  delete static_cast<ReaderStreamPrivateData*>(stream->private_data);
  stream->private_data = nullptr;
  stream->release = nullptr;
}

Result<ArrowArrayStream> MakeArrowArrayStream(std::unique_ptr<Reader> reader) {
  if (!reader) {
    return InvalidArgument("Reader cannot be null");
  }

  auto private_data = std::make_unique<ReaderStreamPrivateData>(std::move(reader));

  ArrowArrayStream stream{.get_schema = GetSchema,
                          .get_next = GetNext,
                          .get_last_error = GetLastError,
                          .release = Release,
                          .private_data = private_data.release()};

  return stream;
}

}  // namespace

// implement FileScanTask
FileScanTask::FileScanTask(std::shared_ptr<DataFile> data_file)
    : data_file_(std::move(data_file)) {}

const std::shared_ptr<DataFile>& FileScanTask::data_file() const { return data_file_; }

int64_t FileScanTask::size_bytes() const { return data_file_->file_size_in_bytes; }

int32_t FileScanTask::files_count() const { return 1; }

int64_t FileScanTask::estimated_row_count() const { return data_file_->record_count; }

Result<ArrowArrayStream> FileScanTask::ToArrow(
    const std::shared_ptr<FileIO>& io, const std::shared_ptr<Schema>& projected_schema,
    const std::shared_ptr<Expression>& filter) const {
  const ReaderOptions options{.path = data_file_->file_path,
                              .length = data_file_->file_size_in_bytes,
                              .io = io,
                              .projection = projected_schema,
                              .filter = filter};

  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ReaderFactoryRegistry::Open(data_file_->file_format, options));

  return MakeArrowArrayStream(std::move(reader));
}

TableScanBuilder::TableScanBuilder(std::shared_ptr<TableMetadata> table_metadata,
                                   std::shared_ptr<FileIO> file_io)
    : file_io_(std::move(file_io)) {
  context_.table_metadata = std::move(table_metadata);
}

TableScanBuilder& TableScanBuilder::WithColumnNames(
    std::vector<std::string> column_names) {
  column_names_ = std::move(column_names);
  return *this;
}

TableScanBuilder& TableScanBuilder::WithProjectedSchema(std::shared_ptr<Schema> schema) {
  context_.projected_schema = std::move(schema);
  return *this;
}

TableScanBuilder& TableScanBuilder::WithSnapshotId(int64_t snapshot_id) {
  snapshot_id_ = snapshot_id;
  return *this;
}

TableScanBuilder& TableScanBuilder::WithFilter(std::shared_ptr<Expression> filter) {
  context_.filter = std::move(filter);
  return *this;
}

TableScanBuilder& TableScanBuilder::WithCaseSensitive(bool case_sensitive) {
  context_.case_sensitive = case_sensitive;
  return *this;
}

TableScanBuilder& TableScanBuilder::WithOption(std::string property, std::string value) {
  context_.options[std::move(property)] = std::move(value);
  return *this;
}

TableScanBuilder& TableScanBuilder::WithLimit(std::optional<int64_t> limit) {
  context_.limit = limit;
  return *this;
}

Result<std::unique_ptr<TableScan>> TableScanBuilder::Build() {
  const auto& table_metadata = context_.table_metadata;
  auto snapshot_id = snapshot_id_ ? snapshot_id_ : table_metadata->current_snapshot_id;
  if (!snapshot_id) {
    return InvalidArgument("No snapshot ID specified for table {}",
                           table_metadata->table_uuid);
  }
  ICEBERG_ASSIGN_OR_RAISE(context_.snapshot, table_metadata->SnapshotById(*snapshot_id));

  if (!context_.projected_schema) {
    const auto& snapshot = context_.snapshot;
    auto schema_id =
        snapshot->schema_id ? snapshot->schema_id : table_metadata->current_schema_id;
    ICEBERG_ASSIGN_OR_RAISE(auto schema, table_metadata->SchemaById(schema_id));

    if (column_names_.empty()) {
      context_.projected_schema = schema;
    } else {
      // TODO(gty404): collect touched columns from filter expression
      std::vector<SchemaField> projected_fields;
      projected_fields.reserve(column_names_.size());
      for (const auto& column_name : column_names_) {
        // TODO(gty404): support case-insensitive column names
        auto field_opt = schema->GetFieldByName(column_name);
        if (!field_opt) {
          return InvalidArgument("Column {} not found in schema '{}'", column_name,
                                 *schema_id);
        }
        projected_fields.emplace_back(field_opt.value()->get());
      }
      context_.projected_schema =
          std::make_shared<Schema>(std::move(projected_fields), schema->schema_id());
    }
  } else if (!column_names_.empty()) {
    return InvalidArgument(
        "Cannot specify column names when a projected schema is provided");
  }

  return std::make_unique<DataTableScan>(std::move(context_), file_io_);
}

TableScan::TableScan(TableScanContext context, std::shared_ptr<FileIO> file_io)
    : context_(std::move(context)), file_io_(std::move(file_io)) {}

const std::shared_ptr<Snapshot>& TableScan::snapshot() const { return context_.snapshot; }

const std::shared_ptr<Schema>& TableScan::projection() const {
  return context_.projected_schema;
}

const TableScanContext& TableScan::context() const { return context_; }

const std::shared_ptr<FileIO>& TableScan::io() const { return file_io_; }

DataTableScan::DataTableScan(TableScanContext context, std::shared_ptr<FileIO> file_io)
    : TableScan(std::move(context), std::move(file_io)) {}

Result<std::vector<std::shared_ptr<FileScanTask>>> DataTableScan::PlanFiles() const {
  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_list_reader,
      ManifestListReader::Make(context_.snapshot->manifest_list, file_io_));
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_files, manifest_list_reader->Files());

  std::vector<std::shared_ptr<FileScanTask>> tasks;
  ICEBERG_ASSIGN_OR_RAISE(auto partition_spec, context_.table_metadata->PartitionSpec());

  // Get the table schema and partition type
  ICEBERG_ASSIGN_OR_RAISE(auto current_schema, context_.table_metadata->Schema());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<StructType> partition_type,
                          partition_spec->PartitionType(*current_schema));

  for (const auto& manifest_file : manifest_files) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto manifest_reader,
        ManifestReader::Make(manifest_file, file_io_, partition_type));
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, manifest_reader->Entries());

    // TODO(gty404): filter manifests using partition spec and filter expression

    for (auto& manifest_entry : manifests) {
      const auto& data_file = manifest_entry.data_file;
      switch (data_file->content) {
        case DataFile::Content::kData:
          tasks.emplace_back(std::make_shared<FileScanTask>(manifest_entry.data_file));
          break;
        case DataFile::Content::kPositionDeletes:
        case DataFile::Content::kEqualityDeletes:
          return NotSupported("Equality/Position deletes are not supported in data scan");
      }
    }
  }

  return tasks;
}

}  // namespace iceberg
