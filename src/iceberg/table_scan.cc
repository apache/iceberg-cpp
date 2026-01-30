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
#include <iterator>

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expression.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_group.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"
#include "iceberg/util/timepoint.h"
#include "iceberg/util/type_util.h"

namespace iceberg {

namespace {

const std::vector<std::string> kScanColumns = {
    "snapshot_id",         "file_path",          "file_ordinal",  "file_format",
    "block_size_in_bytes", "file_size_in_bytes", "record_count",  "partition",
    "key_metadata",        "split_offsets",      "sort_order_id",
};

const std::vector<std::string> kStatsColumns = {
    "value_counts", "null_value_counts", "nan_value_counts",
    "lower_bounds", "upper_bounds",      "column_sizes",
};

const std::vector<std::string> kScanColumnsWithStats = [] {
  auto cols = kScanColumns;
  cols.insert(cols.end(), kStatsColumns.begin(), kStatsColumns.end());
  return cols;
}();

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

namespace internal {

Status TableScanContext::Validate() const {
  ICEBERG_CHECK(columns_to_keep_stats.empty() || return_column_stats,
                "Cannot select columns to keep stats when column stats are not returned");
  ICEBERG_CHECK(projected_schema == nullptr || selected_columns.empty(),
                "Cannot set projection schema and selected columns at the same time");
  ICEBERG_CHECK(!snapshot_id.has_value() ||
                    (!from_snapshot_id.has_value() && !to_snapshot_id.has_value()),
                "Cannot mix snapshot scan and incremental scan");
  ICEBERG_CHECK(!min_rows_requested.has_value() || min_rows_requested.value() >= 0,
                "Min rows requested cannot be negative");
  return {};
}

}  // namespace internal

ScanTask::~ScanTask() = default;

// FileScanTask implementation

FileScanTask::FileScanTask(std::shared_ptr<DataFile> data_file,
                           std::vector<std::shared_ptr<DataFile>> delete_files,
                           std::shared_ptr<Expression> residual_filter)
    : data_file_(std::move(data_file)),
      delete_files_(std::move(delete_files)),
      residual_filter_(std::move(residual_filter)) {
  ICEBERG_DCHECK(data_file_ != nullptr, "Data file cannot be null for FileScanTask");
}

int64_t FileScanTask::size_bytes() const { return data_file_->file_size_in_bytes; }

int32_t FileScanTask::files_count() const { return 1; }

int64_t FileScanTask::estimated_row_count() const { return data_file_->record_count; }

Result<ArrowArrayStream> FileScanTask::ToArrow(
    const std::shared_ptr<FileIO>& io, std::shared_ptr<Schema> projected_schema) const {
  if (!delete_files_.empty()) {
    return NotSupported("Reading data files with delete files is not yet supported.");
  }

  const ReaderOptions options{.path = data_file_->file_path,
                              .length = data_file_->file_size_in_bytes,
                              .io = io,
                              .projection = std::move(projected_schema),
                              .filter = residual_filter_};

  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ReaderFactoryRegistry::Open(data_file_->file_format, options));

  return MakeArrowArrayStream(std::move(reader));
}

Result<std::unique_ptr<TableScanBuilder>> TableScanBuilder::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<FileIO> io) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<TableScanBuilder>(
      new TableScanBuilder(std::move(metadata), std::move(io)));
}

TableScanBuilder::TableScanBuilder(std::shared_ptr<TableMetadata> table_metadata,
                                   std::shared_ptr<FileIO> file_io)
    : metadata_(std::move(table_metadata)), io_(std::move(file_io)) {}

TableScanBuilder& TableScanBuilder::Option(std::string key, std::string value) {
  context_.options[std::move(key)] = std::move(value);
  return *this;
}

TableScanBuilder& TableScanBuilder::Project(std::shared_ptr<Schema> schema) {
  context_.projected_schema = std::move(schema);
  return *this;
}

TableScanBuilder& TableScanBuilder::CaseSensitive(bool case_sensitive) {
  context_.case_sensitive = case_sensitive;
  return *this;
}

TableScanBuilder& TableScanBuilder::IncludeColumnStats() {
  context_.return_column_stats = true;
  return *this;
}

TableScanBuilder& TableScanBuilder::IncludeColumnStats(
    const std::vector<std::string>& requested_columns) {
  context_.return_column_stats = true;
  context_.columns_to_keep_stats.clear();
  context_.columns_to_keep_stats.reserve(requested_columns.size());

  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto schema_ref, ResolveSnapshotSchema());
  const auto& schema = schema_ref.get();
  for (const auto& column_name : requested_columns) {
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto field, schema->FindFieldByName(column_name));
    if (field.has_value()) {
      context_.columns_to_keep_stats.insert(field.value().get().field_id());
    }
  }

  return *this;
}

TableScanBuilder& TableScanBuilder::Select(const std::vector<std::string>& column_names) {
  context_.selected_columns = column_names;
  return *this;
}

TableScanBuilder& TableScanBuilder::Filter(std::shared_ptr<Expression> filter) {
  context_.filter = std::move(filter);
  return *this;
}

TableScanBuilder& TableScanBuilder::IgnoreResiduals() {
  context_.ignore_residuals = true;
  return *this;
}

TableScanBuilder& TableScanBuilder::MinRowsRequested(int64_t num_rows) {
  context_.min_rows_requested = num_rows;
  return *this;
}

TableScanBuilder& TableScanBuilder::UseSnapshot(int64_t snapshot_id) {
  ICEBERG_BUILDER_CHECK(!context_.snapshot_id.has_value(),
                        "Cannot override snapshot, already set snapshot id={}",
                        context_.snapshot_id.value());
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(std::ignore, metadata_->SnapshotById(snapshot_id));
  context_.snapshot_id = snapshot_id;
  return *this;
}

TableScanBuilder& TableScanBuilder::UseRef(const std::string& ref) {
  if (ref == SnapshotRef::kMainBranch) {
    snapshot_schema_ = nullptr;
    context_.snapshot_id.reset();
    return *this;
  }

  ICEBERG_BUILDER_CHECK(!context_.snapshot_id.has_value(),
                        "Cannot override ref, already set snapshot id={}",
                        context_.snapshot_id.value());
  auto iter = metadata_->refs.find(ref);
  ICEBERG_BUILDER_CHECK(iter != metadata_->refs.end(), "Cannot find ref {}", ref);
  ICEBERG_BUILDER_CHECK(iter->second != nullptr, "Ref {} is null", ref);
  int32_t snapshot_id = iter->second->snapshot_id;
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(std::ignore, metadata_->SnapshotById(snapshot_id));
  context_.snapshot_id = snapshot_id;

  return *this;
}

TableScanBuilder& TableScanBuilder::AsOfTime(int64_t timestamp_millis) {
  auto time_point_ms = TimePointMsFromUnixMs(timestamp_millis);
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(
      auto snapshot_id, SnapshotUtil::SnapshotIdAsOfTime(*metadata_, time_point_ms));
  return UseSnapshot(snapshot_id);
}

TableScanBuilder& TableScanBuilder::FromSnapshot(
    [[maybe_unused]] int64_t from_snapshot_id, [[maybe_unused]] bool inclusive) {
  return AddError(NotImplemented("Incremental scan is not implemented"));
}

TableScanBuilder& TableScanBuilder::FromSnapshot([[maybe_unused]] const std::string& ref,
                                                 [[maybe_unused]] bool inclusive) {
  return AddError(NotImplemented("Incremental scan is not implemented"));
}

TableScanBuilder& TableScanBuilder::ToSnapshot([[maybe_unused]] int64_t to_snapshot_id) {
  return AddError(NotImplemented("Incremental scan is not implemented"));
}

TableScanBuilder& TableScanBuilder::ToSnapshot([[maybe_unused]] const std::string& ref) {
  return AddError(NotImplemented("Incremental scan is not implemented"));
}

TableScanBuilder& TableScanBuilder::UseBranch(const std::string& branch) {
  context_.branch = branch;
  return *this;
}

Result<std::reference_wrapper<const std::shared_ptr<Schema>>>
TableScanBuilder::ResolveSnapshotSchema() {
  if (snapshot_schema_ == nullptr) {
    if (context_.snapshot_id.has_value()) {
      ICEBERG_ASSIGN_OR_RAISE(auto snapshot,
                              metadata_->SnapshotById(*context_.snapshot_id));
      int32_t schema_id = snapshot->schema_id.value_or(Schema::kInitialSchemaId);
      ICEBERG_ASSIGN_OR_RAISE(snapshot_schema_, metadata_->SchemaById(schema_id));
    } else {
      ICEBERG_ASSIGN_OR_RAISE(snapshot_schema_, metadata_->Schema());
    }
  }
  ICEBERG_CHECK(snapshot_schema_ != nullptr, "Snapshot schema is null");
  return snapshot_schema_;
}

bool TableScanBuilder::IsIncrementalScan() const {
  return context_.from_snapshot_id.has_value() || context_.to_snapshot_id.has_value();
}

Result<std::unique_ptr<TableScan>> TableScanBuilder::Build() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_RETURN_UNEXPECTED(context_.Validate());

  if (IsIncrementalScan()) {
    return NotImplemented("Incremental scan is not yet implemented");
  }

  ICEBERG_ASSIGN_OR_RAISE(auto schema, ResolveSnapshotSchema());
  return DataTableScan::Make(metadata_, schema.get(), io_, std::move(context_));
}

TableScan::TableScan(std::shared_ptr<TableMetadata> metadata,
                     std::shared_ptr<Schema> schema, std::shared_ptr<FileIO> file_io,
                     internal::TableScanContext context)
    : metadata_(std::move(metadata)),
      schema_(std::move(schema)),
      io_(std::move(file_io)),
      context_(std::move(context)) {}

TableScan::~TableScan() = default;

const std::shared_ptr<TableMetadata>& TableScan::metadata() const { return metadata_; }

Result<std::shared_ptr<Snapshot>> TableScan::snapshot() const {
  auto snapshot_id = context_.snapshot_id ? context_.snapshot_id.value()
                                          : metadata_->current_snapshot_id;
  if (snapshot_id == kInvalidSnapshotId) {
    return std::shared_ptr<Snapshot>{nullptr};
  }
  return metadata_->SnapshotById(snapshot_id);
}

Result<std::shared_ptr<Schema>> TableScan::schema() const {
  return ResolveProjectedSchema();
}

const internal::TableScanContext& TableScan::context() const { return context_; }

const std::shared_ptr<FileIO>& TableScan::io() const { return io_; }

const std::shared_ptr<Expression>& TableScan::filter() const {
  const static std::shared_ptr<Expression> true_expr = True::Instance();
  if (!context_.filter) {
    return true_expr;
  }
  return context_.filter;
}

bool TableScan::is_case_sensitive() const { return context_.case_sensitive; }

Result<std::reference_wrapper<const std::shared_ptr<Schema>>>
TableScan::ResolveProjectedSchema() const {
  if (projected_schema_ != nullptr) {
    return projected_schema_;
  }

  if (!context_.selected_columns.empty()) {
    std::unordered_set<int32_t> required_field_ids;

    // Include columns referenced by filter
    if (context_.filter != nullptr) {
      ICEBERG_ASSIGN_OR_RAISE(auto is_bound, IsBoundVisitor::IsBound(context_.filter));
      if (is_bound) {
        ICEBERG_ASSIGN_OR_RAISE(required_field_ids,
                                ReferenceVisitor::GetReferencedFieldIds(context_.filter));
      } else {
        ICEBERG_ASSIGN_OR_RAISE(auto filter, Binder::Bind(*schema_, context_.filter,
                                                          context_.case_sensitive));
        ICEBERG_ASSIGN_OR_RAISE(required_field_ids,
                                ReferenceVisitor::GetReferencedFieldIds(filter));
      }
    }

    // Include columns selected by option
    ICEBERG_ASSIGN_OR_RAISE(auto selected, schema_->Select(context_.selected_columns,
                                                           context_.case_sensitive));
    ICEBERG_ASSIGN_OR_RAISE(
        auto selected_field_ids,
        GetProjectedIdsVisitor::GetProjectedIds(*selected, /*include_struct_ids=*/true));
    required_field_ids.insert(std::make_move_iterator(selected_field_ids.begin()),
                              std::make_move_iterator(selected_field_ids.end()));

    ICEBERG_ASSIGN_OR_RAISE(projected_schema_, schema_->Project(required_field_ids));
  } else if (context_.projected_schema != nullptr) {
    projected_schema_ = context_.projected_schema;
  } else {
    projected_schema_ = schema_;
  }

  return projected_schema_;
}

const std::vector<std::string>& TableScan::ScanColumns() const {
  return context_.return_column_stats ? kScanColumnsWithStats : kScanColumns;
}

Result<std::unique_ptr<DataTableScan>> DataTableScan::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<DataTableScan>(new DataTableScan(
      std::move(metadata), std::move(schema), std::move(io), std::move(context)));
}

DataTableScan::DataTableScan(std::shared_ptr<TableMetadata> metadata,
                             std::shared_ptr<Schema> schema, std::shared_ptr<FileIO> io,
                             internal::TableScanContext context)
    : TableScan(std::move(metadata), std::move(schema), std::move(io),
                std::move(context)) {}

Result<std::vector<std::shared_ptr<FileScanTask>>> DataTableScan::PlanFiles() const {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, this->snapshot());
  if (!snapshot) {
    return std::vector<std::shared_ptr<FileScanTask>>{};
  }

  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());

  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_ASSIGN_OR_RAISE(auto data_manifests, snapshot_cache.DataManifests(io_));
  ICEBERG_ASSIGN_OR_RAISE(auto delete_manifests, snapshot_cache.DeleteManifests(io_));

  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_group,
      ManifestGroup::Make(io_, schema_, specs_by_id,
                          {data_manifests.begin(), data_manifests.end()},
                          {delete_manifests.begin(), delete_manifests.end()}));
  manifest_group->CaseSensitive(context_.case_sensitive)
      .Select(ScanColumns())
      .FilterData(filter())
      .IgnoreDeleted()
      .ColumnsToKeepStats(context_.columns_to_keep_stats);
  if (context_.ignore_residuals) {
    manifest_group->IgnoreResiduals();
  }
  return manifest_group->PlanFiles();
}

}  // namespace iceberg
