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

#include <algorithm>
#include <ranges>

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_reader.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {
/// \brief Use indexed data structures for efficient lookups
class DeleteFileIndex {
 public:
  /// \brief Build the index from a list of manifest entries.
  explicit DeleteFileIndex(const std::vector<std::unique_ptr<ManifestEntry>>& entries) {
    for (const auto& entry : entries) {
      const int64_t seq_num =
          entry->sequence_number.value_or(TableMetadata::kInitialSequenceNumber);
      sequence_index.emplace(seq_num, entry.get());
    }
  }

  /// \brief Find delete files that match the sequence number of a data entry.
  std::vector<ManifestEntry*> FindRelevantEntries(const ManifestEntry& data_entry) const {
    std::vector<ManifestEntry*> relevant_deletes;

    // Use lower_bound for efficient range search
    auto data_sequence_number =
        data_entry.sequence_number.value_or(TableMetadata::kInitialSequenceNumber);
    for (auto it = sequence_index.lower_bound(data_sequence_number);
         it != sequence_index.end(); ++it) {
      // Additional filtering logic here
      relevant_deletes.push_back(it->second);
    }

    return relevant_deletes;
  }

 private:
  /// \brief Index by sequence number for quick filtering
  std::multimap<int64_t, ManifestEntry*> sequence_index;
};

/// \brief Get matched delete files for a given data entry.
std::vector<std::shared_ptr<DataFile>> GetMatchedDeletes(
    const ManifestEntry& data_entry, const DeleteFileIndex& delete_file_index) {
  const auto relevant_entries = delete_file_index.FindRelevantEntries(data_entry);
  std::vector<std::shared_ptr<DataFile>> matched_deletes;
  if (relevant_entries.empty()) {
    return matched_deletes;
  }

  matched_deletes.reserve(relevant_entries.size());
  for (const auto& delete_entry : relevant_entries) {
    // TODO(gty404): check if the delete entry contains the data entry's file path
    matched_deletes.emplace_back(delete_entry->data_file);
  }
  return matched_deletes;
}
}  // namespace

// implement FileScanTask
FileScanTask::FileScanTask(std::shared_ptr<DataFile> file,
                           std::vector<std::shared_ptr<DataFile>> delete_files,
                           std::shared_ptr<Expression> residual)
    : data_file_(std::move(file)),
      delete_files_(std::move(delete_files)),
      residual_(std::move(residual)) {}

const std::shared_ptr<DataFile>& FileScanTask::data_file() const { return data_file_; }

const std::vector<std::shared_ptr<DataFile>>& FileScanTask::delete_files() const {
  return delete_files_;
}

int64_t FileScanTask::SizeBytes() const {
  int64_t sizeInBytes = data_file_->file_size_in_bytes;
  std::ranges::for_each(delete_files_, [&sizeInBytes](const auto& delete_file) {
    sizeInBytes += delete_file->file_size_in_bytes;
  });
  return sizeInBytes;
}

int32_t FileScanTask::FilesCount() const {
  return static_cast<int32_t>(delete_files_.size() + 1);
}

int64_t FileScanTask::EstimatedRowCount() const {
  if (data_file_->file_size_in_bytes == 0) {
    return 0;
  }
  const auto sizeInBytes = data_file_->file_size_in_bytes;
  const double scannedFileFraction =
      static_cast<double>(sizeInBytes) / data_file_->file_size_in_bytes;
  return static_cast<int64_t>(scannedFileFraction * data_file_->record_count);
}

const std::shared_ptr<Expression>& FileScanTask::residual() const { return residual_; }

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
  auto iter = std::ranges::find_if(table_metadata->snapshots,
                                   [&snapshot_id](const auto& snapshot) {
                                     return snapshot->snapshot_id == *snapshot_id;
                                   });
  if (iter == table_metadata->snapshots.end() || *iter == nullptr) {
    return NotFound("Snapshot with ID {} is not found", *snapshot_id);
  }
  context_.snapshot = *iter;

  if (!context_.projected_schema) {
    const auto& snapshot = context_.snapshot;
    auto schema_id =
        snapshot->schema_id ? snapshot->schema_id : table_metadata->current_schema_id;
    if (!schema_id) {
      return InvalidArgument("No schema ID found in snapshot {} for table {}",
                             snapshot->snapshot_id, table_metadata->table_uuid);
    }

    const auto& schemas = table_metadata->schemas;
    const auto it = std::ranges::find_if(schemas, [&schema_id](const auto& schema) {
      return schema->schema_id() == *schema_id;
    });
    if (it == schemas.end()) {
      return InvalidArgument("Schema {} in snapshot {} is not found",
                             *snapshot->schema_id, snapshot->snapshot_id);
    }
    const auto& schema = *it;

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
          return InvalidArgument("Column {} not found in schema", column_name);
        }
        projected_fields.emplace_back(field_opt.value().get());
      }
      context_.projected_schema =
          std::make_shared<Schema>(std::move(projected_fields), schema->schema_id());
    }
  }

  return std::make_unique<DataScan>(std::move(context_), file_io_);
}

TableScan::TableScan(TableScanContext context, std::shared_ptr<FileIO> file_io)
    : context_(std::move(context)), file_io_(std::move(file_io)) {}

const std::shared_ptr<Snapshot>& TableScan::snapshot() const { return context_.snapshot; }

const std::shared_ptr<Schema>& TableScan::projection() const {
  return context_.projected_schema;
}

const TableScanContext& TableScan::context() const { return context_; }

const std::shared_ptr<FileIO>& TableScan::io() const { return file_io_; }

DataScan::DataScan(TableScanContext context, std::shared_ptr<FileIO> file_io)
    : TableScan(std::move(context), std::move(file_io)) {}

Result<std::vector<std::shared_ptr<FileScanTask>>> DataScan::PlanFiles() const {
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_list_reader,
                          CreateManifestListReader(context_.snapshot->manifest_list));
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_files, manifest_list_reader->Files());

  std::vector<std::unique_ptr<ManifestEntry>> data_entries;
  std::vector<std::unique_ptr<ManifestEntry>> positional_delete_entries;
  for (const auto& manifest_file : manifest_files) {
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_reader,
                            CreateManifestReader(manifest_file->manifest_path));
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, manifest_reader->Entries());

    // TODO(gty404): filter manifests using partition spec and filter expression

    for (auto& manifest_entry : manifests) {
      const auto& data_file = manifest_entry->data_file;
      switch (data_file->content) {
        case DataFile::Content::kData:
          data_entries.push_back(std::move(manifest_entry));
          break;
        case DataFile::Content::kPositionDeletes:
          // TODO(gty404): check if the sequence number is greater than or equal to the
          // minimum sequence number of all manifest entries
          positional_delete_entries.push_back(std::move(manifest_entry));
          break;
        case DataFile::Content::kEqualityDeletes:
          return NotSupported("Equality deletes are not supported in data scan");
      }
    }
  }

  // TODO(gty404): build residual expression from filter
  std::shared_ptr<Expression> residual;
  std::vector<std::shared_ptr<FileScanTask>> tasks;
  DeleteFileIndex delete_file_index(positional_delete_entries);
  for (const auto& data_entry : data_entries) {
    auto matched_deletes = GetMatchedDeletes(*data_entry, delete_file_index);
    const auto& data_file = data_entry->data_file;
    tasks.emplace_back(
        std::make_shared<FileScanTask>(data_file, std::move(matched_deletes), residual));
  }
  return tasks;
}

}  // namespace iceberg
