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
#include "iceberg/table.h"
#include "iceberg/util/macros.h"

namespace iceberg {

// implement FileScanTask
FileScanTask::FileScanTask(std::shared_ptr<DataFile> file,
                           std::vector<std::shared_ptr<DataFile>> delete_files,
                           int64_t start, int64_t length,
                           std::shared_ptr<Expression> residual)
    : data_file_(std::move(file)),
      delete_files_(std::move(delete_files)),
      start_(start),
      length_(length),
      residual_(std::move(residual)) {}

const std::shared_ptr<DataFile>& FileScanTask::data_file() const { return data_file_; }

const std::vector<std::shared_ptr<DataFile>>& FileScanTask::delete_files() const {
  return delete_files_;
}

int64_t FileScanTask::start() const { return start_; }

int64_t FileScanTask::length() const { return length_; }

int64_t FileScanTask::size_bytes() const {
  int64_t sizeInBytes = length_;
  std::ranges::for_each(delete_files_, [&sizeInBytes](const auto& delete_file) {
    sizeInBytes += delete_file->file_size_in_bytes;
  });
  return sizeInBytes;
}

int32_t FileScanTask::files_count() const {
  return static_cast<int32_t>(delete_files_.size() + 1);
}

int64_t FileScanTask::estimated_row_count() const {
  if (data_file_->file_size_in_bytes == 0) {
    return 0;
  }
  const double scannedFileFraction =
      static_cast<double>(length_) / data_file_->file_size_in_bytes;
  return static_cast<int64_t>(scannedFileFraction * data_file_->record_count);
}

const std::shared_ptr<Expression>& FileScanTask::residual() const { return residual_; }

TableScanBuilder::TableScanBuilder(const Table& table,
                                   std::shared_ptr<TableMetadata> table_metadata)
    : table_(table) {
  context_.table_metadata = std::move(table_metadata);
}

TableScanBuilder& TableScanBuilder::WithColumnNames(
    std::vector<std::string> column_names) {
  column_names_.reserve(column_names.size());
  column_names_ = std::move(column_names);
  return *this;
}

TableScanBuilder& TableScanBuilder::WithSchema(std::shared_ptr<Schema> schema) {
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
  if (snapshot_id_) {
    ICEBERG_ASSIGN_OR_RAISE(context_.snapshot, table_.snapshot(*snapshot_id_));
  } else {
    context_.snapshot = table_.current_snapshot();
  }
  if (context_.snapshot == nullptr) {
    return InvalidArgument("No snapshot found for table {}", table_.name());
  }

  if (!context_.projected_schema) {
    std::shared_ptr<Schema> schema;
    const auto& snapshot = context_.snapshot;
    if (snapshot->schema_id) {
      const auto& schemas = table_.schemas();
      if (const auto it = schemas.find(*snapshot->schema_id); it != schemas.end()) {
        schema = it->second;
      } else {
        return InvalidArgument("Schema {} in snapshot {} is not found",
                               *snapshot->schema_id, snapshot->snapshot_id);
      }
    } else {
      schema = table_.schema();
    }

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

  return std::make_unique<DataScan>(std::move(context_), table_.io());
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

  DeleteFileIndex delete_file_index;
  delete_file_index.BuildIndex(positional_delete_entries);

  // TODO(gty404): build residual expression from filter
  std::shared_ptr<Expression> residual;
  std::vector<std::shared_ptr<FileScanTask>> tasks;
  for (const auto& data_entry : data_entries) {
    auto matched_deletes = GetMatchedDeletes(*data_entry, delete_file_index);
    const auto& data_file = data_entry->data_file;
    tasks.emplace_back(std::make_shared<FileScanTask>(
        data_file, std::move(matched_deletes), 0, data_file->file_size_in_bytes,
        std::move(residual)));
  }
  return tasks;
}

void DataScan::DeleteFileIndex::BuildIndex(
    const std::vector<std::unique_ptr<ManifestEntry>>& entries) {
  sequence_index.clear();

  for (const auto& entry : entries) {
    const int64_t seq_num =
        entry->sequence_number.value_or(Snapshot::kInitialSequenceNumber);
    sequence_index.emplace(seq_num, entry.get());
  }
}

std::vector<ManifestEntry*> DataScan::DeleteFileIndex::FindRelevantEntries(
    const ManifestEntry& data_entry) const {
  std::vector<ManifestEntry*> relevant_deletes;

  // Use lower_bound for efficient range search
  auto data_sequence_number =
      data_entry.sequence_number.value_or(Snapshot::kInitialSequenceNumber);
  for (auto it = sequence_index.lower_bound(data_sequence_number);
       it != sequence_index.end(); ++it) {
    // Additional filtering logic here
    relevant_deletes.push_back(it->second);
  }

  return relevant_deletes;
}

std::vector<std::shared_ptr<DataFile>> DataScan::GetMatchedDeletes(
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

}  // namespace iceberg
