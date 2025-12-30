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

#include "iceberg/manifest/manifest_writer.h"

#include <optional>

#include "iceberg/constants.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/v1_metadata_internal.h"
#include "iceberg/manifest/v2_metadata_internal.h"
#include "iceberg/manifest/v3_metadata_internal.h"
#include "iceberg/partition_summary_internal.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg {

ManifestWriter::ManifestWriter(std::unique_ptr<Writer> writer,
                               std::unique_ptr<ManifestEntryAdapter> adapter,
                               std::string_view manifest_location,
                               std::optional<int64_t> first_row_id)
    : writer_(std::move(writer)),
      adapter_(std::move(adapter)),
      manifest_location_(manifest_location),
      first_row_id_(first_row_id),
      partition_summary_(
          std::make_unique<PartitionSummary>(*adapter_->partition_type())) {}

ManifestWriter::~ManifestWriter() = default;

Status ManifestWriter::WriteAddedEntry(std::shared_ptr<DataFile> file,
                                       std::optional<int64_t> data_sequence_number) {
  if (!file) [[unlikely]] {
    return InvalidArgument("Data file cannot be null");
  }

  ManifestEntry added{
      .status = ManifestStatus::kAdded,
      .snapshot_id = adapter_->snapshot_id(),
      .sequence_number = data_sequence_number,
      .file_sequence_number = std::nullopt,
      .data_file = std::move(file),
  };

  // Suppress first_row_id for added entries
  if (added.data_file->first_row_id.has_value()) {
    added.data_file = std::make_unique<DataFile>(*added.data_file);
    added.data_file->first_row_id = std::nullopt;
  }

  return WriteEntry(added);
}

Status ManifestWriter::WriteAddedEntry(const ManifestEntry& entry) {
  // Update the entry status to `Added`
  auto added = entry.AsAdded();
  added.snapshot_id = adapter_->snapshot_id();
  // Set the sequence number to nullopt if it is invalid (smaller than 0)
  if (added.sequence_number.has_value() &&
      added.sequence_number.value() < TableMetadata::kInitialSequenceNumber) {
    added.sequence_number = std::nullopt;
  }
  added.file_sequence_number = std::nullopt;

  return WriteEntry(added);
}

Status ManifestWriter::WriteExistingEntry(std::shared_ptr<DataFile> file,
                                          int64_t file_snapshot_id,
                                          int64_t data_sequence_number,
                                          std::optional<int64_t> file_sequence_number) {
  if (!file) [[unlikely]] {
    return InvalidArgument("Data file cannot be null");
  }

  ManifestEntry existing;
  existing.status = ManifestStatus::kExisting;
  existing.snapshot_id = file_snapshot_id;
  existing.data_file = std::move(file);
  existing.sequence_number = data_sequence_number;
  existing.file_sequence_number = file_sequence_number;

  return WriteEntry(existing);
}

Status ManifestWriter::WriteExistingEntry(const ManifestEntry& entry) {
  // Update the entry status to `Existing`
  return WriteEntry(entry.AsExisting());
}

Status ManifestWriter::WriteDeletedEntry(std::shared_ptr<DataFile> file,
                                         int64_t data_sequence_number,
                                         std::optional<int64_t> file_sequence_number) {
  if (!file) [[unlikely]] {
    return InvalidArgument("Data file cannot be null");
  }

  ManifestEntry deleted;
  deleted.status = ManifestStatus::kDeleted;
  deleted.snapshot_id = adapter_->snapshot_id();
  deleted.data_file = std::move(file);
  deleted.sequence_number = data_sequence_number;
  deleted.file_sequence_number = file_sequence_number;

  return WriteEntry(deleted);
}

Status ManifestWriter::WriteDeletedEntry(const ManifestEntry& entry) {
  // Update the entry status to `Deleted`
  auto deleted = entry.AsDeleted();
  // Set the snapshot id to the current snapshot id
  deleted.snapshot_id = adapter_->snapshot_id();

  return WriteEntry(deleted);
}

Status ManifestWriter::WriteEntry(const ManifestEntry& entry) {
  if (!entry.data_file) [[unlikely]] {
    return InvalidArgument("Data file cannot be null");
  }

  ICEBERG_RETURN_UNEXPECTED(CheckDataFile(*entry.data_file));
  if (adapter_->size() >= kBatchSize) {
    ICEBERG_ASSIGN_OR_RAISE(auto array, adapter_->FinishAppending());
    ICEBERG_RETURN_UNEXPECTED(writer_->Write(array));
    ICEBERG_RETURN_UNEXPECTED(adapter_->StartAppending());
  }

  ICEBERG_RETURN_UNEXPECTED(partition_summary_->Update(entry.data_file->partition));

  // update statistics
  switch (entry.status) {
    case ManifestStatus::kAdded:
      add_files_count_++;
      add_rows_count_ += entry.data_file->record_count;
      break;
    case ManifestStatus::kExisting:
      existing_files_count_++;
      existing_rows_count_ += entry.data_file->record_count;
      break;
    case ManifestStatus::kDeleted:
      delete_files_count_++;
      delete_rows_count_ += entry.data_file->record_count;
      break;
    default:
      std::unreachable();
  }

  if (entry.IsAlive() && entry.sequence_number.has_value()) {
    if (!min_sequence_number_.has_value() ||
        entry.sequence_number.value() < min_sequence_number_.value()) {
      min_sequence_number_ = entry.sequence_number.value();
    }
  }

  return adapter_->Append(entry);
}

Status ManifestWriter::CheckDataFile(const DataFile& file) const {
  switch (adapter_->content()) {
    case ManifestContent::kData:
      if (file.content != DataFile::Content::kData) {
        return InvalidArgument("Cannot write {} file to data manifest file",
                               ToString(file.content));
      }
      break;
    case ManifestContent::kDeletes:
      if (file.content != DataFile::Content::kPositionDeletes &&
          file.content != DataFile::Content::kEqualityDeletes) {
        return InvalidArgument("Cannot write {} file to delete manifest file",
                               ToString(file.content));
      }
      break;
    default:
      std::unreachable();
  }
  return {};
}

Status ManifestWriter::AddAll(const std::vector<ManifestEntry>& entries) {
  for (const auto& entry : entries) {
    ICEBERG_RETURN_UNEXPECTED(WriteEntry(entry));
  }
  return {};
}

Status ManifestWriter::Close() {
  if (adapter_->size() > 0) {
    ICEBERG_ASSIGN_OR_RAISE(auto array, adapter_->FinishAppending());
    ICEBERG_RETURN_UNEXPECTED(writer_->Write(array));
  }
  ICEBERG_RETURN_UNEXPECTED(writer_->Close());
  closed_ = true;
  return {};
}

ManifestContent ManifestWriter::content() const { return adapter_->content(); }

Result<Metrics> ManifestWriter::metrics() const { return writer_->metrics(); }

Result<int64_t> ManifestWriter::length() const { return writer_->length(); }

Result<ManifestFile> ManifestWriter::ToManifestFile() const {
  if (!closed_) [[unlikely]] {
    return Invalid("Cannot get ManifestFile before closing the writer.");
  }

  ICEBERG_ASSIGN_OR_RAISE(auto partitions, partition_summary_->Summaries());
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_length, writer_->length());

  return ManifestFile{
      .manifest_path = manifest_location_,
      .manifest_length = manifest_length,
      .partition_spec_id = adapter_->partition_spec()->spec_id(),
      .content = adapter_->content(),
      // sequence_number with kUnassignedSequenceNumber will be assigned when committed.
      .sequence_number = kUnassignedSequenceNumber,
      // if the min_sequence_number_ is missing, then no manifests with a sequence number
      // have been written, so the min data sequence number is the one that will be
      // assigned when this is committed. pass kUnassignedSequenceNumber to inherit it.
      .min_sequence_number = min_sequence_number_.value_or(kUnassignedSequenceNumber),
      .added_snapshot_id = adapter_->snapshot_id().value_or(kInvalidSnapshotId),
      .added_files_count = add_files_count_,
      .existing_files_count = existing_files_count_,
      .deleted_files_count = delete_files_count_,
      .added_rows_count = add_rows_count_,
      .existing_rows_count = existing_rows_count_,
      .deleted_rows_count = delete_rows_count_,
      .partitions = std::move(partitions),
      .first_row_id = first_row_id_,
  };
}

Result<std::unique_ptr<Writer>> OpenFileWriter(
    std::string_view location, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> file_io,
    std::unordered_map<std::string, std::string> metadata, std::string_view schema_name) {
  auto writer_properties = WriterProperties::default_properties();
  if (!schema_name.empty()) {
    writer_properties->Set(WriterProperties::kAvroSchemaName, std::string(schema_name));
  }
  ICEBERG_ASSIGN_OR_RAISE(auto writer, WriterFactoryRegistry::Open(
                                           FileFormatType::kAvro,
                                           {
                                               .path = std::string(location),
                                               .schema = std::move(schema),
                                               .io = std::move(file_io),
                                               .metadata = std::move(metadata),
                                               .properties = std::move(writer_properties),
                                           }));
  return writer;
}

Result<std::unique_ptr<ManifestWriter>> ManifestWriter::MakeV1Writer(
    std::optional<int64_t> snapshot_id, std::string_view manifest_location,
    std::shared_ptr<FileIO> file_io, std::shared_ptr<PartitionSpec> partition_spec,
    std::shared_ptr<Schema> current_schema) {
  if (manifest_location.empty()) {
    return InvalidArgument("Manifest location cannot be empty");
  }
  if (!file_io) {
    return InvalidArgument("FileIO cannot be null");
  }
  if (!partition_spec) {
    return InvalidArgument("PartitionSpec cannot be null");
  }
  if (!current_schema) {
    return InvalidArgument("Current schema cannot be null");
  }

  auto adapter = std::make_unique<ManifestEntryAdapterV1>(
      snapshot_id, std::move(partition_spec), std::move(current_schema));
  ICEBERG_RETURN_UNEXPECTED(adapter->Init());
  ICEBERG_RETURN_UNEXPECTED(adapter->StartAppending());

  auto schema = adapter->schema();
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      OpenFileWriter(manifest_location, std::move(schema), std::move(file_io),
                     adapter->metadata(), "manifest_entry"));
  return std::unique_ptr<ManifestWriter>(new ManifestWriter(
      std::move(writer), std::move(adapter), manifest_location, std::nullopt));
}

Result<std::unique_ptr<ManifestWriter>> ManifestWriter::MakeV2Writer(
    std::optional<int64_t> snapshot_id, std::string_view manifest_location,
    std::shared_ptr<FileIO> file_io, std::shared_ptr<PartitionSpec> partition_spec,
    std::shared_ptr<Schema> current_schema, ManifestContent content) {
  if (manifest_location.empty()) {
    return InvalidArgument("Manifest location cannot be empty");
  }
  if (!file_io) {
    return InvalidArgument("FileIO cannot be null");
  }
  if (!partition_spec) {
    return InvalidArgument("PartitionSpec cannot be null");
  }
  if (!current_schema) {
    return InvalidArgument("Current schema cannot be null");
  }
  auto adapter = std::make_unique<ManifestEntryAdapterV2>(
      snapshot_id, std::move(partition_spec), std::move(current_schema), content);
  ICEBERG_RETURN_UNEXPECTED(adapter->Init());
  ICEBERG_RETURN_UNEXPECTED(adapter->StartAppending());

  auto schema = adapter->schema();
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      OpenFileWriter(manifest_location, std::move(schema), std::move(file_io),
                     adapter->metadata(), "manifest_entry"));
  return std::unique_ptr<ManifestWriter>(new ManifestWriter(
      std::move(writer), std::move(adapter), manifest_location, std::nullopt));
}

Result<std::unique_ptr<ManifestWriter>> ManifestWriter::MakeV3Writer(
    std::optional<int64_t> snapshot_id, std::optional<int64_t> first_row_id,
    std::string_view manifest_location, std::shared_ptr<FileIO> file_io,
    std::shared_ptr<PartitionSpec> partition_spec, std::shared_ptr<Schema> current_schema,
    ManifestContent content) {
  if (manifest_location.empty()) {
    return InvalidArgument("Manifest location cannot be empty");
  }
  if (!file_io) {
    return InvalidArgument("FileIO cannot be null");
  }
  if (!partition_spec) {
    return InvalidArgument("PartitionSpec cannot be null");
  }
  if (!current_schema) {
    return InvalidArgument("Current schema cannot be null");
  }
  auto adapter = std::make_unique<ManifestEntryAdapterV3>(
      snapshot_id, first_row_id, std::move(partition_spec), std::move(current_schema),
      content);
  ICEBERG_RETURN_UNEXPECTED(adapter->Init());
  ICEBERG_RETURN_UNEXPECTED(adapter->StartAppending());

  auto schema = adapter->schema();
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      OpenFileWriter(manifest_location, std::move(schema), std::move(file_io),
                     adapter->metadata(), "manifest_entry"));
  return std::unique_ptr<ManifestWriter>(new ManifestWriter(
      std::move(writer), std::move(adapter), manifest_location, first_row_id));
}

ManifestListWriter::ManifestListWriter(std::unique_ptr<Writer> writer,
                                       std::unique_ptr<ManifestFileAdapter> adapter)
    : writer_(std::move(writer)), adapter_(std::move(adapter)) {}

ManifestListWriter::~ManifestListWriter() = default;

Status ManifestListWriter::Add(const ManifestFile& file) {
  if (adapter_->size() >= kBatchSize) {
    ICEBERG_ASSIGN_OR_RAISE(auto array, adapter_->FinishAppending());
    ICEBERG_RETURN_UNEXPECTED(writer_->Write(array));
    ICEBERG_RETURN_UNEXPECTED(adapter_->StartAppending());
  }
  return adapter_->Append(file);
}

Status ManifestListWriter::AddAll(const std::vector<ManifestFile>& files) {
  for (const auto& file : files) {
    ICEBERG_RETURN_UNEXPECTED(Add(file));
  }
  return {};
}

Status ManifestListWriter::Close() {
  if (adapter_->size() > 0) {
    ICEBERG_ASSIGN_OR_RAISE(auto array, adapter_->FinishAppending());
    ICEBERG_RETURN_UNEXPECTED(writer_->Write(array));
  }
  return writer_->Close();
}

std::optional<int64_t> ManifestListWriter::next_row_id() const {
  return adapter_->next_row_id();
}

Result<std::unique_ptr<ManifestListWriter>> ManifestListWriter::MakeV1Writer(
    int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
    std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io) {
  auto adapter = std::make_unique<ManifestFileAdapterV1>(snapshot_id, parent_snapshot_id);
  ICEBERG_RETURN_UNEXPECTED(adapter->Init());
  ICEBERG_RETURN_UNEXPECTED(adapter->StartAppending());

  auto schema = adapter->schema();
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      OpenFileWriter(manifest_list_location, std::move(schema), std::move(file_io),
                     adapter->metadata(), "manifest_file"));
  return std::unique_ptr<ManifestListWriter>(
      new ManifestListWriter(std::move(writer), std::move(adapter)));
}

Result<std::unique_ptr<ManifestListWriter>> ManifestListWriter::MakeV2Writer(
    int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
    int64_t sequence_number, std::string_view manifest_list_location,
    std::shared_ptr<FileIO> file_io) {
  auto adapter = std::make_unique<ManifestFileAdapterV2>(snapshot_id, parent_snapshot_id,
                                                         sequence_number);
  ICEBERG_RETURN_UNEXPECTED(adapter->Init());
  ICEBERG_RETURN_UNEXPECTED(adapter->StartAppending());

  auto schema = adapter->schema();
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      OpenFileWriter(manifest_list_location, std::move(schema), std::move(file_io),
                     adapter->metadata(), "manifest_file"));

  return std::unique_ptr<ManifestListWriter>(
      new ManifestListWriter(std::move(writer), std::move(adapter)));
}

Result<std::unique_ptr<ManifestListWriter>> ManifestListWriter::MakeV3Writer(
    int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
    int64_t sequence_number, int64_t first_row_id,
    std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io) {
  auto adapter = std::make_unique<ManifestFileAdapterV3>(snapshot_id, parent_snapshot_id,
                                                         sequence_number, first_row_id);
  ICEBERG_RETURN_UNEXPECTED(adapter->Init());
  ICEBERG_RETURN_UNEXPECTED(adapter->StartAppending());

  auto schema = adapter->schema();
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      OpenFileWriter(manifest_list_location, std::move(schema), std::move(file_io),
                     adapter->metadata(), "manifest_file"));
  return std::unique_ptr<ManifestListWriter>(
      new ManifestListWriter(std::move(writer), std::move(adapter)));
}

}  // namespace iceberg
