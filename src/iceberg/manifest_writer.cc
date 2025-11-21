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

#include "iceberg/manifest_writer.h"

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/v1_metadata.h"
#include "iceberg/v2_metadata.h"
#include "iceberg/v3_metadata.h"

namespace iceberg {

Status ManifestWriter::WriteAddedEntry(std::shared_ptr<DataFile> file,
                                       std::optional<int64_t> data_sequence_number) {
  ManifestEntry added;
  added.status = ManifestStatus::kAdded;
  added.snapshot_id = adapter_->snapshot_id();
  added.data_file = std::move(file);
  added.sequence_number = data_sequence_number;
  added.file_sequence_number = std::nullopt;

  return WriteEntry(added);
}

Status ManifestWriter::WriteAddedEntry(const ManifestEntry& entry) {
  // Update the entry status to `Added`
  auto added = entry.AsAdded();
  // Set the snapshot id to the current snapshot id
  added.snapshot_id = adapter_->snapshot_id();
  // Set the sequence number to nullopt if it is invalid(smaller than 0)
  if (added.sequence_number.has_value() &&
      added.sequence_number.value() < TableMetadata::kInitialSequenceNumber) {
    added.sequence_number = std::nullopt;
  }
  // Set the file sequence number to nullopt
  added.file_sequence_number = std::nullopt;

  return WriteEntry(added);
}

Status ManifestWriter::WriteExistingEntry(std::shared_ptr<DataFile> file,
                                          int64_t file_snapshot_id,
                                          int64_t data_sequence_number,
                                          std::optional<int64_t> file_sequence_number) {
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
  auto existing = entry.AsExisting();
  return WriteEntry(existing);
}

Status ManifestWriter::WriteDeletedEntry(std::shared_ptr<DataFile> file,
                                         int64_t data_sequence_number,
                                         std::optional<int64_t> file_sequence_number) {
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
  ICEBERG_RETURN_UNEXPECTED(CheckDataFile(*entry.data_file));
  if (adapter_->size() >= kBatchSize) {
    ICEBERG_ASSIGN_OR_RAISE(auto array, adapter_->FinishAppending());
    ICEBERG_RETURN_UNEXPECTED(writer_->Write(array));
    ICEBERG_RETURN_UNEXPECTED(adapter_->StartAppending());
  }
  return adapter_->Append(entry);
}

Status ManifestWriter::CheckDataFile(const DataFile& file) const {
  switch (adapter_->content()) {
    case ManifestContent::kData:
      if (file.content != DataFile::Content::kData) {
        return InvalidArgument(
            "Manifest content type: data, data file content should be: data, but got: {}",
            ToString(file.content));
      }
      break;
    case ManifestContent::kDeletes:
      if (file.content != DataFile::Content::kPositionDeletes &&
          file.content != DataFile::Content::kEqualityDeletes) {
        return InvalidArgument(
            "Manifest content type: deletes, data file content should be: "
            "position_deletes or equality_deletes, but got: {}",
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

Result<ManifestFile> ManifestWriter::ToManifestFile() const {
  if (!closed_) [[unlikely]] {
    return Invalid("Cannot get ManifestFile before closing the writer.");
  }

  ICEBERG_ASSIGN_OR_RAISE(auto manifest_file, adapter_->ToManifestFile());
  manifest_file.manifest_path = manifest_location_;
  manifest_file.manifest_length = writer_->length().value_or(0);
  manifest_file.added_snapshot_id =
      adapter_->snapshot_id().value_or(Snapshot::kInvalidSnapshotId);
  return manifest_file;
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
  return std::make_unique<ManifestWriter>(std::move(writer), std::move(adapter),
                                          manifest_location);
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
  return std::make_unique<ManifestWriter>(std::move(writer), std::move(adapter),
                                          manifest_location);
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
  return std::make_unique<ManifestWriter>(std::move(writer), std::move(adapter),
                                          manifest_location);
}

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
  return std::make_unique<ManifestListWriter>(std::move(writer), std::move(adapter));
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

  return std::make_unique<ManifestListWriter>(std::move(writer), std::move(adapter));
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
  return std::make_unique<ManifestListWriter>(std::move(writer), std::move(adapter));
}

}  // namespace iceberg
