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
#include "iceberg/schema.h"
#include "iceberg/util/macros.h"
#include "iceberg/v1_metadata.h"
#include "iceberg/v2_metadata.h"
#include "iceberg/v3_metadata.h"

namespace iceberg {

Status ManifestWriter::Add(const ManifestEntry& entry) {
  if (adapter_->size() >= kBatchSize) {
    ICEBERG_ASSIGN_OR_RAISE(auto array, adapter_->FinishAppending());
    ICEBERG_RETURN_UNEXPECTED(writer_->Write(array));
    ICEBERG_RETURN_UNEXPECTED(adapter_->StartAppending());
  }
  return adapter_->Append(entry);
}

Status ManifestWriter::AddAll(const std::vector<ManifestEntry>& entries) {
  for (const auto& entry : entries) {
    ICEBERG_RETURN_UNEXPECTED(Add(entry));
  }
  return {};
}

Status ManifestWriter::Close() {
  if (adapter_->size() > 0) {
    ICEBERG_ASSIGN_OR_RAISE(auto array, adapter_->FinishAppending());
    ICEBERG_RETURN_UNEXPECTED(writer_->Write(array));
  }
  return writer_->Close();
}

Result<std::unique_ptr<Writer>> OpenFileWriter(
    std::string_view location, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> file_io,
    std::unordered_map<std::string, std::string> properties) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer, WriterFactoryRegistry::Open(FileFormatType::kAvro,
                                               {.path = std::string(location),
                                                .schema = std::move(schema),
                                                .io = std::move(file_io),
                                                .properties = std::move(properties)}));
  return writer;
}

Result<std::unique_ptr<ManifestWriter>> ManifestWriter::MakeV1Writer(
    std::optional<int64_t> snapshot_id, std::string_view manifest_location,
    std::shared_ptr<FileIO> file_io, std::shared_ptr<PartitionSpec> partition_spec) {
  auto adapter =
      std::make_unique<ManifestEntryAdapterV1>(snapshot_id, std::move(partition_spec));
  ICEBERG_RETURN_UNEXPECTED(adapter->Init());
  ICEBERG_RETURN_UNEXPECTED(adapter->StartAppending());

  auto schema = adapter->schema();
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          OpenFileWriter(manifest_location, std::move(schema),
                                         std::move(file_io), adapter->metadata()));
  return std::make_unique<ManifestWriter>(std::move(writer), std::move(adapter));
}

Result<std::unique_ptr<ManifestWriter>> ManifestWriter::MakeV2Writer(
    std::optional<int64_t> snapshot_id, std::string_view manifest_location,
    std::shared_ptr<FileIO> file_io, std::shared_ptr<PartitionSpec> partition_spec) {
  auto adapter =
      std::make_unique<ManifestEntryAdapterV2>(snapshot_id, std::move(partition_spec));
  ICEBERG_RETURN_UNEXPECTED(adapter->Init());
  ICEBERG_RETURN_UNEXPECTED(adapter->StartAppending());

  auto schema = adapter->schema();
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          OpenFileWriter(manifest_location, std::move(schema),
                                         std::move(file_io), adapter->metadata()));
  return std::make_unique<ManifestWriter>(std::move(writer), std::move(adapter));
}

Result<std::unique_ptr<ManifestWriter>> ManifestWriter::MakeV3Writer(
    std::optional<int64_t> snapshot_id, std::optional<int64_t> first_row_id,
    std::string_view manifest_location, std::shared_ptr<FileIO> file_io,
    std::shared_ptr<PartitionSpec> partition_spec) {
  auto adapter = std::make_unique<ManifestEntryAdapterV3>(snapshot_id, first_row_id,
                                                          std::move(partition_spec));
  ICEBERG_RETURN_UNEXPECTED(adapter->Init());
  ICEBERG_RETURN_UNEXPECTED(adapter->StartAppending());

  auto schema = adapter->schema();
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          OpenFileWriter(manifest_location, std::move(schema),
                                         std::move(file_io), adapter->metadata()));
  return std::make_unique<ManifestWriter>(std::move(writer), std::move(adapter));
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

Result<std::unique_ptr<ManifestListWriter>> ManifestListWriter::MakeV1Writer(
    int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
    std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io) {
  auto adapter = std::make_unique<ManifestFileAdapterV1>(snapshot_id, parent_snapshot_id);
  ICEBERG_RETURN_UNEXPECTED(adapter->Init());
  ICEBERG_RETURN_UNEXPECTED(adapter->StartAppending());

  auto schema = adapter->schema();
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          OpenFileWriter(manifest_list_location, std::move(schema),
                                         std::move(file_io), adapter->metadata()));
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
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          OpenFileWriter(manifest_list_location, std::move(schema),
                                         std::move(file_io), adapter->metadata()));

  return std::make_unique<ManifestListWriter>(std::move(writer), std::move(adapter));
}

Result<std::unique_ptr<ManifestListWriter>> ManifestListWriter::MakeV3Writer(
    int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
    int64_t sequence_number, std::optional<int64_t> first_row_id,
    std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io) {
  auto adapter = std::make_unique<ManifestFileAdapterV3>(snapshot_id, parent_snapshot_id,
                                                         sequence_number, first_row_id);
  ICEBERG_RETURN_UNEXPECTED(adapter->Init());
  ICEBERG_RETURN_UNEXPECTED(adapter->StartAppending());

  auto schema = adapter->schema();
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          OpenFileWriter(manifest_list_location, std::move(schema),
                                         std::move(file_io), adapter->metadata()));
  return std::make_unique<ManifestListWriter>(std::move(writer), std::move(adapter));
}

}  // namespace iceberg
