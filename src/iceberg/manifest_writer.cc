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

/// \brief Write manifest files to a manifest list file.
class ManifestWriterImpl : public ManifestWriter {
 public:
  ManifestWriterImpl(std::unique_ptr<Writer> writer,
                     std::unique_ptr<ManifestEntryAdapter> adapter)
      : writer_(std::move(writer)), adapter_(std::move(adapter)) {}

  Status Add(const ManifestEntry& entry) override {
    if (adapter_->size() >= kBatchSize) {
      ICEBERG_ASSIGN_OR_RAISE(auto array, adapter_->FinishAppending());
      ICEBERG_RETURN_UNEXPECTED(writer_->Write(array));
      ICEBERG_RETURN_UNEXPECTED(adapter_->StartAppending());
    }
    return adapter_->Append(entry);
  }

  Status AddAll(const std::vector<ManifestEntry>& entries) override {
    for (const auto& entry : entries) {
      ICEBERG_RETURN_UNEXPECTED(Add(entry));
    }
    return {};
  }

  Status Close() override {
    if (adapter_->size() > 0) {
      ICEBERG_ASSIGN_OR_RAISE(auto array, adapter_->FinishAppending());
      ICEBERG_RETURN_UNEXPECTED(writer_->Write(array));
    }
    return {};
  }

 private:
  static constexpr int64_t kBatchSize = 1024;
  std::unique_ptr<Writer> writer_;
  std::unique_ptr<ManifestEntryAdapter> adapter_;
};

Result<std::unique_ptr<Writer>> OpenFileWriter(std::string_view location,
                                               const std::shared_ptr<Schema> schema,
                                               std::shared_ptr<FileIO> file_io) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      WriterFactoryRegistry::Open(
          FileFormatType::kAvro,
          {.path = std::string(location), .schema = schema, .io = std::move(file_io)}));
  return writer;
}

Result<std::unique_ptr<ManifestWriter>> ManifestWriter::MakeV1Writer(
    std::optional<int64_t> snapshot_id, std::string_view manifest_location,
    std::shared_ptr<FileIO> file_io, std::shared_ptr<Schema> partition_schema) {
  // TODO(xiao.dong) parse v1 schema
  auto manifest_entry_schema =
      ManifestEntry::TypeFromPartitionType(std::move(partition_schema));
  auto fields_span = manifest_entry_schema->fields();
  std::vector<SchemaField> fields(fields_span.begin(), fields_span.end());
  auto schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          OpenFileWriter(manifest_location, schema, std::move(file_io)));
  auto adapter = std::make_unique<ManifestEntryAdapterV1>(snapshot_id, std::move(schema));
  return std::make_unique<ManifestWriterImpl>(std::move(writer), std::move(adapter));
}

Result<std::unique_ptr<ManifestWriter>> ManifestWriter::MakeV2Writer(
    std::optional<int64_t> snapshot_id, std::string_view manifest_location,
    std::shared_ptr<FileIO> file_io, std::shared_ptr<Schema> partition_schema) {
  // TODO(xiao.dong) parse v2 schema
  auto manifest_entry_schema =
      ManifestEntry::TypeFromPartitionType(std::move(partition_schema));
  auto fields_span = manifest_entry_schema->fields();
  std::vector<SchemaField> fields(fields_span.begin(), fields_span.end());
  auto schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          OpenFileWriter(manifest_location, schema, std::move(file_io)));
  auto adapter = std::make_unique<ManifestEntryAdapterV2>(snapshot_id, std::move(schema));
  return std::make_unique<ManifestWriterImpl>(std::move(writer), std::move(adapter));
}

Result<std::unique_ptr<ManifestWriter>> ManifestWriter::MakeV3Writer(
    std::optional<int64_t> snapshot_id, std::optional<int64_t> first_row_id,
    std::string_view manifest_location, std::shared_ptr<FileIO> file_io,
    std::shared_ptr<Schema> partition_schema) {
  // TODO(xiao.dong) parse v3 schema
  auto manifest_entry_schema =
      ManifestEntry::TypeFromPartitionType(std::move(partition_schema));
  auto fields_span = manifest_entry_schema->fields();
  std::vector<SchemaField> fields(fields_span.begin(), fields_span.end());
  auto schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          OpenFileWriter(manifest_location, schema, std::move(file_io)));
  auto adapter = std::make_unique<ManifestEntryAdapterV3>(snapshot_id, first_row_id,
                                                          std::move(schema));
  return std::make_unique<ManifestWriterImpl>(std::move(writer), std::move(adapter));
}

/// \brief Write manifest files to a manifest list file.
class ManifestListWriterImpl : public ManifestListWriter {
 public:
  ManifestListWriterImpl(std::unique_ptr<Writer> writer,
                         std::unique_ptr<ManifestFileAdapter> adapter)
      : writer_(std::move(writer)), adapter_(std::move(adapter)) {}

  Status Add(const ManifestFile& file) override {
    if (adapter_->size() >= kBatchSize) {
      ICEBERG_ASSIGN_OR_RAISE(auto array, adapter_->FinishAppending());
      ICEBERG_RETURN_UNEXPECTED(writer_->Write(array));
      ICEBERG_RETURN_UNEXPECTED(adapter_->StartAppending());
    }
    return adapter_->Append(file);
  }

  Status AddAll(const std::vector<ManifestFile>& files) override {
    for (const auto& file : files) {
      ICEBERG_RETURN_UNEXPECTED(Add(file));
    }
    return {};
  }

  Status Close() override {
    if (adapter_->size() > 0) {
      ICEBERG_ASSIGN_OR_RAISE(auto array, adapter_->FinishAppending());
      ICEBERG_RETURN_UNEXPECTED(writer_->Write(array));
    }
    return {};
  }

 private:
  static constexpr int64_t kBatchSize = 1024;
  std::unique_ptr<Writer> writer_;
  std::unique_ptr<ManifestFileAdapter> adapter_;
};

Result<std::unique_ptr<ManifestListWriter>> ManifestListWriter::MakeV1Writer(
    int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
    std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io) {
  // TODO(xiao.dong) parse v1 schema
  std::vector<SchemaField> fields(ManifestFile::Type().fields().begin(),
                                  ManifestFile::Type().fields().end());
  auto schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer, OpenFileWriter(manifest_list_location, schema, std::move(file_io)));
  auto adapter = std::make_unique<ManifestFileAdapterV1>(snapshot_id, parent_snapshot_id,
                                                         std::move(schema));
  return std::make_unique<ManifestListWriterImpl>(std::move(writer), std::move(adapter));
}

Result<std::unique_ptr<ManifestListWriter>> ManifestListWriter::MakeV2Writer(
    int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
    int64_t sequence_number, std::string_view manifest_list_location,
    std::shared_ptr<FileIO> file_io) {
  // TODO(xiao.dong) parse v2 schema
  std::vector<SchemaField> fields(ManifestFile::Type().fields().begin(),
                                  ManifestFile::Type().fields().end());
  auto schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer, OpenFileWriter(manifest_list_location, schema, std::move(file_io)));
  auto adapter = std::make_unique<ManifestFileAdapterV2>(
      snapshot_id, parent_snapshot_id, sequence_number, std::move(schema));
  return std::make_unique<ManifestListWriterImpl>(std::move(writer), std::move(adapter));
}

Result<std::unique_ptr<ManifestListWriter>> ManifestListWriter::MakeV3Writer(
    int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
    int64_t sequence_number, std::optional<int64_t> first_row_id,
    std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io) {
  // TODO(xiao.dong) parse v3 schema
  std::vector<SchemaField> fields(ManifestFile::Type().fields().begin(),
                                  ManifestFile::Type().fields().end());
  auto schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer, OpenFileWriter(manifest_list_location, schema, std::move(file_io)));
  auto adapter = std::make_unique<ManifestFileAdapterV3>(
      snapshot_id, parent_snapshot_id, sequence_number, first_row_id, std::move(schema));
  return std::make_unique<ManifestListWriterImpl>(std::move(writer), std::move(adapter));
}

}  // namespace iceberg
