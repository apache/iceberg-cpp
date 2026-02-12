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

#pragma once

#include <chrono>
#include <format>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

/// \brief Base class for scan-related tests providing common test utilities.
///
/// This class provides common setup and helper functions for testing
/// TableScan and IncrementalScan implementations.
class ScanTestBase : public testing::TestWithParam<int8_t> {
 protected:
  void SetUp() override {
    avro::RegisterAll();

    file_io_ = arrow::MakeMockFileIO();
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(/*field_id=*/1, "id", int32()),
        SchemaField::MakeRequired(/*field_id=*/2, "data", string())});
    unpartitioned_spec_ = PartitionSpec::Unpartitioned();

    ICEBERG_UNWRAP_OR_FAIL(
        partitioned_spec_,
        PartitionSpec::Make(
            /*spec_id=*/1, {PartitionField(/*source_id=*/2, /*field_id=*/1000,
                                           "data_bucket_16_2", Transform::Bucket(16))}));
  }

  /// \brief Generate a unique manifest file path.
  std::string MakeManifestPath() {
    return std::format("manifest-{}-{}.avro", manifest_counter_++,
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  /// \brief Generate a unique manifest list file path.
  std::string MakeManifestListPath() {
    return std::format("manifest-list-{}-{}.avro", manifest_list_counter_++,
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  /// \brief Create a manifest entry.
  ManifestEntry MakeEntry(ManifestStatus status, int64_t snapshot_id,
                          int64_t sequence_number, std::shared_ptr<DataFile> file) {
    return ManifestEntry{
        .status = status,
        .snapshot_id = snapshot_id,
        .sequence_number = sequence_number,
        .file_sequence_number = sequence_number,
        .data_file = std::move(file),
    };
  }

  /// \brief Write a data manifest file.
  ManifestFile WriteDataManifest(
      int8_t format_version, int64_t snapshot_id, std::vector<ManifestEntry> entries,
      std::shared_ptr<PartitionSpec> spec = PartitionSpec::Unpartitioned()) {
    const std::string manifest_path = MakeManifestPath();
    auto writer_result = ManifestWriter::MakeWriter(
        format_version, snapshot_id, manifest_path, file_io_, spec, schema_,
        ManifestContent::kData,
        /*first_row_id=*/format_version >= 3 ? std::optional<int64_t>(0L) : std::nullopt);

    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    for (const auto& entry : entries) {
      EXPECT_THAT(writer->WriteEntry(entry), IsOk());
    }

    EXPECT_THAT(writer->Close(), IsOk());
    auto manifest_result = writer->ToManifestFile();
    EXPECT_THAT(manifest_result, IsOk());
    return std::move(manifest_result.value());
  }

  /// \brief Write a delete manifest file.
  ManifestFile WriteDeleteManifest(int8_t format_version, int64_t snapshot_id,
                                   std::vector<ManifestEntry> entries,
                                   std::shared_ptr<PartitionSpec> spec) {
    const std::string manifest_path = MakeManifestPath();
    auto writer_result =
        ManifestWriter::MakeWriter(format_version, snapshot_id, manifest_path, file_io_,
                                   spec, schema_, ManifestContent::kDeletes);

    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    for (const auto& entry : entries) {
      EXPECT_THAT(writer->WriteEntry(entry), IsOk());
    }

    EXPECT_THAT(writer->Close(), IsOk());
    auto manifest_result = writer->ToManifestFile();
    EXPECT_THAT(manifest_result, IsOk());
    return std::move(manifest_result.value());
  }

  /// \brief Write a manifest list file.
  std::string WriteManifestList(int8_t format_version, int64_t snapshot_id,
                                int64_t parent_snapshot_id, int64_t sequence_number,
                                const std::vector<ManifestFile>& manifests) {
    const std::string manifest_list_path = MakeManifestListPath();

    auto writer_result = ManifestListWriter::MakeWriter(
        format_version, snapshot_id, parent_snapshot_id, manifest_list_path, file_io_,
        /*sequence_number=*/format_version >= 2 ? std::optional(sequence_number)
                                                : std::nullopt,
        /*first_row_id=*/format_version >= 3 ? std::optional<int64_t>(0L) : std::nullopt);

    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());
    EXPECT_THAT(writer->AddAll(manifests), IsOk());
    EXPECT_THAT(writer->Close(), IsOk());

    return manifest_list_path;
  }

  /// \brief Extract file paths from scan tasks.
  static std::vector<std::string> GetPaths(
      const std::vector<std::shared_ptr<FileScanTask>>& tasks) {
    return tasks | std::views::transform([](const auto& task) {
             return task->data_file()->file_path;
           }) |
           std::ranges::to<std::vector<std::string>>();
  }

  /// \brief Create table metadata with the given snapshots.
  std::shared_ptr<TableMetadata> MakeTableMetadata(
      const std::vector<std::shared_ptr<Snapshot>>& snapshots,
      int64_t current_snapshot_id,
      const std::unordered_map<std::string, std::shared_ptr<SnapshotRef>>& refs = {},
      std::shared_ptr<PartitionSpec> default_spec = nullptr) {
    TimePointMs timestamp_ms = TimePointMsFromUnixMs(1609459200000L);
    int64_t last_seq = snapshots.empty() ? 0L : snapshots.back()->sequence_number;
    auto effective_spec = default_spec ? default_spec : unpartitioned_spec_;

    return std::make_shared<TableMetadata>(TableMetadata{
        .format_version = GetParam(),
        .table_uuid = "test-table-uuid",
        .location = "/tmp/table",
        .last_sequence_number = last_seq,
        .last_updated_ms = timestamp_ms,
        .last_column_id = 2,
        .schemas = {schema_},
        .current_schema_id = schema_->schema_id(),
        .partition_specs = {partitioned_spec_, unpartitioned_spec_},
        .default_spec_id = effective_spec->spec_id(),
        .last_partition_id = 1000,
        .current_snapshot_id = current_snapshot_id,
        .snapshots = snapshots,
        .snapshot_log = {},
        .default_sort_order_id = 0,
        .refs = refs,
    });
  }

  /// \brief Create a data file with optional partition values.
  std::shared_ptr<DataFile> MakeDataFile(
      const std::string& path,
      PartitionValues partition = PartitionValues(std::vector<Literal>{}),
      std::shared_ptr<PartitionSpec> spec = nullptr, int64_t record_count = 1) {
    auto effective_spec = spec ? spec : unpartitioned_spec_;
    return std::make_shared<DataFile>(DataFile{
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = std::move(partition),
        .record_count = record_count,
        .file_size_in_bytes = 10,
        .sort_order_id = 0,
        .partition_spec_id = effective_spec->spec_id(),
    });
  }

  /// \brief Create an append snapshot with the given files (string paths).
  std::shared_ptr<Snapshot> MakeAppendSnapshot(
      int8_t format_version, int64_t snapshot_id,
      std::optional<int64_t> parent_snapshot_id, int64_t sequence_number,
      const std::vector<std::string>& added_files,
      std::shared_ptr<PartitionSpec> spec = nullptr) {
    std::vector<std::pair<std::string, PartitionValues>> files_with_partitions;
    for (const auto& path : added_files) {
      files_with_partitions.emplace_back(path, kEmptyPartition);
    }
    return MakeAppendSnapshotWithPartitionValues(format_version, snapshot_id,
                                                 parent_snapshot_id, sequence_number,
                                                 files_with_partitions, spec);
  }

  /// \brief Create an append snapshot with the given files (with partition values).
  std::shared_ptr<Snapshot> MakeAppendSnapshotWithPartitionValues(
      int8_t format_version, int64_t snapshot_id,
      std::optional<int64_t> parent_snapshot_id, int64_t sequence_number,
      const std::vector<std::pair<std::string, PartitionValues>>& added_files,
      std::shared_ptr<PartitionSpec> spec = nullptr) {
    auto effective_spec = spec ? spec : unpartitioned_spec_;
    std::vector<ManifestEntry> entries;
    entries.reserve(added_files.size());
    for (const auto& [path, partition] : added_files) {
      auto file = MakeDataFile(path, partition, effective_spec);
      entries.push_back(
          MakeEntry(ManifestStatus::kAdded, snapshot_id, sequence_number, file));
    }

    auto manifest = WriteDataManifest(format_version, snapshot_id, std::move(entries),
                                      effective_spec);
    int64_t parent_id = parent_snapshot_id.value_or(0L);
    auto manifest_list = WriteManifestList(format_version, snapshot_id, parent_id,
                                           sequence_number, {manifest});
    TimePointMs timestamp_ms =
        TimePointMsFromUnixMs(1609459200000L + sequence_number * 1000);
    return std::make_shared<Snapshot>(Snapshot{
        .snapshot_id = snapshot_id,
        .parent_snapshot_id = parent_snapshot_id,
        .sequence_number = sequence_number,
        .timestamp_ms = timestamp_ms,
        .manifest_list = manifest_list,
        .summary = {{"operation", "append"}},
        .schema_id = schema_->schema_id(),
    });
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> partitioned_spec_;
  std::shared_ptr<PartitionSpec> unpartitioned_spec_;

 private:
  int manifest_counter_ = 0;
  int manifest_list_counter_ = 0;
  constexpr static PartitionValues kEmptyPartition{};
};

}  // namespace iceberg
