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

#include <chrono>
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

class TableScanTest : public testing::TestWithParam<int8_t> {
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

    MakeTableMetadata();
  }

  void MakeTableMetadata() {
    constexpr int64_t kSnapshotId = 1000L;
    constexpr int64_t kSequenceNumber = 1L;
    const TimePointMs kTimestampMs =
        TimePointMsFromUnixMs(1609459200000L);  // 2021-01-01 00:00:00 UTC

    auto snapshot = std::make_shared<Snapshot>(
        Snapshot{.snapshot_id = kSnapshotId,
                 .parent_snapshot_id = std::nullopt,
                 .sequence_number = kSequenceNumber,
                 .timestamp_ms = kTimestampMs,
                 .manifest_list = "/tmp/metadata/snap-1000-1-manifest-list.avro",
                 .schema_id = schema_->schema_id()});

    table_metadata_ = std::make_shared<TableMetadata>(
        TableMetadata{.format_version = 2,
                      .table_uuid = "test-table-uuid",
                      .location = "/tmp/table",
                      .last_sequence_number = kSequenceNumber,
                      .last_updated_ms = kTimestampMs,
                      .last_column_id = 2,
                      .schemas = {schema_},
                      .current_schema_id = schema_->schema_id(),
                      .partition_specs = {partitioned_spec_, unpartitioned_spec_},
                      .default_spec_id = partitioned_spec_->spec_id(),
                      .last_partition_id = 1000,
                      .current_snapshot_id = kSnapshotId,
                      .snapshots = {snapshot},
                      .snapshot_log = {SnapshotLogEntry{.timestamp_ms = kTimestampMs,
                                                        .snapshot_id = kSnapshotId}},
                      .default_sort_order_id = 0,
                      .refs = {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                                            .snapshot_id = kSnapshotId,
                                            .retention = SnapshotRef::Branch{}})}}});
  }

  std::shared_ptr<DataFile> MakePositionDeleteFile(
      const std::string& path, const PartitionValues& partition, int32_t spec_id,
      std::optional<std::string> referenced_file = std::nullopt) {
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kPositionDeletes,
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = 1,
        .file_size_in_bytes = 10,
        .referenced_data_file = referenced_file,
        .partition_spec_id = spec_id,
    });
  }

  std::shared_ptr<DataFile> MakeEqualityDeleteFile(const std::string& path,
                                                   const PartitionValues& partition,
                                                   int32_t spec_id,
                                                   std::vector<int> equality_ids = {1}) {
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kEqualityDeletes,
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = 1,
        .file_size_in_bytes = 10,
        .equality_ids = std::move(equality_ids),
        .partition_spec_id = spec_id,
    });
  }

  std::string MakeManifestPath() {
    static int counter = 0;
    return std::format("manifest-{}-{}.avro", counter++,
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& path,
                                         const PartitionValues& partition,
                                         int32_t spec_id, int64_t record_count = 1,
                                         std::optional<int32_t> lower_id = std::nullopt,
                                         std::optional<int32_t> upper_id = std::nullopt) {
    auto file = std::make_shared<DataFile>(DataFile{
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = record_count,
        .file_size_in_bytes = 10,
        .sort_order_id = 0,
        .partition_spec_id = spec_id,
    });
    // Set lower/upper bounds for field_id=1 ("id" column) if provided
    if (lower_id.has_value()) {
      file->lower_bounds[1] = Literal::Int(lower_id.value()).Serialize().value();
    }
    if (upper_id.has_value()) {
      file->upper_bounds[1] = Literal::Int(upper_id.value()).Serialize().value();
    }
    return file;
  }

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

  ManifestFile WriteDataManifest(int8_t format_version, int64_t snapshot_id,
                                 std::vector<ManifestEntry> entries,
                                 std::shared_ptr<PartitionSpec> spec) {
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

  std::string MakeManifestListPath() {
    static int counter = 0;
    return std::format("manifest-list-{}-{}.avro", counter++,
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  std::string WriteManifestList(int8_t format_version, int64_t snapshot_id,
                                int64_t sequence_number,
                                const std::vector<ManifestFile>& manifests) {
    const std::string manifest_list_path = MakeManifestListPath();
    constexpr int64_t kParentSnapshotId = 0L;

    auto writer_result = ManifestListWriter::MakeWriter(
        format_version, snapshot_id, kParentSnapshotId, manifest_list_path, file_io_,
        /*sequence_number=*/format_version >= 2 ? std::optional(sequence_number)
                                                : std::nullopt,
        /*first_row_id=*/format_version >= 3 ? std::optional<int64_t>(0L) : std::nullopt);

    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());
    EXPECT_THAT(writer->AddAll(manifests), IsOk());
    EXPECT_THAT(writer->Close(), IsOk());

    return manifest_list_path;
  }

  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> GetSpecsById() {
    return {{partitioned_spec_->spec_id(), partitioned_spec_},
            {unpartitioned_spec_->spec_id(), unpartitioned_spec_}};
  }

  static std::vector<std::string> GetPaths(
      const std::vector<std::shared_ptr<FileScanTask>>& tasks) {
    return tasks | std::views::transform([](const auto& task) {
             return task->data_file()->file_path;
           }) |
           std::ranges::to<std::vector<std::string>>();
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> partitioned_spec_;
  std::shared_ptr<PartitionSpec> unpartitioned_spec_;
  std::shared_ptr<TableMetadata> table_metadata_;
};

TEST_P(TableScanTest, TableScanBuilderOptions) {
  // Test basic scan creation and default values
  ICEBERG_UNWRAP_OR_FAIL(auto builder, TableScanBuilder::Make(table_metadata_, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto basic_scan, builder->Build());
  EXPECT_NE(basic_scan, nullptr);
  EXPECT_EQ(basic_scan->metadata(), table_metadata_);
  EXPECT_EQ(basic_scan->io(), file_io_);
  EXPECT_TRUE(basic_scan->is_case_sensitive());

  // Test all builder options with method chaining
  auto projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});
  auto filter = Expressions::Equal("id", Literal::Int(42));
  constexpr int64_t kMinRows = 1000;
  constexpr int64_t kSnapshotId = 1000L;
  const std::string branch_name = "test-branch";

  ICEBERG_UNWRAP_OR_FAIL(auto builder2,
                         TableScanBuilder::Make(table_metadata_, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder2->Option("key1", "value1")
                                        .Option("key2", "value2")
                                        .CaseSensitive(false)
                                        .Project(projected_schema)
                                        .Filter(filter)
                                        .IncludeColumnStats({"id", "data"})
                                        .IgnoreResiduals()
                                        .MinRowsRequested(kMinRows)
                                        .UseSnapshot(kSnapshotId)
                                        .UseBranch(branch_name)
                                        .Build());

  // Verify all options were set correctly
  ICEBERG_UNWRAP_OR_FAIL(auto schema, scan->schema());
  EXPECT_EQ(schema, projected_schema);
  EXPECT_EQ(scan->filter(), filter);
  EXPECT_FALSE(scan->is_case_sensitive());

  const auto& context = scan->context();
  EXPECT_EQ(context.options.at("key1"), "value1");
  EXPECT_EQ(context.options.at("key2"), "value2");
  EXPECT_TRUE(context.return_column_stats);
  EXPECT_EQ(context.columns_to_keep_stats.size(), 2);
  EXPECT_TRUE(context.columns_to_keep_stats.contains(1));  // id field
  EXPECT_TRUE(context.columns_to_keep_stats.contains(2));  // data field
  EXPECT_TRUE(context.ignore_residuals);
  EXPECT_TRUE(context.min_rows_requested.has_value());
  EXPECT_EQ(context.min_rows_requested.value(), kMinRows);
  EXPECT_TRUE(context.snapshot_id.has_value());
  EXPECT_EQ(context.snapshot_id.value(), kSnapshotId);
  EXPECT_EQ(context.branch, branch_name);

  // Test UseRef separately
  ICEBERG_UNWRAP_OR_FAIL(auto builder3,
                         TableScanBuilder::Make(table_metadata_, file_io_));
  builder3->UseRef("main");
  ICEBERG_UNWRAP_OR_FAIL(auto ref_scan, builder3->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, ref_scan->snapshot());
  EXPECT_EQ(snapshot->snapshot_id, 1000L);
}

TEST_P(TableScanTest, TableScanBuilderValidationErrors) {
  // Test negative min rows
  ICEBERG_UNWRAP_OR_FAIL(auto builder, TableScanBuilder::Make(table_metadata_, file_io_));
  builder->MinRowsRequested(-1);
  EXPECT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));

  // Test invalid snapshot ID
  ICEBERG_UNWRAP_OR_FAIL(auto builder2,
                         TableScanBuilder::Make(table_metadata_, file_io_));
  builder2->UseSnapshot(9999L);
  EXPECT_THAT(builder2->Build(), IsError(ErrorKind::kValidationFailed));

  // Test invalid ref
  ICEBERG_UNWRAP_OR_FAIL(auto builder3,
                         TableScanBuilder::Make(table_metadata_, file_io_));
  builder3->UseRef("non-existent-ref");
  EXPECT_THAT(builder3->Build(), IsError(ErrorKind::kValidationFailed));

  // Test null inputs
  EXPECT_THAT(TableScanBuilder::Make(nullptr, file_io_),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(TableScanBuilder::Make(table_metadata_, nullptr),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_P(TableScanTest, DataTableScanPlanFilesEmpty) {
  auto empty_metadata = std::make_shared<TableMetadata>(
      TableMetadata{.format_version = 2,
                    .schemas = {schema_},
                    .current_schema_id = schema_->schema_id(),
                    .partition_specs = {unpartitioned_spec_},
                    .default_spec_id = unpartitioned_spec_->spec_id(),
                    .current_snapshot_id = -1,
                    .snapshots = {},
                    .refs = {}});

  ICEBERG_UNWRAP_OR_FAIL(auto builder, TableScanBuilder::Make(empty_metadata, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

TEST_P(TableScanTest, PlanFilesWithDataManifests) {
  auto version = GetParam();

  constexpr int64_t kSnapshotId = 1000L;
  const auto part_value = PartitionValues({Literal::Int(0)});

  std::vector<ManifestEntry> data_entries{
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                MakeDataFile("/path/to/data1.parquet", part_value,
                             partitioned_spec_->spec_id(), /*record_count=*/100)),
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                MakeDataFile("/path/to/data2.parquet", part_value,
                             partitioned_spec_->spec_id(), /*record_count=*/200))};
  auto data_manifest =
      WriteDataManifest(version, kSnapshotId, std::move(data_entries), partitioned_spec_);
  std::string manifest_list_path =
      WriteManifestList(version, kSnapshotId, /*sequence_number=*/1, {data_manifest});

  // Create a snapshot that references this manifest list
  auto timestamp_ms = TimePointMsFromUnixMs(1609459200000L);
  auto snapshot_with_manifest =
      std::make_shared<Snapshot>(Snapshot{.snapshot_id = kSnapshotId,
                                          .parent_snapshot_id = std::nullopt,
                                          .sequence_number = 1L,
                                          .timestamp_ms = timestamp_ms,
                                          .manifest_list = manifest_list_path,
                                          .summary = {},
                                          .schema_id = schema_->schema_id()});

  auto metadata_with_manifest = std::make_shared<TableMetadata>(
      TableMetadata{.format_version = 2,
                    .table_uuid = "test-table-uuid",
                    .location = "/tmp/table",
                    .last_sequence_number = 1L,
                    .last_updated_ms = timestamp_ms,
                    .last_column_id = 2,
                    .schemas = {schema_},
                    .current_schema_id = schema_->schema_id(),
                    .partition_specs = {partitioned_spec_, unpartitioned_spec_},
                    .default_spec_id = partitioned_spec_->spec_id(),
                    .last_partition_id = 1000,
                    .current_snapshot_id = kSnapshotId,
                    .snapshots = {snapshot_with_manifest},
                    .snapshot_log = {SnapshotLogEntry{.timestamp_ms = timestamp_ms,
                                                      .snapshot_id = kSnapshotId}},
                    .default_sort_order_id = 0,
                    .refs = {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                                          .snapshot_id = kSnapshotId,
                                          .retention = SnapshotRef::Branch{},
                                      })}}});

  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         TableScanBuilder::Make(metadata_with_manifest, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 2);
  EXPECT_THAT(GetPaths(tasks), testing::UnorderedElementsAre("/path/to/data1.parquet",
                                                             "/path/to/data2.parquet"));
}

TEST_P(TableScanTest, PlanFilesWithMultipleManifests) {
  auto version = GetParam();

  const auto partition_a = PartitionValues({Literal::Int(0)});
  const auto partition_b = PartitionValues({Literal::Int(1)});

  // Create first data manifest
  std::vector<ManifestEntry> data_entries_1{MakeEntry(
      ManifestStatus::kAdded, /*snapshot_id=*/1000L, /*sequence_number=*/1,
      MakeDataFile("/path/to/data1.parquet", partition_a, partitioned_spec_->spec_id()))};
  auto data_manifest_1 = WriteDataManifest(version, /*snapshot_id=*/1000L,
                                           std::move(data_entries_1), partitioned_spec_);

  // Create second data manifest
  std::vector<ManifestEntry> data_entries_2{MakeEntry(
      ManifestStatus::kAdded, /*snapshot_id=*/1000L, /*sequence_number=*/1,
      MakeDataFile("/path/to/data2.parquet", partition_b, partitioned_spec_->spec_id()))};
  auto data_manifest_2 = WriteDataManifest(version, /*snapshot_id=*/1000L,
                                           std::move(data_entries_2), partitioned_spec_);

  // Write manifest list with multiple manifests
  std::string manifest_list_path =
      WriteManifestList(version, /*snapshot_id=*/1000L, /*sequence_number=*/1,
                        {data_manifest_1, data_manifest_2});

  // Create a snapshot that references this manifest list
  auto timestamp_ms = TimePointMsFromUnixMs(1609459200000L);
  auto snapshot_with_manifests =
      std::make_shared<Snapshot>(Snapshot{.snapshot_id = 1000L,
                                          .parent_snapshot_id = std::nullopt,
                                          .sequence_number = 1L,
                                          .timestamp_ms = timestamp_ms,
                                          .manifest_list = manifest_list_path,
                                          .summary = {},
                                          .schema_id = schema_->schema_id()});

  auto metadata_with_manifests = std::make_shared<TableMetadata>(
      TableMetadata{.format_version = 2,
                    .table_uuid = "test-table-uuid",
                    .location = "/tmp/table",
                    .last_sequence_number = 1L,
                    .last_updated_ms = timestamp_ms,
                    .last_column_id = 2,
                    .schemas = {schema_},
                    .current_schema_id = schema_->schema_id(),
                    .partition_specs = {partitioned_spec_, unpartitioned_spec_},
                    .default_spec_id = partitioned_spec_->spec_id(),
                    .last_partition_id = 1000,
                    .current_snapshot_id = 1000L,
                    .snapshots = {snapshot_with_manifests},
                    .snapshot_log = {SnapshotLogEntry{.timestamp_ms = timestamp_ms,
                                                      .snapshot_id = 1000L}},
                    .default_sort_order_id = 0,
                    .refs = {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                                          .snapshot_id = 1000L,
                                          .retention = SnapshotRef::Branch{},
                                      })}}});

  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         TableScanBuilder::Make(metadata_with_manifests, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 2);
  EXPECT_THAT(GetPaths(tasks), testing::UnorderedElementsAre("/path/to/data1.parquet",
                                                             "/path/to/data2.parquet"));
}

TEST_P(TableScanTest, PlanFilesWithFilter) {
  auto version = GetParam();

  constexpr int64_t kSnapshotId = 1000L;
  const auto part_value = PartitionValues({Literal::Int(0)});

  // Create two data files with non-overlapping id ranges:
  // - data1.parquet: id range [1, 50]
  // - data2.parquet: id range [51, 100]
  std::vector<ManifestEntry> data_entries{
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                MakeDataFile("/path/to/data1.parquet", part_value,
                             partitioned_spec_->spec_id(), /*record_count=*/1,
                             /*lower_id=*/1, /*upper_id=*/50)),
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                MakeDataFile("/path/to/data2.parquet", part_value,
                             partitioned_spec_->spec_id(), /*record_count=*/1,
                             /*lower_id=*/51, /*upper_id=*/100))};
  auto data_manifest =
      WriteDataManifest(version, kSnapshotId, std::move(data_entries), partitioned_spec_);
  std::string manifest_list_path =
      WriteManifestList(version, kSnapshotId, /*sequence_number=*/1, {data_manifest});

  auto timestamp_ms = TimePointMsFromUnixMs(1609459200000L);
  auto snapshot = std::make_shared<Snapshot>(Snapshot{.snapshot_id = kSnapshotId,
                                                      .parent_snapshot_id = std::nullopt,
                                                      .sequence_number = 1L,
                                                      .timestamp_ms = timestamp_ms,
                                                      .manifest_list = manifest_list_path,
                                                      .schema_id = schema_->schema_id()});

  auto metadata = std::make_shared<TableMetadata>(TableMetadata{
      .format_version = 2,
      .table_uuid = "test-table-uuid",
      .location = "/tmp/table",
      .last_sequence_number = 1L,
      .last_updated_ms = timestamp_ms,
      .last_column_id = 2,
      .schemas = {schema_},
      .current_schema_id = schema_->schema_id(),
      .partition_specs = {partitioned_spec_, unpartitioned_spec_},
      .default_spec_id = partitioned_spec_->spec_id(),
      .last_partition_id = 1000,
      .current_snapshot_id = kSnapshotId,
      .snapshots = {snapshot},
      .snapshot_log = {SnapshotLogEntry{.timestamp_ms = timestamp_ms,
                                        .snapshot_id = kSnapshotId}},
      .default_sort_order_id = 0,
      .refs = {{"main",
                std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = kSnapshotId, .retention = SnapshotRef::Branch{}})}}});

  // Test 1: Filter matches only data1.parquet (id=25 is in range [1, 50])
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder, TableScanBuilder::Make(metadata, file_io_));
    builder->Filter(Expressions::Equal("id", Literal::Int(25)));
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(tasks[0]->data_file()->file_path, "/path/to/data1.parquet");
  }

  // Test 2: Filter matches only data2.parquet (id=75 is in range [51, 100])
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder, TableScanBuilder::Make(metadata, file_io_));
    builder->Filter(Expressions::Equal("id", Literal::Int(75)));
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(tasks[0]->data_file()->file_path, "/path/to/data2.parquet");
  }

  // Test 3: Filter matches both files (id > 0 covers both ranges)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder, TableScanBuilder::Make(metadata, file_io_));
    builder->Filter(Expressions::GreaterThan("id", Literal::Int(0)));
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 2);
    EXPECT_THAT(GetPaths(tasks), testing::UnorderedElementsAre("/path/to/data1.parquet",
                                                               "/path/to/data2.parquet"));
  }

  // Test 4: Filter matches no files (id=200 is outside both ranges)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder, TableScanBuilder::Make(metadata, file_io_));
    builder->Filter(Expressions::Equal("id", Literal::Int(200)));
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    EXPECT_TRUE(tasks.empty());
  }
}

TEST_P(TableScanTest, PlanFilesWithDeleteFiles) {
  auto version = GetParam();
  if (version < 2) {
    GTEST_SKIP() << "Delete files only supported in V2+";
  }

  constexpr int64_t kSnapshotId = 1000L;
  const auto part_value = PartitionValues({Literal::Int(0)});

  // Create data manifest with files
  std::vector<ManifestEntry> data_entries{
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                MakeDataFile("/path/to/data1.parquet", part_value,
                             partitioned_spec_->spec_id(), /*record_count=*/100)),
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                MakeDataFile("/path/to/data2.parquet", part_value,
                             partitioned_spec_->spec_id(), /*record_count=*/200))};
  auto data_manifest =
      WriteDataManifest(version, kSnapshotId, std::move(data_entries), partitioned_spec_);

  // Create delete manifest with position delete files
  std::vector<ManifestEntry> delete_entries{
      MakeEntry(
          ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/2,
          MakePositionDeleteFile("/path/to/pos_delete.parquet", part_value,
                                 partitioned_spec_->spec_id(), "/path/to/data1.parquet")),
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/2,
                MakeEqualityDeleteFile("/path/to/eq_delete.parquet", part_value,
                                       partitioned_spec_->spec_id(), {1}))};
  auto delete_manifest = WriteDeleteManifest(
      version, kSnapshotId, std::move(delete_entries), partitioned_spec_);
  std::string manifest_list_path = WriteManifestList(
      version, kSnapshotId, /*sequence_number=*/2, {data_manifest, delete_manifest});

  // Create a snapshot that references this manifest list
  auto timestamp_ms = TimePointMsFromUnixMs(1609459200000L);
  auto snapshot_with_manifests =
      std::make_shared<Snapshot>(Snapshot{.snapshot_id = kSnapshotId,
                                          .parent_snapshot_id = std::nullopt,
                                          .sequence_number = 2L,
                                          .timestamp_ms = timestamp_ms,
                                          .manifest_list = manifest_list_path,
                                          .summary = {},
                                          .schema_id = schema_->schema_id()});

  auto metadata_with_manifests = std::make_shared<TableMetadata>(
      TableMetadata{.format_version = 2,
                    .table_uuid = "test-table-uuid",
                    .location = "/tmp/table",
                    .last_sequence_number = 2L,
                    .last_updated_ms = timestamp_ms,
                    .last_column_id = 2,
                    .schemas = {schema_},
                    .current_schema_id = schema_->schema_id(),
                    .partition_specs = {partitioned_spec_, unpartitioned_spec_},
                    .default_spec_id = partitioned_spec_->spec_id(),
                    .last_partition_id = 1000,
                    .current_snapshot_id = kSnapshotId,
                    .snapshots = {snapshot_with_manifests},
                    .snapshot_log = {SnapshotLogEntry{.timestamp_ms = timestamp_ms,
                                                      .snapshot_id = kSnapshotId}},
                    .default_sort_order_id = 0,
                    .refs = {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                                          .snapshot_id = kSnapshotId,
                                          .retention = SnapshotRef::Branch{},
                                      })}}});

  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         TableScanBuilder::Make(metadata_with_manifests, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 2);
  EXPECT_THAT(GetPaths(tasks), testing::UnorderedElementsAre("/path/to/data1.parquet",
                                                             "/path/to/data2.parquet"));
  // Verify that delete files are associated with the tasks
  for (const auto& task : tasks) {
    EXPECT_GT(task->delete_files().size(), 0);
  }
}

TEST_P(TableScanTest, SchemaWithSelectedColumnsAndFilter) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(/*field_id=*/1, "id", int32()),
      SchemaField::MakeRequired(/*field_id=*/2, "data", string()),
      SchemaField::MakeRequired(/*field_id=*/3, "value", int64())});
  auto timestamp_ms = TimePointMsFromUnixMs(1609459200000L);
  auto metadata = std::make_shared<TableMetadata>(TableMetadata{
      .format_version = 2,
      .table_uuid = "test-table-uuid",
      .location = "/tmp/table",
      .last_sequence_number = 1L,
      .last_updated_ms = timestamp_ms,
      .last_column_id = 3,
      .schemas = {schema},
      .current_schema_id = schema->schema_id(),
      .partition_specs = {unpartitioned_spec_},
      .default_spec_id = unpartitioned_spec_->spec_id(),
      .last_partition_id = 1000,
      .current_snapshot_id = 1000L,
      .snapshots = {std::make_shared<Snapshot>(Snapshot{
          .snapshot_id = 1000L,
          .parent_snapshot_id = std::nullopt,
          .sequence_number = 1L,
          .timestamp_ms = timestamp_ms,
          .manifest_list = "/tmp/metadata/snap-1000-1-manifest-list.avro",
          .schema_id = schema->schema_id(),
      })},
      .snapshot_log = {SnapshotLogEntry{.timestamp_ms = timestamp_ms,
                                        .snapshot_id = 1000L}},
      .default_sort_order_id = 0,
      .refs = {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                            .snapshot_id = 1000L,
                            .retention = SnapshotRef::Branch{},
                        })}},
  });

  // Select "data" column, filter on "id" column
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder, TableScanBuilder::Make(metadata, file_io_));
    builder->Select({"data"}).Filter(Expressions::Equal("id", Literal::Int(42)));
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto projected_schema, scan->schema());

    ASSERT_EQ(projected_schema->fields().size(), 2);

    ICEBERG_UNWRAP_OR_FAIL(auto id_field, projected_schema->FindFieldByName("id"));
    EXPECT_TRUE(id_field.has_value());
    EXPECT_EQ(id_field->get().field_id(), 1);

    ICEBERG_UNWRAP_OR_FAIL(auto data_field, projected_schema->FindFieldByName("data"));
    EXPECT_TRUE(data_field.has_value());
    EXPECT_EQ(data_field->get().field_id(), 2);
  }

  // Select "id" and "value", filter on "data"
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder, TableScanBuilder::Make(metadata, file_io_));
    builder->Select({"id", "value"})
        .Filter(Expressions::Equal("data", Literal::String("test")));
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto projected_schema, scan->schema());

    ASSERT_EQ(projected_schema->fields().size(), 3);

    ICEBERG_UNWRAP_OR_FAIL(auto id_field, projected_schema->FindFieldByName("id"));
    EXPECT_TRUE(id_field.has_value());

    ICEBERG_UNWRAP_OR_FAIL(auto data_field, projected_schema->FindFieldByName("data"));
    EXPECT_TRUE(data_field.has_value());

    ICEBERG_UNWRAP_OR_FAIL(auto value_field, projected_schema->FindFieldByName("value"));
    EXPECT_TRUE(value_field.has_value());
  }

  // Select "id", filter on "id" - should only have "id" once
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder, TableScanBuilder::Make(metadata, file_io_));
    builder->Select({"id"}).Filter(Expressions::Equal("id", Literal::Int(42)));
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto projected_schema, scan->schema());

    ASSERT_EQ(projected_schema->fields().size(), 1);

    ICEBERG_UNWRAP_OR_FAIL(auto id_field, projected_schema->FindFieldByName("id"));
    EXPECT_TRUE(id_field.has_value());
    EXPECT_EQ(id_field->get().field_id(), 1);
  }

  // Select columns without filter
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder, TableScanBuilder::Make(metadata, file_io_));
    builder->Select({"data"});
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto projected_schema, scan->schema());

    ASSERT_EQ(projected_schema->fields().size(), 1);

    ICEBERG_UNWRAP_OR_FAIL(auto data_field, projected_schema->FindFieldByName("data"));
    EXPECT_TRUE(data_field.has_value());
    EXPECT_EQ(data_field->get().field_id(), 2);
  }
}

INSTANTIATE_TEST_SUITE_P(TableScanVersions, TableScanTest, testing::Values(1, 2, 3));

}  // namespace iceberg
