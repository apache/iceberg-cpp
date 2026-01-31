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

#include "iceberg/manifest/manifest_group.h"

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
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

class ManifestGroupTest : public testing::TestWithParam<int8_t> {
 protected:
  void SetUp() override {
    avro::RegisterAll();

    file_io_ = arrow::MakeMockFileIO();

    // Schema with id and data fields
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(/*field_id=*/1, "id", int32()),
        SchemaField::MakeRequired(/*field_id=*/2, "data", string())});

    // Partitioned spec: bucket by data
    ICEBERG_UNWRAP_OR_FAIL(
        partitioned_spec_,
        PartitionSpec::Make(
            /*spec_id=*/1, {PartitionField(/*source_id=*/2, /*field_id=*/1000,
                                           "data_bucket_16_2", Transform::Bucket(16))}));

    // Unpartitioned spec
    unpartitioned_spec_ = PartitionSpec::Unpartitioned();
  }

  std::string MakeManifestPath() {
    static int counter = 0;
    return std::format("manifest-{}-{}.avro", counter++,
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& path,
                                         const PartitionValues& partition,
                                         int32_t spec_id, int64_t record_count = 1) {
    return std::make_shared<DataFile>(DataFile{
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = record_count,
        .file_size_in_bytes = 10,
        .sort_order_id = 0,
        .partition_spec_id = spec_id,
    });
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

    auto writer_result =
        ManifestWriter::MakeWriter(format_version, snapshot_id, manifest_path, file_io_,
                                   spec, schema_, ManifestContent::kData,
                                   /*first_row_id=*/0L);
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
                                   spec, schema_, ManifestContent::kDeletes,
                                   /*first_row_id=*/std::nullopt);
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

  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> GetSpecsById() {
    return {{partitioned_spec_->spec_id(), partitioned_spec_},
            {unpartitioned_spec_->spec_id(), unpartitioned_spec_}};
  }

  std::string MakeManifestListPath() {
    static int counter = 0;
    return std::format("manifest-list-{}-{}.avro", counter++,
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  // Write a ManifestFile to a manifest list and read it back. This is useful for v1
  // to populate all missing fields like sequence_number.
  ManifestFile WriteAndReadManifestListEntry(int8_t format_version, int64_t snapshot_id,
                                             int64_t sequence_number,
                                             const ManifestFile& manifest) {
    const std::string manifest_list_path = MakeManifestListPath();
    constexpr int64_t kParentSnapshotId = 0L;
    constexpr int64_t kSnapshotFirstRowId = 0L;

    auto writer_result = ManifestListWriter::MakeWriter(
        format_version, snapshot_id, kParentSnapshotId, manifest_list_path, file_io_,
        sequence_number, kSnapshotFirstRowId);
    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());
    EXPECT_THAT(writer->Add(manifest), IsOk());
    EXPECT_THAT(writer->Close(), IsOk());

    auto reader_result = ManifestListReader::Make(manifest_list_path, file_io_);
    EXPECT_THAT(reader_result, IsOk());
    auto reader = std::move(reader_result.value());
    auto files_result = reader->Files();
    EXPECT_THAT(files_result, IsOk());

    auto manifests = files_result.value();
    EXPECT_EQ(manifests.size(), 1);
    return manifests[0];
  }

  static std::vector<std::string> GetPaths(
      const std::vector<std::shared_ptr<FileScanTask>>& tasks) {
    return tasks | std::views::transform([](const auto& task) {
             return task->data_file()->file_path;
           }) |
           std::ranges::to<std::vector<std::string>>();
  }

  static std::vector<std::string> GetEntryPaths(
      const std::vector<ManifestEntry>& entries) {
    return entries | std::views::transform([](const auto& entry) {
             return entry.data_file->file_path;
           }) |
           std::ranges::to<std::vector<std::string>>();
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> partitioned_spec_;
  std::shared_ptr<PartitionSpec> unpartitioned_spec_;
};

TEST_P(ManifestGroupTest, CreateAndGetEntries) {
  auto version = GetParam();
  if (version < 2) {
    GTEST_SKIP() << "Delete files only supported in V2+";
  }

  constexpr int64_t kSnapshotId = 1000L;
  const auto part_value = PartitionValues({Literal::Int(0)});

  // Create data manifests
  std::vector<ManifestEntry> data_entries{
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                MakeDataFile("/path/to/data1.parquet", part_value,
                             partitioned_spec_->spec_id())),
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                MakeDataFile("/path/to/data2.parquet", part_value,
                             partitioned_spec_->spec_id()))};
  auto data_manifest =
      WriteDataManifest(version, kSnapshotId, std::move(data_entries), partitioned_spec_);

  // Create delete manifest
  std::vector<ManifestEntry> delete_entries{
      MakeEntry(ManifestStatus::kAdded, kSnapshotId,
                /*sequence_number=*/2,
                MakePositionDeleteFile("/path/to/delete.parquet", part_value,
                                       partitioned_spec_->spec_id()))};
  auto delete_manifest = WriteDeleteManifest(
      version, kSnapshotId, std::move(delete_entries), partitioned_spec_);

  // Create ManifestGroup with pre-separated manifests
  std::vector<ManifestFile> data_manifests = {data_manifest};
  std::vector<ManifestFile> delete_manifests = {delete_manifest};
  ICEBERG_UNWRAP_OR_FAIL(
      auto group,
      ManifestGroup::Make(file_io_, schema_, GetSpecsById(), std::move(data_manifests),
                          std::move(delete_manifests)));

  // Verify Entries() returns only data file entries
  ICEBERG_UNWRAP_OR_FAIL(auto entries, group->Entries());
  ASSERT_EQ(entries.size(), 2);
  EXPECT_THAT(
      GetEntryPaths(entries),
      testing::UnorderedElementsAre("/path/to/data1.parquet", "/path/to/data2.parquet"));

  // Verify PlanFiles returns data files with associated delete files
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, group->PlanFiles());
  ASSERT_EQ(tasks.size(), 2);
  EXPECT_THAT(GetPaths(tasks), testing::UnorderedElementsAre("/path/to/data1.parquet",
                                                             "/path/to/data2.parquet"));
  EXPECT_EQ(tasks[0]->delete_files().size(), 1);
  EXPECT_EQ(tasks[1]->delete_files()[0]->file_path, "/path/to/delete.parquet");
  EXPECT_EQ(tasks[1]->delete_files().size(), 1);
  EXPECT_EQ(tasks[1]->delete_files()[0]->file_path, "/path/to/delete.parquet");
}

TEST_P(ManifestGroupTest, IgnoreDeleted) {
  auto version = GetParam();

  constexpr int64_t kSnapshotId = 1000L;
  constexpr int64_t kSequenceNumber = 1L;
  const auto part_value = PartitionValues({Literal::Int(0)});

  // Create data manifest with ADDED and DELETED entries
  std::vector<ManifestEntry> data_entries{
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, kSequenceNumber,
                MakeDataFile("/path/to/added.parquet", part_value,
                             partitioned_spec_->spec_id())),
      MakeEntry(ManifestStatus::kDeleted, kSnapshotId, kSequenceNumber,
                MakeDataFile("/path/to/deleted.parquet", part_value,
                             partitioned_spec_->spec_id())),
      MakeEntry(ManifestStatus::kExisting, kSnapshotId, kSequenceNumber,
                MakeDataFile("/path/to/existing.parquet", part_value,
                             partitioned_spec_->spec_id()))};
  auto data_manifest =
      WriteDataManifest(version, kSnapshotId, std::move(data_entries), partitioned_spec_);
  auto read_back_manifest =
      WriteAndReadManifestListEntry(version, kSnapshotId, kSequenceNumber, data_manifest);

  // Create ManifestGroup with IgnoreDeleted
  std::vector<ManifestFile> manifests = {read_back_manifest};
  ICEBERG_UNWRAP_OR_FAIL(
      auto group,
      ManifestGroup::Make(file_io_, schema_, GetSpecsById(), std::move(manifests)));
  group->IgnoreDeleted();

  // Plan files - should only return ADDED and EXISTING
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, group->PlanFiles());
  ASSERT_EQ(tasks.size(), 2);
  EXPECT_THAT(GetPaths(tasks),
              testing::UnorderedElementsAre("/path/to/added.parquet",
                                            "/path/to/existing.parquet"));
}

TEST_P(ManifestGroupTest, IgnoreExisting) {
  auto version = GetParam();

  constexpr int64_t kSnapshotId = 1000L;
  constexpr int64_t kSequenceNumber = 1L;
  const auto part_value = PartitionValues({Literal::Int(0)});

  // Create data manifest with ADDED and EXISTING entries
  std::vector<ManifestEntry> data_entries{
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, kSequenceNumber,
                MakeDataFile("/path/to/added.parquet", part_value,
                             partitioned_spec_->spec_id())),
      MakeEntry(ManifestStatus::kExisting, kSnapshotId, kSequenceNumber,
                MakeDataFile("/path/to/existing.parquet", part_value,
                             partitioned_spec_->spec_id())),
      MakeEntry(ManifestStatus::kDeleted, kSnapshotId, kSequenceNumber,
                MakeDataFile("/path/to/deleted.parquet", part_value,
                             partitioned_spec_->spec_id()))};

  auto data_manifest =
      WriteDataManifest(version, kSnapshotId, std::move(data_entries), partitioned_spec_);
  auto read_back_manifest =
      WriteAndReadManifestListEntry(version, kSnapshotId, kSequenceNumber, data_manifest);

  // Create ManifestGroup with IgnoreExisting
  std::vector<ManifestFile> manifests = {read_back_manifest};
  ICEBERG_UNWRAP_OR_FAIL(
      auto group,
      ManifestGroup::Make(file_io_, schema_, GetSpecsById(), std::move(manifests)));
  group->IgnoreExisting();

  // Plan files - should only return ADDED
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, group->PlanFiles());
  ASSERT_EQ(tasks.size(), 2);
  EXPECT_THAT(GetPaths(tasks), testing::UnorderedElementsAre("/path/to/added.parquet",
                                                             "/path/to/deleted.parquet"));
}

TEST_P(ManifestGroupTest, CustomManifestEntriesFilter) {
  auto version = GetParam();

  constexpr int64_t kSnapshotId = 1000L;
  const auto part_value = PartitionValues({Literal::Int(0)});

  // Create data manifest with multiple files
  std::vector<ManifestEntry> data_entries{
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                MakeDataFile("/path/to/data1.parquet", part_value,
                             partitioned_spec_->spec_id())),
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                MakeDataFile("/path/to/data2.parquet", part_value,
                             partitioned_spec_->spec_id())),
      MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                MakeDataFile("/path/to/data3.parquet", part_value,
                             partitioned_spec_->spec_id()))};
  auto data_manifest =
      WriteDataManifest(version, kSnapshotId, std::move(data_entries), partitioned_spec_);

  // Create ManifestGroup with custom entry filter
  std::vector<ManifestFile> manifests = {data_manifest};
  ICEBERG_UNWRAP_OR_FAIL(
      auto group,
      ManifestGroup::Make(file_io_, schema_, GetSpecsById(), std::move(manifests)));
  group->FilterManifestEntries([](const ManifestEntry& entry) {
    // Only include files with "data1" or "data3" in the path
    return entry.data_file->file_path.find("data1") != std::string::npos ||
           entry.data_file->file_path.find("data3") != std::string::npos;
  });

  // Plan files - should only return filtered entries
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, group->PlanFiles());
  ASSERT_EQ(tasks.size(), 2);
  EXPECT_THAT(GetPaths(tasks), testing::UnorderedElementsAre("/path/to/data1.parquet",
                                                             "/path/to/data3.parquet"));
}

TEST_P(ManifestGroupTest, EmptyManifestGroup) {
  std::vector<ManifestFile> manifests;
  ICEBERG_UNWRAP_OR_FAIL(
      auto group,
      ManifestGroup::Make(file_io_, schema_, GetSpecsById(), std::move(manifests)));

  ICEBERG_UNWRAP_OR_FAIL(auto tasks, group->PlanFiles());
  EXPECT_TRUE(tasks.empty());

  ICEBERG_UNWRAP_OR_FAIL(auto entries, group->Entries());
  EXPECT_TRUE(entries.empty());
}

TEST_P(ManifestGroupTest, MultipleDataManifests) {
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
      ManifestStatus::kAdded, /*snapshot_id=*/1001L, /*sequence_number=*/2,
      MakeDataFile("/path/to/data2.parquet", partition_b, partitioned_spec_->spec_id()))};
  auto data_manifest_2 = WriteDataManifest(version, /*snapshot_id=*/1001L,
                                           std::move(data_entries_2), partitioned_spec_);

  // Create ManifestGroup with multiple manifests
  std::vector<ManifestFile> manifests = {data_manifest_1, data_manifest_2};
  ICEBERG_UNWRAP_OR_FAIL(
      auto group,
      ManifestGroup::Make(file_io_, schema_, GetSpecsById(), std::move(manifests)));

  // Plan files - should return files from both manifests
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, group->PlanFiles());
  ASSERT_EQ(tasks.size(), 2);
  EXPECT_THAT(GetPaths(tasks), testing::UnorderedElementsAre("/path/to/data1.parquet",
                                                             "/path/to/data2.parquet"));
}

TEST_P(ManifestGroupTest, PartitionFilter) {
  auto version = GetParam();

  // Create two files with different partition values (bucket 0 and bucket 1)
  const auto partition_bucket_0 = PartitionValues({Literal::Int(0)});
  const auto partition_bucket_1 = PartitionValues({Literal::Int(1)});

  // Create data manifest with two entries in different partitions
  std::vector<ManifestEntry> data_entries{
      MakeEntry(ManifestStatus::kAdded, /*snapshot_id=*/1000L, /*sequence_number=*/1,
                MakeDataFile("/path/to/bucket0.parquet", partition_bucket_0,
                             partitioned_spec_->spec_id())),
      MakeEntry(ManifestStatus::kAdded, /*snapshot_id=*/1000L, /*sequence_number=*/1,
                MakeDataFile("/path/to/bucket1.parquet", partition_bucket_1,
                             partitioned_spec_->spec_id()))};
  auto data_manifest = WriteDataManifest(version, /*snapshot_id=*/1000L,
                                         std::move(data_entries), partitioned_spec_);

  // Create ManifestGroup with partition filter for bucket 0
  std::vector<ManifestFile> manifests = {data_manifest};
  ICEBERG_UNWRAP_OR_FAIL(
      auto group,
      ManifestGroup::Make(file_io_, schema_, GetSpecsById(), std::move(manifests)));

  // Filter on partition field name (data_bucket_16_2 = 0)
  auto partition_filter = Expressions::Equal("data_bucket_16_2", Literal::Int(0));
  group->FilterPartitions(std::move(partition_filter));

  // Plan files - should only return the file in bucket 0
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, group->PlanFiles());
  ASSERT_EQ(tasks.size(), 1);
  EXPECT_THAT(GetPaths(tasks), testing::ElementsAre("/path/to/bucket0.parquet"));
}

INSTANTIATE_TEST_SUITE_P(ManifestGroupVersions, ManifestGroupTest,
                         testing::Values(1, 2, 3));

}  // namespace iceberg
