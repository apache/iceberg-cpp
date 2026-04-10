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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/expression/expressions.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/scan_test_base.h"

namespace iceberg {

namespace {

const std::string& TaskFilePath(const std::shared_ptr<ChangelogScanTask>& task) {
  if (auto added = std::dynamic_pointer_cast<AddedRowsScanTask>(task)) {
    return added->data_file()->file_path;
  }
  if (auto deleted = std::dynamic_pointer_cast<DeletedDataFileScanTask>(task)) {
    return deleted->data_file()->file_path;
  }

  static const std::string empty_path;
  return empty_path;
}

/// \brief Sort changelog scan tasks for deterministic ordering.
/// Sorts by change_ordinal, then by operation type name, then by file path.
template <typename TaskType>
void SortTasks(std::vector<std::shared_ptr<TaskType>>& tasks) {
  std::ranges::sort(tasks, [](const auto& t1, const auto& t2) {
    if (t1->change_ordinal() != t2->change_ordinal()) {
      return t1->change_ordinal() < t2->change_ordinal();
    }
    if (t1->operation() != t2->operation()) {
      return static_cast<uint8_t>(t1->operation()) <
             static_cast<uint8_t>(t2->operation());
    }
    return TaskFilePath(std::static_pointer_cast<ChangelogScanTask>(t1)) <
           TaskFilePath(std::static_pointer_cast<ChangelogScanTask>(t2));
  });
}

}  // namespace

class IncrementalChangelogScanTest : public ScanTestBase {};

TEST_P(IncrementalChangelogScanTest, DataFilters) {
  auto version = GetParam();

  // Bucket transform for "data" column: bucket("a", 16) = 8, bucket("k", 16) = 1
  // By filtering on data="k", we should only match files with bucket=1

  // Create partition value bucket("a") = 8
  auto partition_a = PartitionValues({Literal::Int(8)});
  // Create partition value bucket("k") = 1
  auto partition_b = PartitionValues({Literal::Int(1)});

  // Create snapshot 1 with file_a
  auto snapshot_a = MakeAppendSnapshotWithPartitionValues(
      version, 1000L, std::nullopt, 1L, {{"/path/to/file_a.parquet", partition_a}},
      partitioned_spec_);

  // Create snapshot 2 with file_b (separate manifest list, not inheriting from snap1)
  auto snapshot_b = MakeAppendSnapshotWithPartitionValues(
      version, 2000L, 1000L, 2L, {{"/path/to/file_b.parquet", partition_b}},
      partitioned_spec_);

  // Create metadata with partitioned spec as default
  auto partitioned_metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b}, 2000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 2000L, .retention = SnapshotRef::Branch{}})}},
      partitioned_spec_);

  // Filter by data="k" which should match only file_b (bucket("k", 16) = 1)
  ICEBERG_UNWRAP_OR_FAIL(auto builder, IncrementalChangelogScanBuilder::Make(
                                           partitioned_metadata, file_io_));
  builder->Filter(Expressions::Equal("data", Literal::String("k")));
  builder->ToSnapshot(2000L);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 1);

  auto t1 = tasks[0];
  EXPECT_EQ(t1->change_ordinal(), 1);
  EXPECT_EQ(t1->commit_snapshot_id(), 2000L);
  EXPECT_EQ(t1->operation(), ChangelogOperation::kInsert);
  auto insert_t1 = std::dynamic_pointer_cast<AddedRowsScanTask>(t1);
  ASSERT_NE(insert_t1, nullptr);
  EXPECT_EQ(insert_t1->data_file()->file_path, "/path/to/file_b.parquet");
  EXPECT_TRUE(insert_t1->delete_files().empty());
}

TEST_P(IncrementalChangelogScanTest, Overwrites) {
  auto version = GetParam();

  // Create initial snapshot with 2 files
  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L,
                         {"/path/to/file_a.parquet", "/path/to/file_b.parquet"});

  // Overwrite: add file_a2, delete file_a
  auto snapshot_b =
      MakeOverwriteSnapshot(version, 2000L, 1000L, 2L, {"/path/to/file_a2.parquet"},
                            {"/path/to/file_a.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b}, 2000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 2000L, .retention = SnapshotRef::Branch{}})}});

  // from_snapshot_exclusive(snap1).to_snapshot(snap2) should return 2 tasks
  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalChangelogScanBuilder::Make(metadata, file_io_));
  builder->FromSnapshot(1000L, /*inclusive=*/false).ToSnapshot(2000L);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 2);
  SortTasks(tasks);

  // First task: added file (INSERT operation)
  auto t1 = tasks[0];
  EXPECT_EQ(t1->change_ordinal(), 0);
  EXPECT_EQ(t1->commit_snapshot_id(), 2000L);
  EXPECT_EQ(t1->operation(), ChangelogOperation::kInsert);
  auto insert_t1 = std::dynamic_pointer_cast<AddedRowsScanTask>(t1);
  ASSERT_NE(insert_t1, nullptr);
  EXPECT_EQ(insert_t1->data_file()->file_path, "/path/to/file_a2.parquet");
  EXPECT_TRUE(insert_t1->delete_files().empty());

  // Second task: deleted file (DELETE operation)
  auto t2 = tasks[1];
  EXPECT_EQ(t2->change_ordinal(), 0);
  EXPECT_EQ(t2->commit_snapshot_id(), 2000L);
  EXPECT_EQ(t2->operation(), ChangelogOperation::kDelete);
  auto delete_t2 = std::dynamic_pointer_cast<DeletedDataFileScanTask>(t2);
  ASSERT_NE(delete_t2, nullptr);
  EXPECT_EQ(delete_t2->data_file()->file_path, "/path/to/file_a.parquet");
  EXPECT_TRUE(delete_t2->existing_deletes().empty());
}

TEST_P(IncrementalChangelogScanTest, DuplicatedManifests) {
  auto version = GetParam();

  // Create initial snapshot_a with file_a and extract its manifest
  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  SnapshotCache cache_a(snapshot_a.get());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests_a, cache_a.DataManifests(file_io_));
  ASSERT_EQ(manifests_a.size(), 1);
  auto manifest_a = manifests_a[0];

  // Manually construct snapshot_b that includes BOTH a new manifest AND the manifest from
  // snapshot_a. This simulates what actually happens in Iceberg when appends reuse older
  // manifests.
  std::vector<ManifestEntry> entries_b;
  auto file_b = MakeDataFile("/path/to/file_b.parquet");
  entries_b.push_back(MakeEntry(ManifestStatus::kAdded, 2000L, 2L, file_b));
  auto manifest_b = WriteDataManifest(version, 2000L, std::move(entries_b));

  auto manifest_list_b =
      WriteManifestList(version, 2000L, 1000L, 2L, {manifest_a, manifest_b});
  TimePointMs timestamp_ms = TimePointMsFromUnixMs(1609459200000L + 2000);
  auto snapshot_b = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 2000L,
      .parent_snapshot_id = 1000L,
      .sequence_number = 2L,
      .timestamp_ms = timestamp_ms,
      .manifest_list = manifest_list_b,
      .summary = {{"operation", "append"}},
      .schema_id = schema_->schema_id(),
  });

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b}, 2000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 2000L, .retention = SnapshotRef::Branch{}})}});

  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalChangelogScanBuilder::Make(metadata, file_io_));
  builder->ToSnapshot(2000L);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());

  // We expect exactly 2 tasks, one for file_a and one for file_b.
  // If deduplication is buggy, we might see 3 tasks (file_a twice).
  ASSERT_EQ(tasks.size(), 2);
  SortTasks(tasks);

  auto insert_t1 = std::dynamic_pointer_cast<AddedRowsScanTask>(tasks[0]);
  ASSERT_NE(insert_t1, nullptr);
  EXPECT_EQ(insert_t1->data_file()->file_path, "/path/to/file_a.parquet");
  EXPECT_EQ(tasks[0]->commit_snapshot_id(), 1000L);

  auto insert_t2 = std::dynamic_pointer_cast<AddedRowsScanTask>(tasks[1]);
  ASSERT_NE(insert_t2, nullptr);
  EXPECT_EQ(insert_t2->data_file()->file_path, "/path/to/file_b.parquet");
  EXPECT_EQ(tasks[1]->commit_snapshot_id(), 2000L);
}

TEST_P(IncrementalChangelogScanTest, FileDeletes) {
  auto version = GetParam();

  // Create initial snapshot with 2 files
  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L,
                         {"/path/to/file_a.parquet", "/path/to/file_b.parquet"});

  // Delete file_a
  auto snapshot_b =
      MakeDeleteSnapshot(version, 2000L, 1000L, 2L, {"/path/to/file_a.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b}, 2000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 2000L, .retention = SnapshotRef::Branch{}})}});

  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalChangelogScanBuilder::Make(metadata, file_io_));
  builder->FromSnapshot(1000L, /*inclusive=*/false).ToSnapshot(2000L);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 1);

  auto t1 = tasks[0];
  EXPECT_EQ(t1->change_ordinal(), 0);
  EXPECT_EQ(t1->commit_snapshot_id(), 2000L);
  EXPECT_EQ(t1->operation(), ChangelogOperation::kDelete);
  auto delete_t1 = std::dynamic_pointer_cast<DeletedDataFileScanTask>(t1);
  ASSERT_NE(delete_t1, nullptr);
  EXPECT_EQ(delete_t1->data_file()->file_path, "/path/to/file_a.parquet");
  EXPECT_TRUE(delete_t1->existing_deletes().empty());
}

TEST_P(IncrementalChangelogScanTest, ExistingEntriesInNewDataManifestsAreIgnored) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  auto snapshot_b =
      MakeAppendSnapshot(version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet"});

  // Create snapshot C with a manifest that contains existing entries from A and B
  // plus a new file C. This simulates manifest merging.
  std::vector<ManifestEntry> merged_entries;
  auto file_a = MakeDataFile("/path/to/file_a.parquet");
  merged_entries.push_back(MakeEntry(ManifestStatus::kExisting, 1000L, 1L, file_a));
  auto file_b = MakeDataFile("/path/to/file_b.parquet");
  merged_entries.push_back(MakeEntry(ManifestStatus::kExisting, 2000L, 2L, file_b));
  auto file_c = MakeDataFile("/path/to/file_c.parquet");
  merged_entries.push_back(MakeEntry(ManifestStatus::kAdded, 3000L, 3L, file_c));

  auto manifest = WriteDataManifest(version, 3000L, std::move(merged_entries));
  auto manifest_list = WriteManifestList(version, 3000L, 2000L, 3L, {manifest});
  TimePointMs timestamp_ms = TimePointMsFromUnixMs(1609459200000L + 3000);
  auto snapshot_c = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 3000L,
      .parent_snapshot_id = 2000L,
      .sequence_number = 3L,
      .timestamp_ms = timestamp_ms,
      .manifest_list = manifest_list,
      .summary = {{"operation", "append"}},
      .schema_id = schema_->schema_id(),
  });

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b, snapshot_c}, 3000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 3000L, .retention = SnapshotRef::Branch{}})}});

  // When scanning from_snapshot_inclusive(C).to_snapshot(C), should only return file_c
  // because file_a and file_b are marked as EXISTING entries
  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalChangelogScanBuilder::Make(metadata, file_io_));
  builder->FromSnapshot(3000L, /*inclusive=*/true).ToSnapshot(3000L);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 1);

  auto t1 = tasks[0];
  EXPECT_EQ(t1->change_ordinal(), 0);
  EXPECT_EQ(t1->commit_snapshot_id(), 3000L);
  EXPECT_EQ(t1->operation(), ChangelogOperation::kInsert);
  auto insert_t1 = std::dynamic_pointer_cast<AddedRowsScanTask>(t1);
  ASSERT_NE(insert_t1, nullptr);
  EXPECT_EQ(insert_t1->data_file()->file_path, "/path/to/file_c.parquet");
  EXPECT_TRUE(insert_t1->delete_files().empty());
}

TEST_P(IncrementalChangelogScanTest, DataFileRewrites) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  auto snapshot_b =
      MakeAppendSnapshot(version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet"});

  // Create a rewrite/replace snapshot (simulating file compaction)
  // In a replace operation, the old file is deleted and new file is added,
  // but this shouldn't appear in the changelog
  std::vector<ManifestEntry> entries;
  auto file_a2 = MakeDataFile("/path/to/file_a2.parquet");
  entries.push_back(MakeEntry(ManifestStatus::kAdded, 3000L, 3L, file_a2));
  auto file_a = MakeDataFile("/path/to/file_a.parquet");
  entries.push_back(MakeEntry(ManifestStatus::kDeleted, 3000L, 3L, file_a));
  auto manifest = WriteDataManifest(version, 3000L, std::move(entries));
  auto manifest_list = WriteManifestList(version, 3000L, 2000L, 3L, {manifest});
  TimePointMs timestamp_ms = TimePointMsFromUnixMs(1609459200000L + 3000);
  auto snapshot_c = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 3000L,
      .parent_snapshot_id = 2000L,
      .sequence_number = 3L,
      .timestamp_ms = timestamp_ms,
      .manifest_list = manifest_list,
      .summary = {{"operation", "replace"}},  // Replace operation
      .schema_id = schema_->schema_id(),
  });

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b, snapshot_c}, 3000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 3000L, .retention = SnapshotRef::Branch{}})}});

  // The changelog should only show the original appends (A and B),
  // not the replace operation
  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalChangelogScanBuilder::Make(metadata, file_io_));
  builder->ToSnapshot(3000L);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 2);
  SortTasks(tasks);

  auto t1 = tasks[0];
  EXPECT_EQ(t1->change_ordinal(), 0);
  EXPECT_EQ(t1->commit_snapshot_id(), 1000L);
  EXPECT_EQ(t1->operation(), ChangelogOperation::kInsert);
  auto insert_t1 = std::dynamic_pointer_cast<AddedRowsScanTask>(t1);
  ASSERT_NE(insert_t1, nullptr);
  EXPECT_EQ(insert_t1->data_file()->file_path, "/path/to/file_a.parquet");

  auto t2 = tasks[1];
  EXPECT_EQ(t2->change_ordinal(), 1);
  EXPECT_EQ(t2->commit_snapshot_id(), 2000L);
  EXPECT_EQ(t2->operation(), ChangelogOperation::kInsert);
  auto insert_t2 = std::dynamic_pointer_cast<AddedRowsScanTask>(t2);
  ASSERT_NE(insert_t2, nullptr);
  EXPECT_EQ(insert_t2->data_file()->file_path, "/path/to/file_b.parquet");
}

TEST_P(IncrementalChangelogScanTest, ManifestRewritesAreIgnored) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  auto snapshot_b =
      MakeAppendSnapshot(version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet"});

  // Simulate a manifest rewrite (snapshot 3) - creates a new manifest with EXISTING
  // entries This simulates what happens after table.rewriteManifests()
  std::vector<ManifestEntry> rewritten_entries;
  auto file_a = MakeDataFile("/path/to/file_a.parquet");
  rewritten_entries.push_back(MakeEntry(ManifestStatus::kExisting, 1000L, 1L, file_a));
  auto file_b = MakeDataFile("/path/to/file_b.parquet");
  rewritten_entries.push_back(MakeEntry(ManifestStatus::kExisting, 2000L, 2L, file_b));

  auto rewritten_manifest =
      WriteDataManifest(version, 3000L, std::move(rewritten_entries));
  auto manifest_list_3 =
      WriteManifestList(version, 3000L, 2000L, 3L, {rewritten_manifest});
  TimePointMs timestamp_3 = TimePointMsFromUnixMs(1609459200000L + 3000);
  auto snapshot_c = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 3000L,
      .parent_snapshot_id = 2000L,
      .sequence_number = 3L,
      .timestamp_ms = timestamp_3,
      .manifest_list = manifest_list_3,
      .summary = {{"operation", "replace"}},  // Manifest rewrite uses replace operation
      .schema_id = schema_->schema_id(),
  });

  // Add a new append after the rewrite
  auto snapshot_d =
      MakeAppendSnapshot(version, 4000L, 3000L, 4L, {"/path/to/file_c.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b, snapshot_c, snapshot_d}, 4000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 4000L, .retention = SnapshotRef::Branch{}})}});

  // The changelog should show all 3 files from the original appends,
  // ignoring the manifest rewrite snapshot
  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalChangelogScanBuilder::Make(metadata, file_io_));
  builder->ToSnapshot(4000L);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 3);
  SortTasks(tasks);

  auto t1 = tasks[0];
  EXPECT_EQ(t1->change_ordinal(), 0);
  EXPECT_EQ(t1->commit_snapshot_id(), 1000L);
  EXPECT_EQ(t1->operation(), ChangelogOperation::kInsert);
  auto insert_t1 = std::dynamic_pointer_cast<AddedRowsScanTask>(t1);
  ASSERT_NE(insert_t1, nullptr);
  EXPECT_EQ(insert_t1->data_file()->file_path, "/path/to/file_a.parquet");

  auto t2 = tasks[1];
  EXPECT_EQ(t2->change_ordinal(), 1);
  EXPECT_EQ(t2->commit_snapshot_id(), 2000L);
  EXPECT_EQ(t2->operation(), ChangelogOperation::kInsert);
  auto insert_t2 = std::dynamic_pointer_cast<AddedRowsScanTask>(t2);
  ASSERT_NE(insert_t2, nullptr);
  EXPECT_EQ(insert_t2->data_file()->file_path, "/path/to/file_b.parquet");

  auto t3 = tasks[2];
  EXPECT_EQ(t3->change_ordinal(), 2);
  EXPECT_EQ(t3->commit_snapshot_id(), 4000L);
  EXPECT_EQ(t3->operation(), ChangelogOperation::kInsert);
  auto insert_t3 = std::dynamic_pointer_cast<AddedRowsScanTask>(t3);
  ASSERT_NE(insert_t3, nullptr);
  EXPECT_EQ(insert_t3->data_file()->file_path, "/path/to/file_c.parquet");
}

TEST_P(IncrementalChangelogScanTest, DeleteFilesAreNotSupported) {
  auto version = GetParam();
  if (version < 2) {
    GTEST_SKIP() << "Delete files only exist in format version 2+";
  }

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L,
                         {"/path/to/file_a.parquet", "/path/to/file_b.parquet"});

  // Create a snapshot with delete files (positional deletes)
  // This simulates table.newRowDelta().addDeletes(FILE_A_DELETES).commit()
  std::vector<ManifestEntry> data_entries;
  auto file_a = MakeDataFile("/path/to/file_a.parquet");
  data_entries.push_back(MakeEntry(ManifestStatus::kExisting, 1000L, 1L, file_a));
  auto file_b = MakeDataFile("/path/to/file_b.parquet");
  data_entries.push_back(MakeEntry(ManifestStatus::kExisting, 1000L, 1L, file_b));
  auto data_manifest = WriteDataManifest(version, 2000L, std::move(data_entries));

  // Create a delete file entry
  auto delete_file = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "/path/to/file_a_deletes.parquet",
      .file_format = FileFormatType::kParquet,
      .partition = PartitionValues(std::vector<Literal>{}),
      .record_count = 1,
      .file_size_in_bytes = 10,
      .sort_order_id = 0,
      .partition_spec_id = unpartitioned_spec_->spec_id(),
  });
  std::vector<ManifestEntry> delete_entries;
  delete_entries.push_back(MakeEntry(ManifestStatus::kAdded, 2000L, 2L, delete_file));
  auto delete_manifest =
      WriteDeleteManifest(version, 2000L, std::move(delete_entries), unpartitioned_spec_);

  auto manifest_list =
      WriteManifestList(version, 2000L, 1000L, 2L, {data_manifest, delete_manifest});
  TimePointMs timestamp_ms = TimePointMsFromUnixMs(1609459200000L + 2000);
  auto snapshot_b = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 2000L,
      .parent_snapshot_id = 1000L,
      .sequence_number = 2L,
      .timestamp_ms = timestamp_ms,
      .manifest_list = manifest_list,
      .summary = {{"operation", "delete"}},
      .schema_id = schema_->schema_id(),
  });

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b}, 2000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 2000L, .retention = SnapshotRef::Branch{}})}});

  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalChangelogScanBuilder::Make(metadata, file_io_));
  builder->ToSnapshot(2000L);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  EXPECT_THAT(scan->PlanFiles(),
              ::testing::AllOf(
                  IsError(ErrorKind::kNotSupported),
                  HasErrorMessage(
                      "Delete files are currently not supported in changelog scans")));
}

INSTANTIATE_TEST_SUITE_P(IncrementalChangelogScanVersions, IncrementalChangelogScanTest,
                         testing::Values(1, 2, 3));

}  // namespace iceberg
