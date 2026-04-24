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

#include "iceberg/update/expire_snapshots.h"

#include <optional>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/statistics_file.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"

namespace iceberg {

class ExpireSnapshotsTest : public UpdateTestBase {};

class ExpireSnapshotsCleanupTest : public UpdateTestBase {
 protected:
  static constexpr int64_t kExpiredSnapshotId = 3051729675574597004;
  static constexpr int64_t kCurrentSnapshotId = 3055729675574597004;
  static constexpr int64_t kExpiredSequenceNumber = 0;
  static constexpr int64_t kCurrentSequenceNumber = 1;

  void SetUp() override {
    UpdateTestBase::SetUp();
    avro::RegisterAll();
  }

  ManifestEntry MakeEntry(ManifestStatus status, int64_t snapshot_id,
                          int64_t sequence_number, std::shared_ptr<DataFile> file) const {
    return ManifestEntry{
        .status = status,
        .snapshot_id = snapshot_id,
        .sequence_number = sequence_number,
        .file_sequence_number = sequence_number,
        .data_file = std::move(file),
    };
  }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& path) const {
    return std::make_shared<DataFile>(DataFile{
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = PartitionValues({Literal::Long(1)}),
        .record_count = 1,
        .file_size_in_bytes = 10,
        .sort_order_id = 0,
    });
  }

  std::shared_ptr<DataFile> MakePositionDeleteFile(const std::string& path) const {
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kPositionDeletes,
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = PartitionValues({Literal::Long(1)}),
        .record_count = 1,
        .file_size_in_bytes = 10,
    });
  }

  std::shared_ptr<StatisticsFile> MakeStatisticsFile(int64_t snapshot_id,
                                                     const std::string& path) const {
    auto statistics_file = std::make_shared<StatisticsFile>();
    statistics_file->snapshot_id = snapshot_id;
    statistics_file->path = path;
    statistics_file->file_size_in_bytes = 10;
    statistics_file->file_footer_size_in_bytes = 2;
    return statistics_file;
  }

  std::shared_ptr<PartitionStatisticsFile> MakePartitionStatisticsFile(
      int64_t snapshot_id, const std::string& path) const {
    auto statistics_file = std::make_shared<PartitionStatisticsFile>();
    statistics_file->snapshot_id = snapshot_id;
    statistics_file->path = path;
    statistics_file->file_size_in_bytes = 10;
    return statistics_file;
  }

  std::shared_ptr<Schema> CurrentSchema() {
    auto metadata = ReloadMetadata();
    auto schema_result = metadata->Schema();
    EXPECT_THAT(schema_result, IsOk());
    return schema_result.value();
  }

  std::shared_ptr<PartitionSpec> DefaultSpec() {
    auto metadata = ReloadMetadata();
    auto spec_result = metadata->PartitionSpecById(metadata->default_spec_id);
    EXPECT_THAT(spec_result, IsOk());
    return spec_result.value();
  }

  int8_t FormatVersion() { return ReloadMetadata()->format_version; }

  ManifestFile WriteDataManifest(const std::string& path, int64_t snapshot_id,
                                 std::vector<ManifestEntry> entries) {
    auto writer_result = ManifestWriter::MakeWriter(
        FormatVersion(), snapshot_id, path, file_io_, DefaultSpec(), CurrentSchema(),
        ManifestContent::kData, /*first_row_id=*/std::nullopt);
    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    for (const auto& entry : entries) {
      EXPECT_THAT(writer->WriteEntry(entry), IsOk());
    }

    EXPECT_THAT(writer->Close(), IsOk());
    auto manifest_result = writer->ToManifestFile();
    EXPECT_THAT(manifest_result, IsOk());
    return manifest_result.value();
  }

  ManifestFile WriteDeleteManifest(const std::string& path, int64_t snapshot_id,
                                   std::vector<ManifestEntry> entries) {
    auto writer_result = ManifestWriter::MakeWriter(
        FormatVersion(), snapshot_id, path, file_io_, DefaultSpec(), CurrentSchema(),
        ManifestContent::kDeletes, /*first_row_id=*/std::nullopt);
    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    for (const auto& entry : entries) {
      EXPECT_THAT(writer->WriteEntry(entry), IsOk());
    }

    EXPECT_THAT(writer->Close(), IsOk());
    auto manifest_result = writer->ToManifestFile();
    EXPECT_THAT(manifest_result, IsOk());
    return manifest_result.value();
  }

  std::string WriteManifestList(const std::string& path, int64_t snapshot_id,
                                int64_t parent_snapshot_id, int64_t sequence_number,
                                const std::vector<ManifestFile>& manifests) {
    auto writer_result = ManifestListWriter::MakeWriter(
        FormatVersion(), snapshot_id, parent_snapshot_id, path, file_io_,
        /*sequence_number=*/std::optional<int64_t>(sequence_number),
        /*first_row_id=*/std::nullopt);
    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());
    EXPECT_THAT(writer->AddAll(manifests), IsOk());
    EXPECT_THAT(writer->Close(), IsOk());
    return path;
  }

  void RewriteTableWithManifestLists(const std::string& expired_manifest_list,
                                     const std::string& current_manifest_list) {
    auto metadata = ReloadMetadata();
    ASSERT_EQ(metadata->snapshots.size(), 2);
    metadata->snapshots.at(0)->manifest_list = expired_manifest_list;
    metadata->snapshots.at(1)->manifest_list = current_manifest_list;

    RewriteTable(std::move(metadata));
  }

  void RewriteTable(std::shared_ptr<TableMetadata> metadata) {
    ASSERT_NE(metadata, nullptr);

    const auto metadata_location =
        table_location_ + "/metadata/00002-custom.metadata.json";
    ASSERT_THAT(catalog_->DropTable(table_ident_, /*purge=*/false), IsOk());
    ASSERT_THAT(TableMetadataUtil::Write(*file_io_, metadata_location, *metadata),
                IsOk());
    ICEBERG_UNWRAP_OR_FAIL(table_,
                           catalog_->RegisterTable(table_ident_, metadata_location));
  }

  void RewriteTableWithManifestListsAndStatistics(
      const std::string& expired_manifest_list, const std::string& current_manifest_list,
      std::vector<std::shared_ptr<StatisticsFile>> statistics,
      std::vector<std::shared_ptr<PartitionStatisticsFile>> partition_statistics) {
    auto metadata = ReloadMetadata();
    ASSERT_EQ(metadata->snapshots.size(), 2);
    metadata->snapshots.at(0)->manifest_list = expired_manifest_list;
    metadata->snapshots.at(1)->manifest_list = current_manifest_list;
    metadata->statistics = std::move(statistics);
    metadata->partition_statistics = std::move(partition_statistics);

    RewriteTable(std::move(metadata));
  }
};

TEST_F(ExpireSnapshotsTest, DefaultExpireByAge) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);
  EXPECT_EQ(result.snapshot_ids_to_remove.at(0), 3051729675574597004);
}

TEST_F(ExpireSnapshotsTest, KeepAll) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->RetainLast(2);
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.snapshot_ids_to_remove.empty());
  EXPECT_TRUE(result.refs_to_remove.empty());
}

TEST_F(ExpireSnapshotsTest, ExpireById) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->ExpireSnapshotId(3051729675574597004);
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);
  EXPECT_EQ(result.snapshot_ids_to_remove.at(0), 3051729675574597004);
}

TEST_F(ExpireSnapshotsTest, ExpireOlderThan) {
  struct TestCase {
    int64_t expire_older_than;
    size_t expected_num_expired;
  };
  const std::vector<TestCase> test_cases = {
      {.expire_older_than = 1515100955770 - 1, .expected_num_expired = 0},
      {.expire_older_than = 1515100955770 + 1, .expected_num_expired = 1}};
  for (const auto& test_case : test_cases) {
    ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
    update->ExpireOlderThan(test_case.expire_older_than);
    ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
    EXPECT_EQ(result.snapshot_ids_to_remove.size(), test_case.expected_num_expired);
  }
}

TEST_F(ExpireSnapshotsTest, FinalizeRequiresCommittedMetadata) {
  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  // Apply first so apply_result_ is cached
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);

  // A successful finalize now requires the committed metadata from the catalog.
  auto finalize_status = update->Finalize(static_cast<const TableMetadata*>(nullptr));
  EXPECT_THAT(finalize_status, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(finalize_status,
              HasErrorMessage("Missing committed table metadata for cleanup"));
  EXPECT_TRUE(deleted_files.empty());
}

TEST_F(ExpireSnapshotsTest, CleanupNoneSkipsDeletion) {
  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->CleanupLevel(CleanupLevel::kNone);
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);

  // With kNone cleanup level, Finalize should skip all file deletion
  auto finalize_status = update->Finalize(static_cast<const TableMetadata*>(nullptr));
  EXPECT_THAT(finalize_status, IsOk());
  EXPECT_TRUE(deleted_files.empty());
}

TEST_F(ExpireSnapshotsTest, FinalizeSkippedOnCommitError) {
  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);

  // Simulate a commit failure - Finalize should not delete any files
  auto finalize_status = update->Finalize(Result<const TableMetadata*>(std::unexpected(
      Error{.kind = ErrorKind::kCommitFailed, .message = "simulated failure"})));
  EXPECT_THAT(finalize_status, IsOk());
  EXPECT_TRUE(deleted_files.empty());
}

TEST_F(ExpireSnapshotsTest, FinalizeSkipsWhenNothingExpired) {
  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->RetainLast(2);
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.snapshot_ids_to_remove.empty());

  // No snapshots expired, so Finalize should not delete any files
  auto finalize_status = update->Finalize(static_cast<const TableMetadata*>(nullptr));
  EXPECT_THAT(finalize_status, IsOk());
  EXPECT_TRUE(deleted_files.empty());
}

TEST_F(ExpireSnapshotsTest, CommitWithCleanupNone) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->CleanupLevel(CleanupLevel::kNone);

  // Commit should succeed - Finalize is called internally but skips cleanup
  EXPECT_THAT(update->Commit(), IsOk());

  // Verify snapshot was removed from metadata
  auto metadata = ReloadMetadata();
  EXPECT_EQ(metadata->snapshots.size(), 1);
  EXPECT_EQ(metadata->snapshots.at(0)->snapshot_id, 3055729675574597004);
}

TEST_F(ExpireSnapshotsCleanupTest, IgnoresExpiredDeleteManifestReadFailures) {
  const auto expired_data_file_path = table_location_ + "/data/expired-data.parquet";
  const auto expired_delete_file_path = table_location_ + "/data/expired-delete.parquet";
  const auto expired_data_manifest_path = table_location_ + "/metadata/expired-data.avro";
  const auto expired_delete_manifest_path =
      table_location_ + "/metadata/expired-delete.avro";
  const auto expired_manifest_list_path =
      table_location_ + "/metadata/expired-manifest-list.avro";
  const auto current_manifest_list_path =
      table_location_ + "/metadata/current-manifest-list.avro";

  auto expired_data_manifest = WriteDataManifest(
      expired_data_manifest_path, kExpiredSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kExpiredSnapshotId, kExpiredSequenceNumber,
                 MakeDataFile(expired_data_file_path))});
  auto expired_delete_manifest = WriteDeleteManifest(
      expired_delete_manifest_path, kExpiredSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kExpiredSnapshotId, kExpiredSequenceNumber,
                 MakePositionDeleteFile(expired_delete_file_path))});
  WriteManifestList(expired_manifest_list_path, kExpiredSnapshotId,
                    /*parent_snapshot_id=*/0, kExpiredSequenceNumber,
                    {expired_data_manifest, expired_delete_manifest});
  WriteManifestList(current_manifest_list_path, kCurrentSnapshotId, kExpiredSnapshotId,
                    kCurrentSequenceNumber, {});
  RewriteTableWithManifestLists(expired_manifest_list_path, current_manifest_list_path);

  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(deleted_files, testing::UnorderedElementsAre(expired_data_file_path,
                                                           expired_data_manifest_path,
                                                           expired_delete_manifest_path,
                                                           expired_manifest_list_path));
}

TEST_F(ExpireSnapshotsCleanupTest, DeletesExpiredFiles) {
  const auto expired_data_file_path = table_location_ + "/data/expired-data.parquet";
  const auto expired_delete_file_path = table_location_ + "/data/expired-delete.parquet";
  const auto expired_data_manifest_path = table_location_ + "/metadata/expired-data.avro";
  const auto expired_delete_manifest_path =
      table_location_ + "/metadata/expired-delete.avro";
  const auto expired_manifest_list_path =
      table_location_ + "/metadata/expired-manifest-list.avro";
  const auto current_manifest_list_path =
      table_location_ + "/metadata/current-manifest-list.avro";

  auto expired_data_manifest = WriteDataManifest(
      expired_data_manifest_path, kExpiredSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kExpiredSnapshotId, kExpiredSequenceNumber,
                 MakeDataFile(expired_data_file_path))});
  auto expired_delete_manifest = WriteDeleteManifest(
      expired_delete_manifest_path, kExpiredSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kExpiredSnapshotId, kExpiredSequenceNumber,
                 MakePositionDeleteFile(expired_delete_file_path))});
  WriteManifestList(expired_manifest_list_path, kExpiredSnapshotId,
                    /*parent_snapshot_id=*/0, kExpiredSequenceNumber,
                    {expired_data_manifest, expired_delete_manifest});
  WriteManifestList(current_manifest_list_path, kCurrentSnapshotId, kExpiredSnapshotId,
                    kCurrentSequenceNumber, {});
  RewriteTableWithManifestLists(expired_manifest_list_path, current_manifest_list_path);

  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(deleted_files, testing::UnorderedElementsAre(expired_data_file_path,
                                                           expired_data_manifest_path,
                                                           expired_delete_manifest_path,
                                                           expired_manifest_list_path));
}

TEST_F(ExpireSnapshotsCleanupTest, MetadataOnlySkipsDataDeletion) {
  const auto expired_data_file_path = table_location_ + "/data/expired-data.parquet";
  const auto expired_delete_manifest_path =
      table_location_ + "/metadata/expired-delete.avro";
  const auto expired_data_manifest_path = table_location_ + "/metadata/expired-data.avro";
  const auto expired_manifest_list_path =
      table_location_ + "/metadata/expired-manifest-list.avro";
  const auto current_manifest_list_path =
      table_location_ + "/metadata/current-manifest-list.avro";

  auto expired_data_manifest = WriteDataManifest(
      expired_data_manifest_path, kExpiredSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kExpiredSnapshotId, kExpiredSequenceNumber,
                 MakeDataFile(expired_data_file_path))});
  auto expired_delete_manifest = WriteDeleteManifest(
      expired_delete_manifest_path, kExpiredSnapshotId,
      {MakeEntry(
          ManifestStatus::kAdded, kExpiredSnapshotId, kExpiredSequenceNumber,
          MakePositionDeleteFile(table_location_ + "/data/expired-delete.parquet"))});
  WriteManifestList(expired_manifest_list_path, kExpiredSnapshotId,
                    /*parent_snapshot_id=*/0, kExpiredSequenceNumber,
                    {expired_data_manifest, expired_delete_manifest});
  WriteManifestList(current_manifest_list_path, kCurrentSnapshotId, kExpiredSnapshotId,
                    kCurrentSequenceNumber, {});
  RewriteTableWithManifestLists(expired_manifest_list_path, current_manifest_list_path);

  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->CleanupLevel(CleanupLevel::kMetadataOnly);
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(deleted_files, testing::UnorderedElementsAre(expired_data_manifest_path,
                                                           expired_delete_manifest_path,
                                                           expired_manifest_list_path));
}

TEST_F(ExpireSnapshotsCleanupTest, RetainedDeleteManifestSkipsDataDeletion) {
  const auto expired_data_file_path = table_location_ + "/data/expired-data.parquet";
  const auto current_delete_file_path = table_location_ + "/data/current-delete.parquet";
  const auto expired_data_manifest_path = table_location_ + "/metadata/expired-data.avro";
  const auto current_delete_manifest_path =
      table_location_ + "/metadata/current-delete.avro";
  const auto expired_manifest_list_path =
      table_location_ + "/metadata/expired-manifest-list.avro";
  const auto current_manifest_list_path =
      table_location_ + "/metadata/current-manifest-list.avro";

  auto expired_data_manifest = WriteDataManifest(
      expired_data_manifest_path, kExpiredSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kExpiredSnapshotId, kExpiredSequenceNumber,
                 MakeDataFile(expired_data_file_path))});
  auto current_delete_manifest = WriteDeleteManifest(
      current_delete_manifest_path, kCurrentSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kCurrentSnapshotId, kCurrentSequenceNumber,
                 MakePositionDeleteFile(current_delete_file_path))});
  WriteManifestList(expired_manifest_list_path, kExpiredSnapshotId,
                    /*parent_snapshot_id=*/0, kExpiredSequenceNumber,
                    {expired_data_manifest});
  WriteManifestList(current_manifest_list_path, kCurrentSnapshotId, kExpiredSnapshotId,
                    kCurrentSequenceNumber, {current_delete_manifest});
  RewriteTableWithManifestLists(expired_manifest_list_path, current_manifest_list_path);

  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(deleted_files, testing::UnorderedElementsAre(expired_data_manifest_path,
                                                           expired_manifest_list_path));
}

TEST_F(ExpireSnapshotsCleanupTest, DeletesExpiredStats) {
  const auto expired_manifest_list_path =
      table_location_ + "/metadata/expired-manifest-list.avro";
  const auto current_manifest_list_path =
      table_location_ + "/metadata/current-manifest-list.avro";
  const auto expired_statistics_path = table_location_ + "/metadata/stats-expired.puffin";

  WriteManifestList(expired_manifest_list_path, kExpiredSnapshotId,
                    /*parent_snapshot_id=*/0, kExpiredSequenceNumber, {});
  WriteManifestList(current_manifest_list_path, kCurrentSnapshotId, kExpiredSnapshotId,
                    kCurrentSequenceNumber, {});
  RewriteTableWithManifestListsAndStatistics(
      expired_manifest_list_path, current_manifest_list_path,
      {MakeStatisticsFile(kExpiredSnapshotId, expired_statistics_path)}, {});

  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(deleted_files, testing::Contains(expired_statistics_path));
}

TEST_F(ExpireSnapshotsCleanupTest, KeepsReusedStats) {
  const auto expired_manifest_list_path =
      table_location_ + "/metadata/expired-manifest-list.avro";
  const auto current_manifest_list_path =
      table_location_ + "/metadata/current-manifest-list.avro";
  const auto reused_statistics_path = table_location_ + "/metadata/stats-reused.puffin";

  WriteManifestList(expired_manifest_list_path, kExpiredSnapshotId,
                    /*parent_snapshot_id=*/0, kExpiredSequenceNumber, {});
  WriteManifestList(current_manifest_list_path, kCurrentSnapshotId, kExpiredSnapshotId,
                    kCurrentSequenceNumber, {});
  RewriteTableWithManifestListsAndStatistics(
      expired_manifest_list_path, current_manifest_list_path,
      {MakeStatisticsFile(kExpiredSnapshotId, reused_statistics_path),
       MakeStatisticsFile(kCurrentSnapshotId, reused_statistics_path)},
      {});

  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(deleted_files, testing::Not(testing::Contains(reused_statistics_path)));
}

TEST_F(ExpireSnapshotsCleanupTest, DeletesExpiredPartitionStats) {
  const auto expired_manifest_list_path =
      table_location_ + "/metadata/expired-manifest-list.avro";
  const auto current_manifest_list_path =
      table_location_ + "/metadata/current-manifest-list.avro";
  const auto expired_statistics_path =
      table_location_ + "/metadata/partition-stats-expired.parquet";

  WriteManifestList(expired_manifest_list_path, kExpiredSnapshotId,
                    /*parent_snapshot_id=*/0, kExpiredSequenceNumber, {});
  WriteManifestList(current_manifest_list_path, kCurrentSnapshotId, kExpiredSnapshotId,
                    kCurrentSequenceNumber, {});
  RewriteTableWithManifestListsAndStatistics(
      expired_manifest_list_path, current_manifest_list_path, {},
      {MakePartitionStatisticsFile(kExpiredSnapshotId, expired_statistics_path)});

  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(deleted_files, testing::Contains(expired_statistics_path));
}

TEST_F(ExpireSnapshotsCleanupTest, KeepsReusedPartitionStats) {
  const auto expired_manifest_list_path =
      table_location_ + "/metadata/expired-manifest-list.avro";
  const auto current_manifest_list_path =
      table_location_ + "/metadata/current-manifest-list.avro";
  const auto reused_statistics_path =
      table_location_ + "/metadata/partition-stats-reused.parquet";

  WriteManifestList(expired_manifest_list_path, kExpiredSnapshotId,
                    /*parent_snapshot_id=*/0, kExpiredSequenceNumber, {});
  WriteManifestList(current_manifest_list_path, kCurrentSnapshotId, kExpiredSnapshotId,
                    kCurrentSequenceNumber, {});
  RewriteTableWithManifestListsAndStatistics(
      expired_manifest_list_path, current_manifest_list_path, {},
      {MakePartitionStatisticsFile(kExpiredSnapshotId, reused_statistics_path),
       MakePartitionStatisticsFile(kCurrentSnapshotId, reused_statistics_path)});

  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(deleted_files, testing::Not(testing::Contains(reused_statistics_path)));
}

}  // namespace iceberg
