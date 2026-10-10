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

#include "iceberg/update/cherry_pick_operation.h"

#include <format>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"
#include "iceberg/update/delete_files.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/overwrite_files.h"
#include "iceberg/update/replace_partitions.h"
#include "iceberg/update/snapshot_manager.h"
#include "iceberg/update/update_partition_spec.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class CherryPickOperationTest : public UpdateTestBase {
 public:
  static void SetUpTestSuite() { avro::RegisterAll(); }

 protected:
  /// Most cherry-pick behavior is version independent; only tests that assert
  /// version-dependent metadata derive from CherryPickFormatVersionTest.
  virtual int8_t format_version() const { return 2; }

  std::string MetadataResource() const override {
    return format_version() == 3 ? "TableMetadataV3ValidMinimal.json"
                                 : "TableMetadataV2ValidMinimal.json";
  }

  void SetUp() override {
    UpdateTestBase::SetUp();
    if (format_version() == 1) {
      auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
      metadata->format_version = 1;
      metadata->last_sequence_number = 0;
      const auto metadata_location =
          std::format("{}/metadata/v1.metadata.json", table_location_);
      ASSERT_THAT(TableMetadataUtil::Write(*file_io_, metadata_location, *metadata),
                  IsOk());
      ASSERT_THAT(catalog_->DropTable(table_ident_, /*purge=*/false), IsOk());
      ICEBERG_UNWRAP_OR_FAIL(table_,
                             catalog_->RegisterTable(table_ident_, metadata_location));
    }

    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());

    file_a_ = MakeDataFile("/data/file_a.parquet", /*partition_x=*/1L);
    file_b_ = MakeDataFile("/data/file_b.parquet", /*partition_x=*/2L);
    replacement_a_ = MakeDataFile("/data/file_a_replacement.parquet", /*partition_x=*/1L);
    conflict_a_ = MakeDataFile("/data/file_a_conflict.parquet", /*partition_x=*/1L);
  }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& path, int64_t partition_x) {
    auto f = std::make_shared<DataFile>();
    f->content = DataFile::Content::kData;
    f->file_path = table_location_ + path;
    f->file_format = FileFormatType::kParquet;
    f->partition = PartitionValues(std::vector<Literal>{Literal::Long(partition_x)});
    f->file_size_in_bytes = 1024;
    f->record_count = 100;
    f->partition_spec_id = spec_->spec_id();
    return f;
  }

  Result<std::vector<ManifestEntry>> LiveDataEntries(const Snapshot& snapshot) {
    std::vector<ManifestEntry> entries;
    SnapshotReader snapshot_reader(&snapshot);
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot_reader.DataManifests(file_io_));
    for (const auto& manifest : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto spec, table_->metadata()->PartitionSpecById(manifest.partition_spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto reader,
                              ManifestReader::Make(manifest, file_io_, schema_, spec));
      ICEBERG_ASSIGN_OR_RAISE(auto live, reader->LiveEntries());
      entries.insert(entries.end(), live.begin(), live.end());
    }
    return entries;
  }

  Result<std::vector<std::string>> LiveDataFilePaths() {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    ICEBERG_ASSIGN_OR_RAISE(auto entries, LiveDataEntries(*snapshot));
    std::vector<std::string> paths;
    for (const auto& entry : entries) {
      paths.push_back(entry.data_file->file_path);
    }
    return paths;
  }

  Result<int64_t> CommitAppend(const std::shared_ptr<DataFile>& file) {
    ICEBERG_ASSIGN_OR_RAISE(auto append, table_->NewFastAppend());
    append->AppendFile(file);
    ICEBERG_RETURN_UNEXPECTED(append->Commit());
    ICEBERG_RETURN_UNEXPECTED(table_->Refresh());
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    return snapshot->snapshot_id;
  }

  Result<int64_t> CommitAppendToBranch(const std::string& branch,
                                       const std::shared_ptr<DataFile>& file) {
    ICEBERG_ASSIGN_OR_RAISE(auto append, table_->NewFastAppend());
    append->ToBranch(branch).AppendFile(file);
    ICEBERG_RETURN_UNEXPECTED(append->Commit());
    ICEBERG_RETURN_UNEXPECTED(table_->Refresh());
    return table_->metadata()->refs.at(branch)->snapshot_id;
  }

  Result<int64_t> StageAppend(const std::shared_ptr<DataFile>& file,
                              const std::optional<std::string>& wap_id = std::nullopt) {
    ICEBERG_ASSIGN_OR_RAISE(auto append, table_->NewFastAppend());
    append->StageOnly();
    if (wap_id.has_value()) {
      append->Set(SnapshotSummaryFields::kWAPId, *wap_id);
    }
    append->AppendFile(file);
    ICEBERG_RETURN_UNEXPECTED(append->Commit());
    ICEBERG_RETURN_UNEXPECTED(table_->Refresh());
    return table_->metadata()->snapshots.back()->snapshot_id;
  }

  Result<int64_t> StageReplacePartitions(const std::shared_ptr<DataFile>& file) {
    ICEBERG_ASSIGN_OR_RAISE(auto transaction, table_->NewTransaction());
    ICEBERG_ASSIGN_OR_RAISE(auto replace, transaction->NewReplacePartitions());
    replace->StageOnly();
    replace->Set(SnapshotSummaryFields::kReplacePartitions, "true");
    replace->AddFile(file);
    ICEBERG_RETURN_UNEXPECTED(replace->Commit());
    ICEBERG_RETURN_UNEXPECTED(transaction->Commit());
    ICEBERG_RETURN_UNEXPECTED(table_->Refresh());
    return table_->metadata()->snapshots.back()->snapshot_id;
  }

  Result<int64_t> StageOverwrite(const std::shared_ptr<DataFile>& added,
                                 const std::shared_ptr<DataFile>& removed) {
    ICEBERG_ASSIGN_OR_RAISE(auto transaction, table_->NewTransaction());
    ICEBERG_ASSIGN_OR_RAISE(auto overwrite, transaction->NewOverwrite());
    overwrite->StageOnly();
    overwrite->AddFile(added).DeleteFile(removed);
    ICEBERG_RETURN_UNEXPECTED(overwrite->Commit());
    ICEBERG_RETURN_UNEXPECTED(transaction->Commit());
    ICEBERG_RETURN_UNEXPECTED(table_->Refresh());
    return table_->metadata()->snapshots.back()->snapshot_id;
  }

  Status RollbackTo(int64_t snapshot_id) {
    ICEBERG_ASSIGN_OR_RAISE(auto manager, table_->NewSnapshotManager());
    manager->RollbackTo(snapshot_id);
    ICEBERG_RETURN_UNEXPECTED(manager->Commit());
    return table_->Refresh();
  }

  Status CommitDelete(const std::string& path) {
    ICEBERG_ASSIGN_OR_RAISE(auto delete_files, table_->NewDeleteFiles());
    delete_files->DeleteFile(path);
    ICEBERG_RETURN_UNEXPECTED(delete_files->Commit());
    return table_->Refresh();
  }

  Result<std::shared_ptr<Transaction>> StageCherrypick(int64_t snapshot_id) {
    ICEBERG_ASSIGN_OR_RAISE(auto transaction, table_->NewTransaction());
    ICEBERG_ASSIGN_OR_RAISE(auto manager, transaction->NewSnapshotManager());
    manager->Cherrypick(snapshot_id);
    ICEBERG_RETURN_UNEXPECTED(manager->Commit());
    return transaction;
  }

  Status Cherrypick(int64_t snapshot_id) {
    ICEBERG_ASSIGN_OR_RAISE(auto manager, table_->NewSnapshotManager());
    manager->Cherrypick(snapshot_id);
    ICEBERG_RETURN_UNEXPECTED(manager->Commit());
    return table_->Refresh();
  }

  /// Commit an append through a second table handle while another transaction is still
  /// open, so that transaction must rebase and replay at commit time.
  Result<int64_t> CommitFromOtherHandle(const std::shared_ptr<DataFile>& file) {
    ICEBERG_ASSIGN_OR_RAISE(auto other_table, catalog_->LoadTable(table_ident_));
    ICEBERG_ASSIGN_OR_RAISE(auto append, other_table->NewFastAppend());
    append->AppendFile(file);
    ICEBERG_RETURN_UNEXPECTED(append->Commit());
    ICEBERG_RETURN_UNEXPECTED(other_table->Refresh());
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, other_table->current_snapshot());
    return snapshot->snapshot_id;
  }

  /// A fast-forward publishes the staged snapshot itself: no new snapshot, no new
  /// sequence number or row ids, and one new snapshot-log entry for it.
  void ExpectFastForward(const std::shared_ptr<TableMetadata>& before,
                         int64_t staged_id) {
    ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
    EXPECT_EQ(snapshot->snapshot_id, staged_id);
    EXPECT_EQ(table_->metadata()->snapshots.size(), before->snapshots.size());
    EXPECT_EQ(table_->metadata()->last_sequence_number, before->last_sequence_number);
    EXPECT_EQ(table_->metadata()->next_row_id, before->next_row_id);
    ASSERT_EQ(table_->metadata()->snapshot_log.size(), before->snapshot_log.size() + 1);
    EXPECT_EQ(table_->metadata()->snapshot_log.back().snapshot_id, staged_id);
    EXPECT_FALSE(snapshot->summary.contains(SnapshotSummaryFields::kSourceSnapshotId));
    EXPECT_FALSE(snapshot->summary.contains(SnapshotSummaryFields::kPublishedWAPId));
  }

  /// A pick replayed on top of a moved state writes a new snapshot that is linked to the
  /// picked one and parented on the state it was applied to.
  void ExpectReplayedPick(int64_t staged_id, int64_t parent_snapshot_id) {
    ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
    EXPECT_NE(snapshot->snapshot_id, staged_id);
    EXPECT_EQ(snapshot->parent_snapshot_id, parent_snapshot_id);
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kSourceSnapshotId),
              std::to_string(staged_id));
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
  std::shared_ptr<DataFile> replacement_a_;
  std::shared_ptr<DataFile> conflict_a_;
};

class CherryPickFormatVersionTest : public CherryPickOperationTest,
                                    public ::testing::WithParamInterface<int8_t> {
 protected:
  int8_t format_version() const override { return GetParam(); }
};

// A staged dynamic overwrite is re-applied onto a state that moved on, so a new
// snapshot is produced rather than a fast-forward.
TEST_F(CherryPickOperationTest, CherryPickDynamicOverwrite) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageReplacePartitions(replacement_a_));
  ASSERT_THAT(CommitAppend(file_b_), IsOk());

  ASSERT_THAT(Cherrypick(staged_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_NE(snapshot->snapshot_id, staged_id);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kSourceSnapshotId),
            std::to_string(staged_id));

  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(file_b_->file_path,
                                                    replacement_a_->file_path));
}

// The same pick works when the staged overwrite has no parent snapshot.
TEST_F(CherryPickOperationTest, CherryPickDynamicOverwriteWithoutParent) {
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageReplacePartitions(replacement_a_));
  ASSERT_THAT(CommitAppend(file_b_), IsOk());

  ASSERT_THAT(Cherrypick(staged_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_NE(snapshot->snapshot_id, staged_id);

  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(file_b_->file_path,
                                                    replacement_a_->file_path));
}

// A file added concurrently into a replaced partition blocks the pick.
TEST_F(CherryPickOperationTest, CherryPickDynamicOverwriteConflict) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageReplacePartitions(replacement_a_));
  ICEBERG_UNWRAP_OR_FAIL(auto last_snapshot_id, CommitAppend(conflict_a_));

  EXPECT_THAT(
      Cherrypick(staged_id),
      ::testing::AllOf(
          IsError(ErrorKind::kValidationFailed),
          HasErrorMessage("Cannot cherry-pick replace partitions with changed partition: "
                          "x=1")));

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, last_snapshot_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(
      live, ::testing::UnorderedElementsAre(file_a_->file_path, conflict_a_->file_path));
}

// A file the staged overwrite removed must still be present at pick time.
TEST_F(CherryPickOperationTest, CherryPickDynamicOverwriteDeleteConflict) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageReplacePartitions(replacement_a_));
  ASSERT_THAT(CommitAppend(file_b_), IsOk());
  ASSERT_THAT(CommitDelete(file_a_->file_path), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto before, table_->current_snapshot());
  int64_t last_snapshot_id = before->snapshot_id;

  EXPECT_THAT(Cherrypick(staged_id), IsError(ErrorKind::kValidationFailed));

  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, last_snapshot_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(file_b_->file_path));
}

TEST_P(CherryPickFormatVersionTest, CherryPickAppend) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageAppend(file_b_));
  ICEBERG_UNWRAP_OR_FAIL(auto staged, table_->SnapshotById(staged_id));
  ASSERT_THAT(CommitAppend(conflict_a_), IsOk());
  const auto before = table_->metadata();
  ICEBERG_UNWRAP_OR_FAIL(auto parent, table_->current_snapshot());

  ASSERT_THAT(Cherrypick(staged_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_NE(snapshot->snapshot_id, staged_id);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kSourceSnapshotId),
            std::to_string(staged_id));
  EXPECT_FALSE(snapshot->summary.contains(SnapshotSummaryFields::kPublishedWAPId));
  EXPECT_EQ(snapshot->parent_snapshot_id, parent->snapshot_id);
  const int64_t expected_sequence =
      GetParam() == 1 ? 0 : before->last_sequence_number + 1;
  EXPECT_EQ(snapshot->sequence_number, expected_sequence);
  EXPECT_EQ(table_->metadata()->last_sequence_number, expected_sequence);
  EXPECT_EQ(table_->metadata()->snapshots.size(), before->snapshots.size() + 1);
  ASSERT_EQ(table_->metadata()->snapshot_log.size(), before->snapshot_log.size() + 1);
  EXPECT_EQ(table_->metadata()->snapshot_log.back().snapshot_id, snapshot->snapshot_id);
  ExpectBranch(std::string(SnapshotRef::kMainBranch), snapshot->snapshot_id);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "3");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalRecords), "300");

  ICEBERG_UNWRAP_OR_FAIL(auto entries, LiveDataEntries(*snapshot));
  ASSERT_EQ(entries.size(), 3);
  int added_files = 0;
  for (const auto& entry : entries) {
    if (entry.data_file->file_path == file_b_->file_path) {
      ++added_files;
      EXPECT_EQ(entry.status, ManifestStatus::kAdded);
      EXPECT_EQ(entry.snapshot_id, snapshot->snapshot_id);
      EXPECT_EQ(entry.sequence_number, expected_sequence);
      EXPECT_EQ(entry.file_sequence_number, expected_sequence);
      if (GetParam() == 3) {
        EXPECT_EQ(entry.data_file->first_row_id, before->next_row_id);
        EXPECT_NE(entry.data_file->first_row_id, staged->first_row_id);
      } else {
        EXPECT_FALSE(entry.data_file->first_row_id.has_value());
      }
    }
  }
  EXPECT_EQ(added_files, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(
                        file_a_->file_path, file_b_->file_path, conflict_a_->file_path));
  if (GetParam() == 3) {
    EXPECT_EQ(snapshot->first_row_id, before->next_row_id);
    EXPECT_EQ(snapshot->added_rows, file_b_->record_count);
    EXPECT_EQ(table_->metadata()->next_row_id,
              before->next_row_id + file_b_->record_count);
  }
}

// Main is empty, but the source snapshot has a parent on another branch.
TEST_F(CherryPickOperationTest, CherryPickAppendOntoEmptyMain) {
  ASSERT_THAT(CommitAppendToBranch("b1", file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, CommitAppendToBranch("b1", file_b_));

  ASSERT_THAT(Cherrypick(staged_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_NE(snapshot->snapshot_id, staged_id);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kSourceSnapshotId),
            std::to_string(staged_id));

  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(file_b_->file_path));
}

// When the picked snapshot's parent is the current snapshot, the pick moves the
// current snapshot instead of creating one.
// When the picked snapshot's parent is the current snapshot, the pick moves the
// current snapshot instead of creating one.
TEST_F(CherryPickOperationTest, FastForwardSetsCurrentSnapshot) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageAppend(file_b_));
  const auto before = table_->metadata();

  ASSERT_THAT(Cherrypick(staged_id), IsOk());

  ExpectFastForward(before, staged_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live,
              ::testing::UnorderedElementsAre(file_a_->file_path, file_b_->file_path));
}

// An overwrite without "replace-partitions" is not pickable, but it is still
// fast-forwarded to when it is a child of the current snapshot.
// An overwrite without "replace-partitions" is not pickable, but it is still
// fast-forwarded to when it is a child of the current snapshot.
TEST_F(CherryPickOperationTest, FastForwardOverwriteSetsCurrentSnapshot) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id,
                         StageOverwrite(/*added=*/replacement_a_, /*removed=*/file_a_));
  const auto before = table_->metadata();

  ASSERT_THAT(Cherrypick(staged_id), IsOk());

  ExpectFastForward(before, staged_id);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kOverwrite);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(replacement_a_->file_path));
}

// A dynamic overwrite whose parent has been rolled off the current history
// cannot be picked, because the partitions it replaced cannot be checked.
TEST_F(CherryPickOperationTest, CherryPickDynamicOverwriteParentNotAncestor) {
  ICEBERG_UNWRAP_OR_FAIL(auto first_id, CommitAppend(file_a_));
  ASSERT_THAT(CommitAppend(file_b_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageReplacePartitions(replacement_a_));
  ASSERT_THAT(RollbackTo(first_id), IsOk());

  EXPECT_THAT(Cherrypick(staged_id),
              ::testing::AllOf(
                  IsError(ErrorKind::kValidationFailed),
                  HasErrorMessage(std::format("Cannot cherry-pick overwrite not based on "
                                              "an ancestor of the current state: {}",
                                              staged_id))));

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, first_id);
}

// A WAP id already published by an ancestor is rejected even when the staged
// snapshot is a child of the current one and would otherwise fast-forward.
TEST_F(CherryPickOperationTest, DuplicateWapPublishOnFastForwardRejected) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_staged_id,
                         StageAppend(file_b_, /*wap_id=*/"wap-123"));

  ASSERT_THAT(Cherrypick(first_staged_id), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto picked, table_->current_snapshot());
  EXPECT_EQ(picked->snapshot_id, first_staged_id);

  // Staged on top of the snapshot just published, so this is a fast-forward.
  ICEBERG_UNWRAP_OR_FAIL(auto second_staged_id,
                         StageAppend(conflict_a_, /*wap_id=*/"wap-123"));

  EXPECT_THAT(
      Cherrypick(second_staged_id),
      ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                       HasErrorMessage("Duplicate request to cherry pick wap id that "
                                       "was published already: wap-123")));

  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, first_staged_id);
}

TEST_F(CherryPickOperationTest, NonPickableFastForwardInvalidatedByConcurrentCommit) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageOverwrite(replacement_a_, file_a_));
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, StageCherrypick(staged_id));
  ICEBERG_UNWRAP_OR_FAIL(auto concurrent_id, CommitFromOtherHandle(file_b_));

  EXPECT_THAT(transaction->Commit(),
              ::testing::AllOf(
                  IsError(ErrorKind::kValidationFailed),
                  HasErrorMessage(std::format(
                      "Cannot cherry-pick snapshot {}: not append, dynamic overwrite, "
                      "or fast-forward",
                      staged_id))));
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, concurrent_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live,
              ::testing::UnorderedElementsAre(file_a_->file_path, file_b_->file_path));
}

// A snapshot already in the current history cannot be picked again.
TEST_F(CherryPickOperationTest, CherryPickAncestorRejected) {
  ICEBERG_UNWRAP_OR_FAIL(auto first_id, CommitAppend(file_a_));
  ASSERT_THAT(CommitAppend(file_b_), IsOk());

  EXPECT_THAT(Cherrypick(first_id),
              ::testing::AllOf(
                  IsError(ErrorKind::kValidationFailed),
                  HasErrorMessage(std::format(
                      "Cannot cherrypick snapshot {}: already an ancestor", first_id))));

  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live,
              ::testing::UnorderedElementsAre(file_a_->file_path, file_b_->file_path));
}

// The same WAP id cannot be picked twice.
TEST_F(CherryPickOperationTest, DuplicateWapPublishRejected) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_staged_id,
                         StageAppend(file_b_, /*wap_id=*/"wap-123"));
  ICEBERG_UNWRAP_OR_FAIL(auto second_staged_id,
                         StageAppend(conflict_a_, /*wap_id=*/"wap-123"));

  ASSERT_THAT(Cherrypick(first_staged_id), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto picked, table_->current_snapshot());
  EXPECT_EQ(picked->snapshot_id, first_staged_id);

  EXPECT_THAT(
      Cherrypick(second_staged_id),
      ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                       HasErrorMessage("Duplicate request to cherry pick wap id that "
                                       "was published already: wap-123")));
}

// A snapshot that is neither an append nor a dynamic overwrite can only be
// fast-forwarded.
TEST_F(CherryPickOperationTest, NonPickableOperationRejected) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ASSERT_THAT(CommitAppend(file_b_), IsOk());

  auto delete_files = table_->NewDeleteFiles();
  ASSERT_TRUE(delete_files.has_value());
  delete_files.value()->StageOnly();
  delete_files.value()->DeleteFile(file_a_->file_path);
  ASSERT_THAT(delete_files.value()->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  int64_t staged_id = table_->metadata()->snapshots.back()->snapshot_id;

  ASSERT_THAT(CommitAppend(conflict_a_), IsOk());

  EXPECT_THAT(Cherrypick(staged_id),
              ::testing::AllOf(
                  IsError(ErrorKind::kValidationFailed),
                  HasErrorMessage(std::format(
                      "Cannot cherry-pick snapshot {}: not append, dynamic overwrite, "
                      "or fast-forward",
                      staged_id))));
}

TEST_F(CherryPickOperationTest, UnknownSnapshotRejected) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());

  EXPECT_THAT(
      Cherrypick(/*snapshot_id=*/-99),
      ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                       HasErrorMessage("Cannot cherry-pick unknown snapshot ID: -99")));
}

TEST_F(CherryPickOperationTest, FastForwardOntoEmptyMain) {
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageAppend(file_a_, "wap-123"));
  const auto before = table_->metadata();

  ASSERT_THAT(Cherrypick(staged_id), IsOk());

  ExpectFastForward(before, staged_id);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kWAPId), "wap-123");
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::ElementsAre(file_a_->file_path));
}

TEST_F(CherryPickOperationTest, FastForwardAppendReplayedAfterConcurrentAppend) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageAppend(file_b_, "wap-123"));
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, StageCherrypick(staged_id));
  ICEBERG_UNWRAP_OR_FAIL(auto concurrent_id, CommitFromOtherHandle(conflict_a_));

  ASSERT_THAT(transaction->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ExpectReplayedPick(staged_id, concurrent_id);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kPublishedWAPId), "wap-123");
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(
                        file_a_->file_path, file_b_->file_path, conflict_a_->file_path));
}

TEST_F(CherryPickOperationTest, FastForwardDynamicOverwriteReplayedWithoutConflict) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageReplacePartitions(replacement_a_));
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, StageCherrypick(staged_id));
  ICEBERG_UNWRAP_OR_FAIL(auto concurrent_id, CommitFromOtherHandle(file_b_));

  ASSERT_THAT(transaction->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ExpectReplayedPick(staged_id, concurrent_id);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->Operation(), DataOperation::kOverwrite);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(file_b_->file_path,
                                                    replacement_a_->file_path));
}

TEST_F(CherryPickOperationTest, FastForwardDynamicOverwriteReplayedWithConflict) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageReplacePartitions(replacement_a_));
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, StageCherrypick(staged_id));
  ICEBERG_UNWRAP_OR_FAIL(auto concurrent_id, CommitFromOtherHandle(conflict_a_));

  EXPECT_THAT(transaction->Commit(),
              ::testing::AllOf(
                  IsError(ErrorKind::kValidationFailed),
                  HasErrorMessage("Cannot cherry-pick replace partitions with changed "
                                  "partition")));
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, concurrent_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(
      live, ::testing::UnorderedElementsAre(file_a_->file_path, conflict_a_->file_path));
}

TEST_F(CherryPickOperationTest, CherryPickReplayedAsFastForwardAfterRollback) {
  ICEBERG_UNWRAP_OR_FAIL(auto parent_id, CommitAppend(file_a_));
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageAppend(file_b_));
  ASSERT_THAT(CommitAppend(conflict_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, StageCherrypick(staged_id));
  ICEBERG_UNWRAP_OR_FAIL(auto draft, transaction->current().Snapshot());
  ASSERT_NE(draft->snapshot_id, staged_id);
  ASSERT_THAT(file_io_->ReadFile(draft->manifest_list, std::nullopt), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto other_table, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto rollback, other_table->NewSnapshotManager());
  rollback->RollbackTo(parent_id);
  ASSERT_THAT(rollback->Commit(), IsOk());
  ASSERT_THAT(other_table->Refresh(), IsOk());
  const auto before = other_table->metadata();

  ASSERT_THAT(transaction->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ExpectFastForward(before, staged_id);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_THAT(file_io_->ReadFile(draft->manifest_list, std::nullopt),
              IsError(ErrorKind::kIOError));
  EXPECT_THAT(file_io_->ReadFile(snapshot->manifest_list, std::nullopt), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live,
              ::testing::UnorderedElementsAre(file_a_->file_path, file_b_->file_path));
}

TEST_F(CherryPickOperationTest, FastForwardSucceedsAfterCommitRetries) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageAppend(file_b_, "wap-123"));
  const auto before = table_->metadata();
  FailCommits(2);

  ASSERT_THAT(Cherrypick(staged_id), IsOk());

  ExpectFastForward(before, staged_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live,
              ::testing::UnorderedElementsAre(file_a_->file_path, file_b_->file_path));
}

TEST_F(CherryPickOperationTest, CherryPickRecordsPublishedWapId) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageAppend(file_b_, "wap-123"));
  ICEBERG_UNWRAP_OR_FAIL(auto second_staged_id, StageAppend(replacement_a_, "wap-123"));
  ASSERT_THAT(CommitAppend(conflict_a_), IsOk());

  ASSERT_THAT(Cherrypick(staged_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_NE(snapshot->snapshot_id, staged_id);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kSourceSnapshotId),
            std::to_string(staged_id));
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kPublishedWAPId), "wap-123");
  EXPECT_FALSE(snapshot->summary.contains(SnapshotSummaryFields::kWAPId));
  EXPECT_THAT(Cherrypick(second_staged_id),
              HasErrorMessage("Duplicate request to cherry pick wap id"));
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto after, table_->current_snapshot());
  EXPECT_EQ(after->snapshot_id, snapshot->snapshot_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(
                        file_a_->file_path, file_b_->file_path, conflict_a_->file_path));

  // An explicitly empty wap id is still recorded as published-wap-id.
  auto extra = MakeDataFile("/data/file_extra.parquet", /*partition_x=*/4L);
  auto bridge = MakeDataFile("/data/file_bridge.parquet", /*partition_x=*/5L);
  ICEBERG_UNWRAP_OR_FAIL(auto empty_wap_id, StageAppend(extra, /*wap_id=*/""));
  ASSERT_THAT(CommitAppend(bridge), IsOk());

  ASSERT_THAT(Cherrypick(empty_wap_id), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto empty_wap_snapshot, table_->current_snapshot());
  EXPECT_EQ(empty_wap_snapshot->summary.at(SnapshotSummaryFields::kPublishedWAPId), "");
  EXPECT_EQ(empty_wap_snapshot->summary.at(SnapshotSummaryFields::kSourceSnapshotId),
            std::to_string(empty_wap_id));
}

TEST_F(CherryPickOperationTest, DuplicateSourceSnapshotRejected) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageAppend(file_b_));
  ASSERT_THAT(CommitAppend(conflict_a_), IsOk());
  ASSERT_THAT(Cherrypick(staged_id), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto picked, table_->current_snapshot());

  EXPECT_THAT(Cherrypick(staged_id),
              ::testing::AllOf(
                  IsError(ErrorKind::kValidationFailed),
                  HasErrorMessage(std::format(
                      "Cannot cherrypick snapshot {}: already picked to create ancestor "
                      "{}",
                      staged_id, picked->snapshot_id))));

  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, picked->snapshot_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(
                        file_a_->file_path, file_b_->file_path, conflict_a_->file_path));
}

TEST_F(CherryPickOperationTest, DuplicateWapPublishedDuringReplayRejected) {
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_id, StageAppend(file_b_, "wap-123"));
  ICEBERG_UNWRAP_OR_FAIL(auto second_id, StageAppend(replacement_a_, "wap-123"));
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, StageCherrypick(second_id));

  ICEBERG_UNWRAP_OR_FAIL(auto other_table, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto publish, other_table->NewSnapshotManager());
  publish->Cherrypick(first_id);
  ASSERT_THAT(publish->Commit(), IsOk());

  EXPECT_THAT(
      transaction->Commit(),
      ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                       HasErrorMessage("Duplicate request to cherry pick wap id")));
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, first_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live,
              ::testing::UnorderedElementsAre(file_a_->file_path, file_b_->file_path));
}

TEST_F(CherryPickOperationTest, CherryPickDynamicOverwriteFromMergedManifest) {
  ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
  properties->Set(TableProperties::kManifestMinMergeCount.key(), "1");
  ASSERT_THAT(properties->Commit(), IsOk());
  ASSERT_THAT(CommitAppend(file_a_), IsOk());
  ASSERT_THAT(CommitAppend(file_b_), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_id, StageReplacePartitions(replacement_a_));
  ICEBERG_UNWRAP_OR_FAIL(auto staged, table_->SnapshotById(staged_id));
  ICEBERG_UNWRAP_OR_FAIL(auto staged_entries, LiveDataEntries(*staged));
  ASSERT_THAT(staged_entries, ::testing::Contains(::testing::Field(
                                  &ManifestEntry::status, ManifestStatus::kExisting)));
  auto new_file = MakeDataFile("/data/another_partition.parquet", 3L);
  ASSERT_THAT(CommitAppend(new_file), IsOk());

  ASSERT_THAT(Cherrypick(staged_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(
      live, ::testing::UnorderedElementsAre(file_b_->file_path, replacement_a_->file_path,
                                            new_file->file_path));
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, CherryPickFormatVersionTest,
                         ::testing::Values(1, 2, 3),
                         [](const ::testing::TestParamInfo<int8_t>& info) {
                           return std::format("V{}", info.param);
                         });

}  // namespace iceberg
