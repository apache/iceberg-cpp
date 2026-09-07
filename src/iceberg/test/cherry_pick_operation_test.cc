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
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"
#include "iceberg/update/delete_files.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/overwrite_files.h"
#include "iceberg/update/replace_partitions.h"
#include "iceberg/update/snapshot_manager.h"
#include "iceberg/util/macros.h"

namespace iceberg {

// The base table (TableMetadataV2ValidMinimal.json) has schema {x: long (id 1),
// y: long (id 2), z: long (id 3)} and partitions by identity(x) as spec 0.
class CherryPickOperationTest : public UpdateTestBase {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  std::string MetadataResource() const override {
    return "TableMetadataV2ValidMinimal.json";
  }

  void SetUp() override {
    UpdateTestBase::SetUp();

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

  // Live (non-deleted) data file paths across the current snapshot's manifests.
  Result<std::vector<std::string>> LiveDataFilePaths() {
    std::vector<std::string> paths;
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    SnapshotCache cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, cache.DataManifests(file_io_));
    for (const auto& manifest : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto spec, table_->metadata()->PartitionSpecById(manifest.partition_spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto reader,
                              ManifestReader::Make(manifest, file_io_, schema_, spec));
      ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());
      for (const auto& entry : entries) {
        if (entry.data_file) {
          paths.push_back(entry.data_file->file_path);
        }
      }
    }
    return paths;
  }

  int64_t CommitAppend(const std::shared_ptr<DataFile>& file) {
    auto fa = table_->NewFastAppend();
    EXPECT_TRUE(fa.has_value());
    fa.value()->AppendFile(file);
    EXPECT_THAT(fa.value()->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
    auto snap = table_->current_snapshot();
    EXPECT_TRUE(snap.has_value());
    return snap.value()->snapshot_id;
  }

  // Commit a staged append and return the staged snapshot's ID. The table's
  // current snapshot is unchanged.
  int64_t StageAppend(const std::shared_ptr<DataFile>& file,
                      const std::string& wap_id = "") {
    auto fa = table_->NewFastAppend();
    EXPECT_TRUE(fa.has_value());
    fa.value()->StageOnly();
    if (!wap_id.empty()) {
      fa.value()->Set(SnapshotSummaryFields::kWAPId, wap_id);
    }
    fa.value()->AppendFile(file);
    EXPECT_THAT(fa.value()->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
    return table_->metadata()->snapshots.back()->snapshot_id;
  }

  // Commit a staged dynamic partition overwrite and return its snapshot ID.
  int64_t StageReplacePartitions(const std::shared_ptr<DataFile>& file) {
    auto ctx = TransactionContext::Make(table_, TransactionKind::kUpdate);
    EXPECT_TRUE(ctx.has_value());
    auto op = ReplacePartitions::Make(TableName(), std::move(ctx.value()));
    EXPECT_TRUE(op.has_value());
    op.value()->StageOnly();
    op.value()->AddFile(file);
    EXPECT_THAT(op.value()->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
    return table_->metadata()->snapshots.back()->snapshot_id;
  }

  // Commit a staged overwrite that is not a dynamic partition overwrite, and
  // return its snapshot ID.
  int64_t StageOverwrite(const std::shared_ptr<DataFile>& added,
                         const std::shared_ptr<DataFile>& removed) {
    auto ctx = TransactionContext::Make(table_, TransactionKind::kUpdate);
    EXPECT_TRUE(ctx.has_value());
    auto op = OverwriteFiles::Make(TableName(), std::move(ctx.value()));
    EXPECT_TRUE(op.has_value());
    op.value()->StageOnly();
    op.value()->AddFile(added);
    op.value()->DeleteFile(removed);
    EXPECT_THAT(op.value()->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
    return table_->metadata()->snapshots.back()->snapshot_id;
  }

  void RollbackTo(int64_t snapshot_id) {
    ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
    manager->RollbackTo(snapshot_id);
    EXPECT_THAT(manager->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  void CommitDelete(const std::string& path) {
    ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
    delete_files->DeleteFile(path);
    EXPECT_THAT(delete_files->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  Status Cherrypick(int64_t snapshot_id) {
    ICEBERG_ASSIGN_OR_RAISE(auto manager, table_->NewSnapshotManager());
    manager->Cherrypick(snapshot_id);
    ICEBERG_RETURN_UNEXPECTED(manager->Commit());
    return table_->Refresh();
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
  std::shared_ptr<DataFile> replacement_a_;
  std::shared_ptr<DataFile> conflict_a_;
};

// A staged dynamic overwrite is re-applied onto a state that moved on, so a new
// snapshot is produced rather than a fast-forward.
TEST_F(CherryPickOperationTest, CherryPickDynamicOverwrite) {
  CommitAppend(file_a_);
  int64_t staged_id = StageReplacePartitions(replacement_a_);
  CommitAppend(file_b_);

  EXPECT_THAT(Cherrypick(staged_id), IsOk());

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
  int64_t staged_id = StageReplacePartitions(replacement_a_);
  CommitAppend(file_b_);

  EXPECT_THAT(Cherrypick(staged_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_NE(snapshot->snapshot_id, staged_id);

  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(file_b_->file_path,
                                                    replacement_a_->file_path));
}

// A file added concurrently into a replaced partition blocks the pick.
TEST_F(CherryPickOperationTest, CherryPickDynamicOverwriteConflict) {
  CommitAppend(file_a_);
  int64_t staged_id = StageReplacePartitions(replacement_a_);
  int64_t last_snapshot_id = CommitAppend(conflict_a_);

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
  CommitAppend(file_a_);
  int64_t staged_id = StageReplacePartitions(replacement_a_);
  CommitAppend(file_b_);
  CommitDelete(file_a_->file_path);
  ICEBERG_UNWRAP_OR_FAIL(auto before, table_->current_snapshot());
  int64_t last_snapshot_id = before->snapshot_id;

  EXPECT_THAT(Cherrypick(staged_id), IsError(ErrorKind::kValidationFailed));

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, last_snapshot_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(file_b_->file_path));
}

// A staged append is re-applied on top of the current state.
TEST_F(CherryPickOperationTest, CherryPickAppend) {
  CommitAppend(file_a_);
  int64_t staged_id = StageAppend(replacement_a_);
  CommitAppend(file_b_);

  EXPECT_THAT(Cherrypick(staged_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_NE(snapshot->snapshot_id, staged_id);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kSourceSnapshotId),
            std::to_string(staged_id));
  EXPECT_FALSE(snapshot->summary.contains(SnapshotSummaryFields::kPublishedWAPId));

  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live,
              ::testing::UnorderedElementsAre(file_a_->file_path, file_b_->file_path,
                                              replacement_a_->file_path));
}

// When the picked snapshot's parent is the current snapshot, the pick moves the
// current snapshot instead of creating one.
TEST_F(CherryPickOperationTest, FastForwardSetsCurrentSnapshot) {
  CommitAppend(file_a_);
  int64_t staged_id = StageAppend(file_b_);

  EXPECT_THAT(Cherrypick(staged_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, staged_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live,
              ::testing::UnorderedElementsAre(file_a_->file_path, file_b_->file_path));
}

// An overwrite without "replace-partitions" is not pickable, but it is still
// fast-forwarded to when it is a child of the current snapshot.
TEST_F(CherryPickOperationTest, FastForwardOverwriteSetsCurrentSnapshot) {
  CommitAppend(file_a_);
  int64_t staged_id = StageOverwrite(/*added=*/replacement_a_, /*removed=*/file_a_);

  EXPECT_THAT(Cherrypick(staged_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, staged_id);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kOverwrite);
  // A fast-forward publishes the staged snapshot itself, so it carries no
  // source-snapshot-id.
  EXPECT_FALSE(snapshot->summary.contains(SnapshotSummaryFields::kSourceSnapshotId));

  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(live, ::testing::UnorderedElementsAre(replacement_a_->file_path));
}

// A dynamic overwrite whose parent has been rolled off the current history
// cannot be picked, because the partitions it replaced cannot be checked.
TEST_F(CherryPickOperationTest, CherryPickDynamicOverwriteParentNotAncestor) {
  int64_t first_id = CommitAppend(file_a_);
  CommitAppend(file_b_);
  int64_t staged_id = StageReplacePartitions(replacement_a_);
  RollbackTo(first_id);

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
  CommitAppend(file_a_);
  int64_t first_staged_id = StageAppend(file_b_, /*wap_id=*/"wap-123");

  EXPECT_THAT(Cherrypick(first_staged_id), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto picked, table_->current_snapshot());
  EXPECT_EQ(picked->snapshot_id, first_staged_id);

  // Staged on top of the snapshot just published, so this is a fast-forward.
  int64_t second_staged_id = StageAppend(conflict_a_, /*wap_id=*/"wap-123");

  EXPECT_THAT(
      Cherrypick(second_staged_id),
      ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                       HasErrorMessage("Duplicate request to cherry pick wap id that "
                                       "was published already: wap-123")));

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, first_staged_id);
}

// A fast-forward staged against one state must not be applied to another. The
// commit below is retried after a concurrent append, and re-applying the
// fast-forward there would discard that append.
TEST_F(CherryPickOperationTest, FastForwardInvalidatedByConcurrentCommitRejected) {
  CommitAppend(file_a_);
  int64_t staged_id = StageAppend(file_b_);

  // Stage the fast-forward, but hold the transaction open.
  ICEBERG_UNWRAP_OR_FAIL(auto txn, Transaction::Make(table_, TransactionKind::kUpdate));
  ICEBERG_UNWRAP_OR_FAIL(auto manager, SnapshotManager::Make(txn));
  manager->Cherrypick(staged_id);
  EXPECT_THAT(manager->Commit(), IsOk());

  // A separate table handle advances the current snapshot in the catalog, so
  // the staged snapshot's parent is no longer current.
  ICEBERG_UNWRAP_OR_FAIL(auto other_table, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto append, other_table->NewFastAppend());
  append->AppendFile(conflict_a_);
  ASSERT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(txn->Commit(),
              HasErrorMessage(std::format(
                  "Cannot fast-forward to {}: not a child of the current table state",
                  staged_id)));

  // The concurrent append survives; the staged snapshot was not published.
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_NE(snapshot->snapshot_id, staged_id);
  ICEBERG_UNWRAP_OR_FAIL(auto live, LiveDataFilePaths());
  EXPECT_THAT(
      live, ::testing::UnorderedElementsAre(file_a_->file_path, conflict_a_->file_path));
}

// A snapshot already in the current history cannot be picked again.
TEST_F(CherryPickOperationTest, CherryPickAncestorRejected) {
  int64_t first_id = CommitAppend(file_a_);
  CommitAppend(file_b_);

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
  CommitAppend(file_a_);
  int64_t first_staged_id = StageAppend(file_b_, /*wap_id=*/"wap-123");
  int64_t second_staged_id = StageAppend(conflict_a_, /*wap_id=*/"wap-123");

  EXPECT_THAT(Cherrypick(first_staged_id), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto picked, table_->current_snapshot());
  EXPECT_EQ(picked->snapshot_id, first_staged_id);

  EXPECT_THAT(
      Cherrypick(second_staged_id),
      ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                       HasErrorMessage("Duplicate request to cherry pick wap id that "
                                       "was published already: wap-123")));
}

// A picked snapshot with no WAP id records only the source snapshot.
TEST_F(CherryPickOperationTest, NonWapCherrypick) {
  CommitAppend(file_a_);
  int64_t staged_id = StageAppend(replacement_a_);
  CommitAppend(file_b_);

  EXPECT_THAT(Cherrypick(staged_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_FALSE(snapshot->summary.contains(SnapshotSummaryFields::kPublishedWAPId));
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kSourceSnapshotId),
            std::to_string(staged_id));
}

// A snapshot that is neither an append nor a dynamic overwrite can only be
// fast-forwarded.
TEST_F(CherryPickOperationTest, NonPickableOperationRejected) {
  CommitAppend(file_a_);
  CommitAppend(file_b_);

  auto delete_files = table_->NewDeleteFiles();
  ASSERT_TRUE(delete_files.has_value());
  delete_files.value()->StageOnly();
  delete_files.value()->DeleteFile(file_a_->file_path);
  ASSERT_THAT(delete_files.value()->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  int64_t staged_id = table_->metadata()->snapshots.back()->snapshot_id;

  CommitAppend(conflict_a_);

  EXPECT_THAT(Cherrypick(staged_id),
              ::testing::AllOf(
                  IsError(ErrorKind::kValidationFailed),
                  HasErrorMessage(std::format(
                      "Cannot cherry-pick snapshot {}: not append, dynamic overwrite, "
                      "or fast-forward",
                      staged_id))));
}

TEST_F(CherryPickOperationTest, UnknownSnapshotRejected) {
  CommitAppend(file_a_);

  EXPECT_THAT(
      Cherrypick(/*snapshot_id=*/-99),
      ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                       HasErrorMessage("Cannot cherry-pick unknown snapshot ID: -99")));
}

}  // namespace iceberg
