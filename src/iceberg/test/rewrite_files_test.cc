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

#include "iceberg/update/rewrite_files.h"

#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/update/fast_append.h"

namespace iceberg {

class RewriteFilesTest : public MinimalUpdateTestBase {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  void SetUp() override {
    MinimalUpdateTestBase::SetUp();

    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());

    file_a_ = MakeDataFile("/data/file_a.parquet", /*partition_x=*/1L);
    file_b_ = MakeDataFile("/data/file_b.parquet", /*partition_x=*/2L);
    rewritten_file_a_ =
        MakeDataFile("/data/file_a_rewritten.parquet", /*partition_x=*/1L);
    rewritten_file_b_ =
        MakeDataFile("/data/file_b_rewritten.parquet", /*partition_x=*/2L);
    delete_file_a_ = MakeDeleteFile("/data/delete_a.parquet", /*partition_x=*/1L);
    rewritten_delete_file_a_ =
        MakeDeleteFile("/data/delete_a_rewritten.parquet", /*partition_x=*/1L);
    eq_delete_file_ =
        MakeEqualityDeleteFile("/data/eq_delete_a.parquet", /*partition_x=*/1L);
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

  std::shared_ptr<DataFile> MakeDeleteFile(const std::string& path, int64_t partition_x) {
    auto f = MakeDataFile(path, partition_x);
    f->content = DataFile::Content::kPositionDeletes;
    return f;
  }

  std::shared_ptr<DataFile> MakeEqualityDeleteFile(const std::string& path,
                                                   int64_t partition_x) {
    auto f = MakeDeleteFile(path, partition_x);
    f->content = DataFile::Content::kEqualityDeletes;
    f->equality_ids = {1};
    return f;
  }

  Result<std::shared_ptr<RewriteFiles>> NewRewriteFiles() {
    return table_->NewRewriteFiles();
  }

  /// \brief Commit file_a_ with FastAppend so the table has data to rewrite.
  void CommitFileA() {
    ICEBERG_UNWRAP_OR_FAIL(auto fa, table_->NewFastAppend());
    fa->AppendFile(file_a_);
    EXPECT_THAT(fa->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
  std::shared_ptr<DataFile> rewritten_file_a_;
  std::shared_ptr<DataFile> rewritten_file_b_;
  std::shared_ptr<DataFile> delete_file_a_;
  std::shared_ptr<DataFile> rewritten_delete_file_a_;
  std::shared_ptr<DataFile> eq_delete_file_;
};

// Rewrite a single data file: replace file_a_ with rewritten_file_a_.
TEST_F(RewriteFilesTest, AddAndDelete) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);
  EXPECT_THAT(rw->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kReplace);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles)),
            1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles)), 1);
}

TEST_F(RewriteFilesTest, DeleteDataFileCopiesCallerFile) {
  CommitFileA();

  const std::string original_path = file_a_->file_path;

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);

  file_a_->file_path = file_b_->file_path;

  EXPECT_THAT(rw->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto rw_missing_original, NewRewriteFiles());
  auto missing_file = std::make_shared<DataFile>(*file_a_);
  missing_file->file_path = original_path;
  rw_missing_original->DeleteDataFile(missing_file);
  rw_missing_original->AddDataFile(file_b_);
  auto result = rw_missing_original->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Missing required files to delete"));
}

// Rewrite one of several data files, verifying only the target is affected.
TEST_F(RewriteFilesTest, AddAndDeletePartialRewrite) {
  CommitFileA();

  {
    ICEBERG_UNWRAP_OR_FAIL(auto fa, table_->NewFastAppend());
    fa->AppendFile(file_b_);
    EXPECT_THAT(fa->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);
  EXPECT_THAT(rw->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kReplace);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles)),
            1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles)), 1);
}

// Rewrite with an explicit data sequence number via SetDataSequenceNumber.
TEST_F(RewriteFilesTest, DataSequenceNumber) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->SetDataSequenceNumber(5);
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);
  EXPECT_THAT(rw->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kReplace);
}

// Bulk rewrite with sequence number via the RewriteDataFiles convenience method.
TEST_F(RewriteFilesTest, RewriteDataFiles) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->RewriteDataFiles({file_a_}, {rewritten_file_a_}, /*sequence_number=*/3);
  EXPECT_THAT(rw->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kReplace);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles)),
            1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles)), 1);
}

// Rewrite via the 4-set Rewrite() API replacing data files only.
TEST_F(RewriteFilesTest, Rewrite) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->Rewrite({file_a_}, {}, {rewritten_file_a_}, {});
  EXPECT_THAT(rw->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kReplace);
}

// Only adding files without any deletions must fail validation.
TEST_F(RewriteFilesTest, DeleteOnly) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->AddDataFile(rewritten_file_a_);  // no FileToDelete → must fail
  EXPECT_THAT(rw->Commit(), IsError(ErrorKind::kValidationFailed));
}

// Adding data files without deleting data, or adding delete files without deleting
// delete files, must fail validation.
TEST_F(RewriteFilesTest, AddOnly) {
  // Sub-case 1: adding data files without deleting any data files should fail
  {
    ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
    rw->DeleteDeleteFile(delete_file_a_);
    rw->AddDataFile(rewritten_file_a_);
    EXPECT_THAT(rw->Commit(), IsError(ErrorKind::kValidationFailed));
  }

  // Sub-case 2: adding delete files without deleting any delete files should fail
  {
    CommitFileA();
    ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
    rw->DeleteDataFile(file_a_);
    rw->AddDeleteFile(rewritten_delete_file_a_);
    EXPECT_THAT(rw->Commit(), IsError(ErrorKind::kValidationFailed));
  }
}

// Limiting validation scope to after a given snapshot avoids spurious conflicts.
TEST_F(RewriteFilesTest, ValidateFromSnapshot) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  auto snapshot_id = snapshot->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->ValidateFromSnapshot(snapshot_id);
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);
  EXPECT_THAT(rw->Commit(), IsOk());
}

// Committing a rewrite to the main branch via ToBranch.
TEST_F(RewriteFilesTest, ToBranch) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->ToBranch("main");
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);
  EXPECT_THAT(rw->Commit(), IsOk());
}

// Null check on DeleteDataFile.
TEST_F(RewriteFilesTest, DeleteDataFileNullCheck) {
  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(nullptr);
  EXPECT_THAT(rw->Commit(), IsError(ErrorKind::kValidationFailed));
}

// Null check on AddDataFile.
TEST_F(RewriteFilesTest, AddDataFileNullCheck) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(nullptr);
  EXPECT_THAT(rw->Commit(), IsError(ErrorKind::kValidationFailed));
}

// Null checks on AddDeleteFile
TEST_F(RewriteFilesTest, AddDeleteFileNullCheck) {
  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->AddDeleteFile(nullptr);
  EXPECT_THAT(rw->Commit(), IsError(ErrorKind::kValidationFailed));
}

// Adding a data file after deleting one — the basic RewriteFiles pattern.
TEST_F(RewriteFilesTest, AddDataFile) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);
  EXPECT_THAT(rw->Commit(), IsOk());
}

// Deleting a file from an empty table fails with missing required files.
TEST_F(RewriteFilesTest, EmptyTable) {
  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);
  auto result = rw->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Missing required files to delete"));
}

// Deleting a file that was never added fails with missing required files.
TEST_F(RewriteFilesTest, DeleteNonExistentFile) {
  CommitFileA();  // table now has file_a_

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  // file_b_ was never added — deleting it should fail with missing required files
  rw->DeleteDataFile(file_b_);
  rw->AddDataFile(rewritten_file_b_);
  auto result = rw->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Missing required files to delete"));
}

// Rewriting a file that was already deleted in a prior commit must fail.
TEST_F(RewriteFilesTest, AlreadyDeletedFile) {
  CommitFileA();  // table now has file_a_

  // First rewrite: file_a_ → rewritten_file_a_
  {
    ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
    rw->DeleteDataFile(file_a_);
    rw->AddDataFile(rewritten_file_a_);
    EXPECT_THAT(rw->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  // Second rewrite: try to delete file_a_ again (already deleted)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
    rw->DeleteDataFile(file_a_);
    rw->AddDataFile(file_b_);
    auto result = rw->Commit();
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(result, HasErrorMessage("Missing required files to delete"));
  }
}

// ============================================================================
// TODO(WZhuo): Tests blocked on missing infrastructure in iceberg-cpp.
// ============================================================================
//
// TODO(RewriteDataAndDeleteFiles):
//   Blocked by: RowDelta not yet ported.
//   Creates data+delete files via RowDelta, then uses RewriteFiles to replace both.
//   Verifies manifest entry statuses (ADDED/DELETED/EXISTING), snapshot IDs, file
//   identity.
//
// TODO(RewriteDataAndAssignOldSequenceNumber):
//   Blocked by: RowDelta not yet ported.
//   Creates delete files via RowDelta, rewrites data files with old sequence number.
//   Verifies sequence number propagation to new manifests.
//
// TODO(Failure):
//   Blocked by: commit fail injection (failCommits) not yet ported.
//   Injects commit failures to verify retry and manifest cleanup on failure.
//
// TODO(FailureWhenRewriteBothDataAndDeleteFiles):
//   Blocked by: RowDelta + failCommits not yet ported.
//   Creates data+delete files via RowDelta, injects commit failures.
//   Verifies both data and delete manifests are cleaned up on failure.
//
// TODO(Recovery):
//   Blocked by: commit fail injection (failCommits) not yet ported.
//   Injects transient commit failures, then succeeds. Verifies committed manifests
//   persist.
//
// TODO(RecoverWhenRewriteBothDataAndDeleteFiles):
//   Blocked by: RowDelta + failCommits not yet ported.
//   Same as Recovery but with both data and delete file rewrites.
//
// TODO(ReplaceEqualityDeletesWithPositionDeletes):
//   Blocked by: RowDelta not yet ported.
//   Creates equality deletes via RowDelta, rewrites them to position deletes.
//
// TODO(RemoveAllDeletes):
//   Blocked by: RowDelta not yet ported.
//   Creates data file + equality delete via RowDelta, rewrites with empty add sets.
//
// TODO(NewDeleteFile):
//   Blocked by: RowDelta not yet ported (end-to-end commit path).
//   Commits data file, then commits equality delete via RowDelta, then verifies
//   RewriteFiles detects conflict when validated from before the delete.
//   The static validation function is already covered by
//   ValidateNoNewDeletesForDataFilesDetectsConflict in merging_snapshot_update_test.cc.
//
// TODO(RemovingDataFileAlsoRemovesDV):
//   Blocked by: RowDelta not yet ported + format v3 support.
//   Creates data+delete files via RowDelta (v3), rewrites with deleteFile.
//   Verifies the DV for the removed data file is automatically cleaned up.
//
// TODO(DeleteWithDuplicateEntriesInManifest):
//   Blocked by: cannot yet append the same file twice to create duplicate manifest
//   entries. Appends FILE_A twice, then rewrites one copy. Verifies manifest entry
//   statuses (DELETED for the rewritten copy, EXISTING for the other).

}  // namespace iceberg
