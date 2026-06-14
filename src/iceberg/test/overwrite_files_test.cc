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

#include "iceberg/update/overwrite_files.h"

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/constants.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"
#include "iceberg/transform.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/util/data_file_set.h"
#include "iceberg/util/macros.h"

namespace iceberg {

// =====================================================================================
// Test harness for OverwriteFiles.
//
// Modeled on src/iceberg/test/merging_snapshot_update_test.cc (same fixture style,
// same in-memory mock FileIO / catalog setup, same DataFile / commit helpers). Unlike
// that file, OverwriteFiles is the production class with a private constructor, so the
// tests drive it exclusively through its public builder surface (AddFile / DeleteFile /
// OverwriteByRowFilter / ... / operation() / Validate() / Commit()) and observe its
// behavior through the public API: operation() classification, the committed snapshot
// summary, and the public Validate(...) entry point that the commit kernel invokes.
//
// The base table (TableMetadataV2ValidMinimal.json) has schema {x: long (id 1),
// y: long (id 2), z: long (id 3)} and a single partition spec (spec id 0) that
// partitions by identity(x).
// =====================================================================================
class OverwriteFilesTest : public UpdateTestBase {
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
  }

  // A plain data file in spec 0 (identity(x)) with the given partition value for x.
  std::shared_ptr<DataFile> MakeDataFile(const std::string& path, int64_t partition_x,
                                         int64_t record_count = 100) {
    auto f = std::make_shared<DataFile>();
    f->content = DataFile::Content::kData;
    f->file_path = table_location_ + path;
    f->file_format = FileFormatType::kParquet;
    f->partition = PartitionValues(std::vector<Literal>{Literal::Long(partition_x)});
    f->file_size_in_bytes = 1024;
    f->record_count = record_count;
    f->partition_spec_id = spec_->spec_id();
    return f;
  }

  // A data file carrying column metrics for column y (field id 2): lower/upper bounds
  // plus value/null counts, so the StrictMetricsEvaluator can reason about it.
  std::shared_ptr<DataFile> MakeDataFileWithYBounds(const std::string& path,
                                                    int64_t partition_x, int64_t y_lower,
                                                    int64_t y_upper) {
    auto f = MakeDataFile(path, partition_x);
    f->lower_bounds = {{2, Literal::Long(y_lower).Serialize().value()}};
    f->upper_bounds = {{2, Literal::Long(y_upper).Serialize().value()}};
    f->value_counts = {{2, f->record_count}};
    f->null_value_counts = {{2, 0}};
    return f;
  }

  std::shared_ptr<DataFile> MakeDeleteFile(const std::string& path, int64_t partition_x) {
    auto f = MakeDataFile(path, partition_x);
    f->content = DataFile::Content::kPositionDeletes;
    return f;
  }

  // An equality delete file in spec 0 (partition x = partition_x).
  std::shared_ptr<DataFile> MakeEqualityDeleteFile(const std::string& path,
                                                   int64_t partition_x) {
    auto f = MakeDeleteFile(path, partition_x);
    f->content = DataFile::Content::kEqualityDeletes;
    f->equality_ids = {1};
    return f;
  }

  // Write a delete manifest containing the given delete files, with the snapshot id and
  // sequence number assigned on each entry (so the manifest list writer does not need to
  // inherit them).
  Result<ManifestFile> WriteDeleteManifest(
      const std::string& path, const std::vector<std::shared_ptr<DataFile>>& files,
      int64_t snapshot_id, int64_t sequence_number) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        ManifestWriter::MakeWriter(/*format_version=*/2, snapshot_id, path, file_io_,
                                   spec_, schema_, ManifestContent::kDeletes));
    for (const auto& f : files) {
      ManifestEntry entry;
      entry.status = ManifestStatus::kAdded;
      entry.snapshot_id = snapshot_id;
      entry.sequence_number = sequence_number;
      entry.data_file = f;
      ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(entry));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  // Build a synthetic snapshot from the given manifests (mirrors the merging-update test
  // harness). Used to inject a concurrent commit between read and validate.
  Result<std::shared_ptr<Snapshot>> MakeSyntheticSnapshot(
      std::string operation, int64_t snapshot_id,
      std::optional<int64_t> parent_snapshot_id, int64_t sequence_number,
      const std::vector<ManifestFile>& manifests) {
    auto manifest_list_path = table_location_ + "/metadata/manifest-list-" +
                              std::to_string(snapshot_id) + ".avro";
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        ManifestListWriter::MakeWriter(table_->metadata()->format_version, snapshot_id,
                                       parent_snapshot_id, manifest_list_path, file_io_,
                                       sequence_number));
    ICEBERG_RETURN_UNEXPECTED(writer->AddAll(manifests));
    ICEBERG_RETURN_UNEXPECTED(writer->Close());

    ICEBERG_ASSIGN_OR_RAISE(
        auto snapshot,
        Snapshot::Make(sequence_number, snapshot_id, parent_snapshot_id, TimePointMs{},
                       std::move(operation), {}, table_->metadata()->current_schema_id,
                       manifest_list_path));
    return std::shared_ptr<Snapshot>(std::move(snapshot));
  }

  // Inject a concurrent snapshot that adds an equality delete file covering partition
  // `partition_x`, layered on top of `parent`. Returns the metadata containing the new
  // snapshot (as current) plus the new snapshot itself, so a subsequent Validate(...) can
  // scan the range (parent, new] for new deletes.
  struct ConcurrentDelete {
    std::shared_ptr<TableMetadata> metadata;
    std::shared_ptr<Snapshot> snapshot;
  };
  ConcurrentDelete InjectConcurrentEqualityDelete(const std::shared_ptr<Snapshot>& parent,
                                                  const std::string& delete_path,
                                                  int64_t partition_x) {
    ConcurrentDelete out;
    auto del_file = MakeEqualityDeleteFile(delete_path, partition_x);
    const int64_t new_snapshot_id = parent->snapshot_id + 1000;
    const int64_t new_sequence_number = parent->sequence_number + 1;
    auto manifest =
        WriteDeleteManifest(table_location_ + "/metadata/concurrent-del-manifest.avro",
                            {del_file}, new_snapshot_id, new_sequence_number);
    EXPECT_TRUE(manifest.has_value());
    auto snap = MakeSyntheticSnapshot(DataOperation::kOverwrite, new_snapshot_id,
                                      parent->snapshot_id, new_sequence_number,
                                      {manifest.value()});
    if (!snap.has_value()) {
      ADD_FAILURE() << "MakeSyntheticSnapshot failed: " << snap.error().message;
    }
    EXPECT_TRUE(snap.has_value());
    out.snapshot = snap.value();
    out.metadata = std::make_shared<TableMetadata>(*table_->metadata());
    out.metadata->snapshots.push_back(out.snapshot);
    out.metadata->current_snapshot_id = out.snapshot->snapshot_id;
    out.metadata->last_sequence_number = out.snapshot->sequence_number;
    return out;
  }

  Result<std::shared_ptr<OverwriteFiles>> NewOverwrite() {
    return table_->NewOverwrite();
  }

  // Commit file_a_ with FastAppend and refresh the table; returns its snapshot id.
  int64_t CommitFileA() {
    auto fa = table_->NewFastAppend();
    EXPECT_TRUE(fa.has_value());
    fa.value()->AppendFile(file_a_);
    EXPECT_THAT(fa.value()->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
    auto snap = table_->current_snapshot();
    EXPECT_TRUE(snap.has_value());
    return snap.value()->snapshot_id;
  }

  // Append a single file via FastAppend and refresh; returns the new snapshot.
  std::shared_ptr<Snapshot> CommitFastAppend(const std::shared_ptr<DataFile>& file) {
    auto fa = table_->NewFastAppend();
    EXPECT_TRUE(fa.has_value());
    fa.value()->AppendFile(file);
    EXPECT_THAT(fa.value()->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
    auto snap = table_->current_snapshot();
    EXPECT_TRUE(snap.has_value());
    return snap.value();
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
};

// =====================================================================================
// 9.2 Entry-point and builder-method tests
// =====================================================================================

// Req 1.1: Table::NewOverwrite() returns a valid builder for a standalone operation.
TEST_F(OverwriteFilesTest, TableNewOverwriteReturnsBuilder) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  ASSERT_NE(op, nullptr);
}

// Req 1.2, 1.3: Transaction::NewOverwrite() returns a valid builder registered with the
// transaction (commit deferred until the transaction commits).
TEST_F(OverwriteFilesTest, TransactionNewOverwriteReturnsBuilder) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, Transaction::Make(table_, TransactionKind::kUpdate));
  ICEBERG_UNWRAP_OR_FAIL(auto op, txn->NewOverwrite());
  ASSERT_NE(op, nullptr);

  (*op).OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L))).AddFile(file_a_);

  // Within a transaction, the builder is staged by its own Commit() before the
  // transaction is committed.
  EXPECT_THAT(op->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto committed, txn->Commit());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kOverwrite);
}

// Builder methods all return *this and chain (Req 2.1-2.3, 3.1, 4.1-4.2, 6.1, 6.3, 6.4).
TEST_F(OverwriteFilesTest, BuilderMethodsReturnSelfAndChain) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  auto* self = op.get();

  EXPECT_EQ(&op->AddFile(file_a_), self);
  EXPECT_EQ(&op->DeleteFile(file_b_), self);
  EXPECT_EQ(&op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L))), self);
  EXPECT_EQ(&op->ValidateFromSnapshot(42), self);
  EXPECT_EQ(&op->ConflictDetectionFilter(Expressions::Equal("x", Literal::Long(1L))),
            self);
  EXPECT_EQ(&op->ValidateNoConflictingData(), self);
  EXPECT_EQ(&op->ValidateNoConflictingDeletes(), self);
  EXPECT_EQ(&op->ValidateAddedFilesMatchOverwriteFilter(), self);
  EXPECT_EQ(&op->WithCaseSensitivity(false), self);

  // A single fluent chain compiles and returns the same instance.
  OverwriteFiles& chained =
      (*op)
          .AddFile(MakeDataFile("/data/chain.parquet", 1L))
          .OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)))
          .ValidateNoConflictingData();
  EXPECT_EQ(&chained, self);
}

// =====================================================================================
// 9.3 operation() truth-table tests (Req 5.1-5.4)
// =====================================================================================

TEST_F(OverwriteFilesTest, OperationAddOnlyIsAppend) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->AddFile(file_a_);
  EXPECT_EQ(op->operation(), DataOperation::kAppend);
}

TEST_F(OverwriteFilesTest, OperationDeleteOnlyIsDelete) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFile(file_a_);
  EXPECT_EQ(op->operation(), DataOperation::kDelete);
}

TEST_F(OverwriteFilesTest, OperationAddAndDeleteIsOverwrite) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFile(file_a_);
  op->AddFile(file_b_);
  EXPECT_EQ(op->operation(), DataOperation::kOverwrite);
}

TEST_F(OverwriteFilesTest, OperationPureRowFilterAndAddIsOverwrite) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(file_b_);
  EXPECT_EQ(op->operation(), DataOperation::kOverwrite);
}

TEST_F(OverwriteFilesTest, OperationNeitherIsOverwrite) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  EXPECT_EQ(op->operation(), DataOperation::kOverwrite);
}

// =====================================================================================
// 9.4 Commit-path and snapshot-control tests (Req 11.2-11.5)
// =====================================================================================

// DeleteFile + AddFile commits as overwrite and the recorded operation matches
// operation() (Req 11.4).
TEST_F(OverwriteFilesTest, CommitDeleteAndAddIsOverwrite) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFile(file_a_);
  op->AddFile(file_b_);
  const std::string expected_operation = op->operation();
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation), expected_operation);
  EXPECT_EQ(expected_operation, DataOperation::kOverwrite);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
}

// OverwriteByRowFilter + AddFile commits as overwrite.
TEST_F(OverwriteFilesTest, CommitRowFilterAndAddIsOverwrite) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(MakeDataFile("/data/new_x1.parquet", 1L));
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kOverwrite);
}

// An empty overwrite (no adds, no deletes) commits and records the overwrite operation.
TEST_F(OverwriteFilesTest, CommitEmptyOverwrite) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  EXPECT_EQ(op->operation(), DataOperation::kOverwrite);
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kOverwrite);
}

// Duplicate AddFile / DeleteFile are deduplicated by the underlying set types (Req 3.7).
TEST_F(OverwriteFilesTest, CommitDeduplicatesDuplicateAddAndDelete) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  auto add = MakeDataFile("/data/dup_add.parquet", 1L);
  op->DeleteFile(file_a_);
  op->DeleteFile(file_a_);  // duplicate delete
  op->AddFile(add);
  op->AddFile(add);  // duplicate add
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
}

// StageOnly commits the snapshot without advancing the target branch (Req 11.2).
TEST_F(OverwriteFilesTest, CommitStageOnlyDoesNotAdvanceCurrentSnapshot) {
  const int64_t base_snapshot_id = CommitFileA();
  const size_t base_snapshot_count = table_->metadata()->snapshots.size();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->StageOnly();
  op->AddFile(file_b_);
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  // The staged snapshot is recorded but the main branch still points at file_a's
  // snapshot.
  ICEBERG_UNWRAP_OR_FAIL(auto current, table_->current_snapshot());
  EXPECT_EQ(current->snapshot_id, base_snapshot_id);
  EXPECT_GT(table_->metadata()->snapshots.size(), base_snapshot_count);
}

// SetTargetBranch commits to a named branch (Req 11.2).
TEST_F(OverwriteFilesTest, CommitToTargetBranch) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->SetTargetBranch("audit");
  op->AddFile(file_b_);
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  EXPECT_TRUE(table_->metadata()->refs.contains("audit"));
}

// A custom Set(property, value) is carried into the committed snapshot summary
// (Req 11.2).
TEST_F(OverwriteFilesTest, CommitCustomSummaryProperty) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->Set("custom-prop", "custom-value");
  op->AddFile(file_b_);
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("custom-prop"), "custom-value");
}

// =====================================================================================
// 9.5 Bulk DeleteFiles tests (Req 3.3-3.7)
// =====================================================================================

// A DataFileSet + DeleteFileSet forwards data files to DeleteDataFile and delete files
// to DeleteDeleteFile; the committed snapshot reflects the data-file removal. (The
// delete file is forwarded to DeleteDeleteFile; with no matching committed delete file
// present its removal is a harmless no-op, mirroring the inherited missing-delete
// behavior.)
TEST_F(OverwriteFilesTest, BulkDeleteFilesRemovesDataAndDeleteFiles) {
  // Seed the table with a data file.
  {
    ICEBERG_UNWRAP_OR_FAIL(auto seed, NewOverwrite());
    seed->AddFile(file_a_);
    EXPECT_THAT(seed->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  auto del_file = MakeDeleteFile("/delete/del_a.parquet", 1L);

  // Build the bulk delete: remove file_a (data) plus del_file (delete file).
  DataFileSet data_files;
  data_files.insert(file_a_);
  DeleteFileSet delete_files;
  delete_files.insert(del_file);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFiles(data_files, delete_files);
  op->AddFile(file_b_);
  // Both a data file deletion and an add => overwrite.
  EXPECT_EQ(op->operation(), DataOperation::kOverwrite);
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
}

// Empty sets are a no-op: with no adds or deletes the builder is a bare overwrite.
TEST_F(OverwriteFilesTest, BulkDeleteFilesEmptySetsAreNoOp) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFiles(DataFileSet{}, DeleteFileSet{});
  EXPECT_EQ(op->operation(), DataOperation::kOverwrite);  // neither adds nor deletes
}

// A DataFileSet portion of DeleteFiles records data files such that DeletesDataFiles
// becomes true (observed via operation() == delete), equivalent to repeated DeleteFile.
TEST_F(OverwriteFilesTest, BulkDeleteFilesDataPortionMarksDelete) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  DataFileSet data_files;
  data_files.insert(file_a_);
  data_files.insert(file_b_);
  op->DeleteFiles(data_files, DeleteFileSet{});
  EXPECT_EQ(op->operation(), DataOperation::kDelete);
}

// DeleteFiles is equivalent to repeated DeleteFile for the data-file portion: both
// classify as delete and (after committing against a seeded table) remove the files.
TEST_F(OverwriteFilesTest, BulkDeleteFilesEquivalentToRepeatedDeleteFile) {
  // Seed file_a and file_b.
  {
    ICEBERG_UNWRAP_OR_FAIL(auto seed, NewOverwrite());
    seed->AddFile(file_a_);
    seed->AddFile(file_b_);
    EXPECT_THAT(seed->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  DataFileSet data_files;
  data_files.insert(file_a_);
  data_files.insert(file_b_);
  op->DeleteFiles(data_files, DeleteFileSet{});
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "0");
}

// =====================================================================================
// 9.6 Concurrency-validation tests (Req 8.2, 8.3, 9.2-9.5; Properties 6, 7)
// =====================================================================================

// ValidateNoConflictingData: a competing FastAppend that added a data file matching the
// resolved conflict-detection filter makes the overwrite commit fail (Req 8.3).
TEST_F(OverwriteFilesTest, ValidateNoConflictingDataDetectsConflictingAdd) {
  const int64_t first_id = CommitFileA();
  // Competing append of file_b (partition x=2) between read and commit.
  CommitFastAppend(file_b_);

  // The overwrite targets the x=2 range, so the concurrent add of file_b conflicts.
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(2L)));
  op->AddFile(MakeDataFile("/data/replacement_x2.parquet", 2L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingData();
  EXPECT_THAT(op->Commit(), IsError(ErrorKind::kValidationFailed));
}

// A non-conflicting concurrent change still commits, and the recorded operation matches
// operation() (Req 11.4, 11.5; Property 6).
TEST_F(OverwriteFilesTest, ValidateNoConflictingDataAllowsNonConflictingChange) {
  const int64_t first_id = CommitFileA();
  // Competing append of file_b in partition x=2.
  CommitFastAppend(file_b_);

  // The overwrite targets the x=1 range; the concurrent x=2 add does not conflict.
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(MakeDataFile("/data/replacement_x1.parquet", 1L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingData();
  op->ValidateNoConflictingDeletes();
  const std::string expected_operation = op->operation();
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation), expected_operation);
}

// ValidateNoConflictingDeletes: a competing snapshot that deleted a data file in the
// overwrite range makes the commit fail (Req 9.2-9.4).
TEST_F(OverwriteFilesTest, ValidateNoConflictingDeletesDetectsConflictingDelete) {
  const int64_t first_id = CommitFileA();

  // Competing overwrite removes file_a (partition x=1) between read and commit.
  {
    ICEBERG_UNWRAP_OR_FAIL(auto competing, NewOverwrite());
    competing->DeleteFile(file_a_);
    EXPECT_THAT(competing->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(MakeDataFile("/data/replacement_after_delete.parquet", 1L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingDeletes();
  EXPECT_THAT(op->Commit(), IsError(ErrorKind::kValidationFailed));
}

// ValidateNoConflictingDeletes allows a non-conflicting concurrent append to commit.
TEST_F(OverwriteFilesTest, ValidateNoConflictingDeletesAllowsNonConflictingChange) {
  const int64_t first_id = CommitFileA();
  // Competing append in partition x=2 (no deletes in the x=1 range).
  CommitFastAppend(file_b_);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(MakeDataFile("/data/replacement_no_conflict.parquet", 1L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingDeletes();
  EXPECT_THAT(op->Commit(), IsOk());
}

// =====================================================================================
// Fix #1: the explicit replaced-files delete branch (Path B) honors the
// ConflictDetectionFilter.
//
// Path B fires when explicit data files were registered for replacement
// (deleted_data_files_ non-empty) under ValidateNoConflictingDeletes(). It now calls the
// STATIC data_filter overload of ValidateNoNewDeletesForDataFiles, passing the raw
// conflict_detection_filter_ (which may be nullptr = "no filter, consider all delete
// files"). These tests inject a concurrent equality-delete file that
// covers the replaced data file (file_a, partition x=1) and assert that:
//   * with NO conflict filter, the concurrent delete is a conflict  => FAIL;
//   * with a conflict filter that does NOT cover x=1 (here x=2), the delete is filtered
//     out of the conflict scope                                     => SUCCEED.
//
// To exercise Path B in isolation, the builder uses explicit DeleteFile (no
// OverwriteByRowFilter), so RowFilter() stays AlwaysFalse and Path A is skipped.
// =====================================================================================

// No conflict filter => the concurrent delete on the replaced file is detected (Path B
// passes nullptr through, so all delete files are considered).
TEST_F(OverwriteFilesTest, PathBExplicitDeletesDetectsConcurrentDeleteWithoutFilter) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  // Concurrent commit adds an equality delete covering file_a (partition x=1).
  auto concurrent = InjectConcurrentEqualityDelete(
      first_snapshot, "/delete/concurrent_x1.parquet", /*partition_x=*/1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFile(file_a_);  // explicit replacement => deleted_data_files_ non-empty
  op->AddFile(MakeDataFile("/data/rewrite_x1.parquet", 1L));
  op->ValidateFromSnapshot(first_snapshot->snapshot_id);
  op->ValidateNoConflictingDeletes();
  // No ConflictDetectionFilter => Path B considers all concurrent deletes => conflict.
  EXPECT_THAT(op->Validate(*concurrent.metadata, concurrent.snapshot),
              IsError(ErrorKind::kValidationFailed));
}

// A conflict filter that does not cover the replaced file's partition narrows the scope,
// so the concurrent delete is filtered out and validation succeeds.
TEST_F(OverwriteFilesTest, PathBExplicitDeletesConflictFilterNarrowsScope) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  // Same concurrent equality delete covering file_a (partition x=1).
  auto concurrent = InjectConcurrentEqualityDelete(
      first_snapshot, "/delete/concurrent_x1.parquet", /*partition_x=*/1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFile(file_a_);
  op->AddFile(MakeDataFile("/data/rewrite_x1.parquet", 1L));
  op->ValidateFromSnapshot(first_snapshot->snapshot_id);
  // Conflict filter targets x=2, which does NOT cover the x=1 delete => filtered out.
  op->ConflictDetectionFilter(Expressions::Equal("x", Literal::Long(2L)));
  op->ValidateNoConflictingDeletes();
  EXPECT_THAT(op->Validate(*concurrent.metadata, concurrent.snapshot), IsOk());
}

// =====================================================================================
// 9.7 Strict added-file range validation tests (Req 10.1-10.6; Properties 9, 10)
//
// These exercise OverwriteFiles::Validate(...) directly (the same entry point the commit
// kernel invokes), which is sufficient and deterministic: the strict-range branch does
// not depend on concurrent snapshots.
// =====================================================================================

// Strict partition projection proves containment directly (Req 10.3).
TEST_F(OverwriteFilesTest, StrictRangeAcceptedByStrictProjection) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(MakeDataFile("/data/in_partition.parquet", 1L));
  op->ValidateAddedFilesMatchOverwriteFilter();
  EXPECT_THAT(op->Validate(*table_->metadata(), /*snapshot=*/nullptr), IsOk());
}

// Strict partition projection is insufficient (filter on a non-partition column) but the
// StrictMetricsEvaluator proves containment from the file's bounds (Req 10.3).
TEST_F(OverwriteFilesTest, StrictRangeAcceptedByStrictMetrics) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("y", Literal::Long(5L)));
  // y bounds [5, 5] => every row has y == 5, fully contained in the filter.
  op->AddFile(MakeDataFileWithYBounds("/data/y_eq_5.parquet", 1L, 5L, 5L));
  op->ValidateAddedFilesMatchOverwriteFilter();
  EXPECT_THAT(op->Validate(*table_->metadata(), /*snapshot=*/nullptr), IsOk());
}

// Neither the strict projection nor the metrics can prove containment => fail (Req 10.5).
TEST_F(OverwriteFilesTest, StrictRangeRejectedWhenNotProvable) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("y", Literal::Long(5L)));
  // y bounds [1, 10] => not all rows are y == 5.
  op->AddFile(MakeDataFileWithYBounds("/data/y_range.parquet", 1L, 1L, 10L));
  op->ValidateAddedFilesMatchOverwriteFilter();
  EXPECT_THAT(op->Validate(*table_->metadata(), /*snapshot=*/nullptr),
              IsError(ErrorKind::kValidationFailed));
}

// A file whose partition falls outside the inclusive projection is rejected (Req 10.4).
TEST_F(OverwriteFilesTest, StrictRangeRejectsFileOutsidePartitionRange) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(MakeDataFile("/data/wrong_partition.parquet", /*partition_x=*/2L));
  op->ValidateAddedFilesMatchOverwriteFilter();
  EXPECT_THAT(op->Validate(*table_->metadata(), /*snapshot=*/nullptr),
              IsError(ErrorKind::kValidationFailed));
}

// ValidateAddedFilesMatchOverwriteFilter without a row filter fails (Req 10.1, 10.2;
// Property 10).
TEST_F(OverwriteFilesTest, StrictRangeRequiresRowFilter) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->AddFile(MakeDataFile("/data/no_filter.parquet", 1L));
  op->ValidateAddedFilesMatchOverwriteFilter();
  EXPECT_THAT(op->Validate(*table_->metadata(), /*snapshot=*/nullptr),
              IsError(ErrorKind::kValidationFailed));
}

// Fix #2: added files belonging to MORE THAN ONE partition spec are rejected, since the
// validation resolves a single spec via DataSpec() (which requires exactly one spec among
// added files). DataSpec() fails fast with a multi-spec error.
TEST_F(OverwriteFilesTest, StrictRangeRejectsMultiplePartitionSpecs) {
  // Add a second partition spec (id 1) to the table metadata BEFORE creating the builder,
  // so the producer's base metadata can resolve both specs when files are staged.
  ICEBERG_UNWRAP_OR_FAIL(
      auto spec1, PartitionSpec::Make(*schema_, /*spec_id=*/1,
                                      {PartitionField(/*source_id=*/1, /*field_id=*/1001,
                                                      "x_v1", Transform::Identity())},
                                      /*allow_missing_fields=*/false));
  table_->metadata()->partition_specs.push_back(
      std::shared_ptr<PartitionSpec>(std::move(spec1)));
  // Confirm both specs resolve.
  ASSERT_THAT(table_->metadata()->PartitionSpecById(0), IsOk());
  ASSERT_THAT(table_->metadata()->PartitionSpecById(1), IsOk());

  auto file_spec0 = MakeDataFile("/data/spec0_x1.parquet", 1L);  // partition_spec_id 0
  auto file_spec1 = MakeDataFile("/data/spec1_x1.parquet", 1L);
  file_spec1->partition_spec_id = 1;

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(file_spec0);
  op->AddFile(file_spec1);
  op->ValidateAddedFilesMatchOverwriteFilter();
  // DataSpec() rejects the two distinct specs with an InvalidArgument error.
  EXPECT_THAT(op->Validate(*table_->metadata(), /*snapshot=*/nullptr),
              IsError(ErrorKind::kInvalidArgument));
}

// Fix #3: enabling the strict added-file range validation with a row filter set but NO
// added data files (e.g. a pure overwrite-by-filter with no AddFile) fails, because
// DataSpec() rejects an empty added-files set.
TEST_F(OverwriteFilesTest, StrictRangeRejectsEmptyAddedFiles) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  // Row filter is set (precondition satisfied) but no AddFile was called.
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->ValidateAddedFilesMatchOverwriteFilter();
  // DataSpec() raises InvalidArgument because no data file was added.
  EXPECT_THAT(op->Validate(*table_->metadata(), /*snapshot=*/nullptr),
              IsError(ErrorKind::kInvalidArgument));
}

// =====================================================================================
// 9.8 Case-sensitivity and null-rejection tests (Req 6.4, 12.1-12.4; Property 4)
// =====================================================================================

// Case-insensitive binding accepts a differently-cased column name where the
// case-sensitive (default) binding rejects it. Observed through the strict-range
// validation, which binds the row filter using the configured case sensitivity.
TEST_F(OverwriteFilesTest, CaseSensitivityAffectsFilterBinding) {
  // Case-sensitive (default): the filter references "X" which does not match column "x".
  {
    ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
    op->OverwriteByRowFilter(Expressions::Equal("X", Literal::Long(1L)));
    op->AddFile(MakeDataFile("/data/cs.parquet", 1L));
    op->ValidateAddedFilesMatchOverwriteFilter();
    // Binding "X" against schema {x, y, z} fails case-sensitively.
    auto result = op->Validate(*table_->metadata(), /*snapshot=*/nullptr);
    EXPECT_FALSE(result.has_value());
  }
  // Case-insensitive: "X" binds to column "x" and the in-range file validates.
  {
    ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
    op->WithCaseSensitivity(false);
    op->OverwriteByRowFilter(Expressions::Equal("X", Literal::Long(1L)));
    op->AddFile(MakeDataFile("/data/ci.parquet", 1L));
    op->ValidateAddedFilesMatchOverwriteFilter();
    EXPECT_THAT(op->Validate(*table_->metadata(), /*snapshot=*/nullptr), IsOk());
  }
}

// Null arguments to the pointer-taking builder methods surface at Commit() without
// crashing (Req 12.1, 12.2; Property 4). The ErrorCollector aggregates deferred builder
// errors and surfaces them as a kValidationFailed error at Commit() that preserves the
// underlying invalid-argument message.
TEST_F(OverwriteFilesTest, NullAddFileRejectedAtCommit) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->AddFile(nullptr);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid data file: null"));
}

TEST_F(OverwriteFilesTest, NullDeleteFileRejectedAtCommit) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFile(nullptr);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid data file: null"));
}

TEST_F(OverwriteFilesTest, NullOverwriteByRowFilterRejectedAtCommit) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(nullptr);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid row filter expression: null"));
}

TEST_F(OverwriteFilesTest, NullConflictDetectionFilterRejectedAtCommit) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->ConflictDetectionFilter(nullptr);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid conflict detection filter: null"));
}

// The builder chain continues without crashing after a null argument is recorded.
TEST_F(OverwriteFilesTest, NullArgumentDoesNotCrashBuilderChain) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  (*op).AddFile(nullptr).AddFile(file_a_).OverwriteByRowFilter(
      Expressions::Equal("x", Literal::Long(1L)));
  // The recorded error still surfaces at commit.
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid data file: null"));
}

// A null element cannot enter a DataFileSet / DeleteFileSet (insert() rejects nullptr),
// so DeleteFiles({null...}, {null...}) is a no-op rather than an error. The deferred
// null-element rejection inside DeleteFiles is therefore a defensive guard that is
// unreachable through the public set API; this test documents that observable behavior.
TEST_F(OverwriteFilesTest, DeleteFilesNullElementsCannotEnterSets) {
  DataFileSet data_files;
  data_files.insert(std::shared_ptr<DataFile>{nullptr});
  DeleteFileSet delete_files;
  delete_files.insert(std::shared_ptr<DataFile>{nullptr});
  EXPECT_TRUE(data_files.empty());
  EXPECT_TRUE(delete_files.empty());

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFiles(data_files, delete_files);
  // No data files were deleted => still a bare overwrite, and commit succeeds.
  EXPECT_EQ(op->operation(), DataOperation::kOverwrite);
  EXPECT_THAT(op->Commit(), IsOk());
}

// ValidateFromSnapshot accepts a non-negative id (0 is in the generated id range
// [0, INT64_MAX]) and rejects a negative id (including kInvalidSnapshotId == -1) as a
// deferred error surfaced at Commit(). Req 6.1.
TEST_F(OverwriteFilesTest, ValidateFromSnapshotRejectsNegativeSnapshotId) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->AddFile(file_a_).ValidateFromSnapshot(-1);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid snapshot id"));
}

TEST_F(OverwriteFilesTest, ValidateFromSnapshotAcceptsZeroSnapshotId) {
  // 0 is a legal generated snapshot id (the generator masks with int64_t::max()), so it
  // must not be rejected at the builder stage. With no concurrency validation enabled,
  // the starting id is merely recorded and the commit succeeds.
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->AddFile(file_a_).ValidateFromSnapshot(0);
  EXPECT_THAT(op->Commit(), IsOk());
}

// =====================================================================================
// Property-style tests (parameterized via loops; no PBT library is available).
// =====================================================================================

// Property 2 (builder forwarding) + Property 3 (delete dual-tracking), Task 2.4.
//
// AddedDataFiles() / deleted_data_files_ are not publicly observable, so the properties
// are checked indirectly through operation(): for any non-null file, AddFile-only yields
// `append` (the file was added and the row filter is untouched), while DeleteFile-only
// and DeleteFiles-only (data portion) yield `delete` (the file was registered for
// deletion and DeletesDataFiles() became true). Validates Req 2.1, 2.2, 3.1-3.5.
TEST_F(OverwriteFilesTest, PropertyBuilderForwardingAndDualTracking) {
  const std::vector<std::shared_ptr<DataFile>> files = {
      MakeDataFile("/data/p0.parquet", 1L),
      MakeDataFile("/data/p1.parquet", 2L),
      MakeDataFile("/data/p2.parquet", 3L),
      MakeDataFile("/data/p3.parquet", 7L),
  };

  for (const auto& file : files) {
    {
      // AddFile preserves "no deletes" => append, proving the row filter is unchanged.
      ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
      op->AddFile(file);
      EXPECT_EQ(op->operation(), DataOperation::kAppend) << file->file_path;
    }
    {
      // DeleteFile dual-tracks => DeletesDataFiles() true, no adds => delete.
      ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
      op->DeleteFile(file);
      EXPECT_EQ(op->operation(), DataOperation::kDelete) << file->file_path;
    }
    {
      // Bulk DeleteFiles data portion behaves like repeated DeleteFile.
      ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
      DataFileSet data_files;
      data_files.insert(file);
      op->DeleteFiles(data_files, DeleteFileSet{});
      EXPECT_EQ(op->operation(), DataOperation::kDelete) << file->file_path;
    }
  }
}

// Property 4 (null rejection), Task 2.5: every pointer-taking builder mutator records a
// deferred error that surfaces as an InvalidArgument-class error at Commit() without
// crashing. Validates Req 12.1, 12.2, 12.3, 12.4.
TEST_F(OverwriteFilesTest, PropertyNullArgumentRejection) {
  using Mutator = std::function<void(OverwriteFiles&)>;
  const std::vector<Mutator> mutators = {
      [](OverwriteFiles& op) { op.AddFile(nullptr); },
      [](OverwriteFiles& op) { op.DeleteFile(nullptr); },
      [](OverwriteFiles& op) { op.OverwriteByRowFilter(nullptr); },
      [](OverwriteFiles& op) { op.ConflictDetectionFilter(nullptr); },
  };

  for (const auto& mutate : mutators) {
    ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
    mutate(*op);
    // Deferred builder errors are aggregated and surfaced as kValidationFailed at commit.
    EXPECT_THAT(op->Commit(), IsError(ErrorKind::kValidationFailed));
  }
}

// Property 1 (operation() reflects content), Task 3.2: exhaustive truth table over
// {adds, deletes (explicit or via row filter)}. Validates Req 5.1-5.4.
TEST_F(OverwriteFilesTest, PropertyOperationTruthTable) {
  struct Case {
    bool add;
    bool delete_file;
    bool row_filter;
    std::string expected;
  };
  const std::vector<Case> cases = {
      {/*add=*/true, /*delete_file=*/false, /*row_filter=*/false, DataOperation::kAppend},
      {false, true, false, DataOperation::kDelete},
      {false, false, true, DataOperation::kDelete},  // row filter counts as a delete
      {true, true, false, DataOperation::kOverwrite},
      {true, false, true, DataOperation::kOverwrite},
      {false, true, true, DataOperation::kDelete},  // deletes only (no adds) => delete
      {false, false, false, DataOperation::kOverwrite},  // neither
  };

  int index = 0;
  for (const auto& c : cases) {
    ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
    if (c.add) {
      op->AddFile(MakeDataFile("/data/tt_add" + std::to_string(index) + ".parquet", 1L));
    }
    if (c.delete_file) {
      op->DeleteFile(
          MakeDataFile("/data/tt_del" + std::to_string(index) + ".parquet", 1L));
    }
    if (c.row_filter) {
      op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
    }
    EXPECT_EQ(op->operation(), c.expected) << "case index " << index;
    ++index;
  }
}

// Property 5 (conflict filter resolution), Task 4.4.
//
// DataConflictDetectionFilter() is private, so its three resolution outcomes are observed
// indirectly through ValidateNoConflictingData against a competing concurrent add of
// file_b (partition x=2):
//   * explicit filter set      -> the explicit filter is used (overrides the row filter);
//   * row filter only          -> the row filter is used;
//   * file-replacement present -> AlwaysTrue (any concurrent add conflicts).
// Validates Req 7.1, 7.2, 7.3.
TEST_F(OverwriteFilesTest, PropertyConflictFilterResolution) {
  // Resolution case 2 (row filter only): row filter x=1 does NOT match the concurrent
  // x=2 add -> no conflict.
  {
    const int64_t first_id = CommitFileA();
    CommitFastAppend(file_b_);
    ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
    op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
    op->AddFile(MakeDataFile("/data/r2_ok.parquet", 1L));
    op->ValidateFromSnapshot(first_id);
    op->ValidateNoConflictingData();
    EXPECT_THAT(op->Validate(*table_->metadata(), table_->current_snapshot().value()),
                IsOk());
  }
  // Resolution case 2 (row filter only): row filter x=2 DOES match -> conflict, proving
  // the row filter (not AlwaysFalse / AlwaysTrue blindly) is what was used.
  {
    SetUp();  // fresh table
    const int64_t first_id = CommitFileA();
    CommitFastAppend(file_b_);
    ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
    op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(2L)));
    op->AddFile(MakeDataFile("/data/r2_conflict.parquet", 2L));
    op->ValidateFromSnapshot(first_id);
    op->ValidateNoConflictingData();
    EXPECT_THAT(op->Validate(*table_->metadata(), table_->current_snapshot().value()),
                IsError(ErrorKind::kValidationFailed));
  }
  // Resolution case 1 (explicit filter overrides row filter): row filter x=1 would NOT
  // conflict, but the explicit conflict filter x=2 does -> conflict.
  {
    SetUp();
    const int64_t first_id = CommitFileA();
    CommitFastAppend(file_b_);
    ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
    op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
    op->ConflictDetectionFilter(Expressions::Equal("x", Literal::Long(2L)));
    op->AddFile(MakeDataFile("/data/r1.parquet", 1L));
    op->ValidateFromSnapshot(first_id);
    op->ValidateNoConflictingData();
    EXPECT_THAT(op->Validate(*table_->metadata(), table_->current_snapshot().value()),
                IsError(ErrorKind::kValidationFailed));
  }
  // Resolution case 3 (file replacement -> AlwaysTrue): with an explicit DeleteFile and
  // no explicit conflict filter, ANY concurrent add conflicts (here file_b in x=2, even
  // though the row filter is x=1).
  {
    SetUp();
    const int64_t first_id = CommitFileA();
    CommitFastAppend(file_b_);
    ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
    op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
    op->DeleteFile(file_a_);  // makes deleted_data_files_ non-empty => AlwaysTrue
    op->AddFile(MakeDataFile("/data/r3.parquet", 1L));
    op->ValidateFromSnapshot(first_id);
    op->ValidateNoConflictingData();
    EXPECT_THAT(op->Validate(*table_->metadata(), table_->current_snapshot().value()),
                IsError(ErrorKind::kValidationFailed));
  }
}

}  // namespace iceberg
