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

#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/constants.h"
#include "iceberg/expression/expressions.h"
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
#include "iceberg/update/row_delta.h"
#include "iceberg/util/data_file_set.h"
#include "iceberg/util/macros.h"

namespace iceberg {

// The base table (TableMetadataV2ValidMinimal.json) has schema {x: long (id 1),
// y: long (id 2), z: long (id 3)} and partitions by identity(x).
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

  // Add y metrics so StrictMetricsEvaluator can prove row-filter containment.
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

  std::shared_ptr<DataFile> MakeEqualityDeleteFile(const std::string& path,
                                                   int64_t partition_x) {
    auto f = MakeDeleteFile(path, partition_x);
    f->content = DataFile::Content::kEqualityDeletes;
    f->equality_ids = {1};
    return f;
  }

  void CommitEqualityDelete(const std::string& delete_path, int64_t partition_x) {
    auto del_file = MakeEqualityDeleteFile(delete_path, partition_x);
    ICEBERG_UNWRAP_OR_FAIL(auto row_delta, table_->NewRowDelta());
    row_delta->AddDeletes(del_file);
    EXPECT_THAT(row_delta->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  Result<std::shared_ptr<OverwriteFiles>> NewOverwrite() {
    return table_->NewOverwrite();
  }

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

TEST_F(OverwriteFilesTest, TxnNewOverwrite) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, Transaction::Make(table_, TransactionKind::kUpdate));
  ICEBERG_UNWRAP_OR_FAIL(auto op, txn->NewOverwrite());
  ASSERT_NE(op, nullptr);

  (*op).OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L))).AddFile(file_a_);

  EXPECT_THAT(op->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto committed, txn->Commit());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kOverwrite);
}

TEST_F(OverwriteFilesTest, DeleteAndAddCommit) {
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

TEST_F(OverwriteFilesTest, RowFilterAndAddCommit) {
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

TEST_F(OverwriteFilesTest, EmptyCommit) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  EXPECT_EQ(op->operation(), DataOperation::kOverwrite);
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kOverwrite);
}

TEST_F(OverwriteFilesTest, DeduplicatesFiles) {
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

TEST_F(OverwriteFilesTest, StageOnly) {
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

TEST_F(OverwriteFilesTest, TargetBranch) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->ToBranch("audit");
  op->AddFile(file_b_);
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  EXPECT_TRUE(table_->metadata()->refs.contains("audit"));
}

TEST_F(OverwriteFilesTest, CustomSummary) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->Set("custom-prop", "custom-value");
  op->AddFile(file_b_);
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("custom-prop"), "custom-value");
}

// With no matching committed delete file, deleting `del_file` is a harmless no-op.
TEST_F(OverwriteFilesTest, BulkDeleteCommit) {
  {
    ICEBERG_UNWRAP_OR_FAIL(auto seed, NewOverwrite());
    seed->AddFile(file_a_);
    EXPECT_THAT(seed->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  auto del_file = MakeDeleteFile("/delete/del_a.parquet", 1L);

  DataFileSet data_files;
  data_files.insert(file_a_);
  DeleteFileSet delete_files;
  delete_files.insert(del_file);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFiles(data_files, delete_files);
  op->AddFile(file_b_);
  EXPECT_EQ(op->operation(), DataOperation::kOverwrite);
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
}

TEST_F(OverwriteFilesTest, BulkDeleteData) {
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

// OverwriteFiles validates content because the C++ API stores data and delete files in
// DataFile pointers, while Java uses separate DataFile/DeleteFile types.
TEST_F(OverwriteFilesTest, AddRejectsDeleteContent) {
  auto del_file = MakeDeleteFile("/delete/del_as_data.parquet", 1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->AddFile(del_file);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid data file to add"));
  EXPECT_THAT(result, HasErrorMessage("has delete-file content"));
}

TEST_F(OverwriteFilesTest, DeleteRejectsDeleteContent) {
  auto del_file = MakeDeleteFile("/delete/del_as_delete.parquet", 1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFile(del_file);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid data file to delete"));
  EXPECT_THAT(result, HasErrorMessage("has delete-file content"));
}

TEST_F(OverwriteFilesTest, BulkRejectsDeleteAsData) {
  auto del_file =
      MakeDeleteFile("/delete/del_a.parquet", 1L);  // content = positionDeletes
  DataFileSet data_files;
  data_files.insert(del_file);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFiles(data_files, DeleteFileSet{});
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("has delete-file content"));
}

TEST_F(OverwriteFilesTest, BulkRejectsDataAsDelete) {
  DeleteFileSet delete_files;
  delete_files.insert(file_a_);  // content = kData

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFiles(DataFileSet{}, delete_files);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("has data-file content"));
}

TEST_F(OverwriteFilesTest, BulkAcceptsEqualityDelete) {
  auto eq_delete = MakeEqualityDeleteFile("/delete/eq_a.parquet", 1L);
  DeleteFileSet delete_files;
  delete_files.insert(eq_delete);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFiles(DataFileSet{}, delete_files);
  op->AddFile(file_b_);
  EXPECT_THAT(op->Commit(), IsOk());
}

TEST_F(OverwriteFilesTest, NoConflictingDeletesFails) {
  const int64_t first_id = CommitFileA();

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

TEST_F(OverwriteFilesTest, NoConflictingDeletesPasses) {
  const int64_t first_id = CommitFileA();
  CommitFastAppend(file_b_);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(MakeDataFile("/data/replacement_no_conflict.parquet", 1L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingDeletes();
  EXPECT_THAT(op->Commit(), IsOk());
}

// Explicit replaced-file validation checks concurrent deletes covering replaced files.
TEST_F(OverwriteFilesTest, ExplicitDeleteConflict) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  CommitEqualityDelete("/delete/concurrent_x1.parquet", /*partition_x=*/1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFile(file_a_);
  op->AddFile(MakeDataFile("/data/rewrite_x1.parquet", 1L));
  op->ValidateFromSnapshot(first_snapshot->snapshot_id);
  op->ValidateNoConflictingDeletes();
  EXPECT_THAT(op->Commit(), IsError(ErrorKind::kValidationFailed));
}

// A narrower conflict filter can exclude the concurrent delete.
TEST_F(OverwriteFilesTest, ExplicitDeleteFilterScope) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  CommitEqualityDelete("/delete/concurrent_x1.parquet", /*partition_x=*/1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFile(file_a_);
  op->AddFile(MakeDataFile("/data/rewrite_x1.parquet", 1L));
  op->ValidateFromSnapshot(first_snapshot->snapshot_id);
  op->ConflictDetectionFilter(Expressions::Equal("x", Literal::Long(2L)));
  op->ValidateNoConflictingDeletes();
  EXPECT_THAT(op->Commit(), IsOk());
}

TEST_F(OverwriteFilesTest, StrictRangeByProjection) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(MakeDataFile("/data/in_partition.parquet", 1L));
  op->ValidateAddedFilesMatchOverwriteFilter();
  EXPECT_THAT(op->Commit(), IsOk());
}

TEST_F(OverwriteFilesTest, StrictRangeByMetrics) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("y", Literal::Long(5L)));
  // y bounds [5, 5] prove every row has y == 5.
  op->AddFile(MakeDataFileWithYBounds("/data/y_eq_5.parquet", 1L, 5L, 5L));
  op->ValidateAddedFilesMatchOverwriteFilter();
  EXPECT_THAT(op->Commit(), IsOk());
}

TEST_F(OverwriteFilesTest, StrictRangeRejectsPartialMetrics) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("y", Literal::Long(5L)));
  // y bounds [1, 10] do not prove every row has y == 5.
  op->AddFile(MakeDataFileWithYBounds("/data/y_range.parquet", 1L, 1L, 10L));
  op->ValidateAddedFilesMatchOverwriteFilter();
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result,
              HasErrorMessage("Cannot append file with rows that do not match filter"));
}

TEST_F(OverwriteFilesTest, StrictRangeRejectsOutsidePartition) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(MakeDataFile("/data/wrong_partition.parquet", /*partition_x=*/2L));
  op->ValidateAddedFilesMatchOverwriteFilter();
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result,
              HasErrorMessage("Cannot append file with rows that do not match filter"));
}

TEST_F(OverwriteFilesTest, StrictRangeRequiresFilter) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->AddFile(MakeDataFile("/data/no_filter.parquet", 1L));
  op->ValidateAddedFilesMatchOverwriteFilter();
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result,
              HasErrorMessage("Cannot append file with rows that do not match filter"));
}

TEST_F(OverwriteFilesTest, StrictRangeRejectsMultipleSpecs) {
  // Add the second spec before creating the builder so staged files can resolve it.
  ICEBERG_UNWRAP_OR_FAIL(
      auto spec1, PartitionSpec::Make(*schema_, /*spec_id=*/1,
                                      {PartitionField(/*source_id=*/1, /*field_id=*/1001,
                                                      "x_v1", Transform::Identity())},
                                      /*allow_missing_fields=*/false));
  table_->metadata()->partition_specs.push_back(
      std::shared_ptr<PartitionSpec>(std::move(spec1)));
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
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Cannot return a single partition spec"));
}

TEST_F(OverwriteFilesTest, StrictRangeRejectsNoAdds) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->ValidateAddedFilesMatchOverwriteFilter();
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Cannot determine partition specs"));
}

// Strict-range validation binds the row filter with the configured case sensitivity.
TEST_F(OverwriteFilesTest, StrictRangeCaseSensitivity) {
  {
    ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
    op->OverwriteByRowFilter(Expressions::Equal("X", Literal::Long(1L)));
    op->AddFile(MakeDataFile("/data/cs.parquet", 1L));
    op->ValidateAddedFilesMatchOverwriteFilter();
    auto result = op->Commit();
    EXPECT_FALSE(result.has_value());
  }
  {
    ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
    op->CaseSensitive(false);
    op->OverwriteByRowFilter(Expressions::Equal("X", Literal::Long(1L)));
    op->AddFile(MakeDataFile("/data/ci.parquet", 1L));
    op->ValidateAddedFilesMatchOverwriteFilter();
    EXPECT_THAT(op->Commit(), IsOk());
  }
}

TEST_F(OverwriteFilesTest, NullAddFileRejected) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->AddFile(nullptr);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid data file: null"));
}

TEST_F(OverwriteFilesTest, NullDeleteFileRejected) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->DeleteFile(nullptr);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid data file: null"));
}

TEST_F(OverwriteFilesTest, NullRowFilterRejected) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(nullptr);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid row filter expression: null"));
}

TEST_F(OverwriteFilesTest, NullConflictFilterRejected) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->ConflictDetectionFilter(nullptr);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid conflict detection filter: null"));
}

TEST_F(OverwriteFilesTest, RejectsNegativeSnapshotId) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->AddFile(file_a_).ValidateFromSnapshot(-1);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid snapshot id"));
}

TEST_F(OverwriteFilesTest, AcceptsZeroSnapshotId) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->AddFile(file_a_).ValidateFromSnapshot(0);
  EXPECT_THAT(op->Commit(), IsOk());
}

TEST_F(OverwriteFilesTest, OperationMatrix) {
  struct Case {
    bool add;
    bool delete_file;
    bool row_filter;
    std::string expected;
  };
  const std::vector<Case> cases = {
      {.add = true,
       .delete_file = false,
       .row_filter = false,
       .expected = DataOperation::kAppend},
      {.add = false,
       .delete_file = true,
       .row_filter = false,
       .expected = DataOperation::kDelete},
      {.add = false,
       .delete_file = false,
       .row_filter = true,
       .expected = DataOperation::kDelete},  // row filter counts as a delete
      {.add = true,
       .delete_file = true,
       .row_filter = false,
       .expected = DataOperation::kOverwrite},
      {.add = true,
       .delete_file = false,
       .row_filter = true,
       .expected = DataOperation::kOverwrite},
      {.add = false,
       .delete_file = true,
       .row_filter = true,
       .expected = DataOperation::kDelete},  // deletes only
      {.add = false,
       .delete_file = false,
       .row_filter = false,
       .expected = DataOperation::kOverwrite},  // neither
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

TEST_F(OverwriteFilesTest, DefaultConflictFilter) {
  const int64_t first_id = CommitFileA();
  CommitFastAppend(file_b_);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->AddFile(MakeDataFile("/data/r2_ok.parquet", 1L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingData();
  EXPECT_THAT(op->Commit(), IsOk());
}

TEST_F(OverwriteFilesTest, ConflictFilterMatchesAdd) {
  const int64_t first_id = CommitFileA();
  CommitFastAppend(file_b_);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(2L)));
  op->AddFile(MakeDataFile("/data/r2_conflict.parquet", 2L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingData();
  EXPECT_THAT(op->Commit(), IsError(ErrorKind::kValidationFailed));
}

TEST_F(OverwriteFilesTest, ConflictFilterUsesExplicitFilter) {
  const int64_t first_id = CommitFileA();
  CommitFastAppend(file_b_);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->ConflictDetectionFilter(Expressions::Equal("x", Literal::Long(2L)));
  op->AddFile(MakeDataFile("/data/r1.parquet", 1L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingData();
  EXPECT_THAT(op->Commit(), IsError(ErrorKind::kValidationFailed));
}

TEST_F(OverwriteFilesTest, ExplicitReplaceConflicts) {
  const int64_t first_id = CommitFileA();
  CommitFastAppend(file_b_);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwrite());
  op->OverwriteByRowFilter(Expressions::Equal("x", Literal::Long(1L)));
  op->DeleteFile(file_a_);
  op->AddFile(MakeDataFile("/data/r3.parquet", 1L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingData();
  EXPECT_THAT(op->Commit(), IsError(ErrorKind::kValidationFailed));
}

}  // namespace iceberg
