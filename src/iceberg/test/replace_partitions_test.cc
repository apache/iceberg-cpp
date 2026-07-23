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

#include "iceberg/update/replace_partitions.h"

#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
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
#include "iceberg/util/macros.h"

namespace iceberg {

// The base table (TableMetadataV2ValidMinimal.json) has schema {x: long (id 1),
// y: long (id 2), z: long (id 3)} and partitions by identity(x) as spec 0.
class ReplacePartitionsTest : public UpdateTestBase {
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

  // A data file that belongs to an unpartitioned spec (empty partition tuple).
  std::shared_ptr<DataFile> MakeUnpartitionedFile(const std::string& path,
                                                  int32_t spec_id) {
    auto f = std::make_shared<DataFile>();
    f->content = DataFile::Content::kData;
    f->file_path = table_location_ + path;
    f->file_format = FileFormatType::kParquet;
    f->partition = PartitionValues(std::vector<Literal>{});
    f->file_size_in_bytes = 1024;
    f->record_count = 100;
    f->partition_spec_id = spec_id;
    return f;
  }

  std::shared_ptr<DataFile> MakeEqualityDeleteFile(const std::string& path,
                                                   int64_t partition_x) {
    auto f = MakeDataFile(path, partition_x);
    f->content = DataFile::Content::kEqualityDeletes;
    f->equality_ids = {1};
    return f;
  }

  // Add an extra spec to the in-memory metadata so staged files can resolve it.
  // The empty field list makes it unpartitioned (PartitionSpec::isUnpartitioned).
  void AddUnpartitionedSpec(int32_t spec_id) {
    ICEBERG_UNWRAP_OR_FAIL(auto spec,
                           PartitionSpec::Make(*schema_, spec_id, {},
                                               /*allow_missing_fields=*/false));
    table_->metadata()->partition_specs.push_back(
        std::shared_ptr<PartitionSpec>(std::move(spec)));
    ASSERT_THAT(table_->metadata()->PartitionSpecById(spec_id), IsOk());
  }

  void AddIdentitySpec(int32_t spec_id, int32_t field_id, const std::string& name) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto spec, PartitionSpec::Make(*schema_, spec_id,
                                       {PartitionField(/*source_id=*/1, field_id, name,
                                                       Transform::Identity())},
                                       /*allow_missing_fields=*/false));
    table_->metadata()->partition_specs.push_back(
        std::shared_ptr<PartitionSpec>(std::move(spec)));
    ASSERT_THAT(table_->metadata()->PartitionSpecById(spec_id), IsOk());
  }

  Result<std::unique_ptr<ReplacePartitions>> NewReplace() {
    ICEBERG_ASSIGN_OR_RAISE(auto ctx,
                            TransactionContext::Make(table_, TransactionKind::kUpdate));
    return ReplacePartitions::Make(TableName(), std::move(ctx));
  }

  int64_t CommitFastAppend(const std::shared_ptr<DataFile>& file) {
    auto fa = table_->NewFastAppend();
    EXPECT_TRUE(fa.has_value());
    fa.value()->AppendFile(file);
    EXPECT_THAT(fa.value()->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
    auto snap = table_->current_snapshot();
    EXPECT_TRUE(snap.has_value());
    return snap.value()->snapshot_id;
  }

  void CommitEqualityDelete(const std::string& delete_path, int64_t partition_x) {
    auto del_file = MakeEqualityDeleteFile(delete_path, partition_x);
    ICEBERG_UNWRAP_OR_FAIL(auto row_delta, table_->NewRowDelta());
    row_delta->AddDeletes(del_file);
    EXPECT_THAT(row_delta->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
};

TEST_F(ReplacePartitionsTest, OperationIsOverwrite) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  EXPECT_EQ(op->operation(), DataOperation::kOverwrite);
}

// Replacing a partition drops its existing file and records the summary flag.
TEST_F(ReplacePartitionsTest, PartitionedReplaceCommit) {
  CommitFastAppend(file_a_);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(MakeDataFile("/data/file_a_new.parquet", /*partition_x=*/1L));
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kOverwrite);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kReplacePartitions), "true");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
}

// Only the referenced partition is replaced; other partitions are untouched.
TEST_F(ReplacePartitionsTest, ReplaceLeavesOtherPartitions) {
  CommitFastAppend(file_a_);
  CommitFastAppend(file_b_);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(MakeDataFile("/data/file_a_new.parquet", /*partition_x=*/1L));
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
  // file_b in partition 2 plus the new file in partition 1.
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "2");
}

// An unpartitioned spec triggers a table-wide replace of every existing file.
TEST_F(ReplacePartitionsTest, UnpartitionedReplacesWholeTable) {
  CommitFastAppend(file_a_);
  CommitFastAppend(file_b_);

  AddUnpartitionedSpec(/*spec_id=*/1);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(MakeUnpartitionedFile("/data/all.parquet", /*spec_id=*/1));
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  // Both partitioned files replaced by the single unpartitioned file.
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "1");
}

// No staged files means there is no partition spec to act on.
TEST_F(ReplacePartitionsTest, EmptyReplaceRejected) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Cannot determine partition specs"));
}

// Files from two different partitioned specs cannot be replaced together.
TEST_F(ReplacePartitionsTest, MixedSpecsRejected) {
  AddIdentitySpec(/*spec_id=*/1, /*field_id=*/1001, "x_v1");

  auto file_spec0 = MakeDataFile("/data/spec0.parquet", /*partition_x=*/1L);
  auto file_spec1 = MakeDataFile("/data/spec1.parquet", /*partition_x=*/1L);
  file_spec1->partition_spec_id = 1;

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(file_spec0);
  op->AddFile(file_spec1);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Cannot return a single partition spec"));
}

// Regression: an unpartitioned file staged first must not let a later file from
// another spec skip the single-spec check and commit a table-wide replace.
TEST_F(ReplacePartitionsTest, UnpartitionedFirstThenMixedRejected) {
  AddUnpartitionedSpec(/*spec_id=*/1);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(MakeUnpartitionedFile("/data/all.parquet", /*spec_id=*/1));
  op->AddFile(MakeDataFile("/data/part.parquet", /*partition_x=*/1L));  // spec 0
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Cannot return a single partition spec"));
}

// ValidateAppendOnly fails when data already exists in the target partition and
// the error is translated into Java's partition-conflict wording.
TEST_F(ReplacePartitionsTest, AppendOnlyConflictTranslated) {
  CommitFastAppend(file_a_);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(MakeDataFile("/data/file_a_new.parquet", /*partition_x=*/1L));
  op->ValidateAppendOnly();
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage(
                          "Cannot commit file that conflicts with existing partition"));
}

// ValidateAppendOnly passes when the target partition holds no existing files.
TEST_F(ReplacePartitionsTest, AppendOnlyPassesEmptyPartition) {
  CommitFastAppend(file_a_);  // partition 1

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(MakeDataFile("/data/file_b_new.parquet", /*partition_x=*/2L));
  op->ValidateAppendOnly();
  EXPECT_THAT(op->Commit(), IsOk());
}

// A concurrent append in the replaced partition is a conflict.
TEST_F(ReplacePartitionsTest, NoConflictingDataSamePartitionFails) {
  const int64_t first_id = CommitFastAppend(file_a_);
  CommitFastAppend(MakeDataFile("/data/concurrent_x1.parquet", /*partition_x=*/1L));

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(MakeDataFile("/data/replacement_x1.parquet", /*partition_x=*/1L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingData();
  EXPECT_THAT(op->Commit(), IsError(ErrorKind::kValidationFailed));
}

// A concurrent append in a different partition does not conflict.
TEST_F(ReplacePartitionsTest, NoConflictingDataDifferentPartitionPasses) {
  const int64_t first_id = CommitFastAppend(file_a_);
  CommitFastAppend(MakeDataFile("/data/concurrent_x2.parquet", /*partition_x=*/2L));

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(MakeDataFile("/data/replacement_x1.parquet", /*partition_x=*/1L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingData();
  EXPECT_THAT(op->Commit(), IsOk());
}

// A concurrent delete file in the replaced partition is a conflict.
TEST_F(ReplacePartitionsTest, NoConflictingDeletesFails) {
  const int64_t first_id = CommitFastAppend(file_a_);
  CommitEqualityDelete("/delete/concurrent_x1.parquet", /*partition_x=*/1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(MakeDataFile("/data/replacement_x1.parquet", /*partition_x=*/1L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingDeletes();
  EXPECT_THAT(op->Commit(), IsError(ErrorKind::kValidationFailed));
}

// A concurrent delete file in another partition does not conflict.
TEST_F(ReplacePartitionsTest, NoConflictingDeletesDifferentPartitionPasses) {
  const int64_t first_id = CommitFastAppend(file_a_);
  CommitEqualityDelete("/delete/concurrent_x2.parquet", /*partition_x=*/2L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(MakeDataFile("/data/replacement_x1.parquet", /*partition_x=*/1L));
  op->ValidateFromSnapshot(first_id);
  op->ValidateNoConflictingDeletes();
  EXPECT_THAT(op->Commit(), IsOk());
}

TEST_F(ReplacePartitionsTest, NullAddFileRejected) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewReplace());
  op->AddFile(nullptr);
  auto result = op->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid data file: null"));
}

}  // namespace iceberg
