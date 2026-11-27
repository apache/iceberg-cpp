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

#include "iceberg/update/row_delta.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/data_table_scan.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"
#include "iceberg/update/delete_files.h"
#include "iceberg/update/expire_snapshots.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/overwrite_files.h"
#include "iceberg/update/rewrite_files.h"
#include "iceberg/update/rewrite_manifests.h"
#include "iceberg/update/snapshot_manager.h"
#include "iceberg/update/update_partition_spec.h"
#include "iceberg/update/update_properties.h"

namespace iceberg {

class RowDeltaTest : public MinimalUpdateTestBase {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  void SetUp() override {
    MinimalUpdateTestBase::SetUp();

    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());

    file_a_ = MakeDataFile("/data/file_a.parquet", /*partition_x=*/1L);
    file_b_ = MakeDataFile("/data/file_b.parquet", /*partition_x=*/2L);
  }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& path, int64_t partition_x) {
    auto file = std::make_shared<DataFile>();
    file->content = DataFile::Content::kData;
    file->file_path = table_location_ + path;
    file->file_format = FileFormatType::kParquet;
    file->partition = PartitionValues(std::vector<Literal>{Literal::Long(partition_x)});
    file->file_size_in_bytes = 1024;
    file->record_count = 100;
    file->partition_spec_id = spec_->spec_id();
    return file;
  }

  std::shared_ptr<DataFile> MakeDeleteFile(const std::string& path, int64_t partition_x) {
    auto file = MakeDataFile(path, partition_x);
    file->content = DataFile::Content::kPositionDeletes;
    file->file_size_in_bytes = 256;
    file->record_count = 7;
    return file;
  }

  std::shared_ptr<DataFile> MakeDeletionVector(const std::string& path,
                                               const std::string& referenced_data_file,
                                               int64_t partition_x,
                                               int64_t content_offset = 0) {
    auto file = MakeDeleteFile(path, partition_x);
    file->file_format = FileFormatType::kPuffin;
    file->referenced_data_file = referenced_data_file;
    file->content_offset = content_offset;
    file->content_size_in_bytes = 10;
    return file;
  }

  void AppendFileAToTable() {
    ICEBERG_UNWRAP_OR_FAIL(auto fast_append, table_->NewFastAppend());
    fast_append->AppendFile(file_a_);
    EXPECT_THAT(fast_append->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  void SetTableFormatVersion(int8_t format_version) {
    table_->metadata()->format_version = format_version;
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
};

class RowDeltaV3Test : public RowDeltaTest {
 protected:
  int8_t format_version() const override { return 3; }
};

class RowDeltaMultipleSpecsTest : public RowDeltaTest,
                                  public ::testing::WithParamInterface<int8_t> {
 protected:
  int8_t format_version() const override { return GetParam(); }

  void SetUp() override {
    RowDeltaTest::SetUp();
    ICEBERG_UNWRAP_OR_FAIL(auto first, table_->NewFastAppend());
    ASSERT_THAT(CommitSnapshotUpdate(first->AppendFile(file_a_)), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto evolve, table_->NewUpdatePartitionSpec());
    ASSERT_THAT(evolve->RemoveField("x").Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto unpartitioned, table_->spec());
    ASSERT_TRUE(unpartitioned->IsUnpartitioned());
    file_b_->partition = PartitionValues{};
    file_b_->partition_spec_id = unpartitioned->spec_id();
    ICEBERG_UNWRAP_OR_FAIL(auto second, table_->NewFastAppend());
    ASSERT_THAT(CommitSnapshotUpdate(second->AppendFile(file_b_)), IsOk());
  }

  std::shared_ptr<DataFile> MakeDeletes(const std::string& path,
                                        const DataFile& data_file) {
    auto deletes = format_version() == 3
                       ? MakeDeletionVector(path + ".puffin", data_file.file_path, 0)
                       : MakeDeleteFile(path + ".parquet", 0);
    deletes->partition = data_file.partition;
    deletes->partition_spec_id = data_file.partition_spec_id;
    return deletes;
  }
};

TEST_F(RowDeltaTest, AddRowsCommitsAppendOperation) {
  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->AddRows(file_a_);

  EXPECT_THAT(row_delta->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->Operation(), std::make_optional(DataOperation::kAppend));
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedRecords), "100");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedFileSize), "1024");
}

TEST_F(RowDeltaTest, AddDeletesCommitsDeleteOperation) {
  auto delete_file = MakeDeleteFile("/delete/file_a_pos_deletes.parquet",
                                    /*partition_x=*/1L);

  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->AddDeletes(delete_file);

  EXPECT_THAT(row_delta->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->Operation(), std::make_optional(DataOperation::kDelete));
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDeleteFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedPosDeleteFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedPosDeletes), "7");
}

TEST_F(RowDeltaTest, RemoveRowsCommitsOverwriteOperation) {
  AppendFileAToTable();

  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->RemoveRows(file_a_);

  EXPECT_THAT(row_delta->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->Operation(), std::make_optional(DataOperation::kOverwrite));
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedRecords), "100");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kRemovedFileSize), "1024");
}

TEST_F(RowDeltaTest, RemoveRowsAndAddDeletesCommitsDeleteOperation) {
  AppendFileAToTable();

  auto delete_file = MakeDeleteFile("/delete/file_a_pos_deletes.parquet",
                                    /*partition_x=*/1L);

  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->RemoveRows(file_a_);
  row_delta->AddDeletes(delete_file);

  EXPECT_THAT(row_delta->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->Operation(), std::make_optional(DataOperation::kDelete));
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDeleteFiles), "1");
}

TEST_F(RowDeltaTest, AddRowsAndRemoveDeletesCommitsAppendOperation) {
  auto delete_file = MakeDeleteFile("/delete/file_a_pos_deletes.parquet",
                                    /*partition_x=*/1L);
  {
    std::shared_ptr<RowDelta> row_delta;
    ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
    row_delta->AddDeletes(delete_file);
    EXPECT_THAT(row_delta->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->AddRows(file_a_);
  row_delta->RemoveDeletes(delete_file);

  EXPECT_THAT(row_delta->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->Operation(), std::make_optional(DataOperation::kAppend));
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kRemovedDeleteFiles), "1");
}

TEST_F(RowDeltaTest, AddDeletesAndRemoveDeletesCommitsDeleteOperation) {
  auto old_delete_file = MakeDeleteFile("/delete/file_a_pos_deletes.parquet",
                                        /*partition_x=*/1L);
  {
    std::shared_ptr<RowDelta> row_delta;
    ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
    row_delta->AddDeletes(old_delete_file);
    EXPECT_THAT(row_delta->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  auto new_delete_file = MakeDeleteFile("/delete/file_b_pos_deletes.parquet",
                                        /*partition_x=*/2L);
  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->AddDeletes(new_delete_file);
  row_delta->RemoveDeletes(old_delete_file);

  EXPECT_THAT(row_delta->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->Operation(), std::make_optional(DataOperation::kDelete));
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDeleteFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kRemovedDeleteFiles), "1");
}

TEST_F(RowDeltaTest, ValidateNoConflictingDataFilesFailsForConcurrentAppend) {
  AppendFileAToTable();
  ICEBERG_UNWRAP_OR_FAIL(auto starting_snapshot, table_->current_snapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto concurrent_append, table_->NewFastAppend());
  concurrent_append->AppendFile(file_b_);
  EXPECT_THAT(concurrent_append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  auto file_c = MakeDataFile("/data/file_c.parquet", /*partition_x=*/3L);
  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->ValidateFromSnapshot(starting_snapshot->snapshot_id);
  row_delta->ValidateNoConflictingDataFiles();
  row_delta->AddRows(file_c);

  auto result = row_delta->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Found conflicting files"));
  EXPECT_THAT(result, HasErrorMessage(file_b_->file_path));
}

TEST_F(RowDeltaTest, ValidateNoConflictingDeleteFilesFailsForConcurrentDelete) {
  AppendFileAToTable();
  ICEBERG_UNWRAP_OR_FAIL(auto starting_snapshot, table_->current_snapshot());

  auto delete_file = MakeDeleteFile("/delete/file_a_pos_deletes.parquet",
                                    /*partition_x=*/1L);
  std::shared_ptr<RowDelta> concurrent_delta;
  ICEBERG_UNWRAP_OR_FAIL(concurrent_delta, table_->NewRowDelta());
  concurrent_delta->AddDeletes(delete_file);
  EXPECT_THAT(concurrent_delta->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  auto file_c = MakeDataFile("/data/file_c.parquet", /*partition_x=*/3L);
  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->ValidateFromSnapshot(starting_snapshot->snapshot_id);
  row_delta->ValidateNoConflictingDeleteFiles();
  row_delta->AddRows(file_c);

  auto result = row_delta->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Found new conflicting delete files"));
  EXPECT_THAT(result, HasErrorMessage(delete_file->file_path));
}

TEST_F(RowDeltaTest, ValidateDataFilesExistSkipsConcurrentDeleteByDefault) {
  AppendFileAToTable();
  ICEBERG_UNWRAP_OR_FAIL(auto starting_snapshot, table_->current_snapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFile(file_a_);
  EXPECT_THAT(delete_files->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  auto delete_file = MakeDeleteFile("/delete/file_a_pos_deletes.parquet",
                                    /*partition_x=*/1L);
  delete_file->referenced_data_file = file_a_->file_path;

  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->ValidateFromSnapshot(starting_snapshot->snapshot_id);
  std::vector<std::string> referenced_files{file_a_->file_path};
  row_delta->ValidateDataFilesExist(referenced_files);
  row_delta->AddDeletes(delete_file);

  EXPECT_THAT(row_delta->Commit(), IsOk());
}

TEST_F(RowDeltaTest,
       ValidateDataFilesExistFailsForConcurrentDeleteWithValidateDeletedFiles) {
  AppendFileAToTable();
  ICEBERG_UNWRAP_OR_FAIL(auto starting_snapshot, table_->current_snapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFile(file_a_);
  EXPECT_THAT(delete_files->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  auto delete_file = MakeDeleteFile("/delete/file_a_pos_deletes.parquet",
                                    /*partition_x=*/1L);
  delete_file->referenced_data_file = file_a_->file_path;

  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->ValidateFromSnapshot(starting_snapshot->snapshot_id);
  std::vector<std::string> referenced_files{file_a_->file_path};
  row_delta->ValidateDataFilesExist(referenced_files);
  row_delta->ValidateDeletedFiles();
  row_delta->AddDeletes(delete_file);

  auto result = row_delta->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot commit, missing data files"));
  EXPECT_THAT(result, HasErrorMessage(file_a_->file_path));
}

TEST_F(RowDeltaTest, CannotRemoveReferencedDataFile) {
  AppendFileAToTable();

  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  std::vector<std::string> referenced_files{file_a_->file_path};
  row_delta->ValidateDataFilesExist(referenced_files);
  row_delta->RemoveRows(file_a_);

  auto result = row_delta->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot delete data files"));
  EXPECT_THAT(result, HasErrorMessage(file_a_->file_path));
}

TEST_F(RowDeltaTest, ValidateDataFilesExistWithConflictDetectionFilter) {
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  ICEBERG_UNWRAP_OR_FAIL(
      auto base, CommitSnapshotUpdate(append->AppendFile(file_a_).AppendFile(file_b_)));
  const std::vector<std::string> required{file_a_->file_path, file_b_->file_path};
  auto deletes = MakeDeleteFile("/data/deletes-a.parquet", 1);
  deletes->referenced_data_file = file_a_->file_path;
  ICEBERG_UNWRAP_OR_FAIL(auto pending, table_->NewRowDelta());
  pending->AddDeletes(deletes)
      .ValidateDataFilesExist(required)
      .ValidateDeletedFiles()
      .ValidateFromSnapshot(base->snapshot_id)
      .ConflictDetectionFilter(Expressions::Equal("x", Literal::Long(1)))
      .ValidateNoConflictingDataFiles();
  ICEBERG_UNWRAP_OR_FAIL(auto remove, table_->NewDeleteFiles());
  ASSERT_THAT(CommitSnapshotUpdate(remove->DeleteFile(file_b_)), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto overlapping, table_->NewRowDelta());
  overlapping->AddDeletes(deletes)
      .ValidateDataFilesExist(required)
      .ValidateDeletedFiles()
      .ValidateFromSnapshot(base->snapshot_id)
      .ConflictDetectionFilter(Expressions::Equal("x", Literal::Long(2)));
  ExpectCommitError(overlapping->Commit(), ErrorKind::kValidationFailed,
                    "Cannot commit, missing data files: [" + file_b_->file_path + "]");

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CommitSnapshotUpdate(*pending));
  SnapshotReader reader(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, reader.DeleteManifests(file_io_));
  ASSERT_EQ(manifests.size(), 1);
  ExpectManifestEntries(manifests[0], {deletes->file_path}, {ManifestStatus::kAdded});
}

TEST_F(RowDeltaTest, RequiredFilesCheckOverwritesAndRewritesByDefault) {
  auto file_c = MakeDataFile("/data/file_c.parquet", 3);
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  ICEBERG_UNWRAP_OR_FAIL(
      auto base, CommitSnapshotUpdate(
                     append->AppendFile(file_a_).AppendFile(file_b_).AppendFile(file_c)));
  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteFiles());
  ASSERT_THAT(CommitSnapshotUpdate(rewrite->Rewrite(
                  {file_a_}, {}, {MakeDataFile("/data/new_a.parquet", 1)}, {})),
              IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto overwrite, table_->NewOverwrite());
  ASSERT_THAT(CommitSnapshotUpdate(overwrite->DeleteFile(file_b_).AddFile(
                  MakeDataFile("/data/new_b.parquet", 2))),
              IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto remove, table_->NewDeleteFiles());
  ICEBERG_UNWRAP_OR_FAIL(auto last, CommitSnapshotUpdate(remove->DeleteFile(file_c)));

  const std::vector<std::string> required{file_a_->file_path, file_b_->file_path,
                                          file_c->file_path};
  ICEBERG_UNWRAP_OR_FAIL(auto check, table_->NewRowDelta());
  check->AddDeletes(MakeDeleteFile("/data/pending.parquet", 1))
      .ValidateFromSnapshot(base->snapshot_id)
      .ValidateDataFilesExist(required);
  ExpectCommitError(check->Commit(), ErrorKind::kValidationFailed,
                    "Cannot commit, missing data files: [" + file_b_->file_path + ", " +
                        file_a_->file_path + "]");
  ExpectCurrentSnapshot(last->snapshot_id);
}

TEST_F(RowDeltaTest, RemovingRowsChecksNewDeletesForTheRemovedFile) {
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  ICEBERG_UNWRAP_OR_FAIL(
      auto base, CommitSnapshotUpdate(append->AppendFile(file_a_).AppendFile(file_b_)));
  auto delete_b = MakeDeleteFile("/data/delete_b.parquet", 2);
  delete_b->referenced_data_file = file_b_->file_path;
  ICEBERG_UNWRAP_OR_FAIL(auto concurrent, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(auto last,
                         CommitSnapshotUpdate(concurrent->AddDeletes(delete_b)));

  const std::vector<std::string> required{file_a_->file_path};
  auto delete_a = MakeDeleteFile("/data/delete_a.parquet", 1);
  delete_a->referenced_data_file = file_a_->file_path;
  ICEBERG_UNWRAP_OR_FAIL(auto pending, table_->NewRowDelta());
  pending->AddDeletes(delete_a)
      .AddRows(MakeDataFile("/data/new_a.parquet", 1))
      .RemoveRows(file_b_)
      .ValidateFromSnapshot(base->snapshot_id)
      .ValidateNoConflictingDeleteFiles()
      .ValidateDataFilesExist(required);
  ExpectCommitError(
      pending->Commit(), ErrorKind::kValidationFailed,
      "Cannot commit, found new delete for replaced data file: " + file_b_->file_path);
  ExpectCurrentSnapshot(last->snapshot_id);
}

TEST_F(RowDeltaTest, AddDeleteFileForRemovedDataFileCommitsDeleteOperation) {
  AppendFileAToTable();

  auto delete_file = MakeDeleteFile("/delete/file_a_pos_deletes.parquet",
                                    /*partition_x=*/1L);
  delete_file->referenced_data_file = file_a_->file_path;

  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->RemoveRows(file_a_);
  row_delta->AddDeletes(delete_file);

  EXPECT_THAT(row_delta->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->Operation(), std::make_optional(DataOperation::kDelete));
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDeleteFiles), "1");
}

TEST_F(RowDeltaTest, ValidateDeletedFilesAllowsMissingRowsOnEmptyTable) {
  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->ValidateDeletedFiles();
  row_delta->RemoveRows(file_a_);

  EXPECT_THAT(row_delta->Commit(), IsOk());
}

TEST_F(RowDeltaTest, ValidateDeletedFilesAllowsMissingDeletesOnEmptyTable) {
  auto delete_file = MakeDeleteFile("/delete/file_a_pos_deletes.parquet",
                                    /*partition_x=*/1L);

  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->ValidateDeletedFiles();
  row_delta->RemoveDeletes(delete_file);

  EXPECT_THAT(row_delta->Commit(), IsOk());
}

TEST_F(RowDeltaTest, AddDeletionVectorValidatesConcurrentDVs) {
  AppendFileAToTable();
  ICEBERG_UNWRAP_OR_FAIL(auto starting_snapshot, table_->current_snapshot());
  SetTableFormatVersion(3);

  auto concurrent_dv =
      MakeDeletionVector("/delete/concurrent-dv-a.puffin", file_a_->file_path,
                         /*partition_x=*/1L, /*content_offset=*/0);
  std::shared_ptr<RowDelta> concurrent_delta;
  ICEBERG_UNWRAP_OR_FAIL(concurrent_delta, table_->NewRowDelta());
  concurrent_delta->AddDeletes(concurrent_dv);
  EXPECT_THAT(concurrent_delta->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  SetTableFormatVersion(3);

  auto dv = MakeDeletionVector("/delete/dv-a.puffin", file_a_->file_path,
                               /*partition_x=*/1L, /*content_offset=*/10);
  std::shared_ptr<RowDelta> row_delta;
  ICEBERG_UNWRAP_OR_FAIL(row_delta, table_->NewRowDelta());
  row_delta->ValidateFromSnapshot(starting_snapshot->snapshot_id);
  row_delta->AddDeletes(dv);

  auto result = row_delta->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Found concurrently added DV"));
  EXPECT_THAT(result, HasErrorMessage(file_a_->file_path));
}

TEST_F(RowDeltaV3Test, ConcurrentSuccessiveDVsReportedAsConflict) {
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  ICEBERG_UNWRAP_OR_FAIL(
      auto base, CommitSnapshotUpdate(append->AppendFile(file_a_).AppendFile(file_b_)));
  auto pending_dv = MakeDeletionVector("/data/dv-b.puffin", file_b_->file_path, 2);
  ICEBERG_UNWRAP_OR_FAIL(auto pending, table_->NewRowDelta());
  pending->AddDeletes(pending_dv)
      .ValidateFromSnapshot(base->snapshot_id)
      .ValidateNoConflictingDeleteFiles();

  auto dv1 = MakeDeletionVector("/data/dv-a-1.puffin", file_a_->file_path, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto first, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot,
                         CommitSnapshotUpdate(first->AddDeletes(dv1)));
  auto dv2 = MakeDeletionVector("/data/dv-a-2.puffin", file_a_->file_path, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto second, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      CommitSnapshotUpdate(
          second->RemoveDeletes(dv1).AddDeletes(dv2).ValidateFromSnapshot(
              first_snapshot->snapshot_id)));

  ExpectCommitError(
      pending->Commit(), ErrorKind::kValidationFailed,
      "Found new conflicting delete files that can apply to records matching true: [" +
          dv2->file_path + ", " + dv1->file_path + "]");
  ExpectCurrentSnapshot(second_snapshot->snapshot_id);
}

TEST_F(RowDeltaV3Test, DVsForAddedDataFilesSkipExpiredHistory) {
  ICEBERG_UNWRAP_OR_FAIL(auto first, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(auto expired, CommitSnapshotUpdate(first->AddRows(file_a_)));
  ICEBERG_UNWRAP_OR_FAIL(auto second, table_->NewRowDelta());
  ASSERT_THAT(CommitSnapshotUpdate(second->AddRows(file_b_)), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto expiration, table_->NewExpireSnapshots());
  ASSERT_THAT(expiration->ExpireSnapshotId(expired->snapshot_id).Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  auto file_c = MakeDataFile("/data/file_c.parquet", 1);
  auto dv_c = MakeDeletionVector("/data/dv-c.puffin", file_c->file_path, 1);
  auto dv_b = MakeDeletionVector("/data/dv-b.puffin", file_b_->file_path, 2);
  ICEBERG_UNWRAP_OR_FAIL(auto mixed, table_->NewRowDelta());
  ExpectCommitError(mixed->AddRows(file_c).AddDeletes(dv_c).AddDeletes(dv_b).Commit(),
                    ErrorKind::kValidationFailed, "Cannot determine history");

  ICEBERG_UNWRAP_OR_FAIL(auto add, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot,
                         CommitSnapshotUpdate(add->AddRows(file_c).AddDeletes(dv_c)));
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDVs), "1");
}

TEST_F(RowDeltaV3Test, DVConflictUsesPartitionFilterAndScanExecutor) {
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  ICEBERG_UNWRAP_OR_FAIL(
      auto base, CommitSnapshotUpdate(append->AppendFile(file_a_).AppendFile(file_b_)));
  auto dv_b = MakeDeletionVector("/data/dv-b.puffin", file_b_->file_path, 2);
  ICEBERG_UNWRAP_OR_FAIL(auto other_partition, table_->NewRowDelta());
  ASSERT_THAT(CommitSnapshotUpdate(other_partition->AddDeletes(dv_b)), IsOk());
  auto dv_a = MakeDeletionVector("/data/dv-a.puffin", file_a_->file_path, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto same_partition, table_->NewRowDelta());
  ASSERT_THAT(CommitSnapshotUpdate(same_partition->AddDeletes(dv_a)), IsOk());

  test::ThreadExecutor executor;
  auto file_c = MakeDataFile("/data/file_c.parquet", 1);
  auto pending_a = MakeDeletionVector("/data/pending-a.puffin", file_a_->file_path, 1);
  auto pending_c = MakeDeletionVector("/data/pending-c.puffin", file_c->file_path, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto pending, table_->NewRowDelta());
  pending->ScanManifestsWith(executor);
  pending->AddDeletes(pending_a)
      .AddRows(file_c)
      .AddDeletes(pending_c)
      .ValidateFromSnapshot(base->snapshot_id)
      .CaseSensitive(false)
      .ConflictDetectionFilter(Expressions::Equal("X", Literal::Long(1)));
  ExpectCommitError(pending->Commit(), ErrorKind::kValidationFailed,
                    "Found concurrently added DV for " + file_a_->file_path);
  EXPECT_EQ(executor.submit_count(), 1);
}

TEST_F(RowDeltaV3Test, ConcurrentDVsInDifferentPartitionsWithFilter) {
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  ICEBERG_UNWRAP_OR_FAIL(
      auto base, CommitSnapshotUpdate(append->AppendFile(file_a_).AppendFile(file_b_)));
  auto dv_a = MakeDeletionVector("/data/dv-a.puffin", file_a_->file_path, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto pending, table_->NewRowDelta());
  pending->AddDeletes(dv_a)
      .ValidateFromSnapshot(base->snapshot_id)
      .ConflictDetectionFilter(Expressions::Equal("x", Literal::Long(1)))
      .ValidateNoConflictingDataFiles()
      .ValidateNoConflictingDeleteFiles();

  auto dv_b = MakeDeletionVector("/data/dv-b.puffin", file_b_->file_path, 2);
  ICEBERG_UNWRAP_OR_FAIL(auto concurrent, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(auto other_snapshot,
                         CommitSnapshotUpdate(concurrent->AddDeletes(dv_b)));
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CommitSnapshotUpdate(*pending));
  SnapshotReader reader(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, reader.DeleteManifests(file_io_));
  ASSERT_EQ(manifests.size(), 2);
  ExpectManifestEntriesWithSequenceNumbers(
      manifests[0], {dv_a->file_path}, {ManifestStatus::kAdded}, {snapshot->snapshot_id},
      {snapshot->sequence_number}, {snapshot->sequence_number});
  ExpectManifestEntriesWithSequenceNumbers(
      manifests[1], {dv_b->file_path}, {ManifestStatus::kAdded},
      {other_snapshot->snapshot_id}, {other_snapshot->sequence_number},
      {other_snapshot->sequence_number});
}

TEST_F(RowDeltaV3Test, DVValidationKeepsManifestsAcrossPartitionSpecChange) {
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  ICEBERG_UNWRAP_OR_FAIL(auto base, CommitSnapshotUpdate(append->AppendFile(file_a_)));
  auto dv_a = MakeDeletionVector("/data/dv-a.puffin", file_a_->file_path, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto old_spec, table_->NewRowDelta());
  ASSERT_THAT(CommitSnapshotUpdate(old_spec->AddDeletes(dv_a)), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto evolve, table_->NewUpdatePartitionSpec());
  ASSERT_THAT(evolve->AddField("y").Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto spec, table_->spec());
  ASSERT_NE(spec->spec_id(), spec_->spec_id());
  auto file_c = MakeDataFile("/data/file_c.parquet", 2);
  file_c->partition_spec_id = spec->spec_id();
  file_c->partition = PartitionValues({Literal::Long(2), Literal::Long(10)});
  auto dv_c = MakeDeletionVector("/data/dv-c.puffin", file_c->file_path, 2);
  dv_c->partition_spec_id = spec->spec_id();
  dv_c->partition = file_c->partition;
  ICEBERG_UNWRAP_OR_FAIL(auto latest, table_->NewRowDelta());
  ASSERT_THAT(CommitSnapshotUpdate(latest->AddRows(file_c).AddDeletes(dv_c)), IsOk());

  test::InlineExecutor executor;
  auto pending_dv = MakeDeletionVector("/data/pending-a.puffin", file_a_->file_path, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto pending, table_->NewRowDelta());
  pending->ScanManifestsWith(executor);
  pending->AddDeletes(pending_dv)
      .ValidateFromSnapshot(base->snapshot_id)
      .ConflictDetectionFilter(Expressions::Equal("x", Literal::Long(1)));
  ExpectCommitError(pending->Commit(), ErrorKind::kValidationFailed,
                    "Found concurrently added DV for " + file_a_->file_path);
  // Both manifests reach the executor. The reader rejects x=2 before matching the older
  // DV.
  EXPECT_EQ(executor.submit_count(), 2);
}

TEST_F(RowDeltaV3Test, ConcurrentDVsFollowTheTargetBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  ICEBERG_UNWRAP_OR_FAIL(auto base, CommitSnapshotUpdate(append->AppendFile(file_a_)));
  ICEBERG_UNWRAP_OR_FAIL(auto refs, table_->NewSnapshotManager());
  ASSERT_THAT(refs->CreateBranch("audit", base->snapshot_id).Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  auto main_dv = MakeDeletionVector("/data/main.puffin", file_a_->file_path, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto main, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(auto main_snapshot,
                         CommitSnapshotUpdate(main->AddDeletes(main_dv)));
  auto audit_dv = MakeDeletionVector("/data/audit.puffin", file_a_->file_path, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto audit, table_->NewRowDelta());
  audit->ToBranch("audit");
  ASSERT_THAT(
      audit->AddDeletes(audit_dv).ValidateFromSnapshot(base->snapshot_id).Commit(),
      IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ExpectCurrentSnapshot(main_snapshot->snapshot_id);
  auto audit_id = table_->metadata()->refs.at("audit")->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(auto pending, table_->NewRowDelta());
  pending->ToBranch("audit");
  auto result =
      pending
          ->AddDeletes(MakeDeletionVector("/data/pending.puffin", file_a_->file_path, 1))
          .ValidateFromSnapshot(base->snapshot_id)
          .Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage(audit_dv->file_path));
  ExpectBranch("audit", audit_id);
}

TEST_F(RowDeltaTest, ManifestMergingAfterUpgradeToV3) {
  ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
  ASSERT_THAT(properties->Set(TableProperties::kManifestMergeEnabled.key(), "true")
                  .Set(TableProperties::kManifestMinMergeCount.key(), "2")
                  .Commit(),
              IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  ASSERT_THAT(CommitSnapshotUpdate(append->AppendFile(file_a_)), IsOk());

  auto position_delete = MakeDeleteFile("/data/positions.parquet", 1);
  position_delete->referenced_data_file = file_a_->file_path;
  ICEBERG_UNWRAP_OR_FAIL(auto v2_delta, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(auto v2_snapshot,
                         CommitSnapshotUpdate(v2_delta->AddDeletes(position_delete)));
  ICEBERG_UNWRAP_OR_FAIL(auto v2_builder, table_->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto v2_scan, v2_builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto before, v2_scan->PlanFiles());
  ASSERT_EQ(before.size(), 1);
  ASSERT_EQ(before.front()->delete_files().size(), 1);
  EXPECT_EQ(before.front()->delete_files().front()->file_path,
            position_delete->file_path);
  ICEBERG_UNWRAP_OR_FAIL(auto upgrade, table_->NewUpdateProperties());
  ASSERT_THAT(upgrade->Set(TableProperties::kFormatVersion.key(), "3").Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  auto dv = MakeDeletionVector("/data/positions.puffin", file_a_->file_path, 1, 20);
  ICEBERG_UNWRAP_OR_FAIL(auto v3_delta, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(auto v3_snapshot,
                         CommitSnapshotUpdate(v3_delta->AddDeletes(dv)));

  SnapshotReader reader(v3_snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, reader.DeleteManifests(file_io_));
  ASSERT_EQ(manifests.size(), 1);
  EXPECT_EQ(v3_snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(v3_snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "1");
  EXPECT_EQ(v3_snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "1");
  ExpectManifestEntriesWithSequenceNumbers(
      manifests[0], {dv->file_path, position_delete->file_path},
      {ManifestStatus::kAdded, ManifestStatus::kExisting},
      {v3_snapshot->snapshot_id, v2_snapshot->snapshot_id},
      {v3_snapshot->sequence_number, v2_snapshot->sequence_number},
      {v3_snapshot->sequence_number, v2_snapshot->sequence_number});

  ICEBERG_UNWRAP_OR_FAIL(auto builder, table_->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 1);
  EXPECT_EQ(tasks.front()->data_file()->file_path, file_a_->file_path);
  const auto& deletes = tasks.front()->delete_files();
  ASSERT_EQ(deletes.size(), 1);
  EXPECT_EQ(deletes.front()->file_path, dv->file_path);
  EXPECT_EQ(deletes.front()->referenced_data_file, dv->referenced_data_file);
  EXPECT_EQ(deletes.front()->content_offset, dv->content_offset);
  EXPECT_EQ(deletes.front()->content_size_in_bytes, dv->content_size_in_bytes);
}

TEST_P(RowDeltaMultipleSpecsTest, ManifestMergingMultipleSpecs) {
  ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
  ASSERT_THAT(properties->Set(TableProperties::kManifestMergeEnabled.key(), "true")
                  .Set(TableProperties::kManifestMinMergeCount.key(), "2")
                  .Commit(),
              IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  auto deletes_a = MakeDeletes("/data/delete-a", *file_a_);
  auto deletes_b = MakeDeletes("/data/delete-b", *file_b_);
  ICEBERG_UNWRAP_OR_FAIL(auto first, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(
      auto original,
      CommitSnapshotUpdate(first->AddDeletes(deletes_a).AddDeletes(deletes_b)));
  EXPECT_EQ(original->summary.at(SnapshotSummaryFields::kManifestsCreated), "2");
  EXPECT_EQ(original->summary.at(SnapshotSummaryFields::kManifestsKept), "2");
  EXPECT_EQ(original->summary.at(SnapshotSummaryFields::kManifestsReplaced), "0");

  auto new_a = MakeDeletes("/data/new-a", *file_a_);
  auto new_b = MakeDeletes("/data/new-b", *file_b_);
  ICEBERG_UNWRAP_OR_FAIL(auto replace, table_->NewRowDelta());
  replace->RemoveDeletes(deletes_a)
      .AddDeletes(new_a)
      .RemoveDeletes(deletes_b)
      .AddDeletes(new_b)
      .ValidateFromSnapshot(original->snapshot_id);
  ICEBERG_UNWRAP_OR_FAIL(auto merged, CommitSnapshotUpdate(*replace));
  SnapshotReader reader(merged.get());
  ICEBERG_UNWRAP_OR_FAIL(auto data, reader.DataManifests(file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, reader.DeleteManifests(file_io_));
  EXPECT_EQ(data.size(), 2);
  ASSERT_EQ(manifests.size(), 2);
  EXPECT_EQ(manifests[0].partition_spec_id, file_b_->partition_spec_id);
  EXPECT_EQ(manifests[1].partition_spec_id, file_a_->partition_spec_id);
  for (const auto& manifest : manifests) {
    auto partitioned = manifest.partition_spec_id == file_a_->partition_spec_id;
    ExpectManifestEntriesWithSequenceNumbers(
        manifest,
        {partitioned ? new_a->file_path : new_b->file_path,
         partitioned ? deletes_a->file_path : deletes_b->file_path},
        {ManifestStatus::kAdded, ManifestStatus::kDeleted},
        {merged->snapshot_id, merged->snapshot_id},
        {merged->sequence_number, original->sequence_number},
        {merged->sequence_number, original->sequence_number});
  }
}

TEST_P(RowDeltaMultipleSpecsTest, AbortMultipleSpecs) {
  auto deletes_a = MakeDeletes("/data/delete-a", *file_a_);
  auto deletes_b = MakeDeletes("/data/delete-b", *file_b_);
  std::vector<std::string> deleted_paths;
  const std::vector<std::string> required{file_a_->file_path};
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto pending, txn->NewRowDelta());
  pending->DeleteWith([&](const std::string& path) {
    deleted_paths.push_back(path);
    return file_io_->DeleteFile(path);
  });
  pending->AddDeletes(deletes_a)
      .AddDeletes(deletes_b)
      .ValidateDeletedFiles()
      .ValidateDataFilesExist(required);
  ASSERT_THAT(pending->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto staged, txn->current().Snapshot());
  SnapshotReader staged_reader(staged.get());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_manifests, staged_reader.DeleteManifests(file_io_));
  ASSERT_EQ(staged_manifests.size(), 2);

  ICEBERG_UNWRAP_OR_FAIL(auto remove, table_->NewDeleteFiles());
  ICEBERG_UNWRAP_OR_FAIL(auto committed,
                         CommitSnapshotUpdate(remove->DeleteFile(file_a_)));
  ExpectCommitError(txn->Commit(), ErrorKind::kValidationFailed,
                    "Cannot commit, missing data files: [" + file_a_->file_path + "]");
  ExpectCurrentSnapshot(committed->snapshot_id);
  EXPECT_THAT(deleted_paths, ::testing::UnorderedElementsAre(
                                 staged->manifest_list, staged_manifests[0].manifest_path,
                                 staged_manifests[1].manifest_path));
  for (const auto& path : deleted_paths) {
    EXPECT_FALSE(file_io_->ReadFile(path, std::nullopt).has_value());
  }
}

TEST_F(RowDeltaTest, RemovesDeleteFilesReadBeforeConcurrentManifestRewrite) {
  auto deletes_a = MakeDeleteFile("/data/a-deletes.parquet", 1);
  auto deletes_b = MakeDeleteFile("/data/b-deletes.parquet", 1);
  ICEBERG_UNWRAP_OR_FAIL(auto first, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(
      auto first_snapshot,
      CommitSnapshotUpdate(first->AddRows(file_a_).AddDeletes(deletes_a)));
  ICEBERG_UNWRAP_OR_FAIL(auto second, table_->NewRowDelta());
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      CommitSnapshotUpdate(second->AddRows(file_a_).AddDeletes(deletes_b)));
  SnapshotReader reader(second_snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto original, reader.DeleteManifests(file_io_));
  ASSERT_EQ(original.size(), 2);
  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadManifestEntries(original));
  ASSERT_EQ(entries.size(), 2);
  ASSERT_EQ(entries[0].data_file->manifest_location, original[0].manifest_path);
  ASSERT_EQ(entries[1].data_file->manifest_location, original[1].manifest_path);
  ICEBERG_UNWRAP_OR_FAIL(auto pending, table_->NewRowDelta());
  pending->RemoveDeletes(entries[0].data_file)
      .RemoveDeletes(entries[1].data_file)
      .ValidateFromSnapshot(second_snapshot->snapshot_id);

  // The replacement manifest carries the same delete paths with revised sequence numbers.
  const auto data_sequence = second_snapshot->sequence_number + 1;
  ICEBERG_UNWRAP_OR_FAIL(
      auto writer, ManifestWriter::MakeWriter(
                       /*format_version=*/2, /*snapshot_id=*/std::nullopt,
                       table_location_ + "/metadata/rewritten-deletes.avro", file_io_,
                       spec_, schema_, ManifestContent::kDeletes));
  ASSERT_THAT(writer->WriteExistingEntry(deletes_a, first_snapshot->snapshot_id,
                                         data_sequence, 0),
              IsOk());
  ASSERT_THAT(writer->WriteExistingEntry(deletes_b, second_snapshot->snapshot_id,
                                         data_sequence, 0),
              IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto replacement, writer->ToManifestFile());
  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(original[0])
      .DeleteManifest(original[1])
      .AddManifest(replacement);
  ASSERT_THAT(CommitSnapshotUpdate(*rewrite), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto removed, CommitSnapshotUpdate(*pending));
  SnapshotReader final_reader(removed.get());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, final_reader.DeleteManifests(file_io_));
  ASSERT_EQ(manifests.size(), 1);
  ExpectManifestEntriesWithSequenceNumbers(
      manifests[0], {deletes_a->file_path, deletes_b->file_path},
      {ManifestStatus::kDeleted, ManifestStatus::kDeleted},
      {removed->snapshot_id, removed->snapshot_id}, {data_sequence, data_sequence},
      {0, 0});
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, RowDeltaMultipleSpecsTest,
                         ::testing::Values(int8_t{2}, int8_t{3}));

}  // namespace iceberg
