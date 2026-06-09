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
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/update/delete_files.h"
#include "iceberg/update/fast_append.h"

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

  void CommitFileA() {
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
  CommitFileA();

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
  CommitFileA();

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
  CommitFileA();
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
  CommitFileA();
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

TEST_F(RowDeltaTest, ValidateDataFilesExistFailsForConcurrentDelete) {
  CommitFileA();
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

  auto result = row_delta->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot commit, missing data files"));
  EXPECT_THAT(result, HasErrorMessage(file_a_->file_path));
}

TEST_F(RowDeltaTest, CannotRemoveReferencedDataFile) {
  CommitFileA();

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

TEST_F(RowDeltaTest, AddDeleteFileForRemovedDataFileCommitsDeleteOperation) {
  CommitFileA();

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
  CommitFileA();
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

}  // namespace iceberg
