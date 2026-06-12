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

#include "iceberg/update/delete_files.h"

#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/literal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/update/fast_append.h"

namespace iceberg {

class DeleteFilesTest : public MinimalUpdateTestBase {
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

  void SetLongBounds(const std::shared_ptr<DataFile>& file, int32_t field_id,
                     int64_t lower, int64_t upper) {
    ASSERT_NE(file, nullptr);
    ICEBERG_UNWRAP_OR_FAIL(auto lower_bound, Literal::Long(lower).Serialize());
    ICEBERG_UNWRAP_OR_FAIL(auto upper_bound, Literal::Long(upper).Serialize());
    file->value_counts[field_id] = file->record_count;
    file->null_value_counts[field_id] = 0;
    file->lower_bounds[field_id] = lower_bound;
    file->upper_bounds[field_id] = upper_bound;
  }

  void CommitFiles(const std::vector<std::shared_ptr<DataFile>>& files) {
    ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
    for (const auto& file : files) {
      append->AppendFile(file);
    }
    ASSERT_THAT(append->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
  }

  void CommitInitialFiles() { CommitFiles({file_a_, file_b_}); }

  void ExpectOneFileDeleted() {
    ASSERT_THAT(table_->Refresh(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
              DataOperation::kDelete);
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedRecords), "100");
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kRemovedFileSize), "1024");
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;

  static constexpr int32_t kYFieldId = 2;
};

TEST_F(DeleteFilesTest, DeleteFileByPath) {
  CommitInitialFiles();

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFile(file_a_->file_path);

  EXPECT_THAT(delete_files->Commit(), IsOk());
  ExpectOneFileDeleted();
}

TEST_F(DeleteFilesTest, DeleteFileByDataFile) {
  CommitInitialFiles();

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFile(file_a_);

  EXPECT_THAT(delete_files->Commit(), IsOk());
  ExpectOneFileDeleted();
}

TEST_F(DeleteFilesTest, DeleteFromRowFilterCaseInsensitive) {
  CommitInitialFiles();

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->CaseSensitive(false).DeleteFromRowFilter(
      Expressions::Equal("X", Literal::Long(1L)));

  EXPECT_THAT(delete_files->Commit(), IsOk());
  ExpectOneFileDeleted();
}

TEST_F(DeleteFilesTest, EmptyDeleteCommit) {
  CommitInitialFiles();
  ICEBERG_UNWRAP_OR_FAIL(auto previous_snapshot, table_->current_snapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());

  EXPECT_THAT(delete_files->Commit(), IsOk());

  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ASSERT_TRUE(snapshot->parent_snapshot_id.has_value());
  EXPECT_EQ(snapshot->parent_snapshot_id.value(), previous_snapshot->snapshot_id);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kDelete);
  EXPECT_EQ(snapshot->summary.count(SnapshotSummaryFields::kDeletedDataFiles), 0U);
  EXPECT_EQ(snapshot->summary.count(SnapshotSummaryFields::kDeletedRecords), 0U);
  EXPECT_EQ(snapshot->summary.count(SnapshotSummaryFields::kRemovedFileSize), 0U);
}

TEST_F(DeleteFilesTest, DeleteFromRowFilter) {
  CommitInitialFiles();

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFromRowFilter(Expressions::Equal("x", Literal::Long(1L)));

  EXPECT_THAT(delete_files->Commit(), IsOk());
  ExpectOneFileDeleted();
}

TEST_F(DeleteFilesTest, DeleteFromRowFilterRejectsPartialMatchFile) {
  auto partial_match_file = MakeDataFile("/data/partial_match.parquet",
                                         /*partition_x=*/1L);
  SetLongBounds(partial_match_file, kYFieldId, /*lower=*/0L, /*upper=*/10L);
  CommitFiles({partial_match_file});
  ICEBERG_UNWRAP_OR_FAIL(auto previous_snapshot, table_->current_snapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFromRowFilter(Expressions::Equal("y", Literal::Long(5L)));

  auto status = delete_files->Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(status,
              HasErrorMessage("Cannot delete file where some, but not all, rows match "
                              "filter"));
  EXPECT_THAT(status, HasErrorMessage(partial_match_file->file_path));

  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, previous_snapshot->snapshot_id);
}

TEST_F(DeleteFilesTest, ValidateFilesExistRejectsMissingPath) {
  CommitInitialFiles();

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFile(table_location_ + "/data/missing.parquet")
      .ValidateFilesExist();

  auto status = delete_files->Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(status, HasErrorMessage("Missing required files to delete"));
}

}  // namespace iceberg
