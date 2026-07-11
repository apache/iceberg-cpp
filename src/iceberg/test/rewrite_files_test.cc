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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <arrow/filesystem/filesystem.h>
#include <arrow/result.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/row_delta.h"
#include "iceberg/update/update_properties.h"

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

  /// \brief Read all manifest entries from a set of manifests.
  Result<std::vector<ManifestEntry>> ReadAllEntries(
      std::span<const ManifestFile> manifests) {
    std::vector<ManifestEntry> result;
    for (const auto& manifest : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto spec, table_->metadata()->PartitionSpecById(manifest.partition_spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto reader,
                              ManifestReader::Make(manifest, file_io_, schema_, spec));
      ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
      result.insert(result.end(), entries.begin(), entries.end());
    }
    return result;
  }

  /// \brief Get data manifests from a snapshot.
  Result<std::vector<ManifestFile>> DataManifests(
      const std::shared_ptr<Snapshot>& snapshot) {
    SnapshotCache snapshot_cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot_cache.DataManifests(file_io_));
    return std::vector<ManifestFile>(manifests.begin(), manifests.end());
  }

  /// \brief Get delete manifests from a snapshot.
  Result<std::vector<ManifestFile>> DeleteManifests(
      const std::shared_ptr<Snapshot>& snapshot) {
    SnapshotCache snapshot_cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot_cache.DeleteManifests(file_io_));
    return std::vector<ManifestFile>(manifests.begin(), manifests.end());
  }

  /// \brief List Avro metadata files in the mock filesystem.
  std::vector<std::string> MetadataAvroFiles() {
    auto arrow_io = std::dynamic_pointer_cast<arrow::ArrowFileSystemFileIO>(file_io_);
    EXPECT_NE(arrow_io, nullptr);
    if (arrow_io == nullptr) {
      return {};
    }

    ::arrow::fs::FileSelector selector;
    selector.base_dir = table_location_ + "/metadata";
    selector.recursive = false;

    auto maybe_infos = arrow_io->fs()->GetFileInfo(selector);
    EXPECT_TRUE(maybe_infos.ok()) << maybe_infos.status().ToString();
    if (!maybe_infos.ok()) {
      return {};
    }

    std::vector<std::string> files;
    for (const auto& info : maybe_infos.ValueOrDie()) {
      if (info.type() == ::arrow::fs::FileType::File && info.path().ends_with(".avro")) {
        files.push_back(info.path());
      }
    }
    std::ranges::sort(files);
    return files;
  }

  /// \brief Set commit retry properties on the table.
  void SetCommitRetryProperties(int32_t retries) {
    ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
    props->Set(std::string(TableProperties::kCommitNumRetries.key()),
               std::to_string(retries));
    props->Set(std::string(TableProperties::kCommitMinRetryWaitMs.key()), "1");
    props->Set(std::string(TableProperties::kCommitMaxRetryWaitMs.key()), "1");
    props->Set(std::string(TableProperties::kCommitTotalRetryTimeMs.key()), "1000");
    EXPECT_THAT(props->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  /// \brief Re-bind table_ to use a mock catalog that fails UpdateTable N times,
  /// then delegates to the real catalog. The call_count pointer is incremented on
  /// each UpdateTable call (successful or not).
  void BindTableWithFailingCommits(int failures, int* update_call_count = nullptr) {
    auto mock_catalog = std::make_shared<::testing::NiceMock<MockCatalog>>();
    std::weak_ptr<::testing::NiceMock<MockCatalog>> weak_catalog = mock_catalog;

    ON_CALL(*mock_catalog, LoadTable(::testing::_))
        .WillByDefault([this, weak_catalog](const TableIdentifier& identifier)
                           -> Result<std::shared_ptr<Table>> {
          ICEBERG_ASSIGN_OR_RAISE(auto loaded, catalog_->LoadTable(identifier));
          auto catalog = weak_catalog.lock();
          ICEBERG_PRECHECK(catalog != nullptr, "Mock catalog expired");
          return Table::Make(loaded->name(), loaded->metadata(),
                             std::string(loaded->metadata_file_location()), loaded->io(),
                             catalog);
        });

    ON_CALL(*mock_catalog, UpdateTable(::testing::_, ::testing::_, ::testing::_))
        .WillByDefault(
            [this, weak_catalog, failures, update_call_count](
                const TableIdentifier& identifier,
                const std::vector<std::unique_ptr<TableRequirement>>& requirements,
                const std::vector<std::unique_ptr<TableUpdate>>& updates) mutable
                -> Result<std::shared_ptr<Table>> {
              if (update_call_count != nullptr) {
                ++*update_call_count;
              }
              if (failures-- > 0) {
                return CommitFailed("Injected failure");
              }
              ICEBERG_ASSIGN_OR_RAISE(
                  auto updated, catalog_->UpdateTable(identifier, requirements, updates));
              auto catalog = weak_catalog.lock();
              ICEBERG_PRECHECK(catalog != nullptr, "Mock catalog expired");
              return Table::Make(updated->name(), updated->metadata(),
                                 std::string(updated->metadata_file_location()),
                                 updated->io(), catalog);
            });

    ICEBERG_UNWRAP_OR_FAIL(auto bound_table,
                           Table::Make(table_->name(), table_->metadata(),
                                       std::string(table_->metadata_file_location()),
                                       table_->io(), mock_catalog));
    table_ = std::move(bound_table);
    mock_catalogs_.push_back(std::move(mock_catalog));
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

  /// \brief Mock catalogs kept alive during the test (for failure injection).
  std::vector<std::shared_ptr<::testing::NiceMock<MockCatalog>>> mock_catalogs_;
};

class RewriteFilesFormatVersionTest : public RewriteFilesTest,
                                      public ::testing::WithParamInterface<int8_t> {
 protected:
  int8_t format_version() const override { return GetParam(); }
};

// ============================================================================
// Tests that run on all format versions (v1+)
// ============================================================================

// Rewrite a single data file: replace file_a_ with rewritten_file_a_.
TEST_P(RewriteFilesFormatVersionTest, AddAndDelete) {
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

TEST_P(RewriteFilesFormatVersionTest, DeleteDataFileCopiesCallerFile) {
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

TEST_P(RewriteFilesFormatVersionTest, AddDataFileRejectsDeleteFileContent) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(delete_file_a_);
  auto result = rw->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Invalid data file to add"));
}

TEST_P(RewriteFilesFormatVersionTest, RewriteDeleteFilesCopiesCallerFiles) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
  CommitFileA();

  {
    ICEBERG_UNWRAP_OR_FAIL(auto row_delta, table_->NewRowDelta());
    row_delta->AddDeletes(delete_file_a_);
    EXPECT_THAT(row_delta->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto after_delta_snapshot, table_->current_snapshot());
  auto old_delete = std::make_shared<DataFile>(*delete_file_a_);
  auto new_delete = std::make_shared<DataFile>(*rewritten_delete_file_a_);

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->ValidateFromSnapshot(after_delta_snapshot->snapshot_id);
  rw->DeleteDeleteFile(old_delete);
  rw->AddDeleteFile(new_delete);

  old_delete->file_path = table_location_ + "/data/delete_a_mutated.parquet";
  new_delete->content = DataFile::Content::kData;

  EXPECT_THAT(rw->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
}

// Rewrite one of several data files, verifying only the target is affected.
TEST_P(RewriteFilesFormatVersionTest, AddAndDeletePartialRewrite) {
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

// Rewrite via the 4-set Rewrite() API replacing data files only.
TEST_P(RewriteFilesFormatVersionTest, Rewrite) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->Rewrite({file_a_}, {}, {rewritten_file_a_}, {});
  EXPECT_THAT(rw->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kReplace);
}

// Limiting validation scope to after a given snapshot avoids spurious conflicts.
TEST_P(RewriteFilesFormatVersionTest, ValidateFromSnapshot) {
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
TEST_P(RewriteFilesFormatVersionTest, ToBranch) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->ToBranch("main");
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);
  EXPECT_THAT(rw->Commit(), IsOk());
}

// Null check on DeleteDataFile.
TEST_P(RewriteFilesFormatVersionTest, DeleteDataFileNullCheck) {
  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(nullptr);
  EXPECT_THAT(rw->Commit(), IsError(ErrorKind::kValidationFailed));
}

// Null check on AddDataFile.
TEST_P(RewriteFilesFormatVersionTest, AddDataFileNullCheck) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(nullptr);
  EXPECT_THAT(rw->Commit(), IsError(ErrorKind::kValidationFailed));
}

// Null checks on AddDeleteFile
TEST_P(RewriteFilesFormatVersionTest, AddDeleteFileNullCheck) {
  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->AddDeleteFile(nullptr);
  EXPECT_THAT(rw->Commit(), IsError(ErrorKind::kValidationFailed));
}

// Adding a data file after deleting one — the basic RewriteFiles pattern.
TEST_P(RewriteFilesFormatVersionTest, AddDataFile) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);
  EXPECT_THAT(rw->Commit(), IsOk());
}

// Deleting a file that was never added fails with missing required files.
TEST_P(RewriteFilesFormatVersionTest, DeleteNonExistentFile) {
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
TEST_P(RewriteFilesFormatVersionTest, AlreadyDeletedFile) {
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

// Inject commit failures that exhaust all retries, then verify the commit
// ultimately fails with CommitFailed.
TEST_P(RewriteFilesFormatVersionTest, Failure) {
  CommitFileA();

  constexpr int32_t kRetries = 3;
  constexpr int32_t kInjectedFailures = kRetries + 1;  // more failures than retries
  int call_count = 0;

  SetCommitRetryProperties(kRetries);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_before, table_->current_snapshot());
  const auto sequence_number_before = table_->metadata()->last_sequence_number;
  const auto metadata_avro_files_before = MetadataAvroFiles();

  BindTableWithFailingCommits(kInjectedFailures, &call_count);

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);

  auto result = rw->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  // We expect 1 (initial attempt) + kRetries (retries) = kRetries + 1 calls,
  // all of which fail.
  EXPECT_EQ(call_count, kRetries + 1);

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_after, table_->current_snapshot());
  EXPECT_EQ(snapshot_after->snapshot_id, snapshot_before->snapshot_id);
  EXPECT_EQ(table_->metadata()->last_sequence_number, sequence_number_before);
  EXPECT_EQ(MetadataAvroFiles(), metadata_avro_files_before);
}

// Inject transient commit failures that stay within the retry budget, then
// verify the commit eventually succeeds with the correct state.
TEST_P(RewriteFilesFormatVersionTest, Recovery) {
  CommitFileA();

  constexpr int32_t kRetries = 4;
  constexpr int32_t kInjectedFailures = 2;  // fewer failures than retries
  int call_count = 0;

  SetCommitRetryProperties(kRetries);
  BindTableWithFailingCommits(kInjectedFailures, &call_count);

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);

  EXPECT_THAT(rw->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  // We expect 1 initial attempt + kInjectedFailures retries =
  // kInjectedFailures + 1 calls, with the last one succeeding.
  EXPECT_EQ(call_count, kInjectedFailures + 1);

  // Verify the rewrite actually succeeded
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kReplace);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles)),
            1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles)), 1);
}

// ============================================================================
// Tests that require format version >= 2
// ============================================================================

// Rewrite with an explicit data sequence number via SetDataSequenceNumber.
TEST_P(RewriteFilesFormatVersionTest, DataSequenceNumber) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
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
TEST_P(RewriteFilesFormatVersionTest, RewriteDataFiles) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
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

// Deleting a file from an empty table fails with missing required files.
TEST_P(RewriteFilesFormatVersionTest, EmptyTable) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);
  auto result = rw->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Missing required files to delete"));
}

// Only adding files without any deletions must fail validation.
TEST_P(RewriteFilesFormatVersionTest, DeleteOnly) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->AddDataFile(rewritten_file_a_);  // no FileToDelete → must fail
  EXPECT_THAT(rw->Commit(), IsError(ErrorKind::kValidationFailed));
}

// Adding data files without deleting data, or adding delete files without deleting
// delete files, must fail validation.
TEST_P(RewriteFilesFormatVersionTest, AddOnly) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
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

// Rewrite both data and delete files in a single commit, verifying that the
// manifest entries have the correct statuses (DELETED for replaced files,
// ADDED for new files). ValidateFromSnapshot scopes conflict detection to
// after the RowDelta commit so the delete being rewritten is not flagged as
// a concurrent addition.
TEST_P(RewriteFilesFormatVersionTest, RewriteDataAndDeleteFiles) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
  // Create data file via FastAppend
  CommitFileA();

  // Create delete file via RowDelta
  {
    ICEBERG_UNWRAP_OR_FAIL(auto row_delta, table_->NewRowDelta());
    row_delta->AddDeletes(delete_file_a_);
    EXPECT_THAT(row_delta->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  // Snapshot after RowDelta: the delete file exists from this point onward.
  ICEBERG_UNWRAP_OR_FAIL(auto after_delta_snapshot, table_->current_snapshot());

  // Rewrite both data and delete files
  {
    ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
    rw->ValidateFromSnapshot(after_delta_snapshot->snapshot_id);
    rw->DeleteDataFile(file_a_);
    rw->DeleteDeleteFile(delete_file_a_);
    rw->AddDataFile(rewritten_file_a_);
    rw->AddDeleteFile(rewritten_delete_file_a_);
    EXPECT_THAT(rw->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  // Verify snapshot summary
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kReplace);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles)),
            1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles)), 1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kRemovedDeleteFiles)),
            1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kAddedDeleteFiles)),
            1);

  // Verify manifest entry statuses
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, DataManifests(snapshot));
  ICEBERG_UNWRAP_OR_FAIL(auto data_entries, ReadAllEntries(data_manifests));
  ASSERT_EQ(data_entries.size(), 2);
  for (const auto& entry : data_entries) {
    if (entry.data_file->file_path == file_a_->file_path) {
      EXPECT_EQ(entry.status, ManifestStatus::kDeleted);
    } else if (entry.data_file->file_path == rewritten_file_a_->file_path) {
      EXPECT_EQ(entry.status, ManifestStatus::kAdded);
      EXPECT_EQ(entry.snapshot_id, snapshot->snapshot_id);
    } else {
      FAIL() << "Unexpected data file: " << entry.data_file->file_path;
    }
  }

  ICEBERG_UNWRAP_OR_FAIL(auto delete_manifests, DeleteManifests(snapshot));
  ICEBERG_UNWRAP_OR_FAIL(auto delete_entries, ReadAllEntries(delete_manifests));
  ASSERT_EQ(delete_entries.size(), 2);
  for (const auto& entry : delete_entries) {
    if (entry.data_file->file_path == delete_file_a_->file_path) {
      EXPECT_EQ(entry.status, ManifestStatus::kDeleted);
    } else if (entry.data_file->file_path == rewritten_delete_file_a_->file_path) {
      EXPECT_EQ(entry.status, ManifestStatus::kAdded);
      EXPECT_EQ(entry.snapshot_id, snapshot->snapshot_id);
    } else {
      FAIL() << "Unexpected delete file: " << entry.data_file->file_path;
    }
  }
}

// Rewrite data files with an explicit old data sequence number, then verify
// that the rewritten manifest entry carries the assigned sequence number.
TEST_P(RewriteFilesFormatVersionTest, RewriteDataAndAssignOldSequenceNumber) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
  CommitFileA();

  constexpr int64_t kOldSequenceNumber = 1;

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->SetDataSequenceNumber(kOldSequenceNumber);
  rw->DeleteDataFile(file_a_);
  rw->AddDataFile(rewritten_file_a_);
  EXPECT_THAT(rw->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, DataManifests(snapshot));
  ICEBERG_UNWRAP_OR_FAIL(auto data_entries, ReadAllEntries(data_manifests));
  ASSERT_EQ(data_entries.size(), 2);
  bool found_rewritten = false;
  for (const auto& entry : data_entries) {
    if (entry.data_file->file_path == rewritten_file_a_->file_path) {
      found_rewritten = true;
      EXPECT_EQ(entry.status, ManifestStatus::kAdded);
      ASSERT_TRUE(entry.sequence_number.has_value());
      EXPECT_EQ(entry.sequence_number.value(), kOldSequenceNumber);
    } else if (entry.data_file->file_path != file_a_->file_path) {
      FAIL() << "Unexpected data file: " << entry.data_file->file_path;
    }
  }
  EXPECT_TRUE(found_rewritten) << "Rewritten data file should be present";
}

// Create equality deletes via RowDelta then rewrite them as position deletes
// in a single RewriteFiles commit.
TEST_P(RewriteFilesFormatVersionTest, ReplaceEqualityDeletesWithPositionDeletes) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
  CommitFileA();

  // Add an equality delete via RowDelta
  {
    ICEBERG_UNWRAP_OR_FAIL(auto row_delta, table_->NewRowDelta());
    row_delta->AddDeletes(eq_delete_file_);
    EXPECT_THAT(row_delta->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto after_delta_snapshot, table_->current_snapshot());

  // Replace the equality delete with a position delete
  {
    ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
    rw->ValidateFromSnapshot(after_delta_snapshot->snapshot_id);
    rw->DeleteDataFile(file_a_);
    rw->DeleteDeleteFile(eq_delete_file_);
    rw->AddDataFile(rewritten_file_a_);
    rw->AddDeleteFile(rewritten_delete_file_a_);
    EXPECT_THAT(rw->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kReplace);
  EXPECT_EQ(
      std::stoll(snapshot->summary.at(SnapshotSummaryFields::kRemovedEqDeleteFiles)), 1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kAddedPosDeleteFiles)),
            1);

  // Verify the delete manifest shows the eq delete as DELETED and pos delete as ADDED
  ICEBERG_UNWRAP_OR_FAIL(auto delete_manifests, DeleteManifests(snapshot));
  ICEBERG_UNWRAP_OR_FAIL(auto delete_entries, ReadAllEntries(delete_manifests));
  bool found_deleted_eq = false;
  bool found_added_pos = false;
  for (const auto& entry : delete_entries) {
    if (entry.status == ManifestStatus::kDeleted &&
        entry.data_file->content == DataFile::Content::kEqualityDeletes) {
      found_deleted_eq = true;
    }
    if (entry.status == ManifestStatus::kAdded &&
        entry.data_file->content == DataFile::Content::kPositionDeletes) {
      found_added_pos = true;
    }
  }
  EXPECT_TRUE(found_deleted_eq) << "Equality delete should be marked DELETED";
  EXPECT_TRUE(found_added_pos) << "Position delete should be marked ADDED";
}

// Remove all deletes: create a data file and an associated equality delete,
// then rewrite the data file while removing the delete file entirely (empty
// delete add set).
TEST_P(RewriteFilesFormatVersionTest, RemoveAllDeletes) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
  CommitFileA();

  // Add an equality delete via RowDelta
  {
    ICEBERG_UNWRAP_OR_FAIL(auto row_delta, table_->NewRowDelta());
    row_delta->AddDeletes(eq_delete_file_);
    EXPECT_THAT(row_delta->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  // Verify delete file exists before rewrite
  {
    ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
    ICEBERG_UNWRAP_OR_FAIL(auto delete_manifests, DeleteManifests(snapshot));
    EXPECT_GT(delete_manifests.size(), 0);
  }

  ICEBERG_UNWRAP_OR_FAIL(auto after_delta_snapshot, table_->current_snapshot());

  // Rewrite: delete the data file and the equality delete, add rewritten data,
  // add no new delete files (empty delete add set).
  {
    ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
    rw->ValidateFromSnapshot(after_delta_snapshot->snapshot_id);
    rw->DeleteDataFile(file_a_);
    rw->DeleteDeleteFile(eq_delete_file_);
    rw->AddDataFile(rewritten_file_a_);
    // no AddDeleteFile call — delete add set is empty
    EXPECT_THAT(rw->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kReplace);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles)),
            1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles)), 1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kRemovedDeleteFiles)),
            1);
  // No added delete files expected
  EXPECT_EQ(snapshot->summary.count(SnapshotSummaryFields::kAddedDeleteFiles), 0);

  // Verify delete manifests show the eq delete as DELETED and no added deletes
  ICEBERG_UNWRAP_OR_FAIL(auto delete_manifests, DeleteManifests(snapshot));
  ICEBERG_UNWRAP_OR_FAIL(auto delete_entries, ReadAllEntries(delete_manifests));
  bool found_deleted_delete = false;
  for (const auto& entry : delete_entries) {
    if (entry.status == ManifestStatus::kDeleted) {
      found_deleted_delete = true;
    }
    EXPECT_NE(entry.status, ManifestStatus::kAdded)
        << "No new delete files should be added";
  }
  EXPECT_TRUE(found_deleted_delete) << "Original delete file should be marked DELETED";
}

// Verify that RewriteFiles detects new delete files that were committed after
// the validation snapshot boundary, preventing data loss.
TEST_P(RewriteFilesFormatVersionTest, NewDeleteFile) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto starting_snapshot, table_->current_snapshot());

  // Concurrently add an equality delete targeting the data file we plan to rewrite
  {
    ICEBERG_UNWRAP_OR_FAIL(auto row_delta, table_->NewRowDelta());
    row_delta->AddDeletes(eq_delete_file_);
    EXPECT_THAT(row_delta->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  // Try to rewrite the data file, validating from before the delete was added
  {
    ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
    rw->ValidateFromSnapshot(starting_snapshot->snapshot_id);
    rw->DeleteDataFile(file_a_);
    rw->AddDataFile(rewritten_file_a_);

    auto result = rw->Commit();
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(result, HasErrorMessage("found new delete for replaced data file"));
    EXPECT_THAT(result, HasErrorMessage(file_a_->file_path));
  }
}

// Inject commit failures that exhaust retries when rewriting both data and
// delete files. Verify the commit fails with CommitFailed after exhausting
// retries.
TEST_P(RewriteFilesFormatVersionTest, FailureWhenRewriteBothDataAndDeleteFiles) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
  CommitFileA();

  // Create delete file via RowDelta first
  {
    ICEBERG_UNWRAP_OR_FAIL(auto row_delta, table_->NewRowDelta());
    row_delta->AddDeletes(delete_file_a_);
    EXPECT_THAT(row_delta->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto after_delta_snapshot, table_->current_snapshot());

  constexpr int32_t kRetries = 2;
  constexpr int32_t kInjectedFailures = kRetries + 1;
  int call_count = 0;

  SetCommitRetryProperties(kRetries);
  const auto sequence_number_before = table_->metadata()->last_sequence_number;
  const auto metadata_avro_files_before = MetadataAvroFiles();

  BindTableWithFailingCommits(kInjectedFailures, &call_count);

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->ValidateFromSnapshot(after_delta_snapshot->snapshot_id);
  rw->DeleteDataFile(file_a_);
  rw->DeleteDeleteFile(delete_file_a_);
  rw->AddDataFile(rewritten_file_a_);
  rw->AddDeleteFile(rewritten_delete_file_a_);

  auto result = rw->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_EQ(call_count, kRetries + 1);

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_after, table_->current_snapshot());
  EXPECT_EQ(snapshot_after->snapshot_id, after_delta_snapshot->snapshot_id);
  EXPECT_EQ(table_->metadata()->last_sequence_number, sequence_number_before);
  EXPECT_EQ(MetadataAvroFiles(), metadata_avro_files_before);
}

// Inject transient commit failures that stay within the retry budget when
// rewriting both data and delete files, then verify success.
TEST_P(RewriteFilesFormatVersionTest, RecoverWhenRewriteBothDataAndDeleteFiles) {
  if (format_version() < 2) {
    GTEST_SKIP() << "Requires format version >= 2";
  }
  CommitFileA();

  // Create delete file via RowDelta
  {
    ICEBERG_UNWRAP_OR_FAIL(auto row_delta, table_->NewRowDelta());
    row_delta->AddDeletes(delete_file_a_);
    EXPECT_THAT(row_delta->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto after_delta_snapshot, table_->current_snapshot());

  constexpr int32_t kRetries = 4;
  constexpr int32_t kInjectedFailures = 2;
  int call_count = 0;

  SetCommitRetryProperties(kRetries);
  BindTableWithFailingCommits(kInjectedFailures, &call_count);

  ICEBERG_UNWRAP_OR_FAIL(auto rw, NewRewriteFiles());
  rw->ValidateFromSnapshot(after_delta_snapshot->snapshot_id);
  rw->DeleteDataFile(file_a_);
  rw->DeleteDeleteFile(delete_file_a_);
  rw->AddDataFile(rewritten_file_a_);
  rw->AddDeleteFile(rewritten_delete_file_a_);

  EXPECT_THAT(rw->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  EXPECT_EQ(call_count, kInjectedFailures + 1);

  // Verify the rewrite succeeded and data is correct
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kReplace);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles)),
            1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles)), 1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kRemovedDeleteFiles)),
            1);
  EXPECT_EQ(std::stoll(snapshot->summary.at(SnapshotSummaryFields::kAddedDeleteFiles)),
            1);
}

// ============================================================================
// TODO(WZhuo): Tests blocked on missing infrastructure in iceberg-cpp.
// ============================================================================
//
// TODO(RemovingDataFileAlsoRemovesDV):
//   Blocked by: format v3 DV auto-cleanup not yet supported.
//   Creates data+delete files via RowDelta (v3), rewrites with deleteFile.
//   Verifies the DV for the removed data file is automatically cleaned up.
//   Java guard: assumeThat(formatVersion).isGreaterThanOrEqualTo(3)
//
// TODO(DeleteWithDuplicateEntriesInManifest):
//   Blocked by: cannot yet append the same file twice to create duplicate manifest
//   entries. Appends FILE_A twice, then rewrites one copy. Verifies manifest entry
//   statuses (DELETED for the rewritten copy, EXISTING for the other).
//   Java guard: none (runs on all versions)

INSTANTIATE_TEST_SUITE_P(FormatVersions, RewriteFilesFormatVersionTest,
                         ::testing::Values(int8_t{1}, int8_t{2}));

}  // namespace iceberg
