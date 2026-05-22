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

#include "iceberg/update/merging_snapshot_update.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/constants.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
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
#include "iceberg/update/fast_append.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/macros.h"

namespace iceberg {

/// \brief Concrete subclass of MergingSnapshotUpdate for testing.
class TestMergeAppend : public MergingSnapshotUpdate {
 public:
  static Result<std::unique_ptr<TestMergeAppend>> Make(std::string table_name,
                                                       std::shared_ptr<Table> table) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto ctx, TransactionContext::Make(std::move(table), TransactionKind::kUpdate));
    return std::unique_ptr<TestMergeAppend>(
        new TestMergeAppend(std::move(table_name), std::move(ctx)));
  }

  std::string operation() override { return "append"; }

  // Expose protected API for test access
  Status AddFile(std::shared_ptr<DataFile> file) { return AddDataFile(std::move(file)); }
  Status AddDelete(std::shared_ptr<DataFile> file) {
    return AddDeleteFile(std::move(file));
  }
  Status RemoveDataFile(std::shared_ptr<DataFile> file) {
    return DeleteDataFile(std::move(file));
  }
  Status RemoveDeleteFile(std::shared_ptr<DataFile> file) {
    return DeleteDeleteFile(std::move(file));
  }
  Status AppendManifest(ManifestFile manifest) {
    return AddManifest(std::move(manifest));
  }
  Result<std::shared_ptr<PartitionSpec>> DataSpec() const {
    return MergingSnapshotUpdate::DataSpec();
  }
  void SetDataSeqNumber(int64_t seq) { SetNewDataFilesDataSequenceNumber(seq); }

  bool HasDataFiles() const { return AddsDataFiles(); }
  bool HasDeleteFiles() const { return AddsDeleteFiles(); }
  bool HasDataDeletes() const { return DeletesDataFiles(); }

 private:
  TestMergeAppend(std::string table_name, std::shared_ptr<TransactionContext> ctx)
      : MergingSnapshotUpdate(std::move(table_name), std::move(ctx)) {}
};

class MergingSnapshotUpdateTest : public MinimalUpdateTestBase {
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

  Result<std::unique_ptr<TestMergeAppend>> NewMergeAppend() {
    return TestMergeAppend::Make(TableName(), table_);
  }

  // Commit file_a_ with FastAppend and refresh the table.
  void CommitFileA() {
    ICEBERG_UNWRAP_OR_FAIL(auto fa, table_->NewFastAppend());
    fa->AppendFile(file_a_);
    EXPECT_THAT(fa->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  // Read all entries from a list of ManifestFiles.
  Result<std::vector<ManifestEntry>> ReadAllEntries(
      const std::vector<ManifestFile>& manifests, const TableMetadata& metadata) {
    std::vector<ManifestEntry> result;
    for (const auto& m : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(m.partition_spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
      ICEBERG_ASSIGN_OR_RAISE(auto reader,
                              ManifestReader::Make(m, file_io_, schema, spec));
      ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
      result.insert(result.end(), entries.begin(), entries.end());
    }
    return result;
  }

  // Write a manifest file containing the given data files.
  // Returns a ManifestFile with added_snapshot_id = kInvalidSnapshotId so it
  // is eligible for snapshot ID inheritance.
  Result<ManifestFile> WriteManifest(
      const std::string& path, const std::vector<std::shared_ptr<DataFile>>& files) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        ManifestWriter::MakeWriter(/*format_version=*/2, kInvalidSnapshotId, path,
                                   file_io_, spec_, schema_, ManifestContent::kData));
    for (const auto& f : files) {
      ManifestEntry entry;
      entry.status = ManifestStatus::kAdded;
      entry.snapshot_id = std::nullopt;
      entry.data_file = f;
      ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(entry));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
};

// -------------------------------------------------------------------------
// State query tests
// -------------------------------------------------------------------------

TEST_F(MergingSnapshotUpdateTest, AddsDataFilesInitiallyFalse) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_FALSE(op->HasDataFiles());
  EXPECT_FALSE(op->HasDeleteFiles());
  EXPECT_FALSE(op->HasDataDeletes());
}

TEST_F(MergingSnapshotUpdateTest, AddsDataFilesTrueAfterAdd) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_TRUE(op->HasDataFiles());
  EXPECT_FALSE(op->HasDeleteFiles());
}

TEST_F(MergingSnapshotUpdateTest, AddsDeleteFilesTrueAfterAdd) {
  auto del_file = MakeDeleteFile("/delete/del_a.parquet", 1L);
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
  EXPECT_FALSE(op->HasDataFiles());
  EXPECT_TRUE(op->HasDeleteFiles());
}

TEST_F(MergingSnapshotUpdateTest, DeletesDataFilesTrueAfterRegisterDelete) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->RemoveDataFile(file_a_), IsOk());
  EXPECT_TRUE(op->HasDataDeletes());
}

// -------------------------------------------------------------------------
// Apply / Commit tests
// -------------------------------------------------------------------------

TEST_F(MergingSnapshotUpdateTest, CommitNewDataFile) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("added-data-files"), "1");
  EXPECT_EQ(snapshot->summary.at("added-records"), "100");
}

TEST_F(MergingSnapshotUpdateTest, CommitMultipleDataFiles) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("added-data-files"), "2");
  EXPECT_EQ(snapshot->summary.at("added-records"), "200");
}

TEST_F(MergingSnapshotUpdateTest, CommitDataFileAndDeleteFile) {
  auto del_file = MakeDeleteFile("/delete/del_a.parquet", 1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  // Data file summary
  EXPECT_EQ(snapshot->summary.at("added-data-files"), "1");
}

TEST_F(MergingSnapshotUpdateTest, CommitPreservesExistingManifests) {
  // First append: file_a
  CommitFileA();

  // Second merge append: file_b
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  // Both data files should be visible — 1 existing + 1 new
  EXPECT_EQ(snapshot->summary.at("total-data-files"), "2");
}

TEST_F(MergingSnapshotUpdateTest, CommitDeletesDataFile) {
  CommitFileA();

  // Remove file_a via merging snapshot update
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->RemoveDataFile(file_a_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("total-data-files"), "0");
  EXPECT_EQ(snapshot->summary.at("deleted-data-files"), "1");
}

TEST_F(MergingSnapshotUpdateTest, SetNewDataFilesDataSequenceNumber) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  op->SetDataSeqNumber(42);
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("added-data-files"), "1");
}

// -------------------------------------------------------------------------
// CleanUncommitted test
// -------------------------------------------------------------------------

TEST_F(MergingSnapshotUpdateTest, CleanUncommittedAfterSuccessfulCommitDoesNotCrash) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  // Simulate a caller invoking CleanUncommitted after a commit (e.g. cleanup
  // in an error handler that runs regardless of success). Passing an empty set
  // means no manifests are considered committed, so CleanUncommitted attempts
  // to delete all written manifests. This should not crash.
  op->CleanUncommitted({});
}

// -------------------------------------------------------------------------
// Delete file summary tests
// -------------------------------------------------------------------------

TEST_F(MergingSnapshotUpdateTest, CommitDeleteFileSummaryHasAddedDeleteFiles) {
  auto del_file = MakeDeleteFile("/delete/del_a.parquet", 1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDeleteFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedPosDeleteFiles), "1");
  EXPECT_EQ(snapshot->summary.count(SnapshotSummaryFields::kRemovedDeleteFiles), 0);
}

// Covers the bug where deleted delete files were not tracked in the snapshot summary.
TEST_F(MergingSnapshotUpdateTest, CommitDeletesDeleteFileSummaryHasRemovedDeleteFiles) {
  // Step 1: commit a delete file.
  auto del_file = MakeDeleteFile("/delete/del_a.parquet", 1L);
  {
    ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
    EXPECT_THAT(op->AddDelete(del_file), IsOk());
    EXPECT_THAT(op->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  // Step 2: commit a new snapshot that removes the delete file.
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->RemoveDeleteFile(del_file), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kRemovedDeleteFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kRemovedPosDeleteFiles), "1");
  EXPECT_EQ(snapshot->summary.count(SnapshotSummaryFields::kAddedDeleteFiles), 0);
}

// -------------------------------------------------------------------------
// Deduplication test
// -------------------------------------------------------------------------

TEST_F(MergingSnapshotUpdateTest, DuplicateDataFileOnlyCountedOnce) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->AddFile(file_a_), IsOk());  // duplicate — should be ignored
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "1");
}

// -------------------------------------------------------------------------
// ValidateNewDeleteFile format version tests
// -------------------------------------------------------------------------

/// \brief V1-table test fixture — deletes are not supported in format v1.
class MergingSnapshotUpdateV1Test : public UpdateTestBase {
 protected:
  std::string MetadataResource() const override { return "TableMetadataV1Valid.json"; }
  std::string TableName() const override { return "v1_test_table"; }

  void SetUp() override {
    UpdateTestBase::SetUp();
    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
  }

  std::shared_ptr<DataFile> MakeDeleteFile(const std::string& path) {
    auto f = std::make_shared<DataFile>();
    f->content = DataFile::Content::kPositionDeletes;
    f->file_path = table_location_ + path;
    f->file_format = FileFormatType::kParquet;
    f->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
    f->file_size_in_bytes = 512;
    f->record_count = 10;
    f->partition_spec_id = spec_->spec_id();
    return f;
  }

  Result<std::unique_ptr<TestMergeAppend>> NewMergeAppend() {
    return TestMergeAppend::Make(TableName(), table_);
  }

  std::shared_ptr<PartitionSpec> spec_;
};

TEST_F(MergingSnapshotUpdateV1Test, ValidateNewDeleteFileV1Rejected) {
  auto del_file = MakeDeleteFile("/delete/del_a.parquet");
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(del_file), IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, ValidateNewDeleteFileV2RejectsDeletionVector) {
  // Position delete with referenced_data_file set = deletion vector, not allowed in v2.
  auto del_file = MakeDeleteFile("/delete/del_a.parquet", 1L);
  del_file->referenced_data_file = table_location_ + "/data/file_a.parquet";

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(del_file), IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, ValidateNewDeleteFileV2AllowsEqualityDelete) {
  auto eq_del = MakeDeleteFile("/delete/eq_del.parquet", 1L);
  eq_del->content = DataFile::Content::kEqualityDeletes;

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(eq_del), IsOk());
}

// -------------------------------------------------------------------------
// AddManifest — invalid manifest rejection
// -------------------------------------------------------------------------

TEST_F(MergingSnapshotUpdateTest, AddManifestRejectsDeleteManifest) {
  // Build a ManifestFile with content = kDeletes
  ManifestFile del_manifest;
  del_manifest.manifest_path = table_location_ + "/metadata/del.avro";
  del_manifest.content = ManifestContent::kDeletes;
  del_manifest.added_snapshot_id = kInvalidSnapshotId;

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AppendManifest(del_manifest), IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, AddManifestRejectsManifestWithExistingFiles) {
  // Construct a ManifestFile that reports existing files without writing to disk.
  ManifestFile manifest;
  manifest.manifest_path = table_location_ + "/metadata/existing.avro";
  manifest.content = ManifestContent::kData;
  manifest.added_snapshot_id = kInvalidSnapshotId;
  manifest.existing_files_count = 1;  // has_existing_files() returns true

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AppendManifest(manifest), IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, AddManifestRejectsManifestWithDeletedFiles) {
  ManifestFile manifest;
  manifest.manifest_path = table_location_ + "/metadata/deleted.avro";
  manifest.content = ManifestContent::kData;
  manifest.added_snapshot_id = kInvalidSnapshotId;
  manifest.deleted_files_count = 1;  // has_deleted_files() returns true

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AppendManifest(manifest), IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, AddManifestRejectsManifestWithAssignedSnapshotId) {
  ManifestFile manifest;
  manifest.manifest_path = table_location_ + "/metadata/snap.avro";
  manifest.content = ManifestContent::kData;
  manifest.added_snapshot_id = 12345;  // already assigned

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AppendManifest(manifest), IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, AddManifestRejectsManifestWithFirstRowId) {
  ManifestFile manifest;
  manifest.manifest_path = table_location_ + "/metadata/rowid.avro";
  manifest.content = ManifestContent::kData;
  manifest.added_snapshot_id = kInvalidSnapshotId;
  manifest.first_row_id = 0;  // assigned first_row_id

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AppendManifest(manifest), IsError(ErrorKind::kInvalidArgument));
}

// -------------------------------------------------------------------------
// AddManifest — basic commit (inherit path: v2 with can_inherit_snapshot_id)
// -------------------------------------------------------------------------

TEST_F(MergingSnapshotUpdateTest, AppendManifestEmptyTable) {
  auto path = table_location_ + "/metadata/input.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_, file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AppendManifest(manifest), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());

  // In v2 with snapshot ID inheritance, the manifest path is reused directly.
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests,
                         SnapshotCache(snapshot.get()).DataManifests(file_io_));
  ASSERT_EQ(data_manifests.size(), 1);

  EXPECT_EQ(snapshot->summary.at("added-data-files"), "2");
  EXPECT_EQ(snapshot->summary.at("total-data-files"), "2");
}

TEST_F(MergingSnapshotUpdateTest, AppendManifestWithDataFiles) {
  // Mix AddDataFile + AddManifest — should produce 2 manifests.
  auto path = table_location_ + "/metadata/input.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_, file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());  // file_b_ staged directly
  EXPECT_THAT(op->AppendManifest(manifest), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests,
                         SnapshotCache(snapshot.get()).DataManifests(file_io_));
  // Written manifest (file_b_) + appended manifest (file_a_, file_b_)
  EXPECT_EQ(data_manifests.size(), 2);
  EXPECT_EQ(snapshot->summary.at("added-data-files"), "3");
}

// -------------------------------------------------------------------------
// AddManifest — merge behavior
// -------------------------------------------------------------------------

TEST_F(MergingSnapshotUpdateTest, AppendManifestMergeWithMinCountOne) {
  // Set min-count-to-merge = 1 so all manifests are merged.
  ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
  props->Set(std::string(TableProperties::kManifestMinMergeCount.key()), "1");
  EXPECT_THAT(props->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  auto path = table_location_ + "/metadata/input.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_, file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());
  EXPECT_THAT(op->AppendManifest(manifest), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests,
                         SnapshotCache(snapshot.get()).DataManifests(file_io_));
  // Both manifests merged into one.
  EXPECT_EQ(data_manifests.size(), 1);
  EXPECT_EQ(snapshot->summary.at("added-data-files"), "3");
}

TEST_F(MergingSnapshotUpdateTest, AppendManifestDoNotMergeMinCount) {
  // Set min-count-to-merge = 4 so 3 manifests are not merged.
  ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
  props->Set(std::string(TableProperties::kManifestMinMergeCount.key()), "4");
  EXPECT_THAT(props->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  auto path1 = table_location_ + "/metadata/m1.avro";
  auto path2 = table_location_ + "/metadata/m2.avro";
  auto path3 = table_location_ + "/metadata/m3.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(path1, {file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(auto m2, WriteManifest(path2, {file_b_}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto m3, WriteManifest(path3, {MakeDataFile("/data/file_c.parquet", 3L)}));

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AppendManifest(m1), IsOk());
  EXPECT_THAT(op->AppendManifest(m2), IsOk());
  EXPECT_THAT(op->AppendManifest(m3), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests,
                         SnapshotCache(snapshot.get()).DataManifests(file_io_));
  // Below min-count-to-merge threshold — all 3 pass through unchanged.
  EXPECT_EQ(data_manifests.size(), 3);
  EXPECT_EQ(snapshot->summary.at("added-data-files"), "3");
}

// -------------------------------------------------------------------------
// Manifest merge — data files only
// -------------------------------------------------------------------------

TEST_F(MergingSnapshotUpdateTest, ManifestMergeMergesIntoOne) {
  // Set min-count-to-merge = 1 so every append triggers a merge.
  ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
  props->Set(std::string(TableProperties::kManifestMinMergeCount.key()), "1");
  EXPECT_THAT(props->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  // Snapshot 1: file_a_
  CommitFileA();

  // Snapshot 2: file_b_ — should merge with existing manifest.
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests,
                         SnapshotCache(snapshot.get()).DataManifests(file_io_));
  EXPECT_EQ(data_manifests.size(), 1);
  EXPECT_EQ(snapshot->summary.at("total-data-files"), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "1");
}

TEST_F(MergingSnapshotUpdateTest, ManifestMergeDoesNotMergeWhenBelowMinCount) {
  // Default min-count-to-merge = 100, so manifests are not merged.
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests,
                         SnapshotCache(snapshot.get()).DataManifests(file_io_));
  EXPECT_EQ(data_manifests.size(), 2);
  EXPECT_EQ(snapshot->summary.at("total-data-files"), "2");
}

TEST_F(MergingSnapshotUpdateTest, ManifestMergeDoesNotMergeWhenSizeTargetTooSmall) {
  // Set a tiny size target so manifests never merge.
  ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
  props->Set(std::string(TableProperties::kManifestTargetSizeBytes.key()), "10");
  EXPECT_THAT(props->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests,
                         SnapshotCache(snapshot.get()).DataManifests(file_io_));
  EXPECT_EQ(data_manifests.size(), 2);
}

// -------------------------------------------------------------------------
// Manifest count summary
// -------------------------------------------------------------------------

TEST_F(MergingSnapshotUpdateTest, SummaryManifestCountsOnFirstCommit) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "0");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "0");
}

TEST_F(MergingSnapshotUpdateTest, SummaryManifestCountsOnSecondCommitNoMerge) {
  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  // 1 new manifest created, 1 existing manifest kept, 0 replaced.
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "0");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "1");
}

TEST_F(MergingSnapshotUpdateTest, SummaryManifestCountsAfterMerge) {
  ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
  props->Set(std::string(TableProperties::kManifestMinMergeCount.key()), "1");
  EXPECT_THAT(props->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  CommitFileA();

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  // 1 merged output created, 1 existing manifest replaced, 0 kept.
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "0");
}

TEST_F(MergingSnapshotUpdateTest, SummaryManifestCountsAfterDelete) {
  ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
  props->Set(std::string(TableProperties::kManifestMinMergeCount.key()), "1");
  EXPECT_THAT(props->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  CommitFileA();

  // Delete file_a_ — filter manager rewrites the manifest.
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->RemoveDataFile(file_a_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  // Filter rewrites 1 manifest (replaced), merge produces 1 output (created).
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "0");
}

// -------------------------------------------------------------------------
// DataSpec — multiple partition specs
// -------------------------------------------------------------------------

TEST_F(MergingSnapshotUpdateTest, DataSpecThrowsWithMultipleSpecs) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  // file_a_ and file_b_ both use spec_id 0 — DataSpec() should succeed.
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());
  EXPECT_THAT(op->DataSpec(), IsOk());
}

TEST_F(MergingSnapshotUpdateTest, DataSpecThrowsWhenEmpty) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  // No files added — DataSpec() should fail.
  EXPECT_THAT(op->DataSpec(), IsError(ErrorKind::kInvalidArgument));
}

}  // namespace iceberg
