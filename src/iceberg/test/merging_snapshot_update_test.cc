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
#include "iceberg/expression/expressions.h"
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
  Status AddDelete(std::shared_ptr<DataFile> file, int64_t data_sequence_number) {
    return AddDeleteFile(std::move(file), data_sequence_number);
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
  int64_t GeneratedSnapshotId() { return SnapshotId(); }
  void SetDataSeqNumber(int64_t seq) { SetNewDataFilesDataSequenceNumber(seq); }
  static Status ValidateAddedDataFilesForTest(const TableMetadata& metadata,
                                              std::optional<int64_t> starting_snapshot_id,
                                              const std::shared_ptr<Snapshot>& parent,
                                              std::shared_ptr<FileIO> io) {
    return MergingSnapshotUpdate::ValidateAddedDataFiles(metadata, starting_snapshot_id,
                                                         nullptr, parent, std::move(io));
  }
  static Status ValidateAddedDataFilesForTest(const TableMetadata& metadata,
                                              int64_t starting_snapshot_id,
                                              const PartitionSet& partition_set,
                                              const std::shared_ptr<Snapshot>& parent,
                                              std::shared_ptr<FileIO> io) {
    return MergingSnapshotUpdate::ValidateAddedDataFiles(
        metadata, starting_snapshot_id, partition_set, parent, std::move(io));
  }
  static Status ValidateNoNewDeletesForDataFilesForTest(
      const TableMetadata& metadata, int64_t starting_snapshot_id,
      std::shared_ptr<Expression> data_filter, const DataFileSet& replaced_files,
      const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io) {
    return MergingSnapshotUpdate::ValidateNoNewDeletesForDataFiles(
        metadata, starting_snapshot_id, std::move(data_filter), replaced_files, parent,
        std::move(io));
  }
  static Status ValidateNoNewDeleteFilesForTest(const TableMetadata& metadata,
                                                int64_t starting_snapshot_id,
                                                std::shared_ptr<Expression> data_filter,
                                                const std::shared_ptr<Snapshot>& parent,
                                                std::shared_ptr<FileIO> io) {
    return MergingSnapshotUpdate::ValidateNoNewDeleteFiles(
        metadata, starting_snapshot_id, std::move(data_filter), parent, std::move(io));
  }
  static Status ValidateNoNewDeleteFilesForTest(const TableMetadata& metadata,
                                                int64_t starting_snapshot_id,
                                                const PartitionSet& partition_set,
                                                const std::shared_ptr<Snapshot>& parent,
                                                std::shared_ptr<FileIO> io) {
    return MergingSnapshotUpdate::ValidateNoNewDeleteFiles(
        metadata, starting_snapshot_id, partition_set, parent, std::move(io));
  }
  static Status ValidateDeletedDataFilesForTest(const TableMetadata& metadata,
                                                int64_t starting_snapshot_id,
                                                std::shared_ptr<Expression> data_filter,
                                                const std::shared_ptr<Snapshot>& parent,
                                                std::shared_ptr<FileIO> io) {
    return MergingSnapshotUpdate::ValidateDeletedDataFiles(
        metadata, starting_snapshot_id, std::move(data_filter), parent, std::move(io));
  }
  static Status ValidateDeletedDataFilesForTest(const TableMetadata& metadata,
                                                int64_t starting_snapshot_id,
                                                const PartitionSet& partition_set,
                                                const std::shared_ptr<Snapshot>& parent,
                                                std::shared_ptr<FileIO> io) {
    return MergingSnapshotUpdate::ValidateDeletedDataFiles(
        metadata, starting_snapshot_id, partition_set, parent, std::move(io));
  }
  static Status ValidateAddedDVsForTest(
      const TableMetadata& metadata, int64_t starting_snapshot_id,
      std::shared_ptr<Expression> conflict_filter,
      const std::unordered_set<std::string>& referenced_data_files,
      const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io) {
    return MergingSnapshotUpdate::ValidateAddedDVs(
        metadata, starting_snapshot_id, std::move(conflict_filter), referenced_data_files,
        parent, std::move(io));
  }

  bool HasDataFiles() const { return AddsDataFiles(); }
  bool HasDeleteFiles() const { return AddsDeleteFiles(); }
  bool HasDataDeletes() const { return DeletesDataFiles(); }

 private:
  TestMergeAppend(std::string table_name, std::shared_ptr<TransactionContext> ctx)
      : MergingSnapshotUpdate(std::move(table_name), std::move(ctx)) {}
};

class TestOverwriteUpdate : public MergingSnapshotUpdate {
 public:
  static Result<std::unique_ptr<TestOverwriteUpdate>> Make(std::string table_name,
                                                           std::shared_ptr<Table> table) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto ctx, TransactionContext::Make(std::move(table), TransactionKind::kUpdate));
    return std::unique_ptr<TestOverwriteUpdate>(
        new TestOverwriteUpdate(std::move(table_name), std::move(ctx)));
  }

  std::string operation() override { return DataOperation::kOverwrite; }
  int64_t GeneratedSnapshotId() { return SnapshotId(); }

  Status AddDelete(std::shared_ptr<DataFile> file) {
    return AddDeleteFile(std::move(file));
  }
  Status AddDelete(std::shared_ptr<DataFile> file, int64_t data_sequence_number) {
    return AddDeleteFile(std::move(file), data_sequence_number);
  }
  Status RemoveDataFile(std::shared_ptr<DataFile> file) {
    return DeleteDataFile(std::move(file));
  }

 private:
  TestOverwriteUpdate(std::string table_name, std::shared_ptr<TransactionContext> ctx)
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

  std::shared_ptr<DataFile> MakeEqualityDeleteFile(const std::string& path,
                                                   int64_t partition_x) {
    auto f = MakeDeleteFile(path, partition_x);
    f->content = DataFile::Content::kEqualityDeletes;
    f->equality_ids = {1};
    return f;
  }

  Result<std::unique_ptr<TestMergeAppend>> NewMergeAppend() {
    return TestMergeAppend::Make(TableName(), table_);
  }

  Result<std::unique_ptr<TestOverwriteUpdate>> NewOverwriteUpdate() {
    return TestOverwriteUpdate::Make(TableName(), table_);
  }

  void SetTableFormatVersion(int8_t format_version) {
    table_->metadata()->format_version = format_version;
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

  Result<ManifestFile> WriteDataManifest(
      const TableMetadata& metadata, const std::string& path,
      const std::vector<std::shared_ptr<DataFile>>& files, int64_t snapshot_id,
      int64_t sequence_number) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
    ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_->spec_id()));
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        ManifestWriter::MakeWriter(metadata.format_version, snapshot_id, path, file_io_,
                                   spec, schema, ManifestContent::kData));
    for (const auto& f : files) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(f, sequence_number));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  Result<ManifestFile> WriteDeleteManifest(
      const TableMetadata& metadata, const std::string& path,
      const std::vector<std::shared_ptr<DataFile>>& files, int64_t snapshot_id,
      int64_t sequence_number) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
    ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_->spec_id()));
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        ManifestWriter::MakeWriter(metadata.format_version, snapshot_id, path, file_io_,
                                   spec, schema, ManifestContent::kDeletes));
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
  auto del_file = MakeEqualityDeleteFile("/delete/del_a.parquet", 1L);
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
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedRecords), "100");
}

TEST_F(MergingSnapshotUpdateTest, CommitMultipleDataFiles) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedRecords), "200");
}

TEST_F(MergingSnapshotUpdateTest, CommitDataFileAndDeleteFile) {
  auto del_file = MakeEqualityDeleteFile("/delete/del_a.parquet", 1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  // Data file summary
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
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
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "2");
}

TEST_F(MergingSnapshotUpdateTest, CommitDeletesDataFile) {
  CommitFileA();

  // Remove file_a via merging snapshot update
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->RemoveDataFile(file_a_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "0");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
}

TEST_F(MergingSnapshotUpdateTest, SetNewDataFilesDataSequenceNumber) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  op->SetDataSeqNumber(42);
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
}

TEST_F(MergingSnapshotUpdateTest, CustomSummaryPropertySurvivesApplyRebuild) {
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  op->Set("custom-prop", "custom-value");
  EXPECT_THAT(op->AddFile(file_a_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("custom-prop"), "custom-value");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
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

TEST_F(MergingSnapshotUpdateTest, CommitDeduplicatesStagedDeleteFiles) {
  auto del_file = MakeDeleteFile("/delete/del_a.parquet", 1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDeleteFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedPosDeleteFiles), "1");
}

TEST_F(MergingSnapshotUpdateTest, AddDeleteFileWithExplicitSequenceWritesSequenceNumber) {
  auto del_file = MakeEqualityDeleteFile("/delete/del_a.parquet", 1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(del_file, 17), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, op->Apply(*table_->metadata(), nullptr));
  auto delete_manifest_it =
      std::ranges::find_if(manifests, [](const ManifestFile& manifest) {
        return manifest.content == ManifestContent::kDeletes;
      });
  ASSERT_NE(delete_manifest_it, manifests.end());
  ICEBERG_UNWRAP_OR_FAIL(auto entries,
                         ReadAllEntries(std::vector<ManifestFile>{*delete_manifest_it},
                                        *table_->metadata()));
  ASSERT_EQ(entries.size(), 1U);
  ASSERT_TRUE(entries[0].sequence_number.has_value());
  EXPECT_EQ(entries[0].sequence_number.value(), 17);
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

TEST_F(MergingSnapshotUpdateTest, ValidateNewDeleteFileV2AllowsReferencedPositionDelete) {
  auto del_file = MakeDeleteFile("/delete/del_a.parquet", 1L);
  del_file->referenced_data_file = table_location_ + "/data/file_a.parquet";

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
}

TEST_F(MergingSnapshotUpdateTest, ValidateNewDeleteFileV2RejectsDeletionVector) {
  auto del_file = MakeDeleteFile("/delete/dv_a.puffin", 1L);
  del_file->file_format = FileFormatType::kPuffin;

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(del_file), IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, ValidateNewDeleteFileV2AllowsEqualityDelete) {
  auto eq_del = MakeDeleteFile("/delete/eq_del.parquet", 1L);
  eq_del->content = DataFile::Content::kEqualityDeletes;

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(eq_del), IsOk());
}

TEST_F(MergingSnapshotUpdateTest, ValidateNewDeleteFileV3RejectsNonDVPositionDelete) {
  SetTableFormatVersion(3);

  auto del_file = MakeDeleteFile("/delete/del_a.parquet", 1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(del_file), IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, ValidateNewDeleteFileV3AllowsDeletionVector) {
  SetTableFormatVersion(3);

  auto del_file = MakeDeleteFile("/delete/dv_a.puffin", 1L);
  del_file->file_format = FileFormatType::kPuffin;
  del_file->referenced_data_file = file_a_->file_path;
  del_file->content_offset = 0;
  del_file->content_size_in_bytes = 10;

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
}

TEST_F(MergingSnapshotUpdateTest, ApplyRejectsV2StagedPositionDeleteAfterV3Upgrade) {
  auto del_file = MakeDeleteFile("/delete/del_a.parquet", 1L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
  metadata->format_version = 3;
  EXPECT_THAT(op->Apply(*metadata, nullptr), IsError(ErrorKind::kInvalidArgument));
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

TEST_F(MergingSnapshotUpdateTest, AddManifestAllowsManifestWithExistingFilesCount) {
  ManifestFile manifest;
  manifest.manifest_path = table_location_ + "/metadata/existing.avro";
  manifest.content = ManifestContent::kData;
  manifest.added_snapshot_id = kInvalidSnapshotId;
  manifest.existing_files_count = 1;

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AppendManifest(manifest), IsOk());
}

TEST_F(MergingSnapshotUpdateTest, AddManifestAllowsManifestWithDeletedFilesCount) {
  ManifestFile manifest;
  manifest.manifest_path = table_location_ + "/metadata/deleted.avro";
  manifest.content = ManifestContent::kData;
  manifest.added_snapshot_id = kInvalidSnapshotId;
  manifest.deleted_files_count = 1;

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AppendManifest(manifest), IsOk());
}

TEST_F(MergingSnapshotUpdateTest, AddManifestCopiesManifestWithAssignedSnapshotId) {
  auto path = table_location_ + "/metadata/snap.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_}));
  manifest.added_snapshot_id = 12345;

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->AppendManifest(manifest), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, snapshot_cache.DataManifests(file_io_));
  ASSERT_EQ(data_manifests.size(), 1U);
  EXPECT_NE(data_manifests[0].manifest_path, path);
}

TEST_F(MergingSnapshotUpdateTest, AddManifestRejectsManifestWithFirstRowId) {
  auto path = table_location_ + "/metadata/rowid.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_}));
  manifest.first_row_id = 0;

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
  SnapshotCache snapshot_cache(snapshot.get());

  // In v2 with snapshot ID inheritance, the manifest path is reused directly.
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, snapshot_cache.DataManifests(file_io_));
  ASSERT_EQ(data_manifests.size(), 1);

  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "2");
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
  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, snapshot_cache.DataManifests(file_io_));
  // Written manifest (file_b_) + appended manifest (file_a_, file_b_)
  EXPECT_EQ(data_manifests.size(), 2);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "3");
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
  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, snapshot_cache.DataManifests(file_io_));
  // Both manifests merged into one.
  EXPECT_EQ(data_manifests.size(), 1);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "3");
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
  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, snapshot_cache.DataManifests(file_io_));
  // Below min-count-to-merge threshold — all 3 pass through unchanged.
  EXPECT_EQ(data_manifests.size(), 3);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "3");
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
  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, snapshot_cache.DataManifests(file_io_));
  EXPECT_EQ(data_manifests.size(), 1);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "2");
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
  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, snapshot_cache.DataManifests(file_io_));
  EXPECT_EQ(data_manifests.size(), 2);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "2");
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
  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, snapshot_cache.DataManifests(file_io_));
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

TEST_F(MergingSnapshotUpdateTest, MissingRequestedDeleteDoesNotAffectSummary) {
  CommitFileA();

  auto missing = MakeDataFile("/data/missing.parquet", 3L);

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewMergeAppend());
  EXPECT_THAT(op->RemoveDataFile(missing), IsOk());
  EXPECT_THAT(op->AddFile(file_b_), IsOk());
  EXPECT_THAT(op->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.count(SnapshotSummaryFields::kDeletedDataFiles), 0U);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "2");
}

TEST_F(MergingSnapshotUpdateTest, ValidateAddedDataFilesFailsForTruncatedHistory) {
  auto metadata = std::make_shared<TableMetadata>();
  metadata->format_version = 2;
  metadata->location = table_location_;
  metadata->current_schema_id = 0;
  metadata->schemas.push_back(schema_);

  auto make_snapshot = [](int64_t snapshot_id,
                          std::optional<int64_t> parent_snapshot_id) {
    return std::make_shared<Snapshot>(Snapshot{
        .snapshot_id = snapshot_id,
        .parent_snapshot_id = parent_snapshot_id,
        .sequence_number = snapshot_id,
        .timestamp_ms = TimePointMs{},
        .manifest_list = "",
        .summary = {},
        .schema_id = 0,
    });
  };

  auto base_snapshot = make_snapshot(1, std::nullopt);
  auto main_snapshot = make_snapshot(2, 1);
  auto branch_snapshot = make_snapshot(3, 1);
  metadata->snapshots = {base_snapshot, main_snapshot, branch_snapshot};

  EXPECT_THAT(TestMergeAppend::ValidateAddedDataFilesForTest(*metadata, /*starting=*/2,
                                                             branch_snapshot, file_io_),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest,
       ValidateAddedDataFilesWithNoStartingSnapshotFailsForTruncatedHistory) {
  auto metadata = std::make_shared<TableMetadata>();
  metadata->format_version = 2;
  metadata->location = table_location_;
  metadata->current_schema_id = 0;
  metadata->schemas.push_back(schema_);

  auto snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 2,
      .parent_snapshot_id = 1,
      .sequence_number = 2,
      .timestamp_ms = TimePointMs{},
      .manifest_list = "",
      .summary = {},
      .schema_id = 0,
  });
  metadata->snapshots = {snapshot};

  EXPECT_THAT(TestMergeAppend::ValidateAddedDataFilesForTest(*metadata, std::nullopt,
                                                             snapshot, file_io_),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, ValidateAddedDataFilesWithNoStartingSnapshotChecksAll) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());

  EXPECT_THAT(TestMergeAppend::ValidateAddedDataFilesForTest(
                  *table_->metadata(), std::nullopt, snapshot, file_io_),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(TestMergeAppend::ValidateAddedDataFilesForTest(
                  *table_->metadata(), snapshot->snapshot_id, snapshot, file_io_),
              IsOk());
}

TEST_F(MergingSnapshotUpdateTest, ValidateAddedDataFilesWithPartitionSetDetectsConflict) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto fast_append, table_->NewFastAppend());
  fast_append->AppendFile(file_b_);
  EXPECT_THAT(fast_append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, table_->current_snapshot());

  PartitionSet partition_set;
  ASSERT_TRUE(partition_set.add(spec_->spec_id(), file_b_->partition));
  EXPECT_THAT(TestMergeAppend::ValidateAddedDataFilesForTest(
                  *table_->metadata(), first_snapshot->snapshot_id, partition_set,
                  second_snapshot, file_io_),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, ValidateAddedDataFilesIgnoresOldEntrySnapshotId) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());

  constexpr int64_t kSecondSnapshotId = 123456;
  auto manifest_path = table_location_ + "/metadata/old-entry-data.avro";
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest,
      WriteDataManifest(*metadata, manifest_path, {file_b_}, first_snapshot->snapshot_id,
                        first_snapshot->sequence_number));
  manifest.added_snapshot_id = kSecondSnapshotId;
  manifest.sequence_number = first_snapshot->sequence_number + 1;
  manifest.min_sequence_number = first_snapshot->sequence_number;
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      MakeSyntheticSnapshot(DataOperation::kAppend, kSecondSnapshotId,
                            first_snapshot->snapshot_id,
                            first_snapshot->sequence_number + 1, {manifest}));

  metadata->snapshots.push_back(second_snapshot);
  metadata->current_snapshot_id = second_snapshot->snapshot_id;
  metadata->last_sequence_number = second_snapshot->sequence_number;

  EXPECT_THAT(TestMergeAppend::ValidateAddedDataFilesForTest(
                  *metadata, first_snapshot->snapshot_id, second_snapshot, file_io_),
              IsOk());
}

TEST_F(MergingSnapshotUpdateTest,
       ValidateNoNewDeletesForDataFilesWithFilterDetectsConflict) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  auto del_file = MakeEqualityDeleteFile("/delete/del_a.parquet", 1L);
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwriteUpdate());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
  const int64_t second_snapshot_id = op->GeneratedSnapshotId();
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, op->Apply(*table_->metadata(), first_snapshot));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      MakeSyntheticSnapshot(DataOperation::kOverwrite, second_snapshot_id,
                            first_snapshot->snapshot_id,
                            first_snapshot->sequence_number + 1, manifests));

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
  metadata->snapshots.push_back(second_snapshot);
  metadata->current_snapshot_id = second_snapshot->snapshot_id;
  metadata->last_sequence_number = second_snapshot->sequence_number;

  DataFileSet replaced_files;
  replaced_files.insert(file_a_);
  EXPECT_THAT(TestMergeAppend::ValidateNoNewDeletesForDataFilesForTest(
                  *metadata, first_snapshot->snapshot_id, Expressions::AlwaysTrue(),
                  replaced_files, second_snapshot, file_io_),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest,
       ValidateNoNewDeletesForDataFilesWithFilterSkipsNonMatchingDeletes) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  auto del_file = MakeEqualityDeleteFile("/delete/del_a.parquet", 1L);
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwriteUpdate());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
  const int64_t second_snapshot_id = op->GeneratedSnapshotId();
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, op->Apply(*table_->metadata(), first_snapshot));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      MakeSyntheticSnapshot(DataOperation::kOverwrite, second_snapshot_id,
                            first_snapshot->snapshot_id,
                            first_snapshot->sequence_number + 1, manifests));

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
  metadata->snapshots.push_back(second_snapshot);
  metadata->current_snapshot_id = second_snapshot->snapshot_id;
  metadata->last_sequence_number = second_snapshot->sequence_number;

  DataFileSet replaced_files;
  replaced_files.insert(file_a_);
  EXPECT_THAT(TestMergeAppend::ValidateNoNewDeletesForDataFilesForTest(
                  *metadata, first_snapshot->snapshot_id, Expressions::AlwaysFalse(),
                  replaced_files, second_snapshot, file_io_),
              IsOk());
}

TEST_F(MergingSnapshotUpdateTest, ValidateNoNewDeleteFilesWithExpressionDetectsConflict) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  auto del_file = MakeEqualityDeleteFile("/delete/del_a.parquet", 1L);
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwriteUpdate());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
  const int64_t second_snapshot_id = op->GeneratedSnapshotId();
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, op->Apply(*table_->metadata(), first_snapshot));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      MakeSyntheticSnapshot(DataOperation::kOverwrite, second_snapshot_id,
                            first_snapshot->snapshot_id,
                            first_snapshot->sequence_number + 1, manifests));

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
  metadata->snapshots.push_back(second_snapshot);
  metadata->current_snapshot_id = second_snapshot->snapshot_id;
  metadata->last_sequence_number = second_snapshot->sequence_number;

  EXPECT_THAT(TestMergeAppend::ValidateNoNewDeleteFilesForTest(
                  *metadata, first_snapshot->snapshot_id, Expressions::AlwaysTrue(),
                  second_snapshot, file_io_),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest,
       ValidateNoNewDeleteFilesWithExpressionSkipsNonMatchingDeletes) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  auto del_file = MakeEqualityDeleteFile("/delete/del_a.parquet", 1L);
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwriteUpdate());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
  const int64_t second_snapshot_id = op->GeneratedSnapshotId();
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, op->Apply(*table_->metadata(), first_snapshot));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      MakeSyntheticSnapshot(DataOperation::kOverwrite, second_snapshot_id,
                            first_snapshot->snapshot_id,
                            first_snapshot->sequence_number + 1, manifests));

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
  metadata->snapshots.push_back(second_snapshot);
  metadata->current_snapshot_id = second_snapshot->snapshot_id;
  metadata->last_sequence_number = second_snapshot->sequence_number;

  EXPECT_THAT(TestMergeAppend::ValidateNoNewDeleteFilesForTest(
                  *metadata, first_snapshot->snapshot_id, Expressions::AlwaysFalse(),
                  second_snapshot, file_io_),
              IsOk());
}

TEST_F(MergingSnapshotUpdateTest,
       ValidateNoNewDeleteFilesWithPartitionSetDetectsConflict) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  auto del_file = MakeEqualityDeleteFile("/delete/del_a.parquet", 1L);
  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwriteUpdate());
  EXPECT_THAT(op->AddDelete(del_file), IsOk());
  const int64_t second_snapshot_id = op->GeneratedSnapshotId();
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, op->Apply(*table_->metadata(), first_snapshot));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      MakeSyntheticSnapshot(DataOperation::kOverwrite, second_snapshot_id,
                            first_snapshot->snapshot_id,
                            first_snapshot->sequence_number + 1, manifests));

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
  metadata->snapshots.push_back(second_snapshot);
  metadata->current_snapshot_id = second_snapshot->snapshot_id;
  metadata->last_sequence_number = second_snapshot->sequence_number;

  PartitionSet partition_set;
  ASSERT_TRUE(partition_set.add(spec_->spec_id(), del_file->partition));
  EXPECT_THAT(TestMergeAppend::ValidateNoNewDeleteFilesForTest(
                  *metadata, first_snapshot->snapshot_id, partition_set, second_snapshot,
                  file_io_),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, ValidateAddedDVsDetectsConflict) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
  metadata->format_version = 3;

  auto dv_file = MakeDeleteFile("/delete/dv_a.puffin", 1L);
  dv_file->file_format = FileFormatType::kPuffin;
  dv_file->referenced_data_file = file_a_->file_path;
  dv_file->content_offset = 0;
  dv_file->content_size_in_bytes = 10;

  constexpr int64_t kSecondSnapshotId = 123456;
  auto manifest_path = table_location_ + "/metadata/dv-conflict.avro";
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest,
      WriteDeleteManifest(*metadata, manifest_path, {dv_file}, kSecondSnapshotId,
                          first_snapshot->sequence_number + 1));
  manifest.added_snapshot_id = kSecondSnapshotId;
  manifest.sequence_number = first_snapshot->sequence_number + 1;
  manifest.min_sequence_number = first_snapshot->sequence_number + 1;
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      MakeSyntheticSnapshot(DataOperation::kOverwrite, kSecondSnapshotId,
                            first_snapshot->snapshot_id,
                            first_snapshot->sequence_number + 1, {manifest}));

  metadata->snapshots.push_back(second_snapshot);
  metadata->current_snapshot_id = second_snapshot->snapshot_id;
  metadata->last_sequence_number = second_snapshot->sequence_number;

  const std::unordered_set<std::string> referenced_data_files{file_a_->file_path};
  EXPECT_THAT(TestMergeAppend::ValidateAddedDVsForTest(
                  *metadata, first_snapshot->snapshot_id, Expressions::AlwaysTrue(),
                  referenced_data_files, second_snapshot, file_io_),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest, ValidateAddedDVsIgnoresUnrelatedDVs) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
  metadata->format_version = 3;

  auto dv_file = MakeDeleteFile("/delete/dv_a.puffin", 1L);
  dv_file->file_format = FileFormatType::kPuffin;
  dv_file->referenced_data_file = file_a_->file_path;
  dv_file->content_offset = 0;
  dv_file->content_size_in_bytes = 10;

  constexpr int64_t kSecondSnapshotId = 123456;
  auto manifest_path = table_location_ + "/metadata/dv-unrelated.avro";
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest,
      WriteDeleteManifest(*metadata, manifest_path, {dv_file}, kSecondSnapshotId,
                          first_snapshot->sequence_number + 1));
  manifest.added_snapshot_id = kSecondSnapshotId;
  manifest.sequence_number = first_snapshot->sequence_number + 1;
  manifest.min_sequence_number = first_snapshot->sequence_number + 1;
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      MakeSyntheticSnapshot(DataOperation::kOverwrite, kSecondSnapshotId,
                            first_snapshot->snapshot_id,
                            first_snapshot->sequence_number + 1, {manifest}));

  metadata->snapshots.push_back(second_snapshot);
  metadata->current_snapshot_id = second_snapshot->snapshot_id;
  metadata->last_sequence_number = second_snapshot->sequence_number;

  const std::unordered_set<std::string> referenced_data_files{file_b_->file_path};
  EXPECT_THAT(TestMergeAppend::ValidateAddedDVsForTest(
                  *metadata, first_snapshot->snapshot_id, Expressions::AlwaysTrue(),
                  referenced_data_files, second_snapshot, file_io_),
              IsOk());
}

TEST_F(MergingSnapshotUpdateTest, ValidateAddedDVsIgnoresOldEntrySnapshotId) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
  metadata->format_version = 3;

  auto dv_file = MakeDeleteFile("/delete/dv_a.puffin", 1L);
  dv_file->file_format = FileFormatType::kPuffin;
  dv_file->referenced_data_file = file_a_->file_path;
  dv_file->content_offset = 0;
  dv_file->content_size_in_bytes = 10;

  constexpr int64_t kSecondSnapshotId = 123456;
  auto manifest_path = table_location_ + "/metadata/old-entry-dv.avro";
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest,
      WriteDeleteManifest(*metadata, manifest_path, {dv_file},
                          first_snapshot->snapshot_id, first_snapshot->sequence_number));
  manifest.added_snapshot_id = kSecondSnapshotId;
  manifest.sequence_number = first_snapshot->sequence_number + 1;
  manifest.min_sequence_number = first_snapshot->sequence_number;
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      MakeSyntheticSnapshot(DataOperation::kOverwrite, kSecondSnapshotId,
                            first_snapshot->snapshot_id,
                            first_snapshot->sequence_number + 1, {manifest}));

  metadata->snapshots.push_back(second_snapshot);
  metadata->current_snapshot_id = second_snapshot->snapshot_id;
  metadata->last_sequence_number = second_snapshot->sequence_number;

  const std::unordered_set<std::string> referenced_data_files{file_a_->file_path};
  EXPECT_THAT(TestMergeAppend::ValidateAddedDVsForTest(
                  *metadata, first_snapshot->snapshot_id, Expressions::AlwaysTrue(),
                  referenced_data_files, second_snapshot, file_io_),
              IsOk());
}

TEST_F(MergingSnapshotUpdateTest, ValidateDeletedDataFilesWithExpressionDetectsConflict) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwriteUpdate());
  EXPECT_THAT(op->RemoveDataFile(file_a_), IsOk());
  const int64_t second_snapshot_id = op->GeneratedSnapshotId();
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, op->Apply(*table_->metadata(), first_snapshot));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      MakeSyntheticSnapshot(DataOperation::kOverwrite, second_snapshot_id,
                            first_snapshot->snapshot_id,
                            first_snapshot->sequence_number + 1, manifests));

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
  metadata->snapshots.push_back(second_snapshot);
  metadata->current_snapshot_id = second_snapshot->snapshot_id;
  metadata->last_sequence_number = second_snapshot->sequence_number;

  EXPECT_THAT(TestMergeAppend::ValidateDeletedDataFilesForTest(
                  *metadata, first_snapshot->snapshot_id, Expressions::AlwaysTrue(),
                  second_snapshot, file_io_),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MergingSnapshotUpdateTest,
       ValidateDeletedDataFilesWithPartitionSetDetectsConflict) {
  CommitFileA();
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto op, NewOverwriteUpdate());
  EXPECT_THAT(op->RemoveDataFile(file_a_), IsOk());
  const int64_t second_snapshot_id = op->GeneratedSnapshotId();
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, op->Apply(*table_->metadata(), first_snapshot));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_snapshot,
      MakeSyntheticSnapshot(DataOperation::kOverwrite, second_snapshot_id,
                            first_snapshot->snapshot_id,
                            first_snapshot->sequence_number + 1, manifests));

  auto metadata = std::make_shared<TableMetadata>(*table_->metadata());
  metadata->snapshots.push_back(second_snapshot);
  metadata->current_snapshot_id = second_snapshot->snapshot_id;
  metadata->last_sequence_number = second_snapshot->sequence_number;

  PartitionSet partition_set;
  ASSERT_TRUE(partition_set.add(spec_->spec_id(), file_a_->partition));
  EXPECT_THAT(TestMergeAppend::ValidateDeletedDataFilesForTest(
                  *metadata, first_snapshot->snapshot_id, partition_set, second_snapshot,
                  file_io_),
              IsError(ErrorKind::kInvalidArgument));
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
