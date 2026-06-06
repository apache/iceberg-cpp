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

#include "iceberg/update/merge_append.h"

#include <algorithm>
#include <format>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <tuple>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/constants.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/test_resource.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/update_partition_spec.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

namespace {

// RollingManifestWriter checks whether to roll every 250 rows.
constexpr size_t kManifestFileGroupSizeForTest = 250;

}  // namespace

class MergeAppendTestBase : public UpdateTestBase {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  std::string TableName() const override { return "minimal_table"; }

  void SetUp() override {
    table_ident_ = TableIdentifier{.name = TableName()};
    table_location_ = "/warehouse/" + TableName();

    InitializeFileIO();
    RegisterMinimalTable(format_version());

    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());

    file_a_ = MakeDataFile("/data/file_a.parquet", /*partition_x=*/1L);
    file_b_ = MakeDataFile("/data/file_b.parquet", /*partition_x=*/2L);
    file_c_ = MakeDataFile("/data/file_c.parquet", /*partition_x=*/3L);
    file_d_ = MakeDataFile("/data/file_d.parquet", /*partition_x=*/4L);
  }

  void RegisterMinimalTable(int8_t format_version) {
    auto metadata_location = std::format("{}/metadata/00001-{}.metadata.json",
                                         table_location_, Uuid::GenerateV7().ToString());
    ICEBERG_UNWRAP_OR_FAIL(
        auto metadata, ReadTableMetadataFromResource("TableMetadataV2ValidMinimal.json"));
    metadata->format_version = format_version;
    metadata->location = table_location_;
    metadata->next_row_id = TableMetadata::kInitialRowId;

    ASSERT_THAT(TableMetadataUtil::Write(*file_io_, metadata_location, *metadata),
                IsOk());
    ICEBERG_UNWRAP_OR_FAIL(table_,
                           catalog_->RegisterTable(table_ident_, metadata_location));
  }

  virtual int8_t format_version() const {
    return TableMetadata::kDefaultTableFormatVersion;
  }

  virtual std::string branch() const { return std::string(SnapshotRef::kMainBranch); }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& path, int64_t partition_x) {
    return MakeDataFile(path, spec_, {Literal::Long(partition_x)});
  }

  std::vector<std::shared_ptr<DataFile>> MakeDataFiles(std::string_view prefix,
                                                       size_t count) {
    std::vector<std::shared_ptr<DataFile>> files;
    files.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      files.push_back(MakeDataFile(std::format("{}/file_{}.parquet", prefix, i),
                                   static_cast<int64_t>(i % 2)));
    }
    return files;
  }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& path,
                                         std::shared_ptr<PartitionSpec> spec,
                                         std::vector<Literal> partition_values) {
    auto file = std::make_shared<DataFile>();
    file->content = DataFile::Content::kData;
    file->file_path = table_location_ + path;
    file->file_format = FileFormatType::kParquet;
    file->partition = PartitionValues(std::move(partition_values));
    file->file_size_in_bytes = 1024;
    file->record_count = 100;
    file->partition_spec_id = spec->spec_id();
    return file;
  }

  Result<ManifestFile> WriteManifest(
      const std::string& path, const std::vector<std::shared_ptr<DataFile>>& files) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, ManifestWriter::MakeWriter(
                         format_version(), std::nullopt, path, file_io_, spec_, schema_,
                         ManifestContent::kData, /*first_row_id=*/std::nullopt));
    for (const auto& file : files) {
      ManifestEntry entry;
      entry.status = ManifestStatus::kAdded;
      entry.snapshot_id = std::nullopt;
      entry.data_file = file;
      ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(entry));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  Result<ManifestFile> WriteManifestEntries(const std::string& path,
                                            const std::vector<ManifestEntry>& entries) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, ManifestWriter::MakeWriter(
                         format_version(), std::nullopt, path, file_io_, spec_, schema_,
                         ManifestContent::kData, /*first_row_id=*/std::nullopt));
    for (const auto& entry : entries) {
      switch (entry.status) {
        case ManifestStatus::kAdded:
          ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(entry));
          break;
        case ManifestStatus::kExisting:
          ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
          break;
        case ManifestStatus::kDeleted:
          ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(entry));
          break;
      }
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  ManifestFile MakeEmptyAppendManifest(std::string path) {
    ManifestFile manifest;
    manifest.manifest_path = std::move(path);
    manifest.content = ManifestContent::kData;
    manifest.added_snapshot_id = kInvalidSnapshotId;
    manifest.sequence_number = kInvalidSequenceNumber;
    manifest.added_files_count = 0;
    manifest.existing_files_count = 0;
    manifest.deleted_files_count = 0;
    return manifest;
  }

  void SetManifestMinMergeCount(int count) {
    ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
    props->Set(std::string(TableProperties::kManifestMinMergeCount.key()),
               std::to_string(count));
    EXPECT_THAT(props->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  void SetManifestTargetSizeBytes(int64_t size_bytes) {
    ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
    props->Set(std::string(TableProperties::kManifestTargetSizeBytes.key()),
               std::to_string(size_bytes));
    EXPECT_THAT(props->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  void SetSnapshotIdInheritanceEnabled() {
    ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
    props->Set(std::string(TableProperties::kSnapshotIdInheritanceEnabled.key()), "true");
    EXPECT_THAT(props->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

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

  // Create a MergeAppend already targeting branch(), so each test runs against
  // both the main branch and a named branch without per-call boilerplate.
  Result<std::shared_ptr<MergeAppend>> NewBranchMergeAppend() {
    ICEBERG_ASSIGN_OR_RAISE(auto append, table_->NewMergeAppend());
    append->SetTargetBranch(branch());
    return append;
  }

  Result<std::shared_ptr<FastAppend>> NewBranchFastAppend() {
    ICEBERG_ASSIGN_OR_RAISE(auto append, table_->NewFastAppend());
    append->SetTargetBranch(branch());
    return append;
  }

  // Returns the snapshot referenced by branch(), so tests work uniformly whether
  // they commit to the main branch or to a named branch.
  Result<std::shared_ptr<Snapshot>> CurrentSnapshot() {
    const auto& refs = table_->metadata()->refs;
    auto it = refs.find(branch());
    if (it == refs.end()) {
      return NotFound("No snapshot ref for branch '{}'", branch());
    }
    return table_->SnapshotById(it->second->snapshot_id);
  }

  Result<std::vector<ManifestFile>> CurrentDataManifests() {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, CurrentSnapshot());
    return DataManifests(snapshot);
  }

  Result<std::shared_ptr<Snapshot>> SnapshotForBranch(const TableMetadata& metadata) {
    auto it = metadata.refs.find(branch());
    if (it == metadata.refs.end()) {
      return NotFound("No snapshot ref for branch '{}'", branch());
    }
    return metadata.SnapshotById(it->second->snapshot_id);
  }

  Result<std::vector<ManifestFile>> DataManifests(
      const std::shared_ptr<Snapshot>& snapshot) {
    SnapshotCache snapshot_cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot_cache.DataManifests(file_io_));
    return std::vector<ManifestFile>(manifests.begin(), manifests.end());
  }

  bool FileExists(const std::string& path) {
    auto input_file = file_io_->NewInputFile(path);
    if (!input_file.has_value()) {
      return false;
    }
    return input_file.value()->Size().has_value();
  }

  Result<std::vector<ManifestEntry>> ReadEntries(const ManifestFile& manifest) {
    return ReadAllEntries(std::span<const ManifestFile>(&manifest, 1));
  }

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

  std::vector<std::string> ManifestPaths(const std::vector<ManifestFile>& manifests) {
    std::vector<std::string> paths;
    paths.reserve(manifests.size());
    for (const auto& manifest : manifests) {
      paths.push_back(manifest.manifest_path);
    }
    return paths;
  }

  const ManifestFile* FindManifestByPath(const std::vector<ManifestFile>& manifests,
                                         const std::string& path) {
    auto it = std::ranges::find_if(manifests, [&path](const ManifestFile& manifest) {
      return manifest.manifest_path == path;
    });
    return it == manifests.end() ? nullptr : &*it;
  }

  const ManifestFile* FindManifestForSpec(const std::vector<ManifestFile>& manifests,
                                          int32_t spec_id) {
    auto it = std::ranges::find_if(manifests, [spec_id](const ManifestFile& manifest) {
      return manifest.partition_spec_id == spec_id;
    });
    return it == manifests.end() ? nullptr : &*it;
  }

  void ExpectManifestEntries(const ManifestFile& manifest,
                             const std::vector<std::shared_ptr<DataFile>>& expected_files,
                             const std::vector<ManifestStatus>& expected_statuses) {
    ASSERT_EQ(expected_files.size(), expected_statuses.size());

    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadEntries(manifest));
    ASSERT_EQ(entries.size(), expected_files.size());
    for (size_t i = 0; i < entries.size(); ++i) {
      EXPECT_EQ(entries[i].status, expected_statuses[i]);
      ASSERT_NE(entries[i].data_file, nullptr);
      EXPECT_EQ(entries[i].data_file->file_path, expected_files[i]->file_path);
      EXPECT_EQ(entries[i].data_file->partition_spec_id,
                expected_files[i]->partition_spec_id);
    }
  }

  void ExpectAllEntriesAcrossManifests(
      std::span<const ManifestFile> manifests,
      const std::vector<std::shared_ptr<DataFile>>& expected_files,
      ManifestStatus expected_status) {
    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(manifests));
    ASSERT_EQ(entries.size(), expected_files.size());

    std::vector<std::string> expected_paths;
    expected_paths.reserve(expected_files.size());
    for (const auto& file : expected_files) {
      expected_paths.push_back(file->file_path);
    }

    std::vector<std::string> actual_paths;
    actual_paths.reserve(entries.size());
    for (const auto& entry : entries) {
      EXPECT_EQ(entry.status, expected_status);
      ASSERT_NE(entry.data_file, nullptr);
      actual_paths.push_back(entry.data_file->file_path);
    }
    EXPECT_THAT(actual_paths, ::testing::UnorderedElementsAreArray(expected_paths));
  }

  void ExpectAllEntryPathsAcrossManifests(
      std::span<const ManifestFile> manifests,
      const std::vector<std::shared_ptr<DataFile>>& expected_files) {
    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(manifests));
    ASSERT_EQ(entries.size(), expected_files.size());

    std::vector<std::string> expected_paths;
    expected_paths.reserve(expected_files.size());
    for (const auto& file : expected_files) {
      expected_paths.push_back(file->file_path);
    }

    std::vector<std::string> actual_paths;
    actual_paths.reserve(entries.size());
    for (const auto& entry : entries) {
      ASSERT_NE(entry.data_file, nullptr);
      actual_paths.push_back(entry.data_file->file_path);
    }
    EXPECT_THAT(actual_paths, ::testing::UnorderedElementsAreArray(expected_paths));
  }

  void ExpectManifestEntriesMatchFiles(
      const ManifestFile& manifest,
      std::span<const std::shared_ptr<DataFile>> expected_files) {
    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadEntries(manifest));
    ASSERT_EQ(entries.size(), expected_files.size());
    for (size_t i = 0; i < entries.size(); ++i) {
      EXPECT_EQ(entries[i].status, ManifestStatus::kAdded);
      ASSERT_NE(entries[i].data_file, nullptr);
      EXPECT_EQ(entries[i].data_file->file_path, expected_files[i]->file_path);
    }
  }

  void ExpectManifestEntries(const ManifestFile& manifest,
                             const std::vector<std::shared_ptr<DataFile>>& expected_files,
                             const std::vector<ManifestStatus>& expected_statuses,
                             const std::vector<int64_t>& expected_snapshot_ids,
                             const std::vector<int64_t>& expected_sequence_numbers,
                             const std::vector<int64_t>& expected_file_sequence_numbers) {
    ASSERT_EQ(expected_files.size(), expected_statuses.size());
    ASSERT_EQ(expected_files.size(), expected_snapshot_ids.size());
    ASSERT_EQ(expected_files.size(), expected_sequence_numbers.size());
    ASSERT_EQ(expected_files.size(), expected_file_sequence_numbers.size());

    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadEntries(manifest));
    ASSERT_EQ(entries.size(), expected_files.size());
    for (size_t i = 0; i < entries.size(); ++i) {
      EXPECT_EQ(entries[i].status, expected_statuses[i]);
      EXPECT_EQ(entries[i].snapshot_id, expected_snapshot_ids[i]);
      EXPECT_EQ(entries[i].sequence_number, expected_sequence_numbers[i]);
      EXPECT_EQ(entries[i].file_sequence_number, expected_file_sequence_numbers[i]);
      ASSERT_NE(entries[i].data_file, nullptr);
      EXPECT_EQ(entries[i].data_file->file_path, expected_files[i]->file_path);
      EXPECT_EQ(entries[i].data_file->partition_spec_id,
                expected_files[i]->partition_spec_id);
    }
  }

  void ExpectAppendManifestError(ManifestFile manifest,
                                 const std::string& expected_message) {
    ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
    append->AppendManifest(manifest);

    auto result = append->Commit();
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(result, HasErrorMessage(expected_message));
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
  std::shared_ptr<DataFile> file_c_;
  std::shared_ptr<DataFile> file_d_;
  std::vector<std::shared_ptr<::testing::NiceMock<MockCatalog>>> mock_catalogs_;
};

class MergeAppendTest
    : public MergeAppendTestBase,
      public ::testing::WithParamInterface<std::tuple<int8_t, std::string>> {
 protected:
  int8_t format_version() const override { return std::get<0>(GetParam()); }
  std::string branch() const override { return std::get<1>(GetParam()); }
};

TEST_F(MergeAppendTestBase, AppendNullFile) {
  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  append->AppendFile(nullptr);

  auto result = append->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot add a null data file"));
}

TEST_P(MergeAppendTest, AddManyFiles) {
  EXPECT_THAT(CurrentSnapshot(), IsError(ErrorKind::kNotFound));

  constexpr size_t kManifestCount = 2;
  constexpr size_t kFileCount = kManifestFileGroupSizeForTest * kManifestCount;
  SetManifestTargetSizeBytes(10);
  auto files = MakeDataFiles("/data/many", kFileCount);

  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  for (const auto& file : files) {
    append->AppendFile(file);
  }
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles),
            std::to_string(kFileCount));

  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  EXPECT_EQ(data_manifests.size(), kManifestCount);
  ExpectAllEntriesAcrossManifests(data_manifests, files, ManifestStatus::kAdded);
}

TEST_P(MergeAppendTest, AddManyFilesWithConsistentOrdering) {
  constexpr size_t kManifestCount = 3;
  constexpr size_t kFileCount = kManifestFileGroupSizeForTest * kManifestCount;
  SetManifestTargetSizeBytes(10);

  auto files = MakeDataFiles("/data/ordered", kFileCount);
  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  for (const auto& file : files) {
    append->AppendFile(file);
  }
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  ASSERT_EQ(data_manifests.size(), kManifestCount);
  for (size_t i = 0; i < kManifestCount; ++i) {
    auto first =
        files.begin() + static_cast<std::ptrdiff_t>(i * kManifestFileGroupSizeForTest);
    auto last = first + static_cast<std::ptrdiff_t>(kManifestFileGroupSizeForTest);
    std::vector<std::shared_ptr<DataFile>> expected(first, last);
    ExpectManifestEntriesMatchFiles(data_manifests[i], expected);
  }
}

TEST_P(MergeAppendTest, EmptyTableAppend) {
  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  append->AppendFile(file_a_).AppendFile(file_b_);

  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedRecords), "200");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedFileSize), "2048");
  EXPECT_EQ(table_->metadata()->last_sequence_number, snapshot->sequence_number);

  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  ASSERT_EQ(data_manifests.size(), 1U);
  EXPECT_EQ(data_manifests[0].added_snapshot_id, snapshot->snapshot_id);
  EXPECT_EQ(data_manifests[0].sequence_number, snapshot->sequence_number);
  ExpectManifestEntries(data_manifests[0], {file_a_, file_b_},
                        {ManifestStatus::kAdded, ManifestStatus::kAdded},
                        {snapshot->snapshot_id, snapshot->snapshot_id},
                        {snapshot->sequence_number, snapshot->sequence_number},
                        {snapshot->sequence_number, snapshot->sequence_number});
}

TEST_P(MergeAppendTest, EmptyTableAppendFilesWithDifferentSpecs) {
  ICEBERG_UNWRAP_OR_FAIL(auto update_spec, table_->NewUpdatePartitionSpec());
  update_spec->AddField("y");
  EXPECT_THAT(update_spec->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto new_spec, table_->spec());

  auto file_new_spec = MakeDataFile("/data/file_new_spec.parquet", new_spec,
                                    {Literal::Long(5L), Literal::Long(6L)});

  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  append->AppendFile(file_a_).AppendFile(file_new_spec);
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, snapshot_cache.DataManifests(file_io_));
  std::vector<ManifestFile> manifest_files(data_manifests.begin(), data_manifests.end());
  ASSERT_EQ(manifest_files.size(), 2U);

  auto* old_spec_manifest = FindManifestForSpec(manifest_files, spec_->spec_id());
  ASSERT_NE(old_spec_manifest, nullptr);
  ExpectManifestEntries(*old_spec_manifest, {file_a_}, {ManifestStatus::kAdded});

  auto* new_spec_manifest = FindManifestForSpec(manifest_files, new_spec->spec_id());
  ASSERT_NE(new_spec_manifest, nullptr);
  ExpectManifestEntries(*new_spec_manifest, {file_new_spec}, {ManifestStatus::kAdded});
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "2");
}

TEST_P(MergeAppendTest, EmptyTableAppendManifest) {
  auto path = table_location_ + "/metadata/input.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_, file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  append->AppendManifest(manifest);
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  ASSERT_EQ(data_manifests.size(), 1U);
  if (format_version() == 1) {
    EXPECT_NE(data_manifests[0].manifest_path, path);
  } else {
    EXPECT_EQ(data_manifests[0].manifest_path, path);
  }
  ExpectManifestEntries(data_manifests[0], {file_a_, file_b_},
                        {ManifestStatus::kAdded, ManifestStatus::kAdded},
                        {snapshot->snapshot_id, snapshot->snapshot_id},
                        {snapshot->sequence_number, snapshot->sequence_number},
                        {snapshot->sequence_number, snapshot->sequence_number});
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "0");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "0");
}

TEST_P(MergeAppendTest, EmptyTableAppendFilesAndManifest) {
  auto path = table_location_ + "/metadata/input.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_, file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  append->AppendFile(file_c_).AppendFile(file_d_).AppendManifest(manifest);
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  EXPECT_EQ(data_manifests.size(), 2U);

  if (format_version() == 1) {
    EXPECT_EQ(FindManifestByPath(data_manifests, path), nullptr);
    ExpectAllEntriesAcrossManifests(data_manifests, {file_a_, file_b_, file_c_, file_d_},
                                    ManifestStatus::kAdded);
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "4");
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "4");
    return;
  }

  auto* inherited_manifest = FindManifestByPath(data_manifests, path);
  ASSERT_NE(inherited_manifest, nullptr);
  ExpectManifestEntries(*inherited_manifest, {file_a_, file_b_},
                        {ManifestStatus::kAdded, ManifestStatus::kAdded});

  auto appended_manifest = std::ranges::find_if(
      data_manifests,
      [&path](const ManifestFile& manifest) { return manifest.manifest_path != path; });
  ASSERT_NE(appended_manifest, data_manifests.end());
  ExpectManifestEntries(*appended_manifest, {file_c_, file_d_},
                        {ManifestStatus::kAdded, ManifestStatus::kAdded});
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "4");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "4");
}

TEST_P(MergeAppendTest, MergeWithAppendFilesAndManifest) {
  SetManifestMinMergeCount(1);

  auto path = table_location_ + "/metadata/input.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_, file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  append->AppendFile(file_c_).AppendFile(file_d_).AppendManifest(manifest);
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, snapshot_cache.DataManifests(file_io_));
  ASSERT_EQ(data_manifests.size(), 1U);
  EXPECT_NE(data_manifests[0].manifest_path, path);

  ExpectManifestEntries(data_manifests[0], {file_c_, file_d_, file_a_, file_b_},
                        {ManifestStatus::kAdded, ManifestStatus::kAdded,
                         ManifestStatus::kAdded, ManifestStatus::kAdded},
                        {snapshot->snapshot_id, snapshot->snapshot_id,
                         snapshot->snapshot_id, snapshot->snapshot_id},
                        {snapshot->sequence_number, snapshot->sequence_number,
                         snapshot->sequence_number, snapshot->sequence_number},
                        {snapshot->sequence_number, snapshot->sequence_number,
                         snapshot->sequence_number, snapshot->sequence_number});
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "4");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
}

TEST_P(MergeAppendTest, MergeWithExistingManifest) {
  SetManifestMinMergeCount(1);

  ICEBERG_UNWRAP_OR_FAIL(auto first_append, NewBranchMergeAppend());
  first_append->AppendFile(file_a_).AppendFile(file_b_);
  EXPECT_THAT(first_append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, CurrentSnapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto second_append, NewBranchMergeAppend());
  second_append->AppendFile(file_c_).AppendFile(file_d_);
  EXPECT_THAT(second_append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  ASSERT_EQ(data_manifests.size(), 1U);
  ExpectManifestEntries(
      data_manifests[0], {file_c_, file_d_, file_a_, file_b_},
      {ManifestStatus::kAdded, ManifestStatus::kAdded, ManifestStatus::kExisting,
       ManifestStatus::kExisting},
      {second_snapshot->snapshot_id, second_snapshot->snapshot_id,
       first_snapshot->snapshot_id, first_snapshot->snapshot_id},
      {second_snapshot->sequence_number, second_snapshot->sequence_number,
       first_snapshot->sequence_number, first_snapshot->sequence_number},
      {second_snapshot->sequence_number, second_snapshot->sequence_number,
       first_snapshot->sequence_number, first_snapshot->sequence_number});
}

TEST_P(MergeAppendTest, ManifestMergeMinCount) {
  if (format_version() >= 3) {
    GTEST_SKIP() << "skips format version 3+";
  }

  auto path_a1 = table_location_ + "/metadata/input-a1.avro";
  auto path_c1 = table_location_ + "/metadata/input-c1.avro";
  auto path_d1 = table_location_ + "/metadata/input-d1.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_a1, WriteManifest(path_a1, {file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_c1, WriteManifest(path_c1, {file_c_}));
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_d1, WriteManifest(path_d1, {file_d_}));

  SetManifestMinMergeCount(2);
  SetManifestTargetSizeBytes(manifest_c1.manifest_length + manifest_d1.manifest_length +
                             100);

  ICEBERG_UNWRAP_OR_FAIL(auto append1, NewBranchMergeAppend());
  append1->AppendManifest(manifest_a1)
      .AppendManifest(manifest_c1)
      .AppendManifest(manifest_d1);
  EXPECT_THAT(append1->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto first_manifests, CurrentDataManifests());
  ASSERT_EQ(first_manifests.size(), 2U);
  ExpectManifestEntries(first_manifests[0], {file_a_}, {ManifestStatus::kAdded},
                        {first_snapshot->snapshot_id}, {first_snapshot->sequence_number},
                        {first_snapshot->sequence_number});
  ExpectManifestEntries(
      first_manifests[1], {file_c_, file_d_},
      {ManifestStatus::kAdded, ManifestStatus::kAdded},
      {first_snapshot->snapshot_id, first_snapshot->snapshot_id},
      {first_snapshot->sequence_number, first_snapshot->sequence_number},
      {first_snapshot->sequence_number, first_snapshot->sequence_number});
  EXPECT_EQ(first_snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "2");
  EXPECT_EQ(first_snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "0");
  EXPECT_EQ(first_snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "0");

  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_a2,
      WriteManifest(table_location_ + "/metadata/input-a2.avro", {file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_c2,
      WriteManifest(table_location_ + "/metadata/input-c2.avro", {file_c_}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_d2,
      WriteManifest(table_location_ + "/metadata/input-d2.avro", {file_d_}));

  ICEBERG_UNWRAP_OR_FAIL(auto append2, NewBranchMergeAppend());
  append2->AppendManifest(manifest_a2)
      .AppendManifest(manifest_c2)
      .AppendManifest(manifest_d2);
  EXPECT_THAT(append2->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto second_manifests, CurrentDataManifests());
  ASSERT_EQ(second_manifests.size(), 3U);
  ExpectManifestEntries(second_manifests[0], {file_a_}, {ManifestStatus::kAdded},
                        {second_snapshot->snapshot_id},
                        {second_snapshot->sequence_number},
                        {second_snapshot->sequence_number});
  ExpectManifestEntries(
      second_manifests[1], {file_c_, file_d_},
      {ManifestStatus::kAdded, ManifestStatus::kAdded},
      {second_snapshot->snapshot_id, second_snapshot->snapshot_id},
      {second_snapshot->sequence_number, second_snapshot->sequence_number},
      {second_snapshot->sequence_number, second_snapshot->sequence_number});
  ExpectManifestEntries(
      second_manifests[2], {file_a_, file_c_, file_d_},
      {ManifestStatus::kExisting, ManifestStatus::kExisting, ManifestStatus::kExisting},
      {first_snapshot->snapshot_id, first_snapshot->snapshot_id,
       first_snapshot->snapshot_id},
      {first_snapshot->sequence_number, first_snapshot->sequence_number,
       first_snapshot->sequence_number},
      {first_snapshot->sequence_number, first_snapshot->sequence_number,
       first_snapshot->sequence_number});
  EXPECT_EQ(second_snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "3");
  EXPECT_EQ(second_snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "3");
  EXPECT_EQ(second_snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "2");
  EXPECT_EQ(second_snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "0");
}

TEST_P(MergeAppendTest, ManifestsMergeIntoOne) {
  ICEBERG_UNWRAP_OR_FAIL(auto first_append, NewBranchMergeAppend());
  first_append->AppendFile(file_a_);
  EXPECT_THAT(first_append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto first_manifests, CurrentDataManifests());
  ASSERT_EQ(first_manifests.size(), 1U);
  ExpectManifestEntries(first_manifests[0], {file_a_}, {ManifestStatus::kAdded},
                        {first_snapshot->snapshot_id}, {first_snapshot->sequence_number},
                        {first_snapshot->sequence_number});

  ICEBERG_UNWRAP_OR_FAIL(auto second_append, NewBranchMergeAppend());
  second_append->AppendFile(file_b_);
  EXPECT_THAT(second_append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto second_manifests, CurrentDataManifests());
  ASSERT_EQ(second_manifests.size(), 2U);
  ExpectManifestEntries(second_manifests[0], {file_b_}, {ManifestStatus::kAdded},
                        {second_snapshot->snapshot_id},
                        {second_snapshot->sequence_number},
                        {second_snapshot->sequence_number});
  ExpectManifestEntries(second_manifests[1], {file_a_}, {ManifestStatus::kAdded},
                        {first_snapshot->snapshot_id}, {first_snapshot->sequence_number},
                        {first_snapshot->sequence_number});

  auto path_c = table_location_ + "/metadata/input-m0.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_c, WriteManifest(path_c, {file_c_}));
  ICEBERG_UNWRAP_OR_FAIL(auto third_append, NewBranchMergeAppend());
  third_append->AppendManifest(manifest_c);
  EXPECT_THAT(third_append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto third_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto third_manifests, CurrentDataManifests());
  ASSERT_EQ(third_manifests.size(), 3U);
  ExpectManifestEntries(third_manifests[0], {file_c_}, {ManifestStatus::kAdded},
                        {third_snapshot->snapshot_id}, {third_snapshot->sequence_number},
                        {third_snapshot->sequence_number});
  ExpectManifestEntries(third_manifests[1], {file_b_}, {ManifestStatus::kAdded},
                        {second_snapshot->snapshot_id},
                        {second_snapshot->sequence_number},
                        {second_snapshot->sequence_number});
  ExpectManifestEntries(third_manifests[2], {file_a_}, {ManifestStatus::kAdded},
                        {first_snapshot->snapshot_id}, {first_snapshot->sequence_number},
                        {first_snapshot->sequence_number});

  SetManifestMinMergeCount(1);

  auto path_d = table_location_ + "/metadata/input-m1.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_d, WriteManifest(path_d, {file_d_}));
  ICEBERG_UNWRAP_OR_FAIL(auto fourth_append, NewBranchMergeAppend());
  fourth_append->AppendManifest(manifest_d);
  EXPECT_THAT(fourth_append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto fourth_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  ASSERT_EQ(data_manifests.size(), 1U);
  ExpectManifestEntries(
      data_manifests[0], {file_d_, file_c_, file_b_, file_a_},
      {ManifestStatus::kAdded, ManifestStatus::kExisting, ManifestStatus::kExisting,
       ManifestStatus::kExisting},
      {fourth_snapshot->snapshot_id, third_snapshot->snapshot_id,
       second_snapshot->snapshot_id, first_snapshot->snapshot_id},
      {fourth_snapshot->sequence_number, third_snapshot->sequence_number,
       second_snapshot->sequence_number, first_snapshot->sequence_number},
      {fourth_snapshot->sequence_number, third_snapshot->sequence_number,
       second_snapshot->sequence_number, first_snapshot->sequence_number});
}

TEST_P(MergeAppendTest, ManifestDoNotMergeMinCount) {
  SetManifestMinMergeCount(4);

  auto path1 = table_location_ + "/metadata/input1.avro";
  auto path2 = table_location_ + "/metadata/input2.avro";
  auto path3 = table_location_ + "/metadata/input3.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest1, WriteManifest(path1, {file_a_, file_b_}));
  ICEBERG_UNWRAP_OR_FAIL(auto manifest2, WriteManifest(path2, {file_c_}));
  ICEBERG_UNWRAP_OR_FAIL(auto manifest3, WriteManifest(path3, {file_d_}));

  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  append->AppendManifest(manifest1).AppendManifest(manifest2).AppendManifest(manifest3);
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  EXPECT_EQ(data_manifests.size(), 3U);

  if (format_version() == 1) {
    EXPECT_EQ(FindManifestByPath(data_manifests, path1), nullptr);
    EXPECT_EQ(FindManifestByPath(data_manifests, path2), nullptr);
    EXPECT_EQ(FindManifestByPath(data_manifests, path3), nullptr);
    ExpectAllEntriesAcrossManifests(data_manifests, {file_a_, file_b_, file_c_, file_d_},
                                    ManifestStatus::kAdded);
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "4");
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "3");
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "0");
    return;
  }

  EXPECT_THAT(ManifestPaths(data_manifests),
              ::testing::UnorderedElementsAre(path1, path2, path3));

  auto* manifest_1 = FindManifestByPath(data_manifests, path1);
  ASSERT_NE(manifest_1, nullptr);
  ExpectManifestEntries(*manifest_1, {file_a_, file_b_},
                        {ManifestStatus::kAdded, ManifestStatus::kAdded});

  auto* manifest_2 = FindManifestByPath(data_manifests, path2);
  ASSERT_NE(manifest_2, nullptr);
  ExpectManifestEntries(*manifest_2, {file_c_}, {ManifestStatus::kAdded});

  auto* manifest_3 = FindManifestByPath(data_manifests, path3);
  ASSERT_NE(manifest_3, nullptr);
  ExpectManifestEntries(*manifest_3, {file_d_}, {ManifestStatus::kAdded});
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "4");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "3");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "0");
}

TEST_P(MergeAppendTest, MinMergeCount) {
  SetManifestMinMergeCount(4);

  ICEBERG_UNWRAP_OR_FAIL(auto fast_append_a, NewBranchFastAppend());
  fast_append_a->AppendFile(file_a_);
  EXPECT_THAT(fast_append_a->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto fast_append_b, NewBranchFastAppend());
  fast_append_b->AppendFile(file_b_);
  EXPECT_THAT(fast_append_b->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto append_c, NewBranchMergeAppend());
  append_c->AppendFile(file_c_);
  EXPECT_THAT(append_c->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_before_merge, CurrentSnapshot());
  SnapshotCache before_cache(snapshot_before_merge.get());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests_before_merge,
                         before_cache.DataManifests(file_io_));
  EXPECT_EQ(manifests_before_merge.size(), 3U);

  ICEBERG_UNWRAP_OR_FAIL(auto append_d, NewBranchMergeAppend());
  append_d->AppendFile(file_d_);
  EXPECT_THAT(append_d->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  EXPECT_EQ(data_manifests.size(), 1U);
  ExpectManifestEntries(data_manifests[0], {file_d_, file_c_, file_b_, file_a_},
                        {ManifestStatus::kAdded, ManifestStatus::kExisting,
                         ManifestStatus::kExisting, ManifestStatus::kExisting});
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "3");
}

TEST_P(MergeAppendTest, MergeSizeTargetWithExistingManifest) {
  SetManifestTargetSizeBytes(10);

  ICEBERG_UNWRAP_OR_FAIL(auto first_append, NewBranchMergeAppend());
  first_append->AppendFile(file_a_).AppendFile(file_b_);
  EXPECT_THAT(first_append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto initial_manifests, CurrentDataManifests());
  ASSERT_EQ(initial_manifests.size(), 1U);
  auto initial_manifest = initial_manifests[0];

  ICEBERG_UNWRAP_OR_FAIL(auto second_append, NewBranchMergeAppend());
  second_append->AppendFile(file_c_).AppendFile(file_d_);
  EXPECT_THAT(second_append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  ASSERT_EQ(data_manifests.size(), 2U);

  auto* kept_manifest =
      FindManifestByPath(data_manifests, initial_manifest.manifest_path);
  ASSERT_NE(kept_manifest, nullptr);
  ExpectManifestEntries(
      *kept_manifest, {file_a_, file_b_},
      {ManifestStatus::kAdded, ManifestStatus::kAdded},
      {first_snapshot->snapshot_id, first_snapshot->snapshot_id},
      {first_snapshot->sequence_number, first_snapshot->sequence_number},
      {first_snapshot->sequence_number, first_snapshot->sequence_number});

  auto new_manifest = std::ranges::find_if(
      data_manifests, [&initial_manifest](const ManifestFile& manifest) {
        return manifest.manifest_path != initial_manifest.manifest_path;
      });
  ASSERT_NE(new_manifest, data_manifests.end());
  ExpectManifestEntries(
      *new_manifest, {file_c_, file_d_}, {ManifestStatus::kAdded, ManifestStatus::kAdded},
      {second_snapshot->snapshot_id, second_snapshot->snapshot_id},
      {second_snapshot->sequence_number, second_snapshot->sequence_number},
      {second_snapshot->sequence_number, second_snapshot->sequence_number});

  EXPECT_EQ(second_snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(second_snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "1");
  EXPECT_EQ(second_snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "0");
}

TEST_P(MergeAppendTest, ChangedPartitionSpec) {
  ICEBERG_UNWRAP_OR_FAIL(auto first_append, NewBranchMergeAppend());
  first_append->AppendFile(file_a_).AppendFile(file_b_);
  EXPECT_THAT(first_append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto initial_manifests, CurrentDataManifests());
  ASSERT_EQ(initial_manifests.size(), 1U);
  auto initial_manifest = initial_manifests[0];

  ICEBERG_UNWRAP_OR_FAIL(auto update_spec, table_->NewUpdatePartitionSpec());
  update_spec->AddField("y");
  EXPECT_THAT(update_spec->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto new_spec, table_->spec());
  auto file_new_spec = MakeDataFile("/data/file_new_spec.parquet", new_spec,
                                    {Literal::Long(5L), Literal::Long(6L)});

  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  append->AppendFile(file_new_spec);
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  ASSERT_EQ(data_manifests.size(), 2U);

  auto* old_spec_manifest =
      FindManifestByPath(data_manifests, initial_manifest.manifest_path);
  ASSERT_NE(old_spec_manifest, nullptr);
  ExpectManifestEntries(
      *old_spec_manifest, {file_a_, file_b_},
      {ManifestStatus::kAdded, ManifestStatus::kAdded},
      {first_snapshot->snapshot_id, first_snapshot->snapshot_id},
      {first_snapshot->sequence_number, first_snapshot->sequence_number},
      {first_snapshot->sequence_number, first_snapshot->sequence_number});

  auto* new_spec_manifest = FindManifestForSpec(data_manifests, new_spec->spec_id());
  ASSERT_NE(new_spec_manifest, nullptr);
  ExpectManifestEntries(*new_spec_manifest, {file_new_spec}, {ManifestStatus::kAdded},
                        {second_snapshot->snapshot_id},
                        {second_snapshot->sequence_number},
                        {second_snapshot->sequence_number});
}

TEST_P(MergeAppendTest, ChangedPartitionSpecMergeExisting) {
  ICEBERG_UNWRAP_OR_FAIL(auto append_a, NewBranchMergeAppend());
  append_a->AppendFile(file_a_);
  EXPECT_THAT(append_a->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, CurrentSnapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto fast_append_b, NewBranchFastAppend());
  fast_append_b->AppendFile(file_b_);
  EXPECT_THAT(fast_append_b->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto unmerged_manifests, CurrentDataManifests());
  ASSERT_EQ(unmerged_manifests.size(), 2U);
  auto unmerged_paths = ManifestPaths(unmerged_manifests);

  ICEBERG_UNWRAP_OR_FAIL(auto update_spec, table_->NewUpdatePartitionSpec());
  update_spec->AddField("y");
  EXPECT_THAT(update_spec->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto new_spec, table_->spec());
  auto file_new_spec = MakeDataFile("/data/file_new_spec.parquet", new_spec,
                                    {Literal::Long(5L), Literal::Long(6L)});

  ICEBERG_UNWRAP_OR_FAIL(auto append_new_spec, NewBranchMergeAppend());
  append_new_spec->AppendFile(file_new_spec);
  EXPECT_THAT(append_new_spec->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto third_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  ASSERT_EQ(data_manifests.size(), 2U);
  for (const auto& old_path : unmerged_paths) {
    EXPECT_EQ(FindManifestByPath(data_manifests, old_path), nullptr);
  }

  auto* new_spec_manifest = FindManifestForSpec(data_manifests, new_spec->spec_id());
  ASSERT_NE(new_spec_manifest, nullptr);
  ExpectManifestEntries(*new_spec_manifest, {file_new_spec}, {ManifestStatus::kAdded},
                        {third_snapshot->snapshot_id}, {third_snapshot->sequence_number},
                        {third_snapshot->sequence_number});

  auto* old_spec_manifest = FindManifestForSpec(data_manifests, spec_->spec_id());
  ASSERT_NE(old_spec_manifest, nullptr);
  ExpectManifestEntries(
      *old_spec_manifest, {file_b_, file_a_},
      {ManifestStatus::kExisting, ManifestStatus::kExisting},
      {second_snapshot->snapshot_id, first_snapshot->snapshot_id},
      {second_snapshot->sequence_number, first_snapshot->sequence_number},
      {second_snapshot->sequence_number, first_snapshot->sequence_number});
}

TEST_P(MergeAppendTest, Failure) {
  SetManifestMinMergeCount(1);
  SetCommitRetryProperties(1);

  ICEBERG_UNWRAP_OR_FAIL(auto first_append, NewBranchMergeAppend());
  first_append->AppendFile(file_a_);
  EXPECT_THAT(first_append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto initial_manifests, CurrentDataManifests());
  ASSERT_EQ(initial_manifests.size(), 1U);
  auto initial_manifest = initial_manifests[0];

  int update_call_count = 0;
  BindTableWithFailingCommits(/*failures=*/2, &update_call_count);

  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto append, txn->NewMergeAppend());
  append->SetTargetBranch(branch());
  append->AppendFile(file_b_);
  EXPECT_THAT(append->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto pending_snapshot, SnapshotForBranch(txn->current()));
  ICEBERG_UNWRAP_OR_FAIL(auto pending_manifests, DataManifests(pending_snapshot));
  ASSERT_EQ(pending_manifests.size(), 1U);
  auto new_manifest = pending_manifests[0];
  EXPECT_TRUE(FileExists(new_manifest.manifest_path));
  ExpectManifestEntries(new_manifest, {file_b_, file_a_},
                        {ManifestStatus::kAdded, ManifestStatus::kExisting});

  auto result = txn->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(result, HasErrorMessage("Injected failure"));
  EXPECT_EQ(update_call_count, 2);

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto current_snapshot, CurrentSnapshot());
  EXPECT_EQ(current_snapshot->snapshot_id, first_snapshot->snapshot_id);
  ICEBERG_UNWRAP_OR_FAIL(auto current_manifests, CurrentDataManifests());
  ASSERT_EQ(current_manifests.size(), 1U);
  EXPECT_EQ(current_manifests[0].manifest_path, initial_manifest.manifest_path);
  ExpectManifestEntries(current_manifests[0], {file_a_}, {ManifestStatus::kAdded});
  EXPECT_FALSE(FileExists(new_manifest.manifest_path));
}

TEST_P(MergeAppendTest, AppendManifestCleanup) {
  SetCommitRetryProperties(1);
  int update_call_count = 0;
  BindTableWithFailingCommits(/*failures=*/2, &update_call_count);
  const auto seq_before = table_->metadata()->last_sequence_number;

  auto path = table_location_ + "/metadata/input.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_, file_b_}));
  EXPECT_TRUE(FileExists(manifest.manifest_path));

  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto append, txn->NewMergeAppend());
  append->SetTargetBranch(branch());
  append->AppendManifest(manifest);
  EXPECT_THAT(append->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto pending_snapshot, SnapshotForBranch(txn->current()));
  ICEBERG_UNWRAP_OR_FAIL(auto pending_manifests, DataManifests(pending_snapshot));
  ASSERT_EQ(pending_manifests.size(), 1U);
  auto new_manifest = pending_manifests[0];
  EXPECT_TRUE(FileExists(new_manifest.manifest_path));
  if (format_version() == 1) {
    EXPECT_NE(new_manifest.manifest_path, manifest.manifest_path);
  } else {
    EXPECT_EQ(new_manifest.manifest_path, manifest.manifest_path);
  }

  auto result = txn->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(result, HasErrorMessage("Injected failure"));
  EXPECT_EQ(update_call_count, 2);

  EXPECT_THAT(table_->Refresh(), IsOk());
  EXPECT_THAT(CurrentSnapshot(), IsError(ErrorKind::kNotFound));
  EXPECT_EQ(table_->metadata()->last_sequence_number, seq_before);
  if (format_version() == 1) {
    EXPECT_FALSE(FileExists(new_manifest.manifest_path));
    EXPECT_TRUE(FileExists(manifest.manifest_path));
  } else {
    EXPECT_TRUE(FileExists(new_manifest.manifest_path));
  }
}

TEST_P(MergeAppendTest, Recovery) {
  SetManifestMinMergeCount(1);
  SetCommitRetryProperties(3);

  ICEBERG_UNWRAP_OR_FAIL(auto first_append, NewBranchMergeAppend());
  first_append->AppendFile(file_a_);
  EXPECT_THAT(first_append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto initial_manifests, CurrentDataManifests());
  ASSERT_EQ(initial_manifests.size(), 1U);
  auto initial_manifest = initial_manifests[0];

  int update_call_count = 0;
  BindTableWithFailingCommits(/*failures=*/3, &update_call_count);

  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto append, txn->NewMergeAppend());
  append->SetTargetBranch(branch());
  append->AppendFile(file_b_);
  EXPECT_THAT(append->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto pending_snapshot, SnapshotForBranch(txn->current()));
  ICEBERG_UNWRAP_OR_FAIL(auto pending_manifests, DataManifests(pending_snapshot));
  ASSERT_EQ(pending_manifests.size(), 1U);
  auto pending_manifest = pending_manifests[0];
  EXPECT_TRUE(FileExists(pending_manifest.manifest_path));
  ExpectManifestEntries(pending_manifest, {file_b_, file_a_},
                        {ManifestStatus::kAdded, ManifestStatus::kExisting});

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_before_commit, CurrentSnapshot());
  EXPECT_EQ(snapshot_before_commit->snapshot_id, first_snapshot->snapshot_id);
  EXPECT_EQ(table_->metadata()->last_sequence_number, first_snapshot->sequence_number);

  EXPECT_THAT(txn->Commit(), IsOk());
  EXPECT_EQ(update_call_count, 4);

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  ASSERT_EQ(data_manifests.size(), 1U);
  EXPECT_EQ(data_manifests[0].manifest_path, pending_manifest.manifest_path);
  EXPECT_TRUE(FileExists(pending_manifest.manifest_path));
  ExpectManifestEntries(
      data_manifests[0], {file_b_, file_a_},
      {ManifestStatus::kAdded, ManifestStatus::kExisting},
      {second_snapshot->snapshot_id, first_snapshot->snapshot_id},
      {second_snapshot->sequence_number, first_snapshot->sequence_number},
      {second_snapshot->sequence_number, first_snapshot->sequence_number});
  EXPECT_NE(data_manifests[0].manifest_path, initial_manifest.manifest_path);
}

TEST_P(MergeAppendTest, AppendManifestWithSnapshotIdInheritance) {
  SetSnapshotIdInheritanceEnabled();

  auto path = table_location_ + "/metadata/input.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_, file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  append->AppendManifest(manifest);
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  ASSERT_EQ(data_manifests.size(), 1U);
  EXPECT_EQ(data_manifests[0].manifest_path, manifest.manifest_path);
  ExpectManifestEntries(data_manifests[0], {file_a_, file_b_},
                        {ManifestStatus::kAdded, ManifestStatus::kAdded},
                        {snapshot->snapshot_id, snapshot->snapshot_id},
                        {snapshot->sequence_number, snapshot->sequence_number},
                        {snapshot->sequence_number, snapshot->sequence_number});
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedRecords), "200");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalDataFiles), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kTotalRecords), "200");
}

TEST_P(MergeAppendTest, MergedAppendManifestCleanupWithSnapshotIdInheritance) {
  SetSnapshotIdInheritanceEnabled();
  SetManifestMinMergeCount(1);

  auto path1 = table_location_ + "/metadata/manifest-file-1.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest1, WriteManifest(path1, {file_a_, file_b_}));
  ICEBERG_UNWRAP_OR_FAIL(auto append1, NewBranchMergeAppend());
  append1->AppendManifest(manifest1);
  EXPECT_THAT(append1->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, CurrentSnapshot());
  EXPECT_TRUE(FileExists(manifest1.manifest_path));

  auto path2 = table_location_ + "/metadata/manifest-file-2.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest2, WriteManifest(path2, {file_c_, file_d_}));
  ICEBERG_UNWRAP_OR_FAIL(auto append2, NewBranchMergeAppend());
  append2->AppendManifest(manifest2);
  EXPECT_THAT(append2->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, CurrentSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentDataManifests());
  ASSERT_EQ(data_manifests.size(), 1U);
  ExpectManifestEntries(
      data_manifests[0], {file_c_, file_d_, file_a_, file_b_},
      {ManifestStatus::kAdded, ManifestStatus::kAdded, ManifestStatus::kExisting,
       ManifestStatus::kExisting},
      {second_snapshot->snapshot_id, second_snapshot->snapshot_id,
       first_snapshot->snapshot_id, first_snapshot->snapshot_id},
      {second_snapshot->sequence_number, second_snapshot->sequence_number,
       first_snapshot->sequence_number, first_snapshot->sequence_number},
      {second_snapshot->sequence_number, second_snapshot->sequence_number,
       first_snapshot->sequence_number, first_snapshot->sequence_number});
  EXPECT_FALSE(FileExists(manifest2.manifest_path));
}

TEST_P(MergeAppendTest, AppendManifestFailureWithSnapshotIdInheritance) {
  SetSnapshotIdInheritanceEnabled();
  SetCommitRetryProperties(1);
  BindTableWithFailingCommits(/*failures=*/2);
  const auto seq_before = table_->metadata()->last_sequence_number;

  auto path = table_location_ + "/metadata/input.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_, file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchMergeAppend());
  append->AppendManifest(manifest);
  auto result = append->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(result, HasErrorMessage("Injected failure"));

  EXPECT_THAT(table_->Refresh(), IsOk());
  EXPECT_THAT(CurrentSnapshot(), IsError(ErrorKind::kNotFound));
  EXPECT_EQ(table_->metadata()->last_sequence_number, seq_before);
  EXPECT_TRUE(FileExists(manifest.manifest_path));
}

TEST_P(MergeAppendTest, TransactionNewMergeAppendCommits) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto append, txn->NewMergeAppend());
  append->SetTargetBranch(branch());
  append->AppendFile(file_a_);
  EXPECT_THAT(append->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto committed_table, txn->Commit());

  const auto& refs = committed_table->metadata()->refs;
  auto ref_it = refs.find(branch());
  ASSERT_NE(ref_it, refs.end());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot,
                         committed_table->SnapshotById(ref_it->second->snapshot_id));
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kAddedDataFiles), "1");
}

TEST_P(MergeAppendTest, InvalidAppendManifestWithExistingEntry) {
  ManifestEntry entry;
  entry.status = ManifestStatus::kExisting;
  entry.snapshot_id = 12345;
  entry.sequence_number = 1;
  entry.file_sequence_number = 1;
  entry.data_file = file_a_;

  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest,
      WriteManifestEntries(table_location_ + "/metadata/existing.avro", {entry}));
  ExpectAppendManifestError(manifest, "Cannot append manifest with existing files");
}

TEST_P(MergeAppendTest, InvalidAppendManifestWithDeletedEntry) {
  ManifestEntry entry;
  entry.status = ManifestStatus::kDeleted;
  entry.snapshot_id = 12345;
  entry.sequence_number = 1;
  entry.file_sequence_number = 1;
  entry.data_file = file_a_;

  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest,
      WriteManifestEntries(table_location_ + "/metadata/deleted.avro", {entry}));
  ExpectAppendManifestError(manifest, "Cannot append manifest with deleted files");
}

TEST_F(MergeAppendTestBase, InvalidAppendManifestWithExistingFilesSummary) {
  auto manifest = MakeEmptyAppendManifest(table_location_ + "/metadata/existing.avro");
  manifest.existing_files_count = 1;

  ExpectAppendManifestError(manifest, "Cannot append manifest with existing files");
}

TEST_F(MergeAppendTestBase, InvalidAppendManifestWithDeletedFilesSummary) {
  auto manifest = MakeEmptyAppendManifest(table_location_ + "/metadata/deleted.avro");
  manifest.deleted_files_count = 1;

  ExpectAppendManifestError(manifest, "Cannot append manifest with deleted files");
}

TEST_F(MergeAppendTestBase, InvalidAppendManifestWithAssignedSnapshotId) {
  auto manifest =
      MakeEmptyAppendManifest(table_location_ + "/metadata/assigned-snapshot.avro");
  manifest.added_snapshot_id = 12345;

  ExpectAppendManifestError(manifest, "Snapshot id must be assigned during commit");
}

TEST_F(MergeAppendTestBase, InvalidAppendManifestWithAssignedSequenceNumber) {
  auto manifest =
      MakeEmptyAppendManifest(table_location_ + "/metadata/assigned-sequence.avro");
  manifest.sequence_number = 7;

  ExpectAppendManifestError(manifest, "Sequence number must be assigned during commit");
}

TEST_P(MergeAppendTest, DefaultPartitionSummaries) {
  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchFastAppend());
  append->AppendFile(file_a_);
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  const auto& summary = snapshot->summary;
  size_t partition_summary_count = 0;
  for (const auto& [key, value] : summary) {
    std::ignore = value;
    if (key.starts_with(SnapshotSummaryFields::kChangedPartitionPrefix)) {
      ++partition_summary_count;
    }
  }
  EXPECT_EQ(partition_summary_count, 0U);
  EXPECT_FALSE(summary.contains(SnapshotSummaryFields::kPartitionSummaryProp));
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kChangedPartitionCountProp), "1");
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kManifestsReplaced), "0");
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kManifestsKept), "0");
}

TEST_P(MergeAppendTest, IncludedPartitionSummaries) {
  ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
  props->Set(std::string(TableProperties::kWritePartitionSummaryLimit.key()), "1");
  EXPECT_THAT(props->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchFastAppend());
  append->AppendFile(file_a_);
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  const auto& summary = snapshot->summary;
  size_t partition_summary_count = 0;
  for (const auto& [key, value] : summary) {
    std::ignore = value;
    if (key.starts_with(SnapshotSummaryFields::kChangedPartitionPrefix)) {
      ++partition_summary_count;
    }
  }
  EXPECT_EQ(partition_summary_count, 1U);
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kPartitionSummaryProp), "true");
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kChangedPartitionCountProp), "1");
  ASSERT_TRUE(summary.contains(SnapshotSummaryFields::kChangedPartitionPrefix + "x=1"));
  const auto& partition_summary =
      summary.at(SnapshotSummaryFields::kChangedPartitionPrefix + "x=1");
  EXPECT_THAT(partition_summary,
              ::testing::HasSubstr(SnapshotSummaryFields::kAddedDataFiles + "=1"));
  EXPECT_THAT(partition_summary,
              ::testing::HasSubstr(SnapshotSummaryFields::kAddedRecords + "=100"));
  EXPECT_THAT(partition_summary,
              ::testing::HasSubstr(SnapshotSummaryFields::kAddedFileSize + "=1024"));
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kManifestsReplaced), "0");
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kManifestsKept), "0");
}

TEST_P(MergeAppendTest, IncludedPartitionSummaryLimit) {
  ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
  props->Set(std::string(TableProperties::kWritePartitionSummaryLimit.key()), "1");
  EXPECT_THAT(props->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto append, NewBranchFastAppend());
  append->AppendFile(file_a_).AppendFile(file_b_);
  EXPECT_THAT(append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, CurrentSnapshot());
  const auto& summary = snapshot->summary;
  size_t partition_summary_count = 0;
  for (const auto& [key, value] : summary) {
    std::ignore = value;
    if (key.starts_with(SnapshotSummaryFields::kChangedPartitionPrefix)) {
      ++partition_summary_count;
    }
  }
  EXPECT_EQ(partition_summary_count, 0U);
  EXPECT_FALSE(summary.contains(SnapshotSummaryFields::kPartitionSummaryProp));
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kChangedPartitionCountProp), "2");
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kManifestsReplaced), "0");
  EXPECT_EQ(summary.at(SnapshotSummaryFields::kManifestsKept), "0");
}

INSTANTIATE_TEST_SUITE_P(
    MergeAppendVersions, MergeAppendTest,
    ::testing::Combine(::testing::Values<int8_t>(1, 2, 3),
                       ::testing::Values(std::string(SnapshotRef::kMainBranch),
                                         std::string("testBranch"))),
    [](const ::testing::TestParamInfo<std::tuple<int8_t, std::string>>& info) {
      const auto& branch = std::get<1>(info.param);
      return "V" + std::to_string(std::get<0>(info.param)) + "_" +
             (branch == SnapshotRef::kMainBranch ? std::string("main") : branch);
    });

}  // namespace iceberg
