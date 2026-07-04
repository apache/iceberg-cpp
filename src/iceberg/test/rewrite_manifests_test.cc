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

#include "iceberg/update/rewrite_manifests.h"

#include <algorithm>
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/constants.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transform.h"
#include "iceberg/update/delete_files.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/row_delta.h"
#include "iceberg/update/update_partition_spec.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class RewriteManifestsTest : public MinimalUpdateTestBase,
                             public ::testing::WithParamInterface<int8_t> {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  void SetUp() override {
    MinimalUpdateTestBase::SetUp();

    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());
    file_a_ = MakeDataFile("a", 10, 100, 1);
    file_b_ = MakeDataFile("b", 20, 200, 2);
    file_c_ = MakeDataFile("c", 30, 300, 3);
    file_d_ = MakeDataFile("d", 40, 400, 4);
  }

  int8_t format_version() const override { return GetParam(); }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& name, int64_t record_count,
                                         int64_t size, int64_t partition_value) {
    return MakeDataFile(name, spec_, record_count, size,
                        {Literal::Long(partition_value)});
  }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& name,
                                         std::shared_ptr<PartitionSpec> spec,
                                         int64_t record_count, int64_t size,
                                         std::vector<Literal> partition_values) {
    auto file = std::make_shared<DataFile>();
    file->content = DataFile::Content::kData;
    file->file_path = std::format("{}/data/{}.parquet", table_location_, name);
    file->file_format = FileFormatType::kParquet;
    file->partition = PartitionValues(std::move(partition_values));
    file->record_count = record_count;
    file->file_size_in_bytes = size;
    file->partition_spec_id = spec->spec_id();
    return file;
  }

  std::shared_ptr<DataFile> MakeDeleteFile(
      const std::string& name, int64_t partition_value,
      std::optional<std::string> referenced_data_file = std::nullopt) {
    auto file = MakeDataFile(name, 7, 50, partition_value);
    file->content = DataFile::Content::kPositionDeletes;
    file->file_path = std::format("{}/delete/{}.parquet", table_location_, name);
    if (table_->metadata()->format_version >= 3) {
      file->file_format = FileFormatType::kPuffin;
      file->referenced_data_file = referenced_data_file.value_or(file_a_->file_path);
      file->content_offset = 0;
      file->content_size_in_bytes = 10;
    }
    return file;
  }

  std::shared_ptr<DataFile> MakeEqualityDeleteFile(const std::string& name,
                                                   int64_t partition_value) {
    auto file = MakeDataFile(name, /*record_count=*/1, /*size=*/10, partition_value);
    file->content = DataFile::Content::kEqualityDeletes;
    file->file_path = std::format("{}/delete/{}.parquet", table_location_, name);
    file->equality_ids = {1};
    return file;
  }

  Status AppendFiles(std::vector<std::shared_ptr<DataFile>> files) {
    ICEBERG_ASSIGN_OR_RAISE(auto append, table_->NewFastAppend());
    for (const auto& file : files) {
      append->AppendFile(file);
    }
    ICEBERG_RETURN_UNEXPECTED(append->Commit());
    return table_->Refresh();
  }

  Status AppendDeleteFiles(std::vector<std::shared_ptr<DataFile>> files) {
    ICEBERG_ASSIGN_OR_RAISE(auto row_delta, table_->NewRowDelta());
    for (const auto& file : files) {
      row_delta->AddDeletes(file);
    }
    ICEBERG_RETURN_UNEXPECTED(row_delta->Commit());
    return table_->Refresh();
  }

  Status RemoveDeleteFiles(std::vector<std::shared_ptr<DataFile>> files) {
    ICEBERG_ASSIGN_OR_RAISE(auto row_delta, table_->NewRowDelta());
    for (const auto& file : files) {
      row_delta->RemoveDeletes(file);
    }
    ICEBERG_RETURN_UNEXPECTED(row_delta->Commit());
    return table_->Refresh();
  }

  void SetSnapshotIdInheritanceEnabled() {
    ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
    props->Set(std::string(TableProperties::kSnapshotIdInheritanceEnabled.key()), "true");
    EXPECT_THAT(props->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  void SetManifestMergeEnabled(bool enabled) {
    ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
    props->Set(std::string(TableProperties::kManifestMergeEnabled.key()),
               enabled ? "true" : "false");
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

  void BindTableWithFailingCommits(int failures) {
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
            [this, weak_catalog, failures](
                const TableIdentifier& identifier,
                const std::vector<std::unique_ptr<TableRequirement>>& requirements,
                const std::vector<std::unique_ptr<TableUpdate>>& updates) mutable
                -> Result<std::shared_ptr<Table>> {
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
    mock_catalog_ = std::move(mock_catalog);
  }

  void ValidateSummary(const std::shared_ptr<Snapshot>& snapshot, int replaced, int kept,
                       int created, int entry_count) {
    ASSERT_NE(snapshot, nullptr);
    const auto& summary = snapshot->summary;
    EXPECT_THAT(summary, ::testing::Contains(::testing::Pair(
                             std::string(SnapshotSummaryFields::kManifestsReplaced),
                             std::to_string(replaced))));
    EXPECT_THAT(summary, ::testing::Contains(::testing::Pair(
                             std::string(SnapshotSummaryFields::kManifestsKept),
                             std::to_string(kept))));
    EXPECT_THAT(summary, ::testing::Contains(::testing::Pair(
                             std::string(SnapshotSummaryFields::kManifestsCreated),
                             std::to_string(created))));
    EXPECT_THAT(summary, ::testing::Contains(::testing::Pair(
                             std::string(SnapshotSummaryFields::kEntriesProcessed),
                             std::to_string(entry_count))));
  }

  void MatchNumberOfManifestFileWithSpecId(std::span<const ManifestFile> manifests,
                                           int32_t partition_spec_id,
                                           int expected_manifest_count) {
    const auto matched_manifest_count = std::ranges::count_if(
        manifests, [partition_spec_id](const ManifestFile& manifest) {
          return manifest.partition_spec_id == partition_spec_id;
        });
    EXPECT_EQ(matched_manifest_count, expected_manifest_count);
  }

  bool FileExists(const std::string& path) {
    auto input_file = file_io_->NewInputFile(path);
    if (!input_file.has_value()) {
      return false;
    }
    return input_file.value()->Size().has_value();
  }

  Result<const ManifestFile*> ManifestContainingPath(
      std::span<const ManifestFile> manifests, const std::string& path) {
    for (const auto& manifest : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(auto entries, ReadManifestEntries(manifest));
      for (const auto& entry : entries) {
        if (entry.data_file != nullptr && entry.data_file->file_path == path) {
          return &manifest;
        }
      }
    }
    return NotFound("Manifest containing path {} is not found", path);
  }

  void ExpectManifestEntryContents(const ManifestFile& manifest,
                                   std::vector<std::string> expected_paths,
                                   std::vector<DataFile::Content> expected_contents) {
    ASSERT_EQ(expected_contents.size(), expected_paths.size());

    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadManifestEntries(manifest));
    ASSERT_EQ(entries.size(), expected_paths.size());

    std::vector<std::pair<std::string, DataFile::Content>> actual;
    actual.reserve(entries.size());
    for (const auto& entry : entries) {
      ASSERT_NE(entry.data_file, nullptr);
      actual.emplace_back(entry.data_file->file_path, entry.data_file->content);
    }

    std::vector<std::pair<std::string, DataFile::Content>> expected;
    expected.reserve(expected_paths.size());
    for (size_t i = 0; i < expected_paths.size(); ++i) {
      expected.emplace_back(std::move(expected_paths[i]), expected_contents[i]);
    }
    EXPECT_THAT(actual, ::testing::UnorderedElementsAreArray(expected));
  }

  Result<std::vector<ManifestFile>> CurrentManifests() {
    ICEBERG_RETURN_UNEXPECTED(table_->Refresh());
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    SnapshotCache cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, cache.DataManifests(file_io_));
    return std::vector<ManifestFile>(manifests.begin(), manifests.end());
  }

  Result<std::vector<ManifestFile>> CurrentDeleteManifests() {
    ICEBERG_RETURN_UNEXPECTED(table_->Refresh());
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    SnapshotCache cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, cache.DeleteManifests(file_io_));
    return std::vector<ManifestFile>(manifests.begin(), manifests.end());
  }

  Result<ManifestFile> WriteExistingManifest(
      const std::string& name, int64_t snapshot_id,
      const std::vector<std::shared_ptr<DataFile>>& files,
      ManifestContent content = ManifestContent::kData,
      std::shared_ptr<PartitionSpec> manifest_spec = nullptr,
      int64_t data_sequence_number = TableMetadata::kInitialSequenceNumber,
      std::optional<int64_t> file_sequence_number = std::nullopt) {
    auto path = std::format("{}/metadata/{}.avro", table_location_, name);
    auto spec = manifest_spec != nullptr ? std::move(manifest_spec) : spec_;
    std::vector<std::shared_ptr<DataFile>> files_to_write;
    files_to_write.reserve(files.size());
    std::optional<int64_t> first_row_id = std::nullopt;
    if (table_->metadata()->format_version >= 3 && content == ManifestContent::kData) {
      first_row_id = TableMetadata::kInitialRowId;
      ICEBERG_ASSIGN_OR_RAISE(auto current_manifests, CurrentManifests());
      std::unordered_map<std::string, std::shared_ptr<DataFile>> current_files_by_path;
      for (const auto& manifest : current_manifests) {
        ICEBERG_ASSIGN_OR_RAISE(auto entries, ReadManifestEntries(manifest));
        for (const auto& entry : entries) {
          if (entry.IsAlive() && entry.data_file != nullptr) {
            current_files_by_path.emplace(entry.data_file->file_path,
                                          std::make_shared<DataFile>(*entry.data_file));
          }
        }
      }
      for (const auto& file : files) {
        auto file_to_write = file;
        if (!file_to_write->first_row_id.has_value()) {
          if (auto it = current_files_by_path.find(file->file_path);
              it != current_files_by_path.end()) {
            file_to_write = it->second;
          }
        }
        files_to_write.push_back(std::move(file_to_write));
      }
    } else {
      files_to_write = files;
    }
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        ManifestWriter::MakeWriter(table_->metadata()->format_version, kInvalidSnapshotId,
                                   path, file_io_, spec, schema_, content, first_row_id));
    for (const auto& file : files_to_write) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(
          file, snapshot_id, data_sequence_number, file_sequence_number));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  struct ExistingManifestEntry {
    std::shared_ptr<DataFile> file;
    int64_t snapshot_id;
    int64_t data_sequence_number;
    int64_t file_sequence_number;
  };

  Result<ManifestFile> WriteExistingDeleteManifest(
      const std::string& name, const std::vector<ExistingManifestEntry>& entries) {
    auto path = std::format("{}/metadata/{}.avro", table_location_, name);
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, ManifestWriter::MakeWriter(table_->metadata()->format_version,
                                                kInvalidSnapshotId, path, file_io_, spec_,
                                                schema_, ManifestContent::kDeletes));
    for (const auto& entry : entries) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry.file, entry.snapshot_id,
                                                           entry.data_sequence_number,
                                                           entry.file_sequence_number));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  Result<ManifestFile> WriteExistingManifest(
      const std::string& name,
      const std::vector<std::pair<std::shared_ptr<DataFile>, int64_t>>& files) {
    auto path = std::format("{}/metadata/{}.avro", table_location_, name);
    std::vector<std::pair<std::shared_ptr<DataFile>, int64_t>> files_to_write;
    files_to_write.reserve(files.size());
    std::optional<int64_t> first_row_id = std::nullopt;
    if (table_->metadata()->format_version >= 3) {
      first_row_id = TableMetadata::kInitialRowId;
      ICEBERG_ASSIGN_OR_RAISE(auto current_manifests, CurrentManifests());
      std::unordered_map<std::string, std::shared_ptr<DataFile>> current_files_by_path;
      for (const auto& manifest : current_manifests) {
        ICEBERG_ASSIGN_OR_RAISE(auto entries, ReadManifestEntries(manifest));
        for (const auto& entry : entries) {
          if (entry.IsAlive() && entry.data_file != nullptr) {
            current_files_by_path.emplace(entry.data_file->file_path,
                                          std::make_shared<DataFile>(*entry.data_file));
          }
        }
      }
      for (const auto& [file, snapshot_id] : files) {
        auto file_to_write = file;
        if (!file_to_write->first_row_id.has_value()) {
          if (auto it = current_files_by_path.find(file->file_path);
              it != current_files_by_path.end()) {
            file_to_write = it->second;
          }
        }
        files_to_write.emplace_back(std::move(file_to_write), snapshot_id);
      }
    } else {
      files_to_write = files;
    }
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, ManifestWriter::MakeWriter(
                         table_->metadata()->format_version, kInvalidSnapshotId, path,
                         file_io_, spec_, schema_, ManifestContent::kData, first_row_id));
    for (const auto& [file, snapshot_id] : files_to_write) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(
          file, snapshot_id, TableMetadata::kInitialSequenceNumber));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  Result<ManifestFile> WriteAddedManifest(
      const std::string& name, const std::vector<std::shared_ptr<DataFile>>& files,
      std::optional<int64_t> entry_snapshot_id = std::nullopt,
      std::optional<int64_t> data_sequence_number = std::nullopt) {
    auto path = std::format("{}/metadata/{}.avro", table_location_, name);
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, ManifestWriter::MakeWriter(table_->metadata()->format_version,
                                                entry_snapshot_id, path, file_io_, spec_,
                                                schema_, ManifestContent::kData));
    for (const auto& file : files) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(file, data_sequence_number));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto manifest, writer->ToManifestFile());
    manifest.added_snapshot_id = kInvalidSnapshotId;
    return manifest;
  }

  Result<ManifestFile> WriteDeletedManifest(
      const std::string& name, const std::vector<std::shared_ptr<DataFile>>& files,
      std::optional<int64_t> entry_snapshot_id = std::nullopt,
      int64_t data_sequence_number = TableMetadata::kInitialSequenceNumber) {
    auto path = std::format("{}/metadata/{}.avro", table_location_, name);
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, ManifestWriter::MakeWriter(table_->metadata()->format_version,
                                                entry_snapshot_id, path, file_io_, spec_,
                                                schema_, ManifestContent::kData));
    for (const auto& file : files) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(file, data_sequence_number));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
  std::shared_ptr<DataFile> file_c_;
  std::shared_ptr<DataFile> file_d_;
  std::shared_ptr<::testing::NiceMock<MockCatalog>> mock_catalog_;
};

TEST_P(RewriteManifestsTest, RewriteManifestsAppendedDirectly) {
  SetSnapshotIdInheritanceEnabled();
  ICEBERG_UNWRAP_OR_FAIL(auto new_manifest,
                         WriteAddedManifest("manifest-file-1", {file_a_}));

  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  append->AppendManifest(new_manifest);
  EXPECT_THAT(append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot, table_->current_snapshot());
  const int64_t append_snapshot_id = append_snapshot->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return ""; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 1U);
  ExpectManifestEntriesWithSnapshotIds(manifests[0], {file_a_->file_path},
                                       {ManifestStatus::kExisting}, {append_snapshot_id});
}

TEST_P(RewriteManifestsTest, RewriteManifestsWithScanExecutor) {
  SetSnapshotIdInheritanceEnabled();
  ICEBERG_UNWRAP_OR_FAIL(auto new_manifest,
                         WriteAddedManifest("manifest-file-1", {file_a_}));

  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  append->AppendManifest(new_manifest);
  EXPECT_THAT(append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 1U);

  test::ThreadExecutor executor;
  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return ""; }).ScanManifestsWith(executor);
  EXPECT_THAT(rewrite->Commit(), IsOk());

  EXPECT_GT(executor.submit_count(), 0);
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 1U);
  ExpectManifestEntries(manifests[0], {file_a_->file_path}, {ManifestStatus::kExisting});
}

TEST_P(RewriteManifestsTest, RewriteManifestsGeneratedAndAppendedDirectly) {
  SetSnapshotIdInheritanceEnabled();
  ICEBERG_UNWRAP_OR_FAIL(auto new_manifest,
                         WriteAddedManifest("manifest-file-1", {file_a_}));

  ICEBERG_UNWRAP_OR_FAIL(auto append_manifest, table_->NewFastAppend());
  append_manifest->AppendManifest(new_manifest);
  EXPECT_THAT(append_manifest->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_append_snapshot, table_->current_snapshot());
  const int64_t manifest_append_id = manifest_append_snapshot->snapshot_id;

  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto file_append_snapshot, table_->current_snapshot());
  const int64_t file_append_id = file_append_snapshot->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 2U);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return ""; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 1U);
  ExpectManifestEntriesWithSnapshotIds(
      manifests[0], {file_a_->file_path, file_b_->file_path},
      {ManifestStatus::kExisting, ManifestStatus::kExisting},
      {manifest_append_id, file_append_id});
}

TEST_P(RewriteManifestsTest, RewriteManifestsWithoutClusterBy) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot, table_->current_snapshot());
  const int64_t append_id = append_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 1U);
  EXPECT_EQ(manifests[0].manifest_path, before[0].manifest_path);
  ExpectManifestEntriesWithSnapshotIds(manifests[0], {file_a_->file_path},
                                       {ManifestStatus::kAdded}, {append_id});
}

TEST_P(RewriteManifestsTest, RewriteAfterV2ToV3UpgradeAssignsRowIds) {
  if (format_version() != 2) {
    GTEST_SKIP() << "This regression starts from a v2 table";
  }

  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ASSERT_THAT(AppendFiles({file_b_}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto original_manifests, CurrentManifests());
  ASSERT_EQ(original_manifests.size(), 2U);
  for (const auto& manifest : original_manifests) {
    EXPECT_FALSE(manifest.first_row_id.has_value());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
  props->Set(std::string(TableProperties::kFormatVersion.key()), "3");
  EXPECT_THAT(props->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  EXPECT_EQ(table_->metadata()->format_version, 3);
  EXPECT_EQ(table_->metadata()->next_row_id, TableMetadata::kInitialRowId);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return ""; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 1U);
  EXPECT_EQ(manifests[0].first_row_id,
            std::make_optional<int64_t>(TableMetadata::kInitialRowId));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadManifestEntries(manifests[0]));
  ASSERT_EQ(entries.size(), 2U);

  int64_t next_row_id = TableMetadata::kInitialRowId;
  int64_t assigned_rows = 0;
  for (const auto& entry : entries) {
    ASSERT_NE(entry.data_file, nullptr);
    ASSERT_TRUE(entry.data_file->first_row_id.has_value());
    EXPECT_EQ(entry.data_file->first_row_id.value(), next_row_id);
    next_row_id += entry.data_file->record_count;
    assigned_rows += entry.data_file->record_count;
  }
  EXPECT_EQ(table_->metadata()->next_row_id,
            TableMetadata::kInitialRowId + assigned_rows);
}

TEST_P(RewriteManifestsTest, ReplaceManifestsSeparate) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot, table_->current_snapshot());
  const int64_t append_id = append_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile& file) { return file.file_path; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 2U);
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_a,
                         ManifestContainingPath(manifests, file_a_->file_path));
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_b,
                         ManifestContainingPath(manifests, file_b_->file_path));
  ExpectManifestEntriesWithSnapshotIds(*manifest_a, {file_a_->file_path},
                                       {ManifestStatus::kExisting}, {append_id});
  ExpectManifestEntriesWithSnapshotIds(*manifest_b, {file_b_->file_path},
                                       {ManifestStatus::kExisting}, {append_id});
}

TEST_P(RewriteManifestsTest, ReplaceManifestsConsolidate) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot_a, table_->current_snapshot());
  const int64_t append_id_a = append_snapshot_a->snapshot_id;
  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot_b, table_->current_snapshot());
  const int64_t append_id_b = append_snapshot_b->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 2U);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "file"; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 1U);
  ExpectManifestEntriesWithSnapshotIds(
      manifests[0], {file_a_->file_path, file_b_->file_path},
      {ManifestStatus::kExisting, ManifestStatus::kExisting}, {append_id_a, append_id_b});
}

TEST_P(RewriteManifestsTest, ReplaceManifestsWithFilter) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot_a, table_->current_snapshot());
  const int64_t append_id_a = append_snapshot_a->snapshot_id;
  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot_b, table_->current_snapshot());
  const int64_t append_id_b = append_snapshot_b->snapshot_id;
  ASSERT_THAT(AppendFiles({file_c_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot_c, table_->current_snapshot());
  const int64_t append_id_c = append_snapshot_c->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 3U);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "file"; });
  rewrite->RewriteIf([&](const ManifestFile& manifest) {
    auto entries_result = ReadManifestEntries(manifest);
    if (!entries_result.has_value() || entries_result.value().empty()) {
      return false;
    }
    return entries_result.value()[0].data_file->file_path != file_a_->file_path;
  });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 2U);

  const ManifestFile* kept_manifest = nullptr;
  const ManifestFile* rewritten_manifest = nullptr;
  for (const auto& manifest : manifests) {
    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadManifestEntries(manifest));
    ASSERT_FALSE(entries.empty());
    ASSERT_NE(entries[0].data_file, nullptr);
    if (entries[0].data_file->file_path == file_a_->file_path) {
      kept_manifest = &manifest;
    } else {
      rewritten_manifest = &manifest;
    }
  }

  ASSERT_NE(rewritten_manifest, nullptr);
  ExpectManifestEntriesWithSnapshotIds(
      *rewritten_manifest, {file_b_->file_path, file_c_->file_path},
      {ManifestStatus::kExisting, ManifestStatus::kExisting}, {append_id_b, append_id_c});

  ASSERT_NE(kept_manifest, nullptr);
  ExpectManifestEntriesWithSnapshotIds(*kept_manifest, {file_a_->file_path},
                                       {ManifestStatus::kAdded}, {append_id_a});
}

TEST_P(RewriteManifestsTest, ReplaceManifestsMaxSize) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot, table_->current_snapshot());
  const int64_t append_snapshot_id = append_snapshot->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
  props->Set(std::string(TableProperties::kManifestTargetSizeBytes.key()), "1");
  EXPECT_THAT(props->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "file"; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 2U);
  std::ranges::sort(manifests, {}, &ManifestFile::manifest_path);
  ExpectManifestEntriesWithSnapshotIds(manifests[0], {file_a_->file_path},
                                       {ManifestStatus::kExisting}, {append_snapshot_id});
  ExpectManifestEntriesWithSnapshotIds(manifests[1], {file_b_->file_path},
                                       {ManifestStatus::kExisting}, {append_snapshot_id});
}

TEST_P(RewriteManifestsTest, ConcurrentRewriteManifest) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot_a, table_->current_snapshot());
  const int64_t append_id_a = append_snapshot_a->snapshot_id;
  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot_b, table_->current_snapshot());
  const int64_t append_id_b = append_snapshot_b->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 2U);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "file"; });
  EXPECT_THAT(static_cast<SnapshotUpdate&>(*rewrite).Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto concurrent, table_->NewRewriteManifests());
  concurrent->ClusterBy([](const DataFile&) { return "file"; });
  concurrent->RewriteIf([&](const ManifestFile& manifest) {
    auto entries_result = ReadManifestEntries(manifest);
    if (!entries_result.has_value() || entries_result.value().empty()) {
      return false;
    }
    return entries_result.value()[0].data_file->file_path != file_a_->file_path;
  });
  EXPECT_THAT(concurrent->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto after_concurrent, CurrentManifests());
  ASSERT_EQ(after_concurrent.size(), 2U);

  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 1U);
  ExpectManifestEntriesWithSnapshotIds(
      manifests[0], {file_a_->file_path, file_b_->file_path},
      {ManifestStatus::kExisting, ManifestStatus::kExisting}, {append_id_a, append_id_b});
}

TEST_P(RewriteManifestsTest, AppendDuringRewriteManifest) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot_a, table_->current_snapshot());
  const int64_t append_id_a = append_snapshot_a->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "file"; });
  EXPECT_THAT(static_cast<SnapshotUpdate&>(*rewrite).Apply(), IsOk());

  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot_b, table_->current_snapshot());
  const int64_t append_id_b = append_snapshot_b->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto after_append, CurrentManifests());
  ASSERT_EQ(after_append.size(), 2U);

  // commit the rewrite manifests in progress
  EXPECT_THAT(rewrite->Commit(), IsOk());

  // the rewrite should only affect the first manifest, so we will end up with 2 manifests
  // even though we have a single cluster key, rewritten one should be the first in the
  // list
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 2U);
  ExpectManifestEntriesWithSnapshotIds(manifests[0], {file_a_->file_path},
                                       {ManifestStatus::kExisting}, {append_id_a});
  ExpectManifestEntriesWithSnapshotIds(manifests[1], {file_b_->file_path},
                                       {ManifestStatus::kAdded}, {append_id_b});
}

TEST_P(RewriteManifestsTest, RewriteManifestDuringAppend) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot, table_->current_snapshot());
  const int64_t append_snapshot_id = append_snapshot->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  append->AppendFile(file_b_);
  EXPECT_THAT(static_cast<SnapshotUpdate&>(*append).Apply(), IsOk());

  // rewrite the manifests - only affects the first
  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "file"; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto rewritten_manifests, CurrentManifests());
  ASSERT_EQ(rewritten_manifests.size(), 1U);

  // commit the append in progress
  EXPECT_THAT(append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto file_append_snapshot, table_->current_snapshot());
  const int64_t file_append_id = file_append_snapshot->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 2U);
  ExpectManifestEntriesWithSnapshotIds(manifests[0], {file_b_->file_path},
                                       {ManifestStatus::kAdded}, {file_append_id});
  ExpectManifestEntriesWithSnapshotIds(manifests[1], {file_a_->file_path},
                                       {ManifestStatus::kExisting}, {append_snapshot_id});
}

TEST_P(RewriteManifestsTest, BasicManifestReplacement) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  const int64_t first_snapshot_id = first_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot_manifests, CurrentManifests());
  ASSERT_EQ(first_snapshot_manifests.size(), 1U);

  ASSERT_THAT(AppendFiles({file_c_, file_d_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, table_->current_snapshot());
  const int64_t second_snapshot_id = second_snapshot->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_a, WriteExistingManifest("rewrite-a", first_snapshot_id, {file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_b, WriteExistingManifest("rewrite-b", first_snapshot_id, {file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(first_snapshot_manifests[0])
      .AddManifest(manifest_a)
      .AddManifest(manifest_b);
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 3U);
  if (GetParam() == 1) {
    EXPECT_NE(manifests[0].manifest_path, manifest_a.manifest_path);
    EXPECT_NE(manifests[1].manifest_path, manifest_b.manifest_path);
  } else {
    EXPECT_EQ(manifests[0].manifest_path, manifest_a.manifest_path);
    EXPECT_EQ(manifests[1].manifest_path, manifest_b.manifest_path);
  }
  ExpectManifestEntriesWithSnapshotIds(manifests[0], {file_a_->file_path},
                                       {ManifestStatus::kExisting}, {first_snapshot_id});
  ExpectManifestEntriesWithSnapshotIds(manifests[1], {file_b_->file_path},
                                       {ManifestStatus::kExisting}, {first_snapshot_id});
  ExpectManifestEntriesWithSnapshotIds(manifests[2],
                                       {file_c_->file_path, file_d_->file_path},
                                       {ManifestStatus::kAdded, ManifestStatus::kAdded},
                                       {second_snapshot_id, second_snapshot_id});

  ICEBERG_UNWRAP_OR_FAIL(auto after, table_->current_snapshot());
  ValidateSummary(after, /*replaced=*/1, /*kept=*/1, /*created=*/2,
                  /*entry_count=*/0);
}

TEST_P(RewriteManifestsTest, BasicManifestReplacementWithSnapshotIdInheritance) {
  SetSnapshotIdInheritanceEnabled();

  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  const int64_t first_snapshot_id = first_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot_manifests, CurrentManifests());
  ASSERT_EQ(first_snapshot_manifests.size(), 1U);

  ASSERT_THAT(AppendFiles({file_c_, file_d_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, table_->current_snapshot());
  const int64_t second_snapshot_id = second_snapshot->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_a,
      WriteExistingManifest("rewrite-inherit-a", first_snapshot_id, {file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_b,
      WriteExistingManifest("rewrite-inherit-b", first_snapshot_id, {file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(first_snapshot_manifests[0])
      .AddManifest(manifest_a)
      .AddManifest(manifest_b);
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 3U);
  EXPECT_EQ(manifests[0].manifest_path, manifest_a.manifest_path);
  EXPECT_EQ(manifests[1].manifest_path, manifest_b.manifest_path);
  ExpectManifestEntriesWithSnapshotIds(manifests[0], {file_a_->file_path},
                                       {ManifestStatus::kExisting}, {first_snapshot_id});
  ExpectManifestEntriesWithSnapshotIds(manifests[1], {file_b_->file_path},
                                       {ManifestStatus::kExisting}, {first_snapshot_id});
  ExpectManifestEntriesWithSnapshotIds(manifests[2],
                                       {file_c_->file_path, file_d_->file_path},
                                       {ManifestStatus::kAdded, ManifestStatus::kAdded},
                                       {second_snapshot_id, second_snapshot_id});

  ICEBERG_UNWRAP_OR_FAIL(auto after, table_->current_snapshot());
  ValidateSummary(after, /*replaced=*/1, /*kept=*/1, /*created=*/2,
                  /*entry_count=*/0);

  // validate that any subsequent operation does not fail
  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFromRowFilter(Expressions::AlwaysTrue());
  EXPECT_THAT(delete_files->Commit(), IsOk());
}

TEST_P(RewriteManifestsTest, WithMultiplePartitionSpec) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto initial_manifests, CurrentManifests());
  ASSERT_EQ(initial_manifests.size(), 1U);
  const int32_t initial_spec_id = initial_manifests[0].partition_spec_id;
  ICEBERG_UNWRAP_OR_FAIL(auto initial_spec,
                         table_->metadata()->PartitionSpecById(initial_spec_id));
  ASSERT_EQ(initial_spec->fields().size(), 1U);
  const int32_t initial_partition_field_id = initial_spec->fields()[0].field_id();

  // Build the new spec using the table's schema, which uses fresh partition IDs.
  ICEBERG_UNWRAP_OR_FAIL(auto update_spec, table_->NewUpdatePartitionSpec());
  update_spec->AddField(Expressions::Bucket("y", 16))
      .AddField(Expressions::Bucket("z", 4));
  EXPECT_THAT(update_spec->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto new_spec, table_->spec());
  ASSERT_EQ(new_spec->fields().size(), 3U);
  EXPECT_EQ(new_spec->fields()[1].name(), "y_bucket_16");
  EXPECT_EQ(new_spec->fields()[1].transform()->ToString(), "bucket[16]");
  EXPECT_GT(new_spec->fields()[1].field_id(), initial_partition_field_id);
  EXPECT_EQ(new_spec->fields()[2].name(), "z_bucket_4");
  EXPECT_EQ(new_spec->fields()[2].transform()->ToString(), "bucket[4]");
  EXPECT_GT(new_spec->fields()[2].field_id(), new_spec->fields()[1].field_id());

  std::vector<Literal> partition_y{Literal::Long(/*x=*/5L),
                                   Literal::Int(/*y_bucket_16=*/2),
                                   Literal::Int(/*z_bucket_4=*/3)};
  auto file_y = MakeDataFile("y", new_spec, /*record_count=*/1, /*size=*/10,
                             std::move(partition_y));
  std::vector<Literal> partition_z{Literal::Long(/*x=*/7L),
                                   Literal::Int(/*y_bucket_16=*/2),
                                   Literal::Int(/*z_bucket_4=*/4)};
  auto file_z = MakeDataFile("z", new_spec, /*record_count=*/1, /*size=*/10,
                             std::move(partition_z));
  ASSERT_THAT(AppendFiles({file_y}), IsOk());
  ASSERT_THAT(AppendFiles({file_z}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 3U);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  // try to cluster in 1 manifest file, but because of 2 partition specs
  // we should still have 2 manifest files.
  rewrite->ClusterBy([](const DataFile&) { return "file"; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 2U);
  EXPECT_NE(manifests[1].partition_spec_id, manifests[0].partition_spec_id);
  MatchNumberOfManifestFileWithSpecId(manifests, initial_spec_id,
                                      /*expected_manifest_count=*/1);
  MatchNumberOfManifestFileWithSpecId(manifests, new_spec->spec_id(),
                                      /*expected_manifest_count=*/1);

  EXPECT_EQ(manifests[0].existing_files_count.value_or(-1), 2);
  EXPECT_EQ(manifests[1].existing_files_count.value_or(-1), 2);
}

TEST_P(RewriteManifestsTest, ManifestSizeWithMultiplePartitionSpec) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto initial_manifests, CurrentManifests());
  ASSERT_EQ(initial_manifests.size(), 1U);
  const int32_t initial_spec_id = initial_manifests[0].partition_spec_id;

  // Build the new spec using the table's schema, which uses fresh partition IDs.
  ICEBERG_UNWRAP_OR_FAIL(auto update_spec, table_->NewUpdatePartitionSpec());
  update_spec->AddField(Expressions::Bucket("y", 16))
      .AddField(Expressions::Bucket("z", 4));
  EXPECT_THAT(update_spec->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto new_spec, table_->spec());

  std::vector<Literal> partition_y{Literal::Long(/*x=*/5L),
                                   Literal::Int(/*y_bucket_16=*/2),
                                   Literal::Int(/*z_bucket_4=*/3)};
  auto file_y = MakeDataFile("y", new_spec, /*record_count=*/1, /*size=*/10,
                             std::move(partition_y));
  ASSERT_THAT(AppendFiles({file_y}), IsOk());

  std::vector<Literal> partition_z{Literal::Long(/*x=*/7L),
                                   Literal::Int(/*y_bucket_16=*/2),
                                   Literal::Int(/*z_bucket_4=*/4)};
  auto file_z = MakeDataFile("z", new_spec, /*record_count=*/1, /*size=*/10,
                             std::move(partition_z));
  ASSERT_THAT(AppendFiles({file_z}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 3U);

  ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
  props->Set(std::string(TableProperties::kManifestTargetSizeBytes.key()), "1");
  EXPECT_THAT(props->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "file"; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 4U);

  MatchNumberOfManifestFileWithSpecId(manifests, initial_spec_id,
                                      /*expected_manifest_count=*/2);
  MatchNumberOfManifestFileWithSpecId(manifests, new_spec->spec_id(),
                                      /*expected_manifest_count=*/2);

  EXPECT_EQ(manifests[0].existing_files_count.value_or(-1), 1);
  EXPECT_EQ(manifests[1].existing_files_count.value_or(-1), 1);
  EXPECT_EQ(manifests[2].existing_files_count.value_or(-1), 1);
  EXPECT_EQ(manifests[3].existing_files_count.value_or(-1), 1);
}

TEST_P(RewriteManifestsTest, ManifestReplacementConcurrentAppend) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  const int64_t first_snapshot_id = first_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot_manifests, CurrentManifests());
  ASSERT_EQ(first_snapshot_manifests.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_a,
      WriteExistingManifest("concurrent-append-a", first_snapshot_id, {file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_b,
      WriteExistingManifest("concurrent-append-b", first_snapshot_id, {file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(first_snapshot_manifests[0])
      .AddManifest(manifest_a)
      .AddManifest(manifest_b);

  ASSERT_THAT(AppendFiles({file_c_, file_d_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, table_->current_snapshot());
  const int64_t second_snapshot_id = second_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto after_append, CurrentManifests());
  ASSERT_EQ(after_append.size(), 2U);

  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 3U);
  ExpectManifestEntriesWithSnapshotIds(manifests[0], {file_a_->file_path},
                                       {ManifestStatus::kExisting}, {first_snapshot_id});
  ExpectManifestEntriesWithSnapshotIds(manifests[1], {file_b_->file_path},
                                       {ManifestStatus::kExisting}, {first_snapshot_id});
  ExpectManifestEntriesWithSnapshotIds(manifests[2],
                                       {file_c_->file_path, file_d_->file_path},
                                       {ManifestStatus::kAdded, ManifestStatus::kAdded},
                                       {second_snapshot_id, second_snapshot_id});

  ICEBERG_UNWRAP_OR_FAIL(auto after, table_->current_snapshot());
  ValidateSummary(after, /*replaced=*/1, /*kept=*/1, /*created=*/2,
                  /*entry_count=*/0);
}

TEST_P(RewriteManifestsTest, ManifestReplacementConcurrentDelete) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  const int64_t first_snapshot_id = first_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot_manifests, CurrentManifests());
  ASSERT_EQ(first_snapshot_manifests.size(), 1U);

  ASSERT_THAT(AppendFiles({file_c_, file_d_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, table_->current_snapshot());
  const int64_t second_snapshot_id = second_snapshot->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_a,
      WriteExistingManifest("concurrent-delete-a", first_snapshot_id, {file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_b,
      WriteExistingManifest("concurrent-delete-b", first_snapshot_id, {file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(first_snapshot_manifests[0])
      .AddManifest(manifest_a)
      .AddManifest(manifest_b);

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFile(file_c_);
  EXPECT_THAT(delete_files->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto third_snapshot, table_->current_snapshot());
  const int64_t third_snapshot_id = third_snapshot->snapshot_id;

  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 3U);
  ExpectManifestEntriesWithSnapshotIds(manifests[0], {file_a_->file_path},
                                       {ManifestStatus::kExisting}, {first_snapshot_id});
  ExpectManifestEntriesWithSnapshotIds(manifests[1], {file_b_->file_path},
                                       {ManifestStatus::kExisting}, {first_snapshot_id});
  ExpectManifestEntriesWithSnapshotIds(
      manifests[2], {file_c_->file_path, file_d_->file_path},
      {ManifestStatus::kDeleted, ManifestStatus::kExisting},
      {third_snapshot_id, second_snapshot_id});

  ICEBERG_UNWRAP_OR_FAIL(auto after, table_->current_snapshot());
  ValidateSummary(after, /*replaced=*/1, /*kept=*/1, /*created=*/2,
                  /*entry_count=*/0);
}

TEST_P(RewriteManifestsTest, ManifestReplacementConcurrentConflictingDelete) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  const int64_t first_snapshot_id = first_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot_manifests, CurrentManifests());
  ASSERT_EQ(first_snapshot_manifests.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_a,
      WriteExistingManifest("conflicting-delete-a", first_snapshot_id, {file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_b,
      WriteExistingManifest("conflicting-delete-b", first_snapshot_id, {file_b_}));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(first_snapshot_manifests[0])
      .AddManifest(manifest_a)
      .AddManifest(manifest_b);

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFile(file_a_);
  EXPECT_THAT(delete_files->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  auto result = rewrite->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("could not be found in the latest snapshot"));
}

TEST_P(RewriteManifestsTest, ManifestReplacementCombinedWithRewrite) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  const int64_t first_snapshot_id = first_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto first_manifests, CurrentManifests());
  ASSERT_EQ(first_manifests.size(), 1U);

  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, table_->current_snapshot());
  const int64_t second_snapshot_id = second_snapshot->snapshot_id;
  ASSERT_THAT(AppendFiles({file_c_}), IsOk());
  ASSERT_THAT(AppendFiles({file_d_}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(
      auto new_manifest,
      WriteExistingManifest("combined-a", first_snapshot_id, {file_a_}));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(first_manifests[0])
      .AddManifest(new_manifest)
      .ClusterBy([](const DataFile&) { return "const-value"; })
      .RewriteIf([&](const ManifestFile& manifest) {
        auto entries_result = ReadManifestEntries(manifest);
        if (!entries_result.has_value() || entries_result.value().empty()) {
          return false;
        }
        return entries_result.value()[0].data_file->file_path != file_b_->file_path;
      });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 3U);

  ExpectManifestEntriesWithSnapshotIds(manifests[1], {file_a_->file_path},
                                       {ManifestStatus::kExisting}, {first_snapshot_id});

  ExpectManifestEntriesWithSnapshotIds(manifests[2], {file_b_->file_path},
                                       {ManifestStatus::kAdded}, {second_snapshot_id});

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ValidateSummary(snapshot, /*replaced=*/3, /*kept=*/1, /*created=*/2,
                  /*entry_count=*/2);
}

TEST_P(RewriteManifestsTest, ManifestReplacementCombinedWithRewriteConcurrentDelete) {
  SetManifestMergeEnabled(false);

  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  const int64_t first_snapshot_id = first_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot_manifests, CurrentManifests());
  ASSERT_EQ(first_snapshot_manifests.size(), 1U);

  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, table_->current_snapshot());
  const int64_t second_snapshot_id = second_snapshot->snapshot_id;

  ASSERT_THAT(AppendFiles({file_c_}), IsOk());
  ASSERT_EQ(table_->snapshots().size(), 3U);

  ICEBERG_UNWRAP_OR_FAIL(
      auto new_manifest,
      WriteExistingManifest("combined-concurrent-delete-a", first_snapshot_id, {file_a_},
                            ManifestContent::kData, nullptr,
                            first_snapshot->sequence_number));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(first_snapshot_manifests[0])
      .AddManifest(new_manifest)
      .ClusterBy([](const DataFile&) { return "const-value"; });

  EXPECT_THAT(static_cast<SnapshotUpdate&>(*rewrite).Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFile(file_c_);
  EXPECT_THAT(delete_files->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());

  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 2U);

  ExpectManifestEntriesWithSnapshotIds(manifests[0], {file_b_->file_path},
                                       {ManifestStatus::kExisting}, {second_snapshot_id});

  ExpectManifestEntriesWithSnapshotIds(manifests[1], {file_a_->file_path},
                                       {ManifestStatus::kExisting}, {first_snapshot_id});

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ValidateSummary(snapshot, /*replaced=*/3, /*kept=*/0, /*created=*/2,
                  /*entry_count=*/1);
}

TEST_P(RewriteManifestsTest, InvalidUsage) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 1U);
  const auto manifest = manifests[0];

  ICEBERG_UNWRAP_OR_FAIL(
      auto invalid_added_manifest,
      WriteAddedManifest("manifest-file-2", {file_a_}, snapshot->snapshot_id,
                         snapshot->sequence_number));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite_added, table_->NewRewriteManifests());
  rewrite_added->DeleteManifest(manifest).AddManifest(invalid_added_manifest);
  auto added_result = rewrite_added->Commit();
  EXPECT_THAT(added_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(added_result, HasErrorMessage("Cannot add manifest with added files"));

  ICEBERG_UNWRAP_OR_FAIL(
      auto invalid_deleted_manifest,
      WriteDeletedManifest("manifest-file-3", {file_a_}, snapshot->snapshot_id,
                           snapshot->sequence_number));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite_deleted, table_->NewRewriteManifests());
  rewrite_deleted->DeleteManifest(manifest).AddManifest(invalid_deleted_manifest);
  auto deleted_result = rewrite_deleted->Commit();
  EXPECT_THAT(deleted_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(deleted_result, HasErrorMessage("Cannot add manifest with deleted files"));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite_missing, table_->NewRewriteManifests());
  rewrite_missing->DeleteManifest(manifest);
  auto missing_result = rewrite_missing->Commit();
  EXPECT_THAT(missing_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(missing_result,
              HasErrorMessage("Replaced and created manifests must have the same"));
}

TEST_P(RewriteManifestsTest, ManifestReplacementFailure) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  const int64_t first_snapshot_id = first_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot_manifests, CurrentManifests());
  ASSERT_EQ(first_snapshot_manifests.size(), 1U);
  const auto first_snapshot_manifest = first_snapshot_manifests[0];

  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, table_->current_snapshot());
  const int64_t second_snapshot_id = second_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot_manifests, CurrentManifests());
  ASSERT_EQ(second_snapshot_manifests.size(), 2U);
  const auto second_snapshot_manifest = second_snapshot_manifests[0];

  ICEBERG_UNWRAP_OR_FAIL(
      auto new_manifest,
      WriteExistingManifest("manifest-file", {{file_a_, first_snapshot_id},
                                              {file_b_, second_snapshot_id}}));
  EXPECT_TRUE(FileExists(new_manifest.manifest_path));

  SetCommitRetryProperties(1);
  BindTableWithFailingCommits(/*failures=*/5);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(first_snapshot_manifest)
      .DeleteManifest(second_snapshot_manifest)
      .AddManifest(new_manifest);

  auto result = rewrite->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(result, HasErrorMessage("Injected failure"));
  EXPECT_TRUE(FileExists(new_manifest.manifest_path));
}

TEST_P(RewriteManifestsTest, ManifestReplacementFailureWithSnapshotIdInheritance) {
  SetSnapshotIdInheritanceEnabled();

  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  const int64_t first_snapshot_id = first_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot_manifests, CurrentManifests());
  ASSERT_EQ(first_snapshot_manifests.size(), 1U);
  const auto first_snapshot_manifest = first_snapshot_manifests[0];

  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot, table_->current_snapshot());
  const int64_t second_snapshot_id = second_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto second_snapshot_manifests, CurrentManifests());
  ASSERT_EQ(second_snapshot_manifests.size(), 2U);
  const auto second_snapshot_manifest = second_snapshot_manifests[0];

  ICEBERG_UNWRAP_OR_FAIL(
      auto new_manifest,
      WriteExistingManifest("manifest-file", {{file_a_, first_snapshot_id},
                                              {file_b_, second_snapshot_id}}));
  EXPECT_TRUE(FileExists(new_manifest.manifest_path));

  SetCommitRetryProperties(1);
  BindTableWithFailingCommits(/*failures=*/5);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(first_snapshot_manifest)
      .DeleteManifest(second_snapshot_manifest)
      .AddManifest(new_manifest);

  auto result = rewrite->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(result, HasErrorMessage("Injected failure"));
  EXPECT_TRUE(FileExists(new_manifest.manifest_path));
}

TEST_P(RewriteManifestsTest, RewriteManifestsOnBranchUnsupported) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ToBranch("someBranch");
  auto result = rewrite->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result,
              HasErrorMessage("Cannot commit to branch someBranch: RewriteManifests "
                              "does not support branch commits"));
}

TEST_P(RewriteManifestsTest, RewriteDataManifestsPreservesDeletes) {
  if (GetParam() == 1) {
    GTEST_SKIP() << "Delete manifests require format version 2 or higher";
  }

  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot, table_->current_snapshot());
  const int64_t append_snapshot_id = append_snapshot->snapshot_id;
  const int64_t append_snapshot_sequence_number = append_snapshot->sequence_number;

  auto delete_a = MakeDeleteFile("a-pos-deletes", 1, file_a_->file_path);
  auto delete_a2 = MakeEqualityDeleteFile("a2-deletes", 1);
  ASSERT_THAT(AppendDeleteFiles({delete_a, delete_a2}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto delete_snapshot, table_->current_snapshot());
  const int64_t delete_snapshot_id = delete_snapshot->snapshot_id;
  const int64_t delete_snapshot_sequence_number = delete_snapshot->sequence_number;

  ICEBERG_UNWRAP_OR_FAIL(auto data_before, CurrentManifests());
  ICEBERG_UNWRAP_OR_FAIL(auto deletes_before, CurrentDeleteManifests());
  ASSERT_EQ(data_before.size(), 1U);
  ASSERT_EQ(deletes_before.size(), 1U);
  const std::string delete_manifest_path = deletes_before[0].manifest_path;

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile& file) { return file.file_path; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentManifests());
  ASSERT_EQ(data_manifests.size(), 2U);
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifest_a,
                         ManifestContainingPath(data_manifests, file_a_->file_path));
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifest_b,
                         ManifestContainingPath(data_manifests, file_b_->file_path));
  ExpectManifestEntriesWithSequenceNumbers(
      *data_manifest_a, {file_a_->file_path}, {ManifestStatus::kExisting},
      {append_snapshot_id}, {append_snapshot_sequence_number},
      {append_snapshot_sequence_number});
  ExpectManifestEntriesWithSequenceNumbers(
      *data_manifest_b, {file_b_->file_path}, {ManifestStatus::kExisting},
      {append_snapshot_id}, {append_snapshot_sequence_number},
      {append_snapshot_sequence_number});

  ICEBERG_UNWRAP_OR_FAIL(auto delete_manifests, CurrentDeleteManifests());
  ASSERT_EQ(delete_manifests.size(), 1U);
  EXPECT_EQ(delete_manifests[0].manifest_path, delete_manifest_path);
  ExpectManifestEntriesWithSequenceNumbers(
      delete_manifests[0], {delete_a->file_path, delete_a2->file_path},
      {ManifestStatus::kAdded, ManifestStatus::kAdded},
      {delete_snapshot_id, delete_snapshot_id},
      {delete_snapshot_sequence_number, delete_snapshot_sequence_number},
      {delete_snapshot_sequence_number, delete_snapshot_sequence_number});
  ICEBERG_UNWRAP_OR_FAIL(auto delete_entries, ReadManifestEntries(delete_manifests[0]));
  std::unordered_map<std::string, DataFile::Content> delete_content_by_path;
  for (const auto& entry : delete_entries) {
    ASSERT_NE(entry.data_file, nullptr);
    delete_content_by_path.emplace(entry.data_file->file_path, entry.data_file->content);
  }
  EXPECT_EQ(delete_content_by_path.at(delete_a->file_path),
            DataFile::Content::kPositionDeletes);
  EXPECT_EQ(delete_content_by_path.at(delete_a2->file_path),
            DataFile::Content::kEqualityDeletes);
}

TEST_P(RewriteManifestsTest, ReplaceDeleteManifestsOnly) {
  if (GetParam() == 1) {
    GTEST_SKIP() << "Delete manifests require format version 2 or higher";
  }

  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot, table_->current_snapshot());
  const int64_t append_snapshot_id = append_snapshot->snapshot_id;
  const int64_t append_snapshot_sequence_number = append_snapshot->sequence_number;

  auto delete_a = MakeDeleteFile("replace-delete-a", 1, file_a_->file_path);
  auto delete_a2 = MakeEqualityDeleteFile("replace-delete-a2", 1);
  ASSERT_THAT(AppendDeleteFiles({delete_a, delete_a2}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto delete_snapshot, table_->current_snapshot());
  const int64_t delete_snapshot_id = delete_snapshot->snapshot_id;
  const int64_t delete_snapshot_sequence_number = delete_snapshot->sequence_number;
  ICEBERG_UNWRAP_OR_FAIL(auto original_delete_manifests, CurrentDeleteManifests());
  ASSERT_EQ(original_delete_manifests.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(
      auto new_delete_manifest_a,
      WriteExistingManifest("delete-manifest-file-1", delete_snapshot_id, {delete_a},
                            ManifestContent::kDeletes, nullptr,
                            delete_snapshot_sequence_number,
                            delete_snapshot_sequence_number));
  ICEBERG_UNWRAP_OR_FAIL(
      auto new_delete_manifest_a2,
      WriteExistingManifest("delete-manifest-file-2", delete_snapshot_id, {delete_a2},
                            ManifestContent::kDeletes, nullptr,
                            delete_snapshot_sequence_number,
                            delete_snapshot_sequence_number));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(original_delete_manifests[0])
      .AddManifest(new_delete_manifest_a)
      .AddManifest(new_delete_manifest_a2);
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentManifests());
  ASSERT_EQ(data_manifests.size(), 1U);
  ExpectManifestEntriesWithSequenceNumbers(
      data_manifests[0], {file_a_->file_path, file_b_->file_path},
      {ManifestStatus::kAdded, ManifestStatus::kAdded},
      {append_snapshot_id, append_snapshot_id},
      {append_snapshot_sequence_number, append_snapshot_sequence_number},
      {append_snapshot_sequence_number, append_snapshot_sequence_number});

  ICEBERG_UNWRAP_OR_FAIL(auto delete_manifests, CurrentDeleteManifests());
  ASSERT_EQ(delete_manifests.size(), 2U);
  ExpectManifestEntriesWithSequenceNumbers(
      delete_manifests[0], {delete_a->file_path}, {ManifestStatus::kExisting},
      {delete_snapshot_id}, {delete_snapshot_sequence_number},
      {delete_snapshot_sequence_number});
  ExpectManifestEntriesWithSequenceNumbers(
      delete_manifests[1], {delete_a2->file_path}, {ManifestStatus::kExisting},
      {delete_snapshot_id}, {delete_snapshot_sequence_number},
      {delete_snapshot_sequence_number});
  ICEBERG_UNWRAP_OR_FAIL(auto delete_entries_a, ReadManifestEntries(delete_manifests[0]));
  ASSERT_EQ(delete_entries_a.size(), 1U);
  ASSERT_NE(delete_entries_a[0].data_file, nullptr);
  EXPECT_EQ(delete_entries_a[0].data_file->content, DataFile::Content::kPositionDeletes);
  ICEBERG_UNWRAP_OR_FAIL(auto delete_entries_a2,
                         ReadManifestEntries(delete_manifests[1]));
  ASSERT_EQ(delete_entries_a2.size(), 1U);
  ASSERT_NE(delete_entries_a2[0].data_file, nullptr);
  EXPECT_EQ(delete_entries_a2[0].data_file->content, DataFile::Content::kEqualityDeletes);
}

TEST_P(RewriteManifestsTest, ReplaceDataAndDeleteManifests) {
  if (GetParam() == 1) {
    GTEST_SKIP() << "Delete manifests require format version 2 or higher";
  }

  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot, table_->current_snapshot());
  const int64_t append_snapshot_id = append_snapshot->snapshot_id;
  const int64_t append_snapshot_sequence_number = append_snapshot->sequence_number;

  auto delete_a = MakeDeleteFile("replace-data-delete-a", 1, file_a_->file_path);
  auto delete_a2 = MakeEqualityDeleteFile("replace-data-delete-a2", 1);
  ASSERT_THAT(AppendDeleteFiles({delete_a, delete_a2}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto delete_snapshot, table_->current_snapshot());
  const int64_t delete_snapshot_id = delete_snapshot->snapshot_id;
  const int64_t delete_snapshot_sequence_number = delete_snapshot->sequence_number;
  ICEBERG_UNWRAP_OR_FAIL(auto original_data_manifests, CurrentManifests());
  ICEBERG_UNWRAP_OR_FAIL(auto original_delete_manifests, CurrentDeleteManifests());
  ASSERT_EQ(original_data_manifests.size(), 1U);
  ASSERT_EQ(original_delete_manifests.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(auto new_data_manifest_a,
                         WriteExistingManifest("manifest-file-1", append_snapshot_id,
                                               {file_a_}, ManifestContent::kData, nullptr,
                                               append_snapshot_sequence_number,
                                               append_snapshot_sequence_number));
  ICEBERG_UNWRAP_OR_FAIL(auto new_data_manifest_b,
                         WriteExistingManifest("manifest-file-2", append_snapshot_id,
                                               {file_b_}, ManifestContent::kData, nullptr,
                                               append_snapshot_sequence_number,
                                               append_snapshot_sequence_number));
  ICEBERG_UNWRAP_OR_FAIL(
      auto new_delete_manifest_a,
      WriteExistingManifest("delete-manifest-file-1", delete_snapshot_id, {delete_a},
                            ManifestContent::kDeletes, nullptr,
                            delete_snapshot_sequence_number,
                            delete_snapshot_sequence_number));
  ICEBERG_UNWRAP_OR_FAIL(
      auto new_delete_manifest_a2,
      WriteExistingManifest("delete-manifest-file-2", delete_snapshot_id, {delete_a2},
                            ManifestContent::kDeletes, nullptr,
                            delete_snapshot_sequence_number,
                            delete_snapshot_sequence_number));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(original_data_manifests[0])
      .AddManifest(new_data_manifest_a)
      .AddManifest(new_data_manifest_b)
      .DeleteManifest(original_delete_manifests[0])
      .AddManifest(new_delete_manifest_a)
      .AddManifest(new_delete_manifest_a2);
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentManifests());
  ASSERT_EQ(data_manifests.size(), 2U);
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifest_a,
                         ManifestContainingPath(data_manifests, file_a_->file_path));
  ICEBERG_UNWRAP_OR_FAIL(auto data_manifest_b,
                         ManifestContainingPath(data_manifests, file_b_->file_path));
  ExpectManifestEntriesWithSequenceNumbers(
      *data_manifest_a, {file_a_->file_path}, {ManifestStatus::kExisting},
      {append_snapshot_id}, {append_snapshot_sequence_number},
      {append_snapshot_sequence_number});
  ExpectManifestEntriesWithSequenceNumbers(
      *data_manifest_b, {file_b_->file_path}, {ManifestStatus::kExisting},
      {append_snapshot_id}, {append_snapshot_sequence_number},
      {append_snapshot_sequence_number});

  ICEBERG_UNWRAP_OR_FAIL(auto delete_manifests, CurrentDeleteManifests());
  ASSERT_EQ(delete_manifests.size(), 2U);
  ExpectManifestEntriesWithSequenceNumbers(
      delete_manifests[0], {delete_a->file_path}, {ManifestStatus::kExisting},
      {delete_snapshot_id}, {delete_snapshot_sequence_number},
      {delete_snapshot_sequence_number});
  ExpectManifestEntryContents(delete_manifests[0], {delete_a->file_path},
                              {DataFile::Content::kPositionDeletes});
  ExpectManifestEntriesWithSequenceNumbers(
      delete_manifests[1], {delete_a2->file_path}, {ManifestStatus::kExisting},
      {delete_snapshot_id}, {delete_snapshot_sequence_number},
      {delete_snapshot_sequence_number});
  ExpectManifestEntryContents(delete_manifests[1], {delete_a2->file_path},
                              {DataFile::Content::kEqualityDeletes});
}

TEST_P(RewriteManifestsTest, DeleteManifestReplacementConcurrentAppend) {
  if (GetParam() == 1) {
    GTEST_SKIP() << "Delete manifests require format version 2 or higher";
  }

  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot, table_->current_snapshot());
  const int64_t append_snapshot_id = append_snapshot->snapshot_id;
  const int64_t append_snapshot_sequence_number = append_snapshot->sequence_number;

  auto delete_a = MakeDeleteFile("concurrent-append-delete-a", 1, file_a_->file_path);
  auto delete_a2 = MakeEqualityDeleteFile("concurrent-append-delete-a2", 1);
  ASSERT_THAT(AppendDeleteFiles({delete_a, delete_a2}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto delete_snapshot, table_->current_snapshot());
  const int64_t delete_snapshot_id = delete_snapshot->snapshot_id;
  const int64_t delete_snapshot_sequence_number = delete_snapshot->sequence_number;
  ICEBERG_UNWRAP_OR_FAIL(auto original_delete_manifests, CurrentDeleteManifests());
  ASSERT_EQ(original_delete_manifests.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(
      auto new_delete_manifest_a,
      WriteExistingManifest("delete-manifest-file-1", delete_snapshot_id, {delete_a},
                            ManifestContent::kDeletes, nullptr,
                            delete_snapshot_sequence_number,
                            delete_snapshot_sequence_number));
  ICEBERG_UNWRAP_OR_FAIL(
      auto new_delete_manifest_a2,
      WriteExistingManifest("delete-manifest-file-2", delete_snapshot_id, {delete_a2},
                            ManifestContent::kDeletes, nullptr,
                            delete_snapshot_sequence_number,
                            delete_snapshot_sequence_number));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(original_delete_manifests[0])
      .AddManifest(new_delete_manifest_a)
      .AddManifest(new_delete_manifest_a2);

  ASSERT_THAT(AppendFiles({file_c_, file_d_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto concurrent_snapshot, table_->current_snapshot());
  const int64_t concurrent_snapshot_id = concurrent_snapshot->snapshot_id;
  const int64_t concurrent_snapshot_sequence_number =
      concurrent_snapshot->sequence_number;
  ICEBERG_UNWRAP_OR_FAIL(auto data_before_rewrite, CurrentManifests());
  ICEBERG_UNWRAP_OR_FAIL(auto deletes_before_rewrite, CurrentDeleteManifests());
  ASSERT_EQ(data_before_rewrite.size(), 2U);
  ASSERT_EQ(deletes_before_rewrite.size(), 1U);

  EXPECT_THAT(rewrite->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto rewrite_snapshot, table_->current_snapshot());
  ValidateSummary(rewrite_snapshot, /*replaced=*/1, /*kept=*/2, /*created=*/2,
                  /*entry_count=*/0);

  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentManifests());
  ASSERT_EQ(data_manifests.size(), 2U);
  ICEBERG_UNWRAP_OR_FAIL(auto original_data_manifest,
                         ManifestContainingPath(data_manifests, file_a_->file_path));
  ICEBERG_UNWRAP_OR_FAIL(auto concurrent_data_manifest,
                         ManifestContainingPath(data_manifests, file_c_->file_path));
  ExpectManifestEntriesWithSequenceNumbers(
      *original_data_manifest, {file_a_->file_path, file_b_->file_path},
      {ManifestStatus::kAdded, ManifestStatus::kAdded},
      {append_snapshot_id, append_snapshot_id},
      {append_snapshot_sequence_number, append_snapshot_sequence_number},
      {append_snapshot_sequence_number, append_snapshot_sequence_number});
  ExpectManifestEntriesWithSequenceNumbers(
      *concurrent_data_manifest, {file_c_->file_path, file_d_->file_path},
      {ManifestStatus::kAdded, ManifestStatus::kAdded},
      {concurrent_snapshot_id, concurrent_snapshot_id},
      {concurrent_snapshot_sequence_number, concurrent_snapshot_sequence_number},
      {concurrent_snapshot_sequence_number, concurrent_snapshot_sequence_number});

  ICEBERG_UNWRAP_OR_FAIL(auto delete_manifests, CurrentDeleteManifests());
  ASSERT_EQ(delete_manifests.size(), 2U);
  ExpectManifestEntriesWithSequenceNumbers(
      delete_manifests[0], {delete_a->file_path}, {ManifestStatus::kExisting},
      {delete_snapshot_id}, {delete_snapshot_sequence_number},
      {delete_snapshot_sequence_number});
  ExpectManifestEntryContents(delete_manifests[0], {delete_a->file_path},
                              {DataFile::Content::kPositionDeletes});
  ExpectManifestEntriesWithSequenceNumbers(
      delete_manifests[1], {delete_a2->file_path}, {ManifestStatus::kExisting},
      {delete_snapshot_id}, {delete_snapshot_sequence_number},
      {delete_snapshot_sequence_number});
  ExpectManifestEntryContents(delete_manifests[1], {delete_a2->file_path},
                              {DataFile::Content::kEqualityDeletes});
}

TEST_P(RewriteManifestsTest, DeleteManifestReplacementConcurrentDeleteFileRemoval) {
  if (GetParam() == 1) {
    GTEST_SKIP() << "Delete manifests require format version 2 or higher";
  }

  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot, table_->current_snapshot());
  const int64_t append_snapshot_id = append_snapshot->snapshot_id;
  const int64_t append_snapshot_sequence_number = append_snapshot->sequence_number;

  auto delete_a = MakeDeleteFile("concurrent-removal-delete-a", 1, file_a_->file_path);
  auto delete_a2 = MakeEqualityDeleteFile("concurrent-removal-delete-a2", 1);
  ASSERT_THAT(AppendDeleteFiles({delete_a, delete_a2}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_delete_snapshot, table_->current_snapshot());
  const int64_t first_delete_snapshot_id = first_delete_snapshot->snapshot_id;
  const int64_t first_delete_snapshot_sequence_number =
      first_delete_snapshot->sequence_number;
  ICEBERG_UNWRAP_OR_FAIL(auto first_delete_manifests, CurrentDeleteManifests());
  ASSERT_EQ(first_delete_manifests.size(), 1U);
  const auto original_delete_manifest = first_delete_manifests[0];

  auto delete_b = MakeDeleteFile("concurrent-removal-delete-b", 2, file_b_->file_path);
  auto delete_c2 = MakeEqualityDeleteFile("concurrent-removal-delete-c2", 3);
  ASSERT_THAT(AppendDeleteFiles({delete_b, delete_c2}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_delete_snapshot, table_->current_snapshot());
  const int64_t second_delete_snapshot_id = second_delete_snapshot->snapshot_id;
  const int64_t second_delete_snapshot_sequence_number =
      second_delete_snapshot->sequence_number;

  ICEBERG_UNWRAP_OR_FAIL(
      auto new_delete_manifest_a,
      WriteExistingManifest("delete-concurrent-removal-a", first_delete_snapshot_id,
                            {delete_a}, ManifestContent::kDeletes, nullptr,
                            first_delete_snapshot_sequence_number,
                            first_delete_snapshot_sequence_number));
  ICEBERG_UNWRAP_OR_FAIL(
      auto new_delete_manifest_a2,
      WriteExistingManifest("delete-concurrent-removal-a2", first_delete_snapshot_id,
                            {delete_a2}, ManifestContent::kDeletes, nullptr,
                            first_delete_snapshot_sequence_number,
                            first_delete_snapshot_sequence_number));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(original_delete_manifest)
      .AddManifest(new_delete_manifest_a)
      .AddManifest(new_delete_manifest_a2);

  ASSERT_THAT(RemoveDeleteFiles({delete_b}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto concurrent_snapshot, table_->current_snapshot());
  const int64_t concurrent_snapshot_id = concurrent_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto data_before_rewrite, CurrentManifests());
  ICEBERG_UNWRAP_OR_FAIL(auto deletes_before_rewrite, CurrentDeleteManifests());
  ASSERT_EQ(data_before_rewrite.size(), 1U);
  ASSERT_EQ(deletes_before_rewrite.size(), 2U);

  EXPECT_THAT(rewrite->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto rewrite_snapshot, table_->current_snapshot());
  ValidateSummary(rewrite_snapshot, /*replaced=*/1, /*kept=*/2, /*created=*/2,
                  /*entry_count=*/0);

  ICEBERG_UNWRAP_OR_FAIL(auto data_manifests, CurrentManifests());
  ASSERT_EQ(data_manifests.size(), 1U);
  ExpectManifestEntriesWithSequenceNumbers(
      data_manifests[0], {file_a_->file_path, file_b_->file_path},
      {ManifestStatus::kAdded, ManifestStatus::kAdded},
      {append_snapshot_id, append_snapshot_id},
      {append_snapshot_sequence_number, append_snapshot_sequence_number},
      {append_snapshot_sequence_number, append_snapshot_sequence_number});

  ICEBERG_UNWRAP_OR_FAIL(auto delete_manifests, CurrentDeleteManifests());
  ASSERT_EQ(delete_manifests.size(), 3U);
  ICEBERG_UNWRAP_OR_FAIL(auto delete_manifest_a,
                         ManifestContainingPath(delete_manifests, delete_a->file_path));
  ICEBERG_UNWRAP_OR_FAIL(auto delete_manifest_a2,
                         ManifestContainingPath(delete_manifests, delete_a2->file_path));
  ICEBERG_UNWRAP_OR_FAIL(auto delete_manifest_b,
                         ManifestContainingPath(delete_manifests, delete_b->file_path));
  ExpectManifestEntriesWithSequenceNumbers(
      *delete_manifest_a, {delete_a->file_path}, {ManifestStatus::kExisting},
      {first_delete_snapshot_id}, {first_delete_snapshot_sequence_number},
      {first_delete_snapshot_sequence_number});
  ExpectManifestEntryContents(*delete_manifest_a, {delete_a->file_path},
                              {DataFile::Content::kPositionDeletes});
  ExpectManifestEntriesWithSequenceNumbers(
      *delete_manifest_a2, {delete_a2->file_path}, {ManifestStatus::kExisting},
      {first_delete_snapshot_id}, {first_delete_snapshot_sequence_number},
      {first_delete_snapshot_sequence_number});
  ExpectManifestEntryContents(*delete_manifest_a2, {delete_a2->file_path},
                              {DataFile::Content::kEqualityDeletes});
  ExpectManifestEntriesWithSequenceNumbers(
      *delete_manifest_b, {delete_b->file_path, delete_c2->file_path},
      {ManifestStatus::kDeleted, ManifestStatus::kExisting},
      {concurrent_snapshot_id, second_delete_snapshot_id},
      {second_delete_snapshot_sequence_number, second_delete_snapshot_sequence_number},
      {second_delete_snapshot_sequence_number, second_delete_snapshot_sequence_number});
  ExpectManifestEntryContents(
      *delete_manifest_b, {delete_b->file_path, delete_c2->file_path},
      {DataFile::Content::kPositionDeletes, DataFile::Content::kEqualityDeletes});
}

TEST_P(RewriteManifestsTest, DeleteManifestReplacementConflictingDeleteFileRemoval) {
  if (GetParam() == 1) {
    GTEST_SKIP() << "Delete manifests require format version 2 or higher";
  }

  ASSERT_THAT(AppendFiles({file_a_, file_b_, file_c_}), IsOk());

  auto delete_a = MakeDeleteFile("conflicting-removal-delete-a", 1, file_a_->file_path);
  auto delete_a2 = MakeEqualityDeleteFile("conflicting-removal-delete-a2", 1);
  ASSERT_THAT(AppendDeleteFiles({delete_a, delete_a2}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto delete_snapshot, table_->current_snapshot());
  const int64_t delete_snapshot_id = delete_snapshot->snapshot_id;
  const int64_t delete_snapshot_sequence_number = delete_snapshot->sequence_number;
  ICEBERG_UNWRAP_OR_FAIL(auto original_delete_manifests, CurrentDeleteManifests());
  ASSERT_EQ(original_delete_manifests.size(), 1U);
  const auto original_delete_manifest = original_delete_manifests[0];

  ICEBERG_UNWRAP_OR_FAIL(
      auto new_delete_manifest_a,
      WriteExistingManifest("delete-conflicting-removal-a", delete_snapshot_id,
                            {delete_a}, ManifestContent::kDeletes, nullptr,
                            delete_snapshot_sequence_number,
                            delete_snapshot_sequence_number));
  ICEBERG_UNWRAP_OR_FAIL(
      auto new_delete_manifest_a2,
      WriteExistingManifest("delete-conflicting-removal-a2", delete_snapshot_id,
                            {delete_a2}, ManifestContent::kDeletes, nullptr,
                            delete_snapshot_sequence_number,
                            delete_snapshot_sequence_number));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(original_delete_manifest)
      .AddManifest(new_delete_manifest_a)
      .AddManifest(new_delete_manifest_a2);

  ASSERT_THAT(RemoveDeleteFiles({delete_a}), IsOk());

  auto result = rewrite->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage(std::format(
                          "Deleted manifest {} could not be found in the latest snapshot",
                          original_delete_manifest.manifest_path)));
}

TEST_P(RewriteManifestsTest, DeleteManifestReplacementFailure) {
  if (GetParam() == 1) {
    GTEST_SKIP() << "Delete manifests require format version 2 or higher";
  }

  ASSERT_THAT(AppendFiles({file_a_}), IsOk());

  auto delete_a = MakeDeleteFile("failure-delete-a", 1, file_a_->file_path);
  ASSERT_THAT(AppendDeleteFiles({delete_a}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_delete_snapshot, table_->current_snapshot());
  const int64_t first_delete_snapshot_id = first_delete_snapshot->snapshot_id;
  const int64_t first_delete_snapshot_sequence_number =
      first_delete_snapshot->sequence_number;

  auto delete_a2 = MakeEqualityDeleteFile("failure-delete-a2", 1);
  ASSERT_THAT(AppendDeleteFiles({delete_a2}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto second_delete_snapshot, table_->current_snapshot());
  const int64_t second_delete_snapshot_id = second_delete_snapshot->snapshot_id;
  const int64_t second_delete_snapshot_sequence_number =
      second_delete_snapshot->sequence_number;
  ICEBERG_UNWRAP_OR_FAIL(auto original_delete_manifests, CurrentDeleteManifests());
  ASSERT_EQ(original_delete_manifests.size(), 2U);

  ICEBERG_UNWRAP_OR_FAIL(
      auto new_delete_manifest,
      WriteExistingDeleteManifest(
          "delete-manifest-file",
          {{delete_a, first_delete_snapshot_id, first_delete_snapshot_sequence_number,
            first_delete_snapshot_sequence_number},
           {delete_a2, second_delete_snapshot_id, second_delete_snapshot_sequence_number,
            second_delete_snapshot_sequence_number}}));
  EXPECT_TRUE(FileExists(new_delete_manifest.manifest_path));

  SetCommitRetryProperties(1);
  BindTableWithFailingCommits(/*failures=*/5);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  for (const auto& original_delete_manifest : original_delete_manifests) {
    rewrite->DeleteManifest(original_delete_manifest);
  }
  rewrite->AddManifest(new_delete_manifest);

  auto result = rewrite->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(result, HasErrorMessage("Injected failure"));
  EXPECT_TRUE(FileExists(new_delete_manifest.manifest_path));
}

INSTANTIATE_TEST_SUITE_P(RewriteManifestVersions, RewriteManifestsTest,
                         ::testing::Values<int8_t>(1, 2, 3),
                         [](const ::testing::TestParamInfo<int8_t>& info) {
                           return "V" + std::to_string(info.param);
                         });

}  // namespace iceberg
