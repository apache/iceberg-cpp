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
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class RewriteManifestsTest : public UpdateTestBase,
                             public ::testing::WithParamInterface<int8_t> {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  void SetUp() override {
    table_ident_ = TableIdentifier{.name = TableName()};
    table_location_ = "/warehouse/" + TableName();

    InitializeFileIO();
    RegisterMinimalTable(GetParam());

    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());
    file_a_ = MakeDataFile("a", 10, 100, 1);
    file_b_ = MakeDataFile("b", 20, 200, 2);
    file_c_ = MakeDataFile("c", 30, 300, 3);
    file_d_ = MakeDataFile("d", 40, 400, 4);
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

  std::shared_ptr<DataFile> MakeDataFile(const std::string& name, int64_t record_count,
                                         int64_t size, int64_t partition_value) {
    auto file = std::make_shared<DataFile>();
    file->content = DataFile::Content::kData;
    file->file_path = std::format("{}/data/{}.parquet", table_location_, name);
    file->file_format = FileFormatType::kParquet;
    file->partition =
        PartitionValues(std::vector<Literal>{Literal::Long(partition_value)});
    file->record_count = record_count;
    file->file_size_in_bytes = size;
    file->partition_spec_id = spec_->spec_id();
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

  Result<std::vector<ManifestFile>> CurrentManifests() {
    ICEBERG_RETURN_UNEXPECTED(table_->Refresh());
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    SnapshotCache cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, cache.DataManifests(file_io_));
    return std::vector<ManifestFile>(manifests.begin(), manifests.end());
  }

  Result<std::vector<ManifestEntry>> Entries(const ManifestFile& manifest) {
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            ManifestReader::Make(manifest, file_io_, schema_, spec_));
    return reader->Entries();
  }

  Result<ManifestFile> WriteExistingManifest(
      const std::string& name, int64_t snapshot_id,
      const std::vector<std::shared_ptr<DataFile>>& files) {
    auto path = std::format("{}/metadata/{}.avro", table_location_, name);
    std::vector<std::shared_ptr<DataFile>> files_to_write;
    files_to_write.reserve(files.size());
    std::optional<int64_t> first_row_id = std::nullopt;
    if (table_->metadata()->format_version >= 3) {
      first_row_id = TableMetadata::kInitialRowId;
      ICEBERG_ASSIGN_OR_RAISE(auto current_manifests, CurrentManifests());
      std::unordered_map<std::string, std::shared_ptr<DataFile>> current_files_by_path;
      for (const auto& manifest : current_manifests) {
        ICEBERG_ASSIGN_OR_RAISE(auto entries, Entries(manifest));
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
        auto writer, ManifestWriter::MakeWriter(
                         table_->metadata()->format_version, kInvalidSnapshotId, path,
                         file_io_, spec_, schema_, ManifestContent::kData, first_row_id));
    for (const auto& file : files_to_write) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(
          file, snapshot_id, TableMetadata::kInitialSequenceNumber));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  Result<ManifestFile> WriteAddedManifest(
      const std::string& name, const std::vector<std::shared_ptr<DataFile>>& files,
      std::optional<int64_t> entry_snapshot_id = std::nullopt) {
    auto path = std::format("{}/metadata/{}.avro", table_location_, name);
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, ManifestWriter::MakeWriter(table_->metadata()->format_version,
                                                entry_snapshot_id, path, file_io_, spec_,
                                                schema_, ManifestContent::kData));
    for (const auto& file : files) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(file));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto manifest, writer->ToManifestFile());
    manifest.added_snapshot_id = kInvalidSnapshotId;
    return manifest;
  }

  Result<ManifestFile> WriteDeletedManifest(
      const std::string& name, const std::vector<std::shared_ptr<DataFile>>& files) {
    auto path = std::format("{}/metadata/{}.avro", table_location_, name);
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, ManifestWriter::MakeWriter(table_->metadata()->format_version,
                                                kInvalidSnapshotId, path, file_io_, spec_,
                                                schema_, ManifestContent::kData));
    for (const auto& file : files) {
      ICEBERG_RETURN_UNEXPECTED(
          writer->WriteDeletedEntry(file, TableMetadata::kInitialSequenceNumber));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  void ExpectEntryPathsAndStatuses(const ManifestFile& manifest,
                                   std::vector<std::string> expected_paths,
                                   std::vector<ManifestStatus> expected_statuses) {
    ICEBERG_UNWRAP_OR_FAIL(auto entries, Entries(manifest));
    ASSERT_EQ(entries.size(), expected_paths.size());
    std::vector<std::pair<std::string, ManifestStatus>> actual;
    for (const auto& entry : entries) {
      ASSERT_NE(entry.data_file, nullptr);
      actual.emplace_back(entry.data_file->file_path, entry.status);
    }
    std::vector<std::pair<std::string, ManifestStatus>> expected;
    for (size_t i = 0; i < expected_paths.size(); ++i) {
      expected.emplace_back(std::move(expected_paths[i]), expected_statuses[i]);
    }
    std::ranges::sort(actual);
    std::ranges::sort(expected);
    EXPECT_EQ(actual, expected);
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
  std::shared_ptr<DataFile> file_c_;
  std::shared_ptr<DataFile> file_d_;
};

TEST_P(RewriteManifestsTest, RewriteManifestsAppendedDirectly) {
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
  ICEBERG_UNWRAP_OR_FAIL(auto entries, Entries(manifests[0]));
  ASSERT_EQ(entries.size(), 1U);
  EXPECT_EQ(entries[0].snapshot_id, append_snapshot_id);
  ASSERT_NE(entries[0].data_file, nullptr);
  EXPECT_EQ(entries[0].data_file->file_path, file_a_->file_path);
  EXPECT_EQ(entries[0].status, ManifestStatus::kExisting);
}

TEST_P(RewriteManifestsTest, RewriteManifestsGeneratedAndAppendedDirectly) {
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

  ICEBERG_UNWRAP_OR_FAIL(auto entries, Entries(manifests[0]));
  ASSERT_EQ(entries.size(), 2U);
  std::vector<std::pair<std::string, int64_t>> actual;
  for (const auto& entry : entries) {
    ASSERT_NE(entry.data_file, nullptr);
    EXPECT_EQ(entry.status, ManifestStatus::kExisting);
    ASSERT_TRUE(entry.snapshot_id.has_value());
    actual.emplace_back(entry.data_file->file_path, entry.snapshot_id.value());
  }
  std::ranges::sort(actual);
  EXPECT_EQ(actual, (std::vector<std::pair<std::string, int64_t>>{
                        {file_a_->file_path, manifest_append_id},
                        {file_b_->file_path, file_append_id}}));
}

TEST_P(RewriteManifestsTest, ReplaceManifestsConsolidate) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 2U);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "all"; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 1U);
  ExpectEntryPathsAndStatuses(manifests[0], {file_a_->file_path, file_b_->file_path},
                              {ManifestStatus::kExisting, ManifestStatus::kExisting});

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "0");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kEntriesProcessed), "2");
}

TEST_P(RewriteManifestsTest, ReplaceManifestsSeparate) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile& file) { return file.file_path; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 2U);
  std::vector<std::string> paths;
  for (const auto& manifest : manifests) {
    ICEBERG_UNWRAP_OR_FAIL(auto entries, Entries(manifest));
    ASSERT_EQ(entries.size(), 1U);
    EXPECT_EQ(entries[0].status, ManifestStatus::kExisting);
    paths.push_back(entries[0].data_file->file_path);
  }
  std::ranges::sort(paths);
  EXPECT_EQ(paths, (std::vector<std::string>{file_a_->file_path, file_b_->file_path}));
}

TEST_P(RewriteManifestsTest, ReplaceManifestsMaxSize) {
  std::vector<std::shared_ptr<DataFile>> files{file_a_, file_b_};
  for (int index = 0; index < 300; ++index) {
    files.push_back(MakeDataFile(std::format("many-{}", index), 1, 100, index));
  }
  ASSERT_THAT(AppendFiles(files), IsOk());
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
  ASSERT_GT(manifests.size(), 1U);

  std::vector<std::pair<std::string, int64_t>> actual;
  for (const auto& manifest : manifests) {
    ICEBERG_UNWRAP_OR_FAIL(auto entries, Entries(manifest));
    for (const auto& entry : entries) {
      EXPECT_EQ(entry.status, ManifestStatus::kExisting);
      ASSERT_NE(entry.data_file, nullptr);
      ASSERT_TRUE(entry.snapshot_id.has_value());
      actual.emplace_back(entry.data_file->file_path, entry.snapshot_id.value());
    }
  }
  std::ranges::sort(actual);
  std::vector<std::pair<std::string, int64_t>> expected;
  expected.reserve(files.size());
  for (const auto& file : files) {
    expected.emplace_back(file->file_path, append_snapshot_id);
  }
  std::ranges::sort(expected);
  EXPECT_EQ(actual, expected);
}

TEST_P(RewriteManifestsTest, ReplaceManifestsWithFilter) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ASSERT_THAT(AppendFiles({file_c_}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "bc"; });
  rewrite->RewriteIf([&](const ManifestFile& manifest) {
    auto entries_result = Entries(manifest);
    if (!entries_result.has_value() || entries_result.value().empty()) {
      return false;
    }
    return entries_result.value()[0].data_file->file_path != file_a_->file_path;
  });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 2U);
  ExpectEntryPathsAndStatuses(manifests[0], {file_b_->file_path, file_c_->file_path},
                              {ManifestStatus::kExisting, ManifestStatus::kExisting});
  ExpectEntryPathsAndStatuses(manifests[1], {file_a_->file_path},
                              {ManifestStatus::kAdded});
}

TEST_P(RewriteManifestsTest, BasicManifestReplacement) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  const int64_t first_snapshot_id = first_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot_manifests, CurrentManifests());
  ASSERT_EQ(first_snapshot_manifests.size(), 1U);

  ASSERT_THAT(AppendFiles({file_c_, file_d_}), IsOk());

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
  ExpectEntryPathsAndStatuses(manifests[0], {file_a_->file_path},
                              {ManifestStatus::kExisting});
  ExpectEntryPathsAndStatuses(manifests[1], {file_b_->file_path},
                              {ManifestStatus::kExisting});
  ExpectEntryPathsAndStatuses(manifests[2], {file_c_->file_path, file_d_->file_path},
                              {ManifestStatus::kAdded, ManifestStatus::kAdded});

  ICEBERG_UNWRAP_OR_FAIL(auto after, table_->current_snapshot());
  EXPECT_EQ(after->summary.at(SnapshotSummaryFields::kManifestsCreated), "2");
  EXPECT_EQ(after->summary.at(SnapshotSummaryFields::kManifestsKept), "1");
  EXPECT_EQ(after->summary.at(SnapshotSummaryFields::kManifestsReplaced), "1");
  EXPECT_EQ(after->summary.at(SnapshotSummaryFields::kEntriesProcessed), "0");
}

TEST_P(RewriteManifestsTest, RejectsReplacementWithDifferentActiveFileCount) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto old_manifests, CurrentManifests());
  ASSERT_EQ(old_manifests.size(), 1U);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_a,
      WriteExistingManifest("rewrite-only-a", snapshot->snapshot_id, {file_a_}));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(old_manifests[0]).AddManifest(manifest_a);
  auto result = rewrite->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result,
              HasErrorMessage("Replaced and created manifests must have the same"));
}

TEST_P(RewriteManifestsTest, RejectsReplacementWithDifferentActiveFiles) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto old_manifests, CurrentManifests());
  ASSERT_EQ(old_manifests.size(), 1U);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_a,
      WriteExistingManifest("rewrite-same-count-a", snapshot->snapshot_id, {file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_c,
      WriteExistingManifest("rewrite-same-count-c", snapshot->snapshot_id, {file_c_}));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(old_manifests[0])
      .AddManifest(manifest_a)
      .AddManifest(manifest_c);
  auto result = rewrite->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("same active files"));
}

TEST_P(RewriteManifestsTest, ReplacementAllowsMissingFileCounts) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto old_manifests, CurrentManifests());
  ASSERT_EQ(old_manifests.size(), 1U);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());

  auto old_manifest_without_counts = old_manifests[0];
  old_manifest_without_counts.added_files_count = std::nullopt;
  old_manifest_without_counts.existing_files_count = std::nullopt;

  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_a,
      WriteExistingManifest("rewrite-missing-count-a", snapshot->snapshot_id, {file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest_b,
      WriteExistingManifest("rewrite-missing-count-b", snapshot->snapshot_id, {file_b_}));
  manifest_a.added_files_count = std::nullopt;
  manifest_a.existing_files_count = std::nullopt;
  manifest_b.added_files_count = std::nullopt;
  manifest_b.existing_files_count = std::nullopt;

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(old_manifest_without_counts)
      .AddManifest(manifest_a)
      .AddManifest(manifest_b);
  EXPECT_THAT(rewrite->Commit(), IsOk());
}

TEST_P(RewriteManifestsTest, AppendDuringRewriteManifest) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "all"; });
  EXPECT_THAT(static_cast<SnapshotUpdate&>(*rewrite).Apply(), IsOk());

  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 2U);
  std::vector<std::pair<std::string, ManifestStatus>> actual;
  for (const auto& manifest : manifests) {
    ICEBERG_UNWRAP_OR_FAIL(auto entries, Entries(manifest));
    for (const auto& entry : entries) {
      ASSERT_NE(entry.data_file, nullptr);
      actual.emplace_back(entry.data_file->file_path, entry.status);
    }
  }
  std::ranges::sort(actual);
  EXPECT_EQ(actual, (std::vector<std::pair<std::string, ManifestStatus>>{
                        {file_a_->file_path, ManifestStatus::kExisting},
                        {file_b_->file_path, ManifestStatus::kAdded}}));
}

TEST_P(RewriteManifestsTest, RewriteManifestDuringAppend) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append_snapshot, table_->current_snapshot());
  const int64_t append_snapshot_id = append_snapshot->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
  append->AppendFile(file_b_);
  EXPECT_THAT(static_cast<SnapshotUpdate&>(*append).Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "file"; });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto rewritten_manifests, CurrentManifests());
  ASSERT_EQ(rewritten_manifests.size(), 1U);

  EXPECT_THAT(append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto file_append_snapshot, table_->current_snapshot());
  const int64_t file_append_id = file_append_snapshot->snapshot_id;

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 2U);

  std::vector<std::pair<std::string, std::pair<ManifestStatus, int64_t>>> actual;
  for (const auto& manifest : manifests) {
    ICEBERG_UNWRAP_OR_FAIL(auto entries, Entries(manifest));
    for (const auto& entry : entries) {
      ASSERT_NE(entry.data_file, nullptr);
      ASSERT_TRUE(entry.snapshot_id.has_value());
      actual.emplace_back(entry.data_file->file_path,
                          std::make_pair(entry.status, entry.snapshot_id.value()));
    }
  }
  std::ranges::sort(actual);
  EXPECT_EQ(actual,
            (std::vector<std::pair<std::string, std::pair<ManifestStatus, int64_t>>>{
                {file_a_->file_path, {ManifestStatus::kExisting, append_snapshot_id}},
                {file_b_->file_path, {ManifestStatus::kAdded, file_append_id}}}));
}

TEST_P(RewriteManifestsTest, RejectsAddManifestWithAddedFiles) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto old_manifests, CurrentManifests());
  ASSERT_EQ(old_manifests.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(auto invalid_added_manifest,
                         WriteAddedManifest("invalid-added", {file_a_}, 1L));
  invalid_added_manifest.added_files_count = std::nullopt;

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(old_manifests[0]).AddManifest(invalid_added_manifest);
  auto result = rewrite->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot add manifest with added files"));
}

TEST_P(RewriteManifestsTest, RejectsAddManifestWithDeletedFiles) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto old_manifests, CurrentManifests());
  ASSERT_EQ(old_manifests.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(auto invalid_deleted_manifest,
                         WriteDeletedManifest("invalid-deleted", {file_a_}));
  invalid_deleted_manifest.deleted_files_count = std::nullopt;

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(old_manifests[0]).AddManifest(invalid_deleted_manifest);
  auto result = rewrite->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot add manifest with deleted files"));
}

TEST_P(RewriteManifestsTest, FailsWhenDeletedManifestMissingFromSnapshot) {
  ASSERT_THAT(AppendFiles({file_a_, file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());

  // A manifest that was never part of the table's current snapshot.
  ICEBERG_UNWRAP_OR_FAIL(
      auto orphan, WriteExistingManifest("orphan", snapshot->snapshot_id, {file_a_}));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(orphan);
  auto result = rewrite->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("could not be found in the latest snapshot"));
}

TEST_P(RewriteManifestsTest, ConcurrentRewriteManifest) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto before, CurrentManifests());
  ASSERT_EQ(before.size(), 2U);

  // Start a rewrite that processes both manifests, but only apply (stage) it.
  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->ClusterBy([](const DataFile&) { return "file"; });
  ICEBERG_UNWRAP_OR_FAIL(auto staged, static_cast<SnapshotUpdate&>(*rewrite).Apply());
  SnapshotCache staged_cache(staged.snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto staged_manifests, staged_cache.DataManifests(file_io_));
  ASSERT_EQ(staged_manifests.size(), 1U);
  const std::string staged_manifest_path = staged_manifests[0].manifest_path;

  // Concurrently rewrite only the manifest that does not contain file_a.
  ICEBERG_UNWRAP_OR_FAIL(auto concurrent, table_->NewRewriteManifests());
  concurrent->ClusterBy([](const DataFile&) { return "file"; });
  concurrent->RewriteIf([&](const ManifestFile& manifest) {
    auto entries_result = Entries(manifest);
    if (!entries_result.has_value() || entries_result.value().empty()) {
      return false;
    }
    return entries_result.value()[0].data_file->file_path != file_a_->file_path;
  });
  EXPECT_THAT(concurrent->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  // Committing the in-progress rewrite must perform a full rewrite because the
  // manifest with file_b is no longer part of the current snapshot.
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 1U);
  EXPECT_NE(manifests[0].manifest_path, staged_manifest_path);
  ExpectEntryPathsAndStatuses(manifests[0], {file_a_->file_path, file_b_->file_path},
                              {ManifestStatus::kExisting, ManifestStatus::kExisting});
}

TEST_P(RewriteManifestsTest, CombinesManifestReplacementWithRewrite) {
  ASSERT_THAT(AppendFiles({file_a_}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto first_snapshot, table_->current_snapshot());
  const int64_t first_snapshot_id = first_snapshot->snapshot_id;
  ICEBERG_UNWRAP_OR_FAIL(auto first_manifests, CurrentManifests());
  ASSERT_EQ(first_manifests.size(), 1U);

  ASSERT_THAT(AppendFiles({file_b_}), IsOk());
  ASSERT_THAT(AppendFiles({file_c_}), IsOk());

  // A manifest that re-adds file_a as an existing file.
  ICEBERG_UNWRAP_OR_FAIL(
      auto new_manifest,
      WriteExistingManifest("combined-a", first_snapshot_id, {file_a_}));

  ICEBERG_UNWRAP_OR_FAIL(auto rewrite, table_->NewRewriteManifests());
  rewrite->DeleteManifest(first_manifests[0])
      .AddManifest(new_manifest)
      .ClusterBy([](const DataFile&) { return "const-value"; })
      .RewriteIf([&](const ManifestFile& manifest) {
        auto entries_result = Entries(manifest);
        if (!entries_result.has_value() || entries_result.value().empty()) {
          return false;
        }
        return entries_result.value()[0].data_file->file_path != file_b_->file_path;
      });
  EXPECT_THAT(rewrite->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentManifests());
  ASSERT_EQ(manifests.size(), 3U);

  // Collect the file path / status pairs across all resulting manifests.
  std::vector<std::pair<std::string, ManifestStatus>> actual;
  for (const auto& manifest : manifests) {
    ICEBERG_UNWRAP_OR_FAIL(auto entries, Entries(manifest));
    for (const auto& entry : entries) {
      ASSERT_NE(entry.data_file, nullptr);
      actual.emplace_back(entry.data_file->file_path, entry.status);
    }
  }
  std::ranges::sort(actual);
  EXPECT_EQ(actual, (std::vector<std::pair<std::string, ManifestStatus>>{
                        {file_a_->file_path, ManifestStatus::kExisting},
                        {file_b_->file_path, ManifestStatus::kAdded},
                        {file_c_->file_path, ManifestStatus::kExisting}}));

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "2");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kEntriesProcessed), "1");
}

INSTANTIATE_TEST_SUITE_P(RewriteManifestVersions, RewriteManifestsTest,
                         ::testing::Values<int8_t>(1, 2, 3),
                         [](const ::testing::TestParamInfo<int8_t>& info) {
                           return "V" + std::to_string(info.param);
                         });

}  // namespace iceberg
