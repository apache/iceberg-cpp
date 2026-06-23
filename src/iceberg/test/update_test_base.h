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

#pragma once

#include <format>
#include <memory>
#include <span>
#include <string>
#include <tuple>
#include <vector>

#include <arrow/filesystem/mockfs.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/catalog/memory/in_memory_catalog.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/test_resource.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

/// \brief Base test fixture for table update operations.
class UpdateTestBase : public ::testing::Test {
 protected:
  virtual std::string MetadataResource() const { return "TableMetadataV2Valid.json"; }
  virtual std::string TableName() const { return "test_table"; }

  void SetUp() override {
    table_ident_ = TableIdentifier{.name = TableName()};
    table_location_ = "/warehouse/" + TableName();

    InitializeFileIO();
    RegisterTableFromResource(MetadataResource());
  }

  /// \brief Initialize file IO and create necessary directories.
  void InitializeFileIO() {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
    catalog_ =
        InMemoryCatalog::Make("test_catalog", file_io_, "/warehouse/", /*properties=*/{});

    // Arrow MockFS cannot automatically create directories.
    auto arrow_fs = std::dynamic_pointer_cast<::arrow::fs::internal::MockFileSystem>(
        static_cast<arrow::ArrowFileSystemFileIO&>(*file_io_).fs());
    ASSERT_TRUE(arrow_fs != nullptr);
    ASSERT_TRUE(arrow_fs->CreateDir(table_location_ + "/metadata").ok());
  }

  /// \brief Register a table from a metadata resource file.
  ///
  /// \param resource_name The name of the metadata resource file
  void RegisterTableFromResource(const std::string& resource_name) {
    std::ignore = catalog_->DropTable(table_ident_, /*purge=*/false);

    auto metadata_location = std::format("{}/metadata/00001-{}.metadata.json",
                                         table_location_, Uuid::GenerateV7().ToString());
    ICEBERG_UNWRAP_OR_FAIL(auto metadata, ReadTableMetadataFromResource(resource_name));
    metadata->location = table_location_;
    ASSERT_THAT(TableMetadataUtil::Write(*file_io_, metadata_location, *metadata),
                IsOk());

    ICEBERG_UNWRAP_OR_FAIL(table_,
                           catalog_->RegisterTable(table_ident_, metadata_location));
  }

  /// \brief Reload the table from catalog and return its metadata.
  std::shared_ptr<TableMetadata> ReloadMetadata() {
    auto result = catalog_->LoadTable(table_ident_);
    EXPECT_TRUE(result.has_value()) << "Failed to reload table";
    return result.value()->metadata();
  }

  /// \brief Assert that a ref exists with the given type and snapshot id.
  void ExpectRef(const std::string& name, SnapshotRefType type, int64_t snapshot_id) {
    auto metadata = ReloadMetadata();
    auto it = metadata->refs.find(name);
    ASSERT_NE(it, metadata->refs.end()) << "Ref not found: " << name;
    EXPECT_EQ(it->second->type(), type);
    EXPECT_EQ(it->second->snapshot_id, snapshot_id);
  }

  void ExpectBranch(const std::string& name, int64_t snapshot_id) {
    ExpectRef(name, SnapshotRefType::kBranch, snapshot_id);
  }

  void ExpectTag(const std::string& name, int64_t snapshot_id) {
    ExpectRef(name, SnapshotRefType::kTag, snapshot_id);
  }

  /// \brief Assert that a ref does not exist.
  void ExpectNoRef(const std::string& name) {
    auto metadata = ReloadMetadata();
    EXPECT_FALSE(metadata->refs.contains(name)) << "Ref should not exist: " << name;
  }

  /// \brief Assert the current snapshot id after reloading.
  void ExpectCurrentSnapshot(int64_t snapshot_id) {
    auto result = catalog_->LoadTable(table_ident_);
    ASSERT_TRUE(result.has_value());
    auto snap_result = result.value()->current_snapshot();
    ASSERT_TRUE(snap_result.has_value());
    EXPECT_EQ(snap_result.value()->snapshot_id, snapshot_id);
  }

  /// \brief Assert that a commit succeeded.
  template <typename T>
  void ExpectCommitOk(const T& result) {
    EXPECT_THAT(result, IsOk());
  }

  /// \brief Assert that a commit failed with the given error kind and message substring.
  template <typename T>
  void ExpectCommitError(const T& result, ErrorKind kind, const std::string& message) {
    EXPECT_THAT(result, IsError(kind));
    EXPECT_THAT(result, HasErrorMessage(message));
  }

  Result<std::vector<ManifestEntry>> ReadManifestEntries(const ManifestFile& manifest) {
    return ReadManifestEntries(std::span<const ManifestFile>(&manifest, 1));
  }

  Result<std::vector<ManifestEntry>> ReadManifestEntries(
      std::span<const ManifestFile> manifests) {
    std::vector<ManifestEntry> result;
    ICEBERG_ASSIGN_OR_RAISE(auto schema, table_->metadata()->Schema());
    for (const auto& manifest : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto spec, table_->metadata()->PartitionSpecById(manifest.partition_spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto reader,
                              ManifestReader::Make(manifest, file_io_, schema, spec));
      ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
      result.insert(result.end(), entries.begin(), entries.end());
    }
    return result;
  }

  void ExpectManifestEntries(const ManifestFile& manifest,
                             std::vector<std::string> expected_paths,
                             std::vector<ManifestStatus> expected_statuses) {
    ASSERT_EQ(expected_statuses.size(), expected_paths.size());

    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadManifestEntries(manifest));
    ASSERT_EQ(entries.size(), expected_paths.size());

    std::vector<std::pair<std::string, ManifestStatus>> actual;
    actual.reserve(entries.size());
    for (const auto& entry : entries) {
      ASSERT_NE(entry.data_file, nullptr);
      actual.emplace_back(entry.data_file->file_path, entry.status);
    }

    std::vector<std::pair<std::string, ManifestStatus>> expected;
    expected.reserve(expected_paths.size());
    for (size_t i = 0; i < expected_paths.size(); ++i) {
      expected.emplace_back(std::move(expected_paths[i]), expected_statuses[i]);
    }
    EXPECT_THAT(actual, ::testing::UnorderedElementsAreArray(expected));
  }

  void ExpectManifestEntriesWithSnapshotIds(const ManifestFile& manifest,
                                            std::vector<std::string> expected_paths,
                                            std::vector<ManifestStatus> expected_statuses,
                                            std::vector<int64_t> expected_snapshot_ids) {
    ASSERT_EQ(expected_statuses.size(), expected_paths.size());
    ASSERT_EQ(expected_snapshot_ids.size(), expected_paths.size());

    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadManifestEntries(manifest));
    ASSERT_EQ(entries.size(), expected_paths.size());

    std::vector<std::tuple<std::string, ManifestStatus, int64_t>> actual;
    actual.reserve(entries.size());
    for (const auto& entry : entries) {
      ASSERT_NE(entry.data_file, nullptr);
      ASSERT_TRUE(entry.snapshot_id.has_value());
      actual.emplace_back(entry.data_file->file_path, entry.status,
                          entry.snapshot_id.value());
    }

    std::vector<std::tuple<std::string, ManifestStatus, int64_t>> expected;
    expected.reserve(expected_paths.size());
    for (size_t i = 0; i < expected_paths.size(); ++i) {
      expected.emplace_back(std::move(expected_paths[i]), expected_statuses[i],
                            expected_snapshot_ids[i]);
    }
    EXPECT_THAT(actual, ::testing::UnorderedElementsAreArray(expected));
  }

  void ExpectManifestEntriesWithSequenceNumbers(
      const ManifestFile& manifest, std::vector<std::string> expected_paths,
      std::vector<ManifestStatus> expected_statuses,
      std::vector<int64_t> expected_snapshot_ids,
      std::vector<int64_t> expected_data_sequence_numbers,
      std::vector<int64_t> expected_file_sequence_numbers) {
    ASSERT_EQ(expected_statuses.size(), expected_paths.size());
    ASSERT_EQ(expected_snapshot_ids.size(), expected_paths.size());
    ASSERT_EQ(expected_data_sequence_numbers.size(), expected_paths.size());
    ASSERT_EQ(expected_file_sequence_numbers.size(), expected_paths.size());

    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadManifestEntries(manifest));
    ASSERT_EQ(entries.size(), expected_paths.size());

    std::vector<std::tuple<std::string, ManifestStatus, int64_t, int64_t, int64_t>>
        actual;
    actual.reserve(entries.size());
    for (const auto& entry : entries) {
      ASSERT_NE(entry.data_file, nullptr);
      ASSERT_TRUE(entry.snapshot_id.has_value());
      ASSERT_TRUE(entry.sequence_number.has_value());
      ASSERT_TRUE(entry.file_sequence_number.has_value());
      actual.emplace_back(entry.data_file->file_path, entry.status,
                          entry.snapshot_id.value(), entry.sequence_number.value(),
                          entry.file_sequence_number.value());
    }

    std::vector<std::tuple<std::string, ManifestStatus, int64_t, int64_t, int64_t>>
        expected;
    expected.reserve(expected_paths.size());
    for (size_t i = 0; i < expected_paths.size(); ++i) {
      expected.emplace_back(std::move(expected_paths[i]), expected_statuses[i],
                            expected_snapshot_ids[i], expected_data_sequence_numbers[i],
                            expected_file_sequence_numbers[i]);
    }
    EXPECT_THAT(actual, ::testing::UnorderedElementsAreArray(expected));
  }

  TableIdentifier table_ident_;
  std::string table_location_;
  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<InMemoryCatalog> catalog_;
  std::shared_ptr<Table> table_;
};

/// \brief Test fixture for table update operations on minimal table metadata.
class MinimalUpdateTestBase : public UpdateTestBase {
 protected:
  virtual int8_t format_version() const {
    return TableMetadata::kDefaultTableFormatVersion;
  }

  std::string MetadataResource() const override {
    switch (format_version()) {
      case 1:
        return "TableMetadataV1Valid.json";
      case 2:
        return "TableMetadataV2ValidMinimal.json";
      case 3:
        return "TableMetadataV3ValidMinimal.json";
      default:
        ADD_FAILURE() << "Unsupported format version: "
                      << static_cast<int>(format_version());
        return "TableMetadataV2ValidMinimal.json";
    }
  }

  std::string TableName() const override { return "minimal_table"; }
};

}  // namespace iceberg
