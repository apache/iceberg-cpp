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

#include "iceberg/manifest/manifest_filter_manager.h"

#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/expression/expression.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transform.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/util/data_file_set.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

class ManifestFilterManagerTest : public MinimalUpdateTestBase {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  void SetUp() override {
    MinimalUpdateTestBase::SetUp();

    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());

    // Two files in different partitions (identity(x))
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

  // Append files, commit, refresh, and return the current snapshot.
  Result<std::shared_ptr<Snapshot>> CommitFiles(
      std::vector<std::shared_ptr<DataFile>> files) {
    ICEBERG_ASSIGN_OR_RAISE(auto fa, table_->NewFastAppend());
    for (const auto& f : files) fa->AppendFile(f);
    ICEBERG_RETURN_UNEXPECTED(fa->Commit());
    ICEBERG_RETURN_UNEXPECTED(table_->Refresh());
    return table_->current_snapshot();
  }

  Result<ManifestFile> WriteDataManifest(
      std::initializer_list<std::shared_ptr<DataFile>> files,
      std::optional<int64_t> first_row_id = std::nullopt) {
    auto path =
        std::format("{}/metadata/input-{}.avro", table_location_, manifest_counter_++);
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, ManifestWriter::MakeWriter(first_row_id ? 3 : 2, /*snapshot_id=*/10,
                                                path, file_io_, spec_, schema_,
                                                ManifestContent::kData, first_row_id));
    for (const auto& file : files) {
      ICEBERG_RETURN_UNEXPECTED(
          writer->WriteAddedEntry(file, /*data_sequence_number=*/5));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto manifest, writer->ToManifestFile());
    manifest.sequence_number = 7;
    return manifest;
  }

  ManifestWriterFactory MakeWriterFactory(const TableMetadata& metadata) {
    auto fv = metadata.format_version;
    return [this, fv, &metadata](int32_t spec_id, ManifestContent content) mutable
               -> Result<std::unique_ptr<ManifestWriter>> {
      ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
      auto path =
          std::format("{}/metadata/flt-{}.avro", table_location_, manifest_counter_++);
      return ManifestWriter::MakeWriter(fv, kTestSnapshotId, path, file_io_, spec, schema,
                                        content);
    };
  }

  ManifestFilterManager::PartitionSpecsById SpecsById(const TableMetadata& metadata) {
    ManifestFilterManager::PartitionSpecsById specs_by_id;
    for (const auto& spec : metadata.partition_specs) {
      specs_by_id.emplace(spec->spec_id(), spec);
    }
    return specs_by_id;
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

  static constexpr int64_t kTestSnapshotId = 55555L;
  int manifest_counter_ = 0;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
};

TEST_F(ManifestFilterManagerTest, NullSnapshotReturnsEmpty) {
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);
  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(*metadata, nullptr, factory));
  EXPECT_TRUE(result.empty());
}

TEST_F(ManifestFilterManagerTest, MakeRejectsNullFileIO) {
  EXPECT_THAT(ManifestFilterManager::Make(ManifestContent::kData, nullptr),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(ManifestFilterManagerTest, ContainsDeletesReturnsCorrectState) {
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  EXPECT_FALSE(mgr->ContainsDeletes());
  ASSERT_THAT(mgr->DeleteFile("/some/path.parquet"), IsOk());
  EXPECT_TRUE(mgr->ContainsDeletes());
}

TEST_F(ManifestFilterManagerTest, ContainsDeletesTrueAfterRowFilter) {
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  EXPECT_FALSE(mgr->ContainsDeletes());
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::Equal("x", Literal::Long(1L))), IsOk());
  EXPECT_TRUE(mgr->ContainsDeletes());
}

TEST_F(ManifestFilterManagerTest, ContainsDeletesFalseForAlwaysFalseRowFilter) {
  // An always-false row filter does not count as a delete condition.
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::AlwaysFalse()), IsOk());
  EXPECT_FALSE(mgr->ContainsDeletes());
}

TEST_F(ManifestFilterManagerTest, ContainsDeletesTrueAfterDropPartition) {
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  EXPECT_FALSE(mgr->ContainsDeletes());
  ASSERT_THAT(
      mgr->DropPartition(spec_->spec_id(),
                         PartitionValues(std::vector<Literal>{Literal::Long(1L)})),
      IsOk());
  EXPECT_TRUE(mgr->ContainsDeletes());
}

TEST_F(ManifestFilterManagerTest, ContainsDeletesTrueAfterDeleteFileObject) {
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  EXPECT_FALSE(mgr->ContainsDeletes());
  EXPECT_THAT(mgr->DeleteFile(file_a_), IsOk());
  EXPECT_TRUE(mgr->ContainsDeletes());
}

TEST_F(ManifestFilterManagerTest, FilesToBeDeletedIncludesRegisteredFileObject) {
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));

  EXPECT_THAT(mgr->DeleteFile(file_a_), IsOk());

  ASSERT_EQ(mgr->FilesToBeDeleted().size(), 1U);
  EXPECT_EQ(mgr->FilesToBeDeleted().begin()->get()->file_path, file_a_->file_path);
}

TEST_F(ManifestFilterManagerTest, FilesToBeDeletedKeepsRegisteredFileObjectAfterFilter) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_b_}));
  auto factory = MakeWriterFactory(*table_->metadata());

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  EXPECT_THAT(mgr->DeleteFile(file_a_), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr->FilterManifests(*table_->metadata(), snap, factory));
  EXPECT_THAT(result, ::testing::Not(::testing::IsEmpty()));

  ASSERT_EQ(mgr->FilesToBeDeleted().size(), 1U);
  EXPECT_EQ(mgr->FilesToBeDeleted().begin()->get()->file_path, file_a_->file_path);
}

TEST_F(ManifestFilterManagerTest, DeleteByRowFilterRejectsNull) {
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  EXPECT_THAT(mgr->DeleteByRowFilter(nullptr), IsError(ErrorKind::kInvalidArgument));
}

TEST_F(ManifestFilterManagerTest, DeleteFileObjectRejectsNull) {
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  std::shared_ptr<DataFile> null_file;
  EXPECT_THAT(mgr->DeleteFile(null_file), IsError(ErrorKind::kInvalidArgument));
}

TEST_F(ManifestFilterManagerTest, NoConditionsReturnsManifestsUnchanged) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  // Load original manifests so we can compare paths
  ICEBERG_UNWRAP_OR_FAIL(auto list_reader,
                         ManifestListReader::Make(snap->manifest_list, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto orig_manifests, list_reader->Files());

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(*metadata, snap, factory));

  ASSERT_EQ(result.size(), orig_manifests.size());
  for (size_t i = 0; i < result.size(); ++i) {
    EXPECT_EQ(result[i].manifest_path, orig_manifests[i].manifest_path);
  }
}

TEST_F(ManifestFilterManagerTest, DeleteFileByPath) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile(file_a_->file_path), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(*metadata, snap, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  int deleted_count = 0;
  int live_count = 0;
  for (const auto& e : entries) {
    if (e.status == ManifestStatus::kDeleted) {
      ++deleted_count;
      ASSERT_NE(e.data_file, nullptr);
      EXPECT_EQ(e.data_file->file_path, file_a_->file_path);
    } else {
      ++live_count;
    }
  }
  EXPECT_EQ(deleted_count, 1);
  EXPECT_EQ(live_count, 1);
}

TEST_F(ManifestFilterManagerTest, FilterManifestsCachesFilteredManifestAcrossRetries) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto list_reader,
                         ManifestListReader::Make(snap->manifest_list, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_files, list_reader->Files());
  std::vector<const ManifestFile*> manifests;
  manifests.reserve(manifest_files.size());
  for (const auto& manifest : manifest_files) {
    manifests.push_back(&manifest);
  }

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile(file_a_->file_path), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());
  auto specs = SpecsById(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto first,
                         mgr->FilterManifests(schema, specs, manifests, factory));
  ASSERT_EQ(first.size(), 1U);
  EXPECT_NE(first[0].manifest_path, manifest_files[0].manifest_path);
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 1);
  int after_first = manifest_counter_;

  ICEBERG_UNWRAP_OR_FAIL(auto second,
                         mgr->FilterManifests(schema, specs, manifests, factory));
  ASSERT_EQ(second.size(), 1U);
  EXPECT_EQ(second[0].manifest_path, first[0].manifest_path);
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 1);
  EXPECT_EQ(manifest_counter_, after_first);
}

TEST_F(ManifestFilterManagerTest, DeleteFileInvalidatesFilteredCache) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto list_reader,
                         ManifestListReader::Make(snap->manifest_list, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_files, list_reader->Files());
  std::vector<const ManifestFile*> manifests;
  manifests.reserve(manifest_files.size());
  for (const auto& manifest : manifest_files) {
    manifests.push_back(&manifest);
  }

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile(file_a_->file_path), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());
  auto specs = SpecsById(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto first,
                         mgr->FilterManifests(schema, specs, manifests, factory));
  ASSERT_EQ(first.size(), 1U);

  ASSERT_THAT(mgr->DeleteFile(file_b_->file_path), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto second,
                         mgr->FilterManifests(schema, specs, manifests, factory));
  EXPECT_NE(second[0].manifest_path, first[0].manifest_path);

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(second, *metadata));
  auto deleted_count =
      std::count_if(entries.begin(), entries.end(), [](const ManifestEntry& entry) {
        return entry.status == ManifestStatus::kDeleted;
      });
  EXPECT_EQ(deleted_count, 2);
}

TEST_F(ManifestFilterManagerTest, CleanUncommittedDropsFilteredCacheAndRollsBackCount) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto list_reader,
                         ManifestListReader::Make(snap->manifest_list, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_files, list_reader->Files());
  std::vector<const ManifestFile*> manifests;
  manifests.reserve(manifest_files.size());
  for (const auto& manifest : manifest_files) {
    manifests.push_back(&manifest);
  }

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile(file_a_->file_path), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());
  auto specs = SpecsById(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto first,
                         mgr->FilterManifests(schema, specs, manifests, factory));
  ASSERT_EQ(first.size(), 1U);
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 1);
  EXPECT_THAT(mgr->CleanUncommitted({}), IsOk());
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 0);

  int after_cleanup = manifest_counter_;
  ICEBERG_UNWRAP_OR_FAIL(auto second,
                         mgr->FilterManifests(schema, specs, manifests, factory));
  ASSERT_EQ(second.size(), 1U);
  EXPECT_NE(second[0].manifest_path, first[0].manifest_path);
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 1);
  EXPECT_GT(manifest_counter_, after_cleanup);
}

TEST_F(ManifestFilterManagerTest, CleanUncommittedIgnoresDeleteCallbackError) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto list_reader,
                         ManifestListReader::Make(snap->manifest_list, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_files, list_reader->Files());
  std::vector<const ManifestFile*> manifests;
  manifests.reserve(manifest_files.size());
  for (const auto& manifest : manifest_files) {
    manifests.push_back(&manifest);
  }

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(
                    ManifestContent::kData, file_io_,
                    [](const std::string&) { return IOError("delete failed"); }));
  ASSERT_THAT(mgr->DeleteFile(file_a_->file_path), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());
  auto specs = SpecsById(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto first,
                         mgr->FilterManifests(schema, specs, manifests, factory));
  ASSERT_EQ(first.size(), 1U);
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 1);

  EXPECT_THAT(mgr->CleanUncommitted({}), IsOk());
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 0);
}

TEST_F(ManifestFilterManagerTest, ExplicitContextFilterManifestsDeletesByPath) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto list_reader,
                         ManifestListReader::Make(snap->manifest_list, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_files, list_reader->Files());
  std::vector<const ManifestFile*> manifests;
  manifests.reserve(manifest_files.size());
  for (const auto& manifest : manifest_files) {
    manifests.push_back(&manifest);
  }

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile(file_a_->file_path), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(schema_, SpecsById(*metadata),
                                                           manifests, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  int deleted_count = 0;
  for (const auto& entry : entries) {
    if (entry.status == ManifestStatus::kDeleted) {
      ++deleted_count;
      ASSERT_NE(entry.data_file, nullptr);
      EXPECT_EQ(entry.data_file->file_path, file_a_->file_path);
    }
  }
  EXPECT_EQ(deleted_count, 1);
}

TEST_F(ManifestFilterManagerTest, RowFilterAlwaysTrueDeletesAll) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::AlwaysTrue()), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(*metadata, snap, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  for (const auto& e : entries) {
    EXPECT_EQ(e.status, ManifestStatus::kDeleted) << "Expected all entries to be DELETED";
  }
}

TEST_F(ManifestFilterManagerTest, RowFilterAlwaysFalseDeletesNone) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::AlwaysFalse()), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(*metadata, snap, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  for (const auto& e : entries) {
    EXPECT_NE(e.status, ManifestStatus::kDeleted) << "Expected no entries to be DELETED";
  }
}

TEST_F(ManifestFilterManagerTest, RowFilterUsesPartitionResiduals) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  mgr->CaseSensitive(false);
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::Equal("X", Literal::Long(1L))), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(*metadata, snap, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  int deleted_count = 0;
  int live_count = 0;
  for (const auto& e : entries) {
    ASSERT_NE(e.data_file, nullptr);
    if (e.status == ManifestStatus::kDeleted) {
      ++deleted_count;
      EXPECT_EQ(e.data_file->file_path, file_a_->file_path);
    } else {
      ++live_count;
      EXPECT_EQ(e.data_file->file_path, file_b_->file_path);
    }
  }

  EXPECT_EQ(deleted_count, 1);
  EXPECT_EQ(live_count, 1);
  ASSERT_EQ(mgr->FilesToBeDeleted().size(), 1U);
  EXPECT_EQ(mgr->FilesToBeDeleted().begin()->get()->file_path, file_a_->file_path);
}

TEST_F(ManifestFilterManagerTest, DropPartition) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  // Drop partition of file_a (partition_x = 1)
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(
      mgr->DropPartition(spec_->spec_id(),
                         PartitionValues(std::vector<Literal>{Literal::Long(1L)})),
      IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(*metadata, snap, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  int deleted_count = 0;
  for (const auto& e : entries) {
    if (e.status == ManifestStatus::kDeleted) {
      ++deleted_count;
      ASSERT_TRUE(e.data_file != nullptr);
      EXPECT_EQ(e.data_file->file_path, file_a_->file_path);
    }
  }
  EXPECT_EQ(deleted_count, 1);
}

TEST_F(ManifestFilterManagerTest, FailMissingDeletePathsReturnsError) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile("/does/not/exist.parquet"), IsOk());
  mgr->FailMissingDeletePaths();

  auto result = mgr->FilterManifests(*metadata, snap, factory);
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Missing required files to delete"));
}

TEST_F(ManifestFilterManagerTest, FailAnyDeleteReportsPartitionPath) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile(file_a_->file_path), IsOk());
  mgr->FailAnyDelete();

  auto result = mgr->FilterManifests(*metadata, snap, factory);
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("x=1"));
}

TEST_F(ManifestFilterManagerTest, MultipleConditionsOrCombined) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  // Both files should be deleted: file_a by path, file_b by AlwaysTrue expression
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile(file_a_->file_path), IsOk());
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::AlwaysTrue()), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(*metadata, snap, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  for (const auto& e : entries) {
    EXPECT_EQ(e.status, ManifestStatus::kDeleted);
  }
}

TEST_F(ManifestFilterManagerTest, MultipleRowFiltersUseCombinedExpression) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::Equal("y", Literal::Long(7L))), IsOk());
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::Equal("x", Literal::Long(1L))), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(*metadata, snap, factory));
  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));

  ASSERT_EQ(entries.size(), 1U);
  EXPECT_EQ(entries[0].status, ManifestStatus::kDeleted);
}

// Helper: write one or more delete-file entries to a new manifest.
// Each entry is (DataFile, data_sequence_number).
using DeleteFileWithSequenceNumber = std::pair<std::shared_ptr<DataFile>, int64_t>;
static Result<ManifestFile> WriteDeleteManifest(
    const std::vector<DeleteFileWithSequenceNumber>& files,
    std::shared_ptr<FileIO> file_io, const TableMetadata& metadata,
    const std::string& path) {
  if (files.empty()) {
    return InvalidArgument("WriteDeleteManifest requires at least one entry");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  int32_t spec_id = files[0].first->partition_spec_id.value_or(0);
  ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_id));
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      ManifestWriter::MakeWriter(metadata.format_version, /*snapshot_id=*/1L, path,
                                 file_io, spec, schema, ManifestContent::kDeletes));
  for (auto& [file, seq] : files) {
    ManifestEntry entry;
    entry.status = ManifestStatus::kAdded;
    entry.snapshot_id = 1L;
    entry.sequence_number = seq;
    entry.data_file = file;
    ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(entry));
  }
  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  return writer->ToManifestFile();
}

// Convenience overload for a single entry.
static Result<ManifestFile> WriteDeleteManifest(std::shared_ptr<DataFile> delete_file,
                                                int64_t data_sequence_number,
                                                std::shared_ptr<FileIO> file_io,
                                                const TableMetadata& metadata,
                                                const std::string& path) {
  return WriteDeleteManifest({{delete_file, data_sequence_number}}, file_io, metadata,
                             path);
}

TEST_F(ManifestFilterManagerTest, DropDeleteFilesOlderThanDoesNotRewriteOnItsOwn) {
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  // Create a position-delete file with data_sequence_number = 2 (below threshold 5).
  auto del_file = std::make_shared<DataFile>();
  del_file->content = DataFile::Content::kPositionDeletes;
  del_file->file_path = table_location_ + "/delete/del_old.parquet";
  del_file->file_format = FileFormatType::kParquet;
  del_file->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
  del_file->file_size_in_bytes = 512;
  del_file->record_count = 10;
  del_file->partition_spec_id = spec_->spec_id();

  auto manifest_path = std::format("{}/metadata/del-manifest-{}.avro", table_location_,
                                   manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(
      auto del_manifest,
      WriteDeleteManifest(del_file, /*data_seq=*/2L, file_io_, *metadata, manifest_path));

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  EXPECT_THAT(mgr->DropDeleteFilesOlderThan(5), IsOk());

  std::vector<const ManifestFile*> manifests{&del_manifest};
  auto specs = SpecsById(*metadata);
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());

  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr->FilterManifests(schema, specs, manifests, factory));

  ASSERT_EQ(result.size(), 1U);
  EXPECT_EQ(result[0].manifest_path, del_manifest.manifest_path);
}

TEST_F(ManifestFilterManagerTest, DropDeleteFilesOlderThanRejectsNegativeSequenceNumber) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  EXPECT_THAT(mgr->DropDeleteFilesOlderThan(-1), IsError(ErrorKind::kInvalidArgument));
}

TEST_F(ManifestFilterManagerTest, DropDeleteFilesOlderThanDuringDeleteManifestRewrite) {
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  auto make_del_file = [&](const std::string& path) {
    auto f = std::make_shared<DataFile>();
    f->content = DataFile::Content::kPositionDeletes;
    f->file_path = path;
    f->file_format = FileFormatType::kParquet;
    f->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
    f->file_size_in_bytes = 512;
    f->record_count = 10;
    f->partition_spec_id = spec_->spec_id();
    return f;
  };
  auto old_file = make_del_file(table_location_ + "/delete/del_old.parquet");
  auto targeted_file = make_del_file(table_location_ + "/delete/del_targeted.parquet");
  auto keep_file = make_del_file(table_location_ + "/delete/del_keep.parquet");

  auto manifest_path = std::format("{}/metadata/del-manifest-{}.avro", table_location_,
                                   manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(
      auto del_manifest,
      WriteDeleteManifest({{old_file, 2L}, {targeted_file, 10L}, {keep_file, 10L}},
                          file_io_, *metadata, manifest_path));

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  ASSERT_THAT(mgr->DeleteFile(targeted_file->file_path), IsOk());
  EXPECT_THAT(mgr->DropDeleteFilesOlderThan(5), IsOk());

  std::vector<const ManifestFile*> manifests{&del_manifest};
  auto specs = SpecsById(*metadata);
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());

  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr->FilterManifests(schema, specs, manifests, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  ASSERT_EQ(entries.size(), 3U);
  auto deleted = std::count_if(
      entries.begin(), entries.end(),
      [](const ManifestEntry& e) { return e.status == ManifestStatus::kDeleted; });
  auto existing = std::count_if(
      entries.begin(), entries.end(),
      [](const ManifestEntry& e) { return e.status == ManifestStatus::kExisting; });
  EXPECT_EQ(deleted, 2);
  EXPECT_EQ(existing, 1);
  for (const auto& e : entries) {
    if (e.status == ManifestStatus::kExisting) {
      EXPECT_EQ(e.data_file->file_path, keep_file->file_path);
    } else {
      EXPECT_THAT(e.data_file->file_path,
                  ::testing::AnyOf(old_file->file_path, targeted_file->file_path));
      EXPECT_EQ(e.status, ManifestStatus::kDeleted);
    }
  }
}

TEST_F(ManifestFilterManagerTest, FailAnyDeleteFailsOnSequenceNumberPruning) {
  // An unrelated object delete opens the manifest; sequence-number pruning is the
  // branch that marks this entry.
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  auto old_file = std::make_shared<DataFile>();
  old_file->content = DataFile::Content::kPositionDeletes;
  old_file->file_path = table_location_ + "/delete/del_old.parquet";
  old_file->file_format = FileFormatType::kParquet;
  old_file->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
  old_file->file_size_in_bytes = 512;
  old_file->record_count = 10;
  old_file->partition_spec_id = spec_->spec_id();

  auto manifest_path = std::format("{}/metadata/del-manifest-{}.avro", table_location_,
                                   manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(
      auto del_manifest,
      WriteDeleteManifest(old_file, /*data_seq=*/2L, file_io_, *metadata, manifest_path));

  // Unrelated registered delete file: opens the manifest without matching its entry.
  auto unrelated = std::make_shared<DataFile>();
  unrelated->content = DataFile::Content::kPositionDeletes;
  unrelated->file_path = table_location_ + "/delete/unrelated.parquet";
  unrelated->file_format = FileFormatType::kParquet;
  unrelated->partition = old_file->partition;
  unrelated->file_size_in_bytes = 512;
  unrelated->record_count = 10;
  unrelated->partition_spec_id = spec_->spec_id();

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  EXPECT_THAT(mgr->DeleteFile(unrelated), IsOk());
  EXPECT_THAT(mgr->DropDeleteFilesOlderThan(5), IsOk());
  mgr->FailAnyDelete();

  std::vector<const ManifestFile*> manifests{&del_manifest};
  auto specs = SpecsById(*metadata);
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());

  auto result = mgr->FilterManifests(schema, specs, manifests, factory);
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Operation would delete existing data"));
}

TEST_F(ManifestFilterManagerTest, FailAnyDeleteFailsOnDanglingDeletionVector) {
  // Dangling DVs honor fail-any-delete just like other delete branches.
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  const std::string data_file_path = table_location_ + "/data/referenced.parquet";

  auto dv_file = std::make_shared<DataFile>();
  dv_file->content = DataFile::Content::kPositionDeletes;
  dv_file->file_path = table_location_ + "/delete/dv.puffin";
  dv_file->file_format = FileFormatType::kPuffin;
  dv_file->referenced_data_file = data_file_path;
  dv_file->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
  dv_file->file_size_in_bytes = 256;
  dv_file->record_count = 5;
  dv_file->partition_spec_id = spec_->spec_id();

  auto manifest_path = std::format("{}/metadata/dv-manifest-{}.avro", table_location_,
                                   manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(
      auto dv_manifest,
      WriteDeleteManifest(dv_file, /*data_seq=*/3L, file_io_, *metadata, manifest_path));

  auto deleted_data_file = std::make_shared<DataFile>();
  deleted_data_file->content = DataFile::Content::kData;
  deleted_data_file->file_path = data_file_path;
  deleted_data_file->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
  deleted_data_file->file_size_in_bytes = 1024;
  deleted_data_file->record_count = 50;
  deleted_data_file->partition_spec_id = spec_->spec_id();

  DataFileSet deleted_files;
  deleted_files.insert(deleted_data_file);

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  mgr->RemoveDanglingDeletesFor(deleted_files);
  mgr->FailAnyDelete();

  std::vector<const ManifestFile*> manifests{&dv_manifest};
  auto specs = SpecsById(*metadata);
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());

  auto result = mgr->FilterManifests(schema, specs, manifests, factory);
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Operation would delete existing data"));
}

TEST_F(ManifestFilterManagerTest, RemoveDanglingDeletesForFiltersDanglingDV) {
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  const std::string data_file_path = table_location_ + "/data/referenced.parquet";

  // Create a DV (position-delete, puffin format) referencing the data file.
  auto dv_file = std::make_shared<DataFile>();
  dv_file->content = DataFile::Content::kPositionDeletes;
  dv_file->file_path = table_location_ + "/delete/dv.puffin";
  dv_file->file_format = FileFormatType::kPuffin;
  dv_file->referenced_data_file = data_file_path;
  dv_file->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
  dv_file->file_size_in_bytes = 256;
  dv_file->record_count = 5;
  dv_file->partition_spec_id = spec_->spec_id();

  auto manifest_path = std::format("{}/metadata/dv-manifest-{}.avro", table_location_,
                                   manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(
      auto dv_manifest,
      WriteDeleteManifest(dv_file, /*data_seq=*/3L, file_io_, *metadata, manifest_path));

  // Register the referenced data file as deleted.
  auto deleted_data_file = std::make_shared<DataFile>();
  deleted_data_file->content = DataFile::Content::kData;
  deleted_data_file->file_path = data_file_path;
  deleted_data_file->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
  deleted_data_file->file_size_in_bytes = 1024;
  deleted_data_file->record_count = 50;
  deleted_data_file->partition_spec_id = spec_->spec_id();

  DataFileSet deleted_files;
  deleted_files.insert(deleted_data_file);

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  mgr->RemoveDanglingDeletesFor(deleted_files);

  std::vector<const ManifestFile*> manifests{&dv_manifest};
  auto specs = SpecsById(*metadata);
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());

  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr->FilterManifests(schema, specs, manifests, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  ASSERT_EQ(entries.size(), 1U);
  EXPECT_EQ(entries[0].status, ManifestStatus::kDeleted);
}

TEST_F(ManifestFilterManagerTest, RemoveDanglingDeletesForKeepsNonDanglingDeletes) {
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  const std::string deleted_data_file_path = table_location_ + "/data/deleted.parquet";

  auto make_delete = [&](std::string path, FileFormatType format,
                         std::optional<std::string> referenced_data_file) {
    auto f = std::make_shared<DataFile>();
    f->content = DataFile::Content::kPositionDeletes;
    f->file_path = std::move(path);
    f->file_format = format;
    f->referenced_data_file = std::move(referenced_data_file);
    f->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
    f->file_size_in_bytes = 256;
    f->record_count = 5;
    f->partition_spec_id = spec_->spec_id();
    return f;
  };

  auto ordinary_position_delete =
      make_delete(table_location_ + "/delete/ordinary.parquet", FileFormatType::kParquet,
                  deleted_data_file_path);
  auto dv_without_reference = make_delete(table_location_ + "/delete/no-ref.puffin",
                                          FileFormatType::kPuffin, std::nullopt);
  auto unrelated_dv =
      make_delete(table_location_ + "/delete/unrelated.puffin", FileFormatType::kPuffin,
                  table_location_ + "/data/other.parquet");

  auto manifest_path = std::format("{}/metadata/dv-keep-manifest-{}.avro",
                                   table_location_, manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(auto manifest,
                         WriteDeleteManifest({{ordinary_position_delete, 3L},
                                              {dv_without_reference, 3L},
                                              {unrelated_dv, 3L}},
                                             file_io_, *metadata, manifest_path));

  auto deleted_data_file = std::make_shared<DataFile>();
  deleted_data_file->content = DataFile::Content::kData;
  deleted_data_file->file_path = deleted_data_file_path;
  deleted_data_file->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
  deleted_data_file->file_size_in_bytes = 1024;
  deleted_data_file->record_count = 50;
  deleted_data_file->partition_spec_id = spec_->spec_id();

  DataFileSet deleted_files;
  deleted_files.insert(deleted_data_file);

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  mgr->RemoveDanglingDeletesFor(deleted_files);

  std::vector<const ManifestFile*> manifests{&manifest};
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());
  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(schema, SpecsById(*metadata),
                                                           manifests, factory));

  ASSERT_EQ(result.size(), 1U);
  EXPECT_EQ(result[0].manifest_path, manifest.manifest_path);
}

TEST_F(ManifestFilterManagerTest, RemoveDanglingDeletesForReplacesRemovedFileSet) {
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  auto make_dv = [&](std::string path, std::string referenced_data_file) {
    auto f = std::make_shared<DataFile>();
    f->content = DataFile::Content::kPositionDeletes;
    f->file_path = std::move(path);
    f->file_format = FileFormatType::kPuffin;
    f->referenced_data_file = std::move(referenced_data_file);
    f->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
    f->file_size_in_bytes = 256;
    f->record_count = 5;
    f->partition_spec_id = spec_->spec_id();
    return f;
  };

  const std::string first_data = table_location_ + "/data/first.parquet";
  const std::string second_data = table_location_ + "/data/second.parquet";
  auto first_dv = make_dv(table_location_ + "/delete/first.puffin", first_data);
  auto second_dv = make_dv(table_location_ + "/delete/second.puffin", second_data);

  auto first_manifest_path =
      std::format("{}/metadata/dv-first-{}.avro", table_location_, manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(
      auto first_manifest,
      WriteDeleteManifest(first_dv, 3L, file_io_, *metadata, first_manifest_path));
  auto second_manifest_path =
      std::format("{}/metadata/dv-second-{}.avro", table_location_, manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_manifest,
      WriteDeleteManifest(second_dv, 3L, file_io_, *metadata, second_manifest_path));

  auto make_deleted_data = [&](std::string path) {
    auto file = std::make_shared<DataFile>();
    file->content = DataFile::Content::kData;
    file->file_path = std::move(path);
    file->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
    file->file_size_in_bytes = 1024;
    file->record_count = 50;
    file->partition_spec_id = spec_->spec_id();
    return file;
  };

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  DataFileSet first_deleted;
  first_deleted.insert(make_deleted_data(first_data));
  mgr->RemoveDanglingDeletesFor(first_deleted);

  DataFileSet second_deleted;
  second_deleted.insert(make_deleted_data(second_data));
  mgr->RemoveDanglingDeletesFor(second_deleted);

  std::vector<const ManifestFile*> manifests{&first_manifest, &second_manifest};
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_result,
      mgr->FilterManifests(schema, SpecsById(*metadata), manifests, factory));
  ASSERT_EQ(second_result.size(), 2U);
  EXPECT_EQ(second_result[0].manifest_path, first_manifest.manifest_path);
  EXPECT_NE(second_result[1].manifest_path, second_manifest.manifest_path);
  ICEBERG_UNWRAP_OR_FAIL(auto second_entries, ReadAllEntries(second_result, *metadata));

  bool saw_first_live = false;
  bool saw_second_deleted = false;
  for (const auto& entry : second_entries) {
    ASSERT_NE(entry.data_file, nullptr);
    if (entry.data_file->file_path == first_dv->file_path) {
      saw_first_live = true;
      EXPECT_NE(entry.status, ManifestStatus::kDeleted);
    } else if (entry.data_file->file_path == second_dv->file_path) {
      saw_second_deleted = true;
      EXPECT_EQ(entry.status, ManifestStatus::kDeleted);
    }
  }
  EXPECT_TRUE(saw_first_live);
  EXPECT_TRUE(saw_second_deleted);
}

TEST_F(ManifestFilterManagerTest, DeleteFileObjectMatchesDeletionVectorByContent) {
  auto metadata = *table_->metadata();
  metadata.format_version = 3;
  auto factory = MakeWriterFactory(metadata);

  auto make_dv = [&](int64_t offset) {
    auto f = std::make_shared<DataFile>();
    f->content = DataFile::Content::kPositionDeletes;
    f->file_path = table_location_ + "/delete/dv.puffin";
    f->file_format = FileFormatType::kPuffin;
    f->referenced_data_file =
        std::format("{}/data/referenced-{}.parquet", table_location_, offset);
    f->content_offset = offset;
    f->content_size_in_bytes = 10;
    f->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
    f->file_size_in_bytes = 256;
    f->record_count = 5;
    f->partition_spec_id = spec_->spec_id();
    return f;
  };
  auto dv0 = make_dv(0);
  auto dv1 = make_dv(10);

  auto manifest_path = std::format("{}/metadata/dv-manifest-{}.avro", table_location_,
                                   manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(
      auto manifest,
      WriteDeleteManifest({{dv0, 3L}, {dv1, 3L}}, file_io_, metadata, manifest_path));

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  EXPECT_THAT(mgr->DeleteFile(dv0), IsOk());

  std::vector<const ManifestFile*> manifests{&manifest};
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata.Schema());
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr->FilterManifests(schema, SpecsById(metadata), manifests, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, metadata));
  ASSERT_EQ(entries.size(), 2U);
  for (const auto& entry : entries) {
    ASSERT_NE(entry.data_file, nullptr);
    if (entry.data_file->content_offset == 0) {
      EXPECT_EQ(entry.status, ManifestStatus::kDeleted);
    } else {
      EXPECT_EQ(entry.status, ManifestStatus::kExisting);
    }
  }
}

TEST_F(ManifestFilterManagerTest, FailMissingDeleteFileObjectUsesDeleteFileIdentity) {
  auto metadata = *table_->metadata();
  metadata.format_version = 3;
  auto factory = MakeWriterFactory(metadata);

  auto make_dv = [&](int64_t offset) {
    auto f = std::make_shared<DataFile>();
    f->content = DataFile::Content::kPositionDeletes;
    f->file_path = table_location_ + "/delete/dv.puffin";
    f->file_format = FileFormatType::kPuffin;
    f->referenced_data_file =
        std::format("{}/data/referenced-{}.parquet", table_location_, offset);
    f->content_offset = offset;
    f->content_size_in_bytes = 10;
    f->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
    f->file_size_in_bytes = 256;
    f->record_count = 5;
    f->partition_spec_id = spec_->spec_id();
    return f;
  };
  auto present_dv = make_dv(0);
  auto missing_dv = make_dv(10);

  auto manifest_path = std::format("{}/metadata/dv-manifest-{}.avro", table_location_,
                                   manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteDeleteManifest(present_dv, 3L, file_io_,
                                                            metadata, manifest_path));

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  EXPECT_THAT(mgr->DeleteFile(missing_dv), IsOk());
  mgr->FailMissingDeletePaths();

  std::vector<const ManifestFile*> manifests{&manifest};
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata.Schema());
  EXPECT_THAT(mgr->FilterManifests(schema, SpecsById(metadata), manifests, factory),
              IsError(ErrorKind::kValidationFailed));
}

TEST_F(ManifestFilterManagerTest, DuplicateDeletesCountRepeatedDeletedFiles) {
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  auto del_file = std::make_shared<DataFile>();
  del_file->content = DataFile::Content::kPositionDeletes;
  del_file->file_path = table_location_ + "/delete/duplicate.parquet";
  del_file->file_format = FileFormatType::kParquet;
  del_file->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
  del_file->file_size_in_bytes = 512;
  del_file->record_count = 10;
  del_file->partition_spec_id = spec_->spec_id();

  auto manifest_path = std::format("{}/metadata/dup-manifest-{}.avro", table_location_,
                                   manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(auto manifest,
                         WriteDeleteManifest({{del_file, 3L}, {del_file, 3L}}, file_io_,
                                             *metadata, manifest_path));

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  EXPECT_THAT(mgr->DeleteFile(del_file), IsOk());

  std::vector<const ManifestFile*> manifests{&manifest};
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());
  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->FilterManifests(schema, SpecsById(*metadata),
                                                           manifests, factory));

  EXPECT_EQ(mgr->DeletedFiles().size(), 1U);
  EXPECT_EQ(mgr->DuplicateDeletesCount(), 1);
}

TEST_F(ManifestFilterManagerTest, DuplicateDeletesCountDoesNotCrossManifestBoundary) {
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  auto del_file = std::make_shared<DataFile>();
  del_file->content = DataFile::Content::kPositionDeletes;
  del_file->file_path = table_location_ + "/delete/reused.parquet";
  del_file->file_format = FileFormatType::kParquet;
  del_file->partition = PartitionValues(std::vector<Literal>{Literal::Long(1L)});
  del_file->file_size_in_bytes = 512;
  del_file->record_count = 10;
  del_file->partition_spec_id = spec_->spec_id();

  auto first_manifest_path =
      std::format("{}/metadata/reused-a-{}.avro", table_location_, manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(
      auto first_manifest,
      WriteDeleteManifest(del_file, 3L, file_io_, *metadata, first_manifest_path));
  auto second_manifest_path =
      std::format("{}/metadata/reused-b-{}.avro", table_location_, manifest_counter_++);
  ICEBERG_UNWRAP_OR_FAIL(
      auto second_manifest,
      WriteDeleteManifest(del_file, 3L, file_io_, *metadata, second_manifest_path));

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  EXPECT_THAT(mgr->DeleteFile(del_file), IsOk());

  std::vector<const ManifestFile*> manifests{&first_manifest, &second_manifest};
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());
  auto specs = SpecsById(*metadata);
  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr->FilterManifests(schema, specs, manifests, factory));
  ICEBERG_UNWRAP_OR_FAIL(auto summary, mgr->BuildSummary(result, specs));

  EXPECT_EQ(mgr->DeletedFiles().size(), 1U);
  EXPECT_EQ(mgr->DuplicateDeletesCount(), 0);
  auto summary_map = summary.Build();
  EXPECT_EQ(summary_map.at(SnapshotSummaryFields::kRemovedDeleteFiles), "2");
  EXPECT_EQ(summary_map.count(SnapshotSummaryFields::kDeletedDuplicatedFiles), 0U);
}

TEST_F(ManifestFilterManagerTest, TrustedSourceSkipsUnrelatedManifestInSamePartition) {
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteDataManifest({file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadManifestEntries(manifest));
  ASSERT_EQ(entries.size(), 1U);
  ASSERT_EQ(entries[0].data_file->manifest_location, manifest.manifest_path);

  // This descriptor has identical bounds. Opening its missing path would fail.
  auto other = manifest;
  other.manifest_path += ".unreadable";
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile(entries[0].data_file), IsOk());
  mgr->FailMissingDeletePaths();
  auto& metadata = *table_->metadata();
  ICEBERG_UNWRAP_OR_FAIL(
      auto filtered,
      mgr->FilterManifests(schema_, SpecsById(metadata), {&other, &manifest},
                           MakeWriterFactory(metadata)));
  ASSERT_EQ(filtered.size(), 2U);
  EXPECT_EQ(filtered[0], other);
  EXPECT_EQ(filtered[1].deleted_files_count, 1);
}

TEST_F(ManifestFilterManagerTest, MixedOrStaleReferencesScanAllCandidates) {
  ICEBERG_UNWRAP_OR_FAIL(auto a, WriteDataManifest({file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(auto b, WriteDataManifest({file_b_}));
  file_a_->manifest_location = a.manifest_path;
  auto stale = std::make_shared<DataFile>(*file_b_);
  stale->manifest_location = "old-manifest.avro";

  using AddDelete = std::function<Status(ManifestFilterManager&)>;
  const std::vector<std::pair<std::string, AddDelete>> cases{
      {"stale manifest", [&](auto& mgr) { return mgr.DeleteFile(stale); }},
      {"no manifest", [&](auto& mgr) { return mgr.DeleteFile(file_b_); }},
      {"path", [&](auto& mgr) { return mgr.DeleteFile(file_b_->file_path); }},
      {"partition",
       [&](auto& mgr) {
         return mgr.DropPartition(spec_->spec_id(), file_b_->partition);
       }},
      {"expression",
       [&](auto& mgr) {
         return mgr.DeleteByRowFilter(Expressions::Equal("x", Literal::Long(2)));
       }},
  };

  auto& metadata = *table_->metadata();
  for (const auto& [name, add] : cases) {
    SCOPED_TRACE(name);
    ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                           ManifestFilterManager::Make(ManifestContent::kData, file_io_));
    ASSERT_THAT(mgr->DeleteFile(file_a_), IsOk());
    ASSERT_THAT(add(*mgr), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto filtered,
                           mgr->FilterManifests(schema_, SpecsById(metadata), {&a, &b},
                                                MakeWriterFactory(metadata)));
    ASSERT_EQ(filtered.size(), 2U);
    EXPECT_EQ(filtered[0].deleted_files_count, 1);
    EXPECT_EQ(filtered[1].deleted_files_count, 1);
  }
}

TEST_F(ManifestFilterManagerTest, ObjectAndPartitionDeletesPruneBySpecAndBounds) {
  ICEBERG_UNWRAP_OR_FAIL(auto a, WriteDataManifest({file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(auto b, WriteDataManifest({file_b_}));
  auto different_spec = b;
  different_spec.manifest_path += ".foreign";
  different_spec.partition_spec_id = spec_->spec_id() + 1;
  a.manifest_path += ".out-of-range";
  b.partitions.clear();  // v1 manifest lists can omit partition summaries.
  auto& metadata = *table_->metadata();
  for (bool by_object : {false, true}) {
    SCOPED_TRACE(by_object);
    ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                           ManifestFilterManager::Make(ManifestContent::kData, file_io_));
    ASSERT_THAT(by_object ? mgr->DeleteFile(file_b_)
                          : mgr->DropPartition(spec_->spec_id(), file_b_->partition),
                IsOk());
    ICEBERG_UNWRAP_OR_FAIL(
        auto filtered,
        mgr->FilterManifests(schema_, SpecsById(metadata), {&different_spec, &a, &b},
                             MakeWriterFactory(metadata)));
    ASSERT_EQ(filtered.size(), 3U);
    EXPECT_EQ(filtered[0], different_spec);
    EXPECT_EQ(filtered[1], a);
    EXPECT_EQ(filtered[2].deleted_files_count, 1);
  }
}

TEST_F(ManifestFilterManagerTest, MissingObjectSpecRetainsPathIdentity) {
  ICEBERG_UNWRAP_OR_FAIL(auto input, WriteDataManifest({file_a_}));
  file_a_->partition_spec_id.reset();
  file_a_->partition = PartitionValues{};
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile(file_a_), IsOk());
  mgr->FailMissingDeletePaths();
  auto& metadata = *table_->metadata();
  ICEBERG_UNWRAP_OR_FAIL(auto filtered,
                         mgr->FilterManifests(schema_, SpecsById(metadata), {&input},
                                              MakeWriterFactory(metadata)));
  ASSERT_EQ(filtered.size(), 1U);
  EXPECT_EQ(filtered[0].deleted_files_count, 1);
}

TEST_F(ManifestFilterManagerTest, RequiredDeletesIncludeFilesFoundOnAnEarlierAttempt) {
  ICEBERG_UNWRAP_OR_FAIL(auto a, WriteDataManifest({file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(auto b, WriteDataManifest({file_b_}));
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::AlwaysTrue()), IsOk());
  mgr->FailMissingDeletePaths();
  auto& metadata = *table_->metadata();
  auto filter = [&](std::vector<const ManifestFile*> inputs) {
    return mgr->FilterManifests(schema_, SpecsById(metadata), inputs,
                                MakeWriterFactory(metadata));
  };

  ASSERT_THAT(filter({&a}), IsOk());
  EXPECT_THAT(filter({}), HasErrorMessage(file_a_->file_path));
  EXPECT_THAT(filter({&b}), HasErrorMessage(file_a_->file_path));
  EXPECT_TRUE(mgr->FilesToBeDeleted().contains(file_a_));
  EXPECT_TRUE(mgr->FilesToBeDeleted().contains(file_b_));
  const auto writers = manifest_counter_;
  ICEBERG_UNWRAP_OR_FAIL(auto retry, filter({&b, &a}));
  ASSERT_EQ(retry.size(), 2U);
  EXPECT_EQ(manifest_counter_, writers);
  ASSERT_EQ(mgr->DeletedFiles().size(), 2U);
  EXPECT_EQ(mgr->DeletedFiles()[0]->file_path, file_b_->file_path);
  EXPECT_EQ(mgr->DeletedFiles()[1]->file_path, file_a_->file_path);
}

TEST_F(ManifestFilterManagerTest, BuildSummaryCountsOnlyDuplicatesInRequestedManifests) {
  ICEBERG_UNWRAP_OR_FAIL(auto a, WriteDataManifest({file_a_, file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(auto b, WriteDataManifest({file_b_}));
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::AlwaysTrue()), IsOk());
  auto& metadata = *table_->metadata();
  auto specs = SpecsById(metadata);
  ICEBERG_UNWRAP_OR_FAIL(
      auto filtered,
      mgr->FilterManifests(schema_, specs, {&a, &b}, MakeWriterFactory(metadata)));
  ASSERT_EQ(filtered.size(), 2U);
  ICEBERG_UNWRAP_OR_FAIL(auto a_summary, mgr->BuildSummary({filtered[0]}, specs));
  EXPECT_EQ(a_summary.Build().at(SnapshotSummaryFields::kDeletedDataFiles), "1");
  EXPECT_EQ(a_summary.Build().at(SnapshotSummaryFields::kDeletedDuplicatedFiles), "1");
  ICEBERG_UNWRAP_OR_FAIL(auto b_summary, mgr->BuildSummary({filtered[1]}, specs));
  EXPECT_EQ(b_summary.Build().at(SnapshotSummaryFields::kDeletedDataFiles), "1");
  EXPECT_FALSE(
      b_summary.Build().contains(SnapshotSummaryFields::kDeletedDuplicatedFiles));
  ASSERT_THAT(mgr->CleanUncommitted({filtered[1].manifest_path}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto kept, mgr->BuildSummary(filtered, specs));
  EXPECT_EQ(kept.Build().at(SnapshotSummaryFields::kDeletedDataFiles), "1");
  EXPECT_FALSE(kept.Build().contains(SnapshotSummaryFields::kDeletedDuplicatedFiles));
}

TEST_F(ManifestFilterManagerTest,
       LaterPartialMatchDiscardsPartialWriterAndCachesCompletedManifest) {
  auto partial = MakeDataFile("/data/partial.parquet", 2L);
  ICEBERG_UNWRAP_OR_FAIL(partial->lower_bounds[2], Literal::Long(0).Serialize());
  ICEBERG_UNWRAP_OR_FAIL(partial->upper_bounds[2], Literal::Long(10).Serialize());
  ICEBERG_UNWRAP_OR_FAIL(auto a, WriteDataManifest({file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(auto b, WriteDataManifest({file_b_, partial}));
  std::vector<std::string> discarded;
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_,
                                                     [&](const std::string& path) {
                                                       discarded.push_back(path);
                                                       return file_io_->DeleteFile(path);
                                                     }));
  ASSERT_THAT(mgr->DeleteFile(file_a_->file_path), IsOk());
  ASSERT_THAT(mgr->DeleteFile(file_b_->file_path), IsOk());
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::Equal("y", Literal::Long(5))), IsOk());
  auto& metadata = *table_->metadata();
  auto filter = [&](std::vector<const ManifestFile*> inputs) {
    return mgr->FilterManifests(schema_, SpecsById(metadata), inputs,
                                MakeWriterFactory(metadata));
  };
  auto result = filter({&a, &b});
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage(partial->file_path));
  ASSERT_EQ(discarded.size(), 1U);
  EXPECT_FALSE(file_io_->ReadFile(discarded.front(), std::nullopt).has_value());
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 1);
  EXPECT_TRUE(mgr->DeletedFiles().empty());
  EXPECT_TRUE(mgr->FilesToBeDeleted().empty());

  const auto writers = manifest_counter_;
  ICEBERG_UNWRAP_OR_FAIL(auto retry, filter({&a}));
  EXPECT_EQ(manifest_counter_, writers);
  ASSERT_EQ(retry.size(), 1U);
  ASSERT_THAT(mgr->CleanUncommitted({}), IsOk());
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 0);
  ASSERT_EQ(discarded.size(), 2U);
  EXPECT_EQ(discarded[1], retry[0].manifest_path);
}

TEST_F(ManifestFilterManagerTest,
       RewriteRestartsRowIdInheritanceAndCachesFilesWithoutStats) {
  file_b_->column_sizes[2] = 75;
  ICEBERG_UNWRAP_OR_FAIL(auto input,
                         WriteDataManifest({file_a_, file_b_}, /*first_row_id=*/200L));
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile(file_b_->file_path), IsOk());
  auto metadata = *table_->metadata();
  metadata.format_version = 3;
  ICEBERG_UNWRAP_OR_FAIL(auto filtered,
                         mgr->FilterManifests(schema_, SpecsById(metadata), {&input},
                                              MakeWriterFactory(metadata)));
  ASSERT_EQ(filtered.size(), 1U);
  ICEBERG_UNWRAP_OR_FAIL(auto reader,
                         ManifestReader::Make(filtered[0], file_io_, schema_, spec_,
                                              /*is_committed=*/false));
  ICEBERG_UNWRAP_OR_FAIL(auto entries, reader->Entries());
  ASSERT_EQ(entries.size(), 2U);
  EXPECT_EQ(entries[0].status, ManifestStatus::kExisting);
  EXPECT_EQ(entries[0].data_file->first_row_id, 200);
  EXPECT_EQ(entries[1].status, ManifestStatus::kDeleted);
  EXPECT_EQ(entries[1].data_file->first_row_id, 200 + file_a_->record_count);
  EXPECT_EQ(entries[1].data_file->column_sizes, file_b_->column_sizes);
  EXPECT_EQ(entries[1].sequence_number, 5);
  EXPECT_EQ(entries[1].file_sequence_number, 7);
  ASSERT_EQ(mgr->DeletedFiles().size(), 1U);
  const auto& deleted = mgr->DeletedFiles().front();
  EXPECT_EQ(deleted->first_row_id, entries[1].data_file->first_row_id);
  EXPECT_EQ(deleted->manifest_location, input.manifest_path);
  EXPECT_TRUE(deleted->column_sizes.empty());
}

TEST_F(ManifestFilterManagerTest, UpdatingCaseSensitivityRebindsCachedFilters) {
  ICEBERG_UNWRAP_OR_FAIL(auto input, WriteDataManifest({file_a_}));
  auto& metadata = *table_->metadata();
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  mgr->CaseSensitive(false);
  ASSERT_THAT(mgr->DeleteByRowFilter(Expressions::Equal("X", Literal::Long(1))), IsOk());
  auto filter = [&] {
    return mgr->FilterManifests(schema_, SpecsById(metadata), {&input},
                                MakeWriterFactory(metadata));
  };
  ASSERT_THAT(filter(), IsOk());
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 1);
  mgr->CaseSensitive(true);
  EXPECT_FALSE(filter().has_value());
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 0);
}

TEST_F(ManifestFilterManagerTest, EnablingFailAnyDeleteChecksCachedTrustedRewrite) {
  ICEBERG_UNWRAP_OR_FAIL(auto input, WriteDataManifest({file_a_}));
  auto& metadata = *table_->metadata();
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  file_a_->manifest_location = input.manifest_path;
  ASSERT_THAT(mgr->DeleteFile(file_a_), IsOk());
  auto filter = [&] {
    return mgr->FilterManifests(schema_, SpecsById(metadata), {&input},
                                MakeWriterFactory(metadata));
  };
  ASSERT_THAT(filter(), IsOk());
  mgr->FailAnyDelete();
  EXPECT_THAT(filter(), HasErrorMessage("Operation would delete existing data: x=1"));
}

TEST_F(ManifestFilterManagerTest, SequenceThresholdChangesInvalidateCachedEntries) {
  auto file = std::make_shared<DataFile>(*file_a_);
  file->content = DataFile::Content::kPositionDeletes;
  auto& metadata = *table_->metadata();
  ICEBERG_UNWRAP_OR_FAIL(
      auto input, WriteDeleteManifest(file, 4, file_io_, metadata,
                                      table_location_ + "/metadata/sequence.avro"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  ASSERT_THAT(mgr->DeleteFile("/unrelated"), IsOk());
  auto filter = [&] {
    return mgr->FilterManifests(schema_, SpecsById(metadata), {&input},
                                MakeWriterFactory(metadata));
  };
  ICEBERG_UNWRAP_OR_FAIL(auto original, filter());
  EXPECT_EQ(original.front(), input);
  ASSERT_THAT(mgr->DropDeleteFilesOlderThan(5), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto stale, filter());
  EXPECT_EQ(stale.front().deleted_files_count, 1);
  ASSERT_THAT(mgr->DropDeleteFilesOlderThan(5), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto retry, filter());
  EXPECT_EQ(retry, stale);
  ASSERT_THAT(mgr->DropDeleteFilesOlderThan(3), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto kept, filter());
  EXPECT_EQ(kept.front(), input);
}

TEST_F(ManifestFilterManagerTest,
       DanglingDVOutsideReferencedManifestsReevaluatedWhenTargetsChange) {
  auto make_dv = [&](const std::shared_ptr<DataFile>& data) {
    auto dv = std::make_shared<DataFile>(*data);
    dv->content = DataFile::Content::kPositionDeletes;
    dv->file_format = FileFormatType::kPuffin;
    dv->referenced_data_file = data->file_path;
    dv->file_path += ".puffin";
    dv->content_offset = 4;
    dv->content_size_in_bytes = 8;
    return dv;
  };
  auto metadata = *table_->metadata();
  metadata.format_version = 3;
  auto a = make_dv(file_a_), b = make_dv(file_b_);
  ICEBERG_UNWRAP_OR_FAIL(auto input_a,
                         WriteDeleteManifest(a, 5, file_io_, metadata,
                                             table_location_ + "/metadata/a-dv.avro"));
  ICEBERG_UNWRAP_OR_FAIL(auto input_b,
                         WriteDeleteManifest(b, 5, file_io_, metadata,
                                             table_location_ + "/metadata/b-dv.avro"));
  a->manifest_location = input_a.manifest_path;
  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestFilterManager::Make(ManifestContent::kDeletes, file_io_));
  ASSERT_THAT(mgr->DeleteFile(a), IsOk());
  auto filter = [&] {
    return mgr->FilterManifests(schema_, SpecsById(metadata), {&input_a, &input_b},
                                MakeWriterFactory(metadata));
  };
  ICEBERG_UNWRAP_OR_FAIL(auto initial, filter());
  EXPECT_EQ(initial[0].deleted_files_count, 1);
  EXPECT_EQ(initial[1], input_b);
  DataFileSet removed;
  removed.insert(file_b_);
  mgr->RemoveDanglingDeletesFor(removed);
  ICEBERG_UNWRAP_OR_FAIL(auto dangling, filter());
  EXPECT_EQ(dangling[0].deleted_files_count, 1);
  EXPECT_EQ(dangling[1].deleted_files_count, 1);
  mgr->RemoveDanglingDeletesFor({});
  ICEBERG_UNWRAP_OR_FAIL(auto retained, filter());
  EXPECT_EQ(retained[0].deleted_files_count, 1);
  EXPECT_EQ(retained[1], input_b);
}

TEST_F(ManifestFilterManagerTest, PartitionBoundsAndDropsUseSpecFieldTypes) {
  ICEBERG_UNWRAP_OR_FAIL(auto id,
                         Uuid::FromString("f34cb878-bfaa-471c-b9f8-e9987dce83a0"));
  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "event_time", timestamp()),
                               SchemaField::MakeRequired(2, "price", decimal(9, 2)),
                               SchemaField::MakeRequired(3, "code", fixed(2)),
                               SchemaField::MakeRequired(4, "id", uuid())});
  ICEBERG_UNWRAP_OR_FAIL(
      spec_, PartitionSpec::Make(*schema_, 0,
                                 {PartitionField(1, 1000, "day", Transform::Day()),
                                  PartitionField(1, 1001, "time", Transform::Identity()),
                                  PartitionField(2, 1002, "price", Transform::Identity()),
                                  PartitionField(3, 1003, "code", Transform::Identity()),
                                  PartitionField(4, 1004, "id", Transform::Identity())},
                                 /*allow_missing_fields=*/false));
  file_a_->partition = PartitionValues(std::vector<Literal>{
      Literal::Date(2), Literal::Timestamp(172800000000L), Literal::Decimal(314, 9, 2),
      Literal::Fixed({0x01, 0x80}), Literal::UUID(id)});
  ICEBERG_UNWRAP_OR_FAIL(auto input, WriteDataManifest({file_a_}));
  auto metadata = *table_->metadata();
  metadata.schemas = {schema_};
  metadata.partition_specs = {spec_};
  auto filter = [&](ManifestFilterManager& mgr) {
    return mgr.FilterManifests(schema_, SpecsById(metadata), {&input},
                               MakeWriterFactory(metadata));
  };

  // A file obtained from a manifest can outlive that manifest on a commit retry.
  ICEBERG_UNWRAP_OR_FAIL(auto reader,
                         ManifestReader::Make(input, file_io_, schema_, spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto entries, reader->Entries());
  ASSERT_EQ(entries.size(), 1U);
  auto old_file = entries[0].data_file;
  old_file->manifest_location = "previous-manifest.avro";
  ASSERT_EQ(old_file->partition.values()[0].type()->type_id(), TypeId::kInt);
  ASSERT_EQ(old_file->partition.values()[1].type()->type_id(), TypeId::kLong);
  ASSERT_EQ(old_file->partition.values()[3], Literal::Fixed({0x01, 0x80}));
  ASSERT_EQ(old_file->partition.values()[4], Literal::UUID(id));
  ICEBERG_UNWRAP_OR_FAIL(auto by_object,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(by_object->DeleteFile(old_file), IsOk());
  by_object->FailMissingDeletePaths();
  ICEBERG_UNWRAP_OR_FAIL(auto deleted, filter(*by_object));
  EXPECT_EQ(deleted.front().deleted_files_count, 1);

  ICEBERG_UNWRAP_OR_FAIL(auto by_partition,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  PartitionValues wanted(std::vector<Literal>{
      Literal::Date(2), Literal::Timestamp(172800000000L), Literal::Decimal(314, 12, 2),
      Literal::Binary({0x01, 0x80}), Literal::UUID(id)});
  ASSERT_THAT(by_partition->DropPartition(0, std::move(wanted)), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto dropped, filter(*by_partition));
  EXPECT_EQ(dropped.front().deleted_files_count, 1);
}

TEST_F(ManifestFilterManagerTest, RetryRechecksSkippedManifestsWhenASourceDisappears) {
  ICEBERG_UNWRAP_OR_FAIL(auto a, WriteDataManifest({file_a_}));
  ICEBERG_UNWRAP_OR_FAIL(auto duplicate, WriteDataManifest({file_a_}));
  file_a_->manifest_location = a.manifest_path;
  auto& metadata = *table_->metadata();
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestFilterManager::Make(ManifestContent::kData, file_io_));
  ASSERT_THAT(mgr->DeleteFile(file_a_), IsOk());
  mgr->FailMissingDeletePaths();
  auto filter = [&](std::vector<const ManifestFile*> input) {
    return mgr->FilterManifests(schema_, SpecsById(metadata), input,
                                MakeWriterFactory(metadata));
  };
  ICEBERG_UNWRAP_OR_FAIL(auto trusted, filter({&a, &duplicate}));
  EXPECT_EQ(trusted[1], duplicate);
  ICEBERG_UNWRAP_OR_FAIL(auto stale, filter({&duplicate}));
  EXPECT_EQ(stale[0].deleted_files_count, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto restored, filter({&a, &duplicate}));
  EXPECT_EQ(restored, trusted);
}

TEST_F(ManifestFilterManagerTest, FixedPartitionSummaryComparesPhysicalBinaryValues) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "x", fixed(2))});
  ICEBERG_UNWRAP_OR_FAIL(
      auto spec,
      PartitionSpec::Make(*schema, 0,
                          {PartitionField(1, 1000, "x", Transform::Identity())}, false));
  ManifestFilterManager::PartitionSpecsById specs{
      {0, std::shared_ptr<PartitionSpec>(std::move(spec))}};
  ManifestFile manifest{
      .manifest_path = "fixed.avro",
      .partitions = {PartitionFieldSummary{
          .contains_null = false, .lower_bound = {{0, 3}}, .upper_bound = {{0, 8}}}}};
  auto filter = [&](std::vector<uint8_t> value) -> Result<std::vector<ManifestFile>> {
    ICEBERG_ASSIGN_OR_RAISE(
        auto mgr, ManifestFilterManager::Make(ManifestContent::kData, file_io_));
    ICEBERG_RETURN_UNEXPECTED(
        mgr->DropPartition(0, PartitionValues(Literal::Binary(std::move(value)))));
    return mgr->FilterManifests(schema, specs, {&manifest}, {});
  };
  // A candidate must open the reader, which requires an assigned snapshot ID.
  EXPECT_THAT(filter({0, 7}), HasErrorMessage("has no snapshot ID"));
  ICEBERG_UNWRAP_OR_FAIL(auto skipped, filter({0, 9}));
  EXPECT_THAT(skipped, ::testing::ElementsAre(manifest));
}

}  // namespace iceberg
