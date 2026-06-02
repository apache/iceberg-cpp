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
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/util/data_file_set.h"
#include "iceberg/util/macros.h"

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
  unrelated->partition = PartitionValues(std::vector<Literal>{Literal::Long(9L)});
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

}  // namespace iceberg
