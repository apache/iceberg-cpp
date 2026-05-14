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
#include "iceberg/exception.h"
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
  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);
  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr.FilterManifests(*metadata, nullptr, factory));
  EXPECT_TRUE(result.empty());
}

TEST_F(ManifestFilterManagerTest, ContainsDeletesReturnsCorrectState) {
  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  EXPECT_FALSE(mgr.ContainsDeletes());
  mgr.DeleteFile("/some/path.parquet");
  EXPECT_TRUE(mgr.ContainsDeletes());
}

TEST_F(ManifestFilterManagerTest, DeleteByRowFilterRejectsNull) {
  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  EXPECT_THROW(mgr.DeleteByRowFilter(nullptr), IcebergError);
}

TEST_F(ManifestFilterManagerTest, DeleteFileObjectRejectsNull) {
  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  std::shared_ptr<DataFile> null_file;
  EXPECT_THROW(mgr.DeleteFile(null_file), IcebergError);
}

TEST_F(ManifestFilterManagerTest, NoConditionsReturnsManifestsUnchanged) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  // Load original manifests so we can compare paths
  ICEBERG_UNWRAP_OR_FAIL(auto list_reader,
                         ManifestListReader::Make(snap->manifest_list, file_io_));
  ICEBERG_UNWRAP_OR_FAIL(auto orig_manifests, list_reader->Files());

  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr.FilterManifests(*metadata, snap, factory));

  ASSERT_EQ(result.size(), orig_manifests.size());
  for (size_t i = 0; i < result.size(); ++i) {
    // No rewrite → same manifest path
    EXPECT_EQ(result[i].manifest_path, orig_manifests[i].manifest_path);
  }
}

TEST_F(ManifestFilterManagerTest, DeleteFileByPath) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  mgr.DeleteFile(file_a_->file_path);

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr.FilterManifests(*metadata, snap, factory));

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

TEST_F(ManifestFilterManagerTest, RowFilterAlwaysTrueDeletesAll) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  mgr.DeleteByRowFilter(Expressions::AlwaysTrue());

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr.FilterManifests(*metadata, snap, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  for (const auto& e : entries) {
    EXPECT_EQ(e.status, ManifestStatus::kDeleted) << "Expected all entries to be DELETED";
  }
}

TEST_F(ManifestFilterManagerTest, RowFilterAlwaysFalseDeletesNone) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  mgr.DeleteByRowFilter(Expressions::AlwaysFalse());

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr.FilterManifests(*metadata, snap, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  for (const auto& e : entries) {
    // AlwaysFalse means nothing can match → entries remain ADDED or EXISTING
    EXPECT_NE(e.status, ManifestStatus::kDeleted) << "Expected no entries to be DELETED";
  }
}

TEST_F(ManifestFilterManagerTest, RowFilterUsesPartitionResiduals) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  mgr.CaseSensitive(false);
  mgr.DeleteByRowFilter(Expressions::Equal("X", Literal::Long(1L)));

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr.FilterManifests(*metadata, snap, factory));

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
  ASSERT_EQ(mgr.FilesToBeDeleted().size(), 1U);
  EXPECT_EQ(mgr.FilesToBeDeleted().begin()->get()->file_path, file_a_->file_path);
}

TEST_F(ManifestFilterManagerTest, DropPartition) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  // Drop partition of file_a (partition_x = 1)
  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  mgr.DropPartition(spec_->spec_id(),
                    PartitionValues(std::vector<Literal>{Literal::Long(1L)}));

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr.FilterManifests(*metadata, snap, factory));

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

  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  mgr.DeleteFile("/does/not/exist.parquet");
  mgr.FailMissingDeletePaths();

  auto result = mgr.FilterManifests(*metadata, snap, factory);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

TEST_F(ManifestFilterManagerTest, MultipleConditionsOrCombined) {
  ICEBERG_UNWRAP_OR_FAIL(auto snap, CommitFiles({file_a_, file_b_}));
  auto* metadata = table_->metadata().get();
  auto factory = MakeWriterFactory(*metadata);

  // Both files should be deleted: file_a by path, file_b by AlwaysTrue expression
  ManifestFilterManager mgr(ManifestContent::kData, file_io_);
  mgr.DeleteFile(file_a_->file_path);
  mgr.DeleteByRowFilter(Expressions::AlwaysTrue());

  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr.FilterManifests(*metadata, snap, factory));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadAllEntries(result, *metadata));
  for (const auto& e : entries) {
    EXPECT_EQ(e.status, ManifestStatus::kDeleted);
  }
}

}  // namespace iceberg
