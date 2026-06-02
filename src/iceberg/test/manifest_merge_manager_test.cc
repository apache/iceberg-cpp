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

#include "iceberg/manifest/manifest_merge_manager.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

constexpr int8_t kFormatVersion = 2;
constexpr int8_t kRowIdFormatVersion = 3;
constexpr int64_t kSnapshotId = 12345L;
constexpr int32_t kSpecId0 = 0;
constexpr int32_t kSpecId1 = 1;

}  // namespace

class ManifestMergeManagerTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  void SetUp() override {
    file_io_ = arrow::MakeMockFileIO();

    // Simple schema: one long column
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(1, "x", int64()),
    });
    spec0_ = PartitionSpec::Make(kSpecId0,
                                 {PartitionField(1, 1000, "x", Transform::Identity())})
                 .value();
    spec1_ = PartitionSpec::Make(
                 kSpecId1, {PartitionField(1, 1001, "x_bucket", Transform::Bucket(8))})
                 .value();

    // Build minimal TableMetadata with both specs
    auto builder = TableMetadataBuilder::BuildFromEmpty(kFormatVersion);
    builder->SetCurrentSchema(schema_, schema_->HighestFieldId().value_or(0));
    builder->SetDefaultPartitionSpec(spec0_);
    builder->AddPartitionSpec(spec1_);
    builder->SetDefaultSortOrder(SortOrder::Unsorted());
    ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
    metadata_ = std::shared_ptr<TableMetadata>(std::move(metadata));
  }

  Result<std::shared_ptr<TableMetadata>> BuildV3Metadata() {
    auto builder = TableMetadataBuilder::BuildFromEmpty(kRowIdFormatVersion);
    builder->SetCurrentSchema(schema_, schema_->HighestFieldId().value_or(0));
    builder->SetDefaultPartitionSpec(spec0_);
    builder->AddPartitionSpec(spec1_);
    builder->SetDefaultSortOrder(SortOrder::Unsorted());
    ICEBERG_ASSIGN_OR_RAISE(auto metadata, builder->Build());
    metadata->next_row_id = 1000;
    return std::shared_ptr<TableMetadata>(std::move(metadata));
  }

  // Write a small manifest with N data files and return the ManifestFile descriptor.
  Result<ManifestFile> WriteManifest(int32_t spec_id, int num_files,
                                     int64_t file_size_override = 512,
                                     ManifestContent content = ManifestContent::kData) {
    auto path = std::format("manifest-{}.avro", manifest_counter_++);
    auto spec = spec_id == kSpecId0 ? spec0_ : spec1_;
    ICEBERG_ASSIGN_OR_RAISE(auto writer,
                            ManifestWriter::MakeWriter(kFormatVersion, kSnapshotId, path,
                                                       file_io_, spec, schema_, content));
    for (int i = 0; i < num_files; ++i) {
      auto f = std::make_shared<DataFile>();
      f->content = (content == ManifestContent::kDeletes)
                       ? DataFile::Content::kPositionDeletes
                       : DataFile::Content::kData;
      f->file_path = std::format("data/file-{}-{}.parquet", manifest_counter_, i);
      f->file_format = FileFormatType::kParquet;
      // Identity spec uses LONG partition values; Bucket spec uses INT
      Literal part_val = (spec_id == kSpecId0) ? Literal::Long(i) : Literal::Int(i % 8);
      f->partition = PartitionValues(std::vector<Literal>{part_val});
      f->file_size_in_bytes = 1024;
      f->record_count = 10;
      f->partition_spec_id = spec_id;
      ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(f));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_file, writer->ToManifestFile());
    // Override length so we can control bin-packing behaviour in tests
    manifest_file.manifest_length = file_size_override;
    return manifest_file;
  }

  Result<ManifestFile> WriteDataManifestWithFileRowIds(
      std::optional<int64_t> manifest_first_row_id,
      std::optional<int64_t> entry_first_row_id, int64_t snapshot_id,
      int64_t file_size_override = 512) {
    auto path = std::format("manifest-{}.avro", manifest_counter_++);
    ICEBERG_ASSIGN_OR_RAISE(auto writer,
                            ManifestWriter::MakeWriter(
                                kRowIdFormatVersion, snapshot_id, path, file_io_, spec0_,
                                schema_, ManifestContent::kData, entry_first_row_id));
    auto f = std::make_shared<DataFile>();
    f->content = DataFile::Content::kData;
    f->file_path = std::format("data/row-id-file-{}.parquet", manifest_counter_);
    f->file_format = FileFormatType::kParquet;
    f->partition = PartitionValues(std::vector<Literal>{Literal::Long(0)});
    f->file_size_in_bytes = 1024;
    f->record_count = 10;
    f->partition_spec_id = kSpecId0;
    f->first_row_id = entry_first_row_id;
    ICEBERG_RETURN_UNEXPECTED(
        writer->WriteExistingEntry(ManifestEntry{.status = ManifestStatus::kExisting,
                                                 .snapshot_id = snapshot_id,
                                                 .sequence_number = kSnapshotId,
                                                 .file_sequence_number = kSnapshotId,
                                                 .data_file = std::move(f)}));
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_file, writer->ToManifestFile());
    manifest_file.manifest_length = file_size_override;
    manifest_file.first_row_id = manifest_first_row_id;
    manifest_file.added_snapshot_id = snapshot_id;
    return manifest_file;
  }

  ManifestWriterFactory MakeWriterFactory() { return MakeWriterFactory(kFormatVersion); }

  Result<std::unique_ptr<ManifestMergeManager>> MakeMergeManager(
      int64_t target_size_bytes, int32_t min_count_to_merge, bool merge_enabled,
      ManifestContent content = ManifestContent::kData) {
    return ManifestMergeManager::Make(
        content, target_size_bytes, min_count_to_merge, merge_enabled, file_io_,
        [] { return kSnapshotId; },
        [this](const std::string& location) { return file_io_->DeleteFile(location); });
  }

  ManifestWriterFactory MakeWriterFactory(int8_t format_version) {
    return [this, format_version](
               int32_t spec_id,
               ManifestContent content) -> Result<std::unique_ptr<ManifestWriter>> {
      ++factory_call_count_;
      auto spec = spec_id == kSpecId0 ? spec0_ : spec1_;
      auto path = std::format("merged-{}.avro", manifest_counter_++);
      return ManifestWriter::MakeWriter(
          format_version, kSnapshotId, path, file_io_, spec, schema_, content,
          content == ManifestContent::kData && format_version >= 3
              ? std::make_optional<int64_t>(1000)
              : std::nullopt);
    };
  }

  // Count total entries across all manifests.
  Result<int> CountEntries(const std::vector<ManifestFile>& manifests) {
    int total = 0;
    for (const auto& m : manifests) {
      auto spec = m.partition_spec_id == kSpecId0 ? spec0_ : spec1_;
      ICEBERG_ASSIGN_OR_RAISE(auto reader,
                              ManifestReader::Make(m, file_io_, schema_, spec));
      ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
      total += static_cast<int>(entries.size());
    }
    return total;
  }

  Result<std::vector<ManifestEntry>> ReadEntries(const ManifestFile& manifest) {
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            ManifestReader::Make(manifest, file_io_, schema_, spec0_));
    return reader->Entries();
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec0_;
  std::shared_ptr<PartitionSpec> spec1_;
  std::shared_ptr<TableMetadata> metadata_;
  int manifest_counter_ = 0;
  int factory_call_count_ = 0;
};

TEST_F(ManifestMergeManagerTest, MergeDisabled) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1));
  ICEBERG_UNWRAP_OR_FAIL(auto m2, WriteManifest(kSpecId0, 1));

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/2,
                                                    /*enabled=*/false));
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr->MergeManifests({m0, m1}, {m2}, *metadata_, MakeWriterFactory()));
  EXPECT_EQ(result.size(), 3U);
  EXPECT_EQ(factory_call_count_, 0);
}

TEST_F(ManifestMergeManagerTest, MakeRejectsNullParameters) {
  EXPECT_THAT(
      ManifestMergeManager::Make(ManifestContent::kData, /*target=*/1024, /*min_count=*/2,
                                 /*enabled=*/true, nullptr, [] { return kSnapshotId; }),
      IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(
      ManifestMergeManager::Make(ManifestContent::kData, /*target=*/1024, /*min_count=*/2,
                                 /*enabled=*/true, file_io_, {}),
      IsError(ErrorKind::kInvalidArgument));
}

TEST_F(ManifestMergeManagerTest, BelowMinCountThreshold) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1));

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/3,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr->MergeManifests({m0}, {m1}, *metadata_, MakeWriterFactory()));
  EXPECT_EQ(result.size(), 2U);
  EXPECT_EQ(factory_call_count_, 0);
}

TEST_F(ManifestMergeManagerTest, MergeOccursAtThreshold) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m2, WriteManifest(kSpecId0, 1, /*size=*/100));

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/3,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr->MergeManifests({m0, m1}, {m2}, *metadata_, MakeWriterFactory()));
  EXPECT_EQ(result.size(), 1U);
  ICEBERG_UNWRAP_OR_FAIL(auto count1, CountEntries(result));
  EXPECT_EQ(count1, 3);
}

TEST_F(ManifestMergeManagerTest, ReplacedManifestCountTracksPreviousSnapshotInputs) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  m0.added_snapshot_id = kSnapshotId - 1;
  m1.added_snapshot_id = kSnapshotId - 2;

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/2,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr->MergeManifests({m0, m1}, {}, *metadata_, MakeWriterFactory()));

  EXPECT_EQ(result.size(), 1U);
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 2);
}

TEST_F(ManifestMergeManagerTest, MergeManifestsCachesMergedBinAcrossRetries) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  m0.added_snapshot_id = kSnapshotId - 1;
  m1.added_snapshot_id = kSnapshotId - 2;

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/2,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(
      auto first, mgr->MergeManifests({m0, m1}, {}, *metadata_, MakeWriterFactory()));
  ASSERT_EQ(first.size(), 1U);
  EXPECT_EQ(factory_call_count_, 1);
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 2);

  ICEBERG_UNWRAP_OR_FAIL(
      auto second, mgr->MergeManifests({m0, m1}, {}, *metadata_, MakeWriterFactory()));
  ASSERT_EQ(second.size(), 1U);
  EXPECT_EQ(second[0].manifest_path, first[0].manifest_path);
  EXPECT_EQ(factory_call_count_, 1);
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 2);
}

TEST_F(ManifestMergeManagerTest, CleanUncommittedDropsMergeCacheAndRollsBackCount) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  m0.added_snapshot_id = kSnapshotId - 1;
  m1.added_snapshot_id = kSnapshotId - 2;

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/2,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(
      auto first, mgr->MergeManifests({m0, m1}, {}, *metadata_, MakeWriterFactory()));
  ASSERT_EQ(first.size(), 1U);
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 2);

  EXPECT_THAT(mgr->CleanUncommitted({}), IsOk());
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 0);

  ICEBERG_UNWRAP_OR_FAIL(
      auto second, mgr->MergeManifests({m0, m1}, {}, *metadata_, MakeWriterFactory()));
  ASSERT_EQ(second.size(), 1U);
  EXPECT_NE(second[0].manifest_path, first[0].manifest_path);
  EXPECT_EQ(factory_call_count_, 2);
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 2);
}

TEST_F(ManifestMergeManagerTest, CleanUncommittedDeletesMergedManifestWithCallback) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  m0.added_snapshot_id = kSnapshotId - 1;
  m1.added_snapshot_id = kSnapshotId - 2;

  std::vector<std::string> deleted_paths;
  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         ManifestMergeManager::Make(
                             ManifestContent::kData, /*target=*/1024, /*min_count=*/2,
                             /*enabled=*/true, file_io_, [] { return kSnapshotId; },
                             [&deleted_paths](const std::string& path) {
                               deleted_paths.push_back(path);
                               return Status{};
                             }));
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr->MergeManifests({m0, m1}, {}, *metadata_, MakeWriterFactory()));
  ASSERT_EQ(result.size(), 1U);

  EXPECT_THAT(mgr->CleanUncommitted({}), IsOk());

  EXPECT_THAT(deleted_paths, ::testing::ElementsAre(result[0].manifest_path));
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 0);
}

TEST_F(ManifestMergeManagerTest, CleanUncommittedIgnoresDeleteCallbackError) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  m0.added_snapshot_id = kSnapshotId - 1;
  m1.added_snapshot_id = kSnapshotId - 2;

  ICEBERG_UNWRAP_OR_FAIL(
      auto mgr, ManifestMergeManager::Make(
                    ManifestContent::kData, /*target=*/1024, /*min_count=*/2,
                    /*enabled=*/true, file_io_, [] { return kSnapshotId; },
                    [](const std::string&) { return IOError("delete failed"); }));
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr->MergeManifests({m0, m1}, {}, *metadata_, MakeWriterFactory()));
  ASSERT_EQ(result.size(), 1U);

  EXPECT_THAT(mgr->CleanUncommitted({}), IsOk());
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 0);
}

TEST_F(ManifestMergeManagerTest, CleanUncommittedKeepsCommittedMergeCache) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  m0.added_snapshot_id = kSnapshotId - 1;
  m1.added_snapshot_id = kSnapshotId - 2;

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/2,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(
      auto first, mgr->MergeManifests({m0, m1}, {}, *metadata_, MakeWriterFactory()));
  ASSERT_EQ(first.size(), 1U);

  EXPECT_THAT(mgr->CleanUncommitted({first[0].manifest_path}), IsOk());
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 2);

  ICEBERG_UNWRAP_OR_FAIL(
      auto second, mgr->MergeManifests({m0, m1}, {}, *metadata_, MakeWriterFactory()));
  ASSERT_EQ(second.size(), 1U);
  EXPECT_EQ(second[0].manifest_path, first[0].manifest_path);
  EXPECT_EQ(factory_call_count_, 1);
}

TEST_F(ManifestMergeManagerTest, ReplacedManifestCountIgnoresCurrentSnapshotInputs) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/2,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr->MergeManifests({}, {m0, m1}, *metadata_, MakeWriterFactory()));

  EXPECT_EQ(result.size(), 1U);
  EXPECT_EQ(mgr->ReplacedManifestsCount(), 0);
}

TEST_F(ManifestMergeManagerTest,
       MergePreservesCurrentSnapshotFileFirstRowIdsWhenManifestFirstRowIdIsNull) {
  constexpr int64_t kEntryFirstRowId = 1234;
  ICEBERG_UNWRAP_OR_FAIL(auto current, WriteDataManifestWithFileRowIds(
                                           /*manifest_first_row_id=*/std::nullopt,
                                           kEntryFirstRowId, kSnapshotId));
  ICEBERG_UNWRAP_OR_FAIL(auto previous, WriteManifest(kSpecId0, 1, /*size=*/512));
  previous.added_snapshot_id = 7;

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/2,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(auto v3_metadata, BuildV3Metadata());
  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr->MergeManifests({previous}, {current}, *v3_metadata,
                                             MakeWriterFactory(kRowIdFormatVersion)));
  ASSERT_EQ(result.size(), 1U);

  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadEntries(result[0]));
  auto current_entry = std::ranges::find_if(entries, [](const ManifestEntry& entry) {
    return entry.data_file != nullptr &&
           entry.data_file->file_path.find("row-id-file") != std::string::npos;
  });
  ASSERT_NE(current_entry, entries.end());
  ASSERT_TRUE(current_entry->data_file->first_row_id.has_value());
  EXPECT_EQ(*current_entry->data_file->first_row_id, kEntryFirstRowId);
}

TEST_F(ManifestMergeManagerTest, OversizedManifestPassedThrough) {
  ICEBERG_UNWRAP_OR_FAIL(auto m_large, WriteManifest(kSpecId0, 2, /*size=*/2000));
  ICEBERG_UNWRAP_OR_FAIL(auto m_small, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m_small2, WriteManifest(kSpecId0, 1, /*size=*/100));

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/2,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr->MergeManifests({m_large, m_small}, {m_small2}, *metadata_,
                                             MakeWriterFactory()));
  EXPECT_EQ(result.size(), 3U);
  ICEBERG_UNWRAP_OR_FAIL(auto count2, CountEntries(result));
  EXPECT_EQ(count2, 4);
}

TEST_F(ManifestMergeManagerTest, CrossSpecManifestsNotMerged) {
  ICEBERG_UNWRAP_OR_FAIL(auto m_spec0a, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m_spec0b, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m_spec1a, WriteManifest(kSpecId1, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m_spec1b, WriteManifest(kSpecId1, 1, /*size=*/100));

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/2,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr->MergeManifests({m_spec0a, m_spec1a}, {m_spec0b, m_spec1b},
                                             *metadata_, MakeWriterFactory()));
  EXPECT_EQ(result.size(), 2U);
  for (const auto& m : result) {
    EXPECT_THAT(m.partition_spec_id, ::testing::AnyOf(kSpecId0, kSpecId1));
  }
}

TEST_F(ManifestMergeManagerTest, WriterFactoryCalledOncePerMergedManifest) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m2, WriteManifest(kSpecId1, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m3, WriteManifest(kSpecId1, 1, /*size=*/100));

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/1024, /*min_count=*/2,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(auto result, mgr->MergeManifests({m0, m2}, {m1, m3}, *metadata_,
                                                          MakeWriterFactory()));
  EXPECT_EQ(result.size(), 2U);
  EXPECT_EQ(factory_call_count_, 2);
}

TEST_F(ManifestMergeManagerTest, UnexpectedContentRejected) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto d0, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kData));

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         MakeMergeManager(/*target=*/1024, /*min_count=*/2,
                                          /*enabled=*/true, ManifestContent::kDeletes));
  EXPECT_THAT(mgr->MergeManifests({}, {d0}, *metadata_, MakeWriterFactory()),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(ManifestMergeManagerTest, DeleteManifestsMerged) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto del0, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kDeletes));
  ICEBERG_UNWRAP_OR_FAIL(
      auto del1, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kDeletes));
  ICEBERG_UNWRAP_OR_FAIL(
      auto del2, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kDeletes));

  ICEBERG_UNWRAP_OR_FAIL(auto mgr,
                         MakeMergeManager(/*target=*/1024, /*min_count=*/3,
                                          /*enabled=*/true, ManifestContent::kDeletes));
  ICEBERG_UNWRAP_OR_FAIL(
      auto result,
      mgr->MergeManifests({del0, del1}, {del2}, *metadata_, MakeWriterFactory()));
  EXPECT_EQ(result.size(), 1U);
  EXPECT_EQ(result[0].content, ManifestContent::kDeletes);
  ICEBERG_UNWRAP_OR_FAIL(auto count, CountEntries(result));
  EXPECT_EQ(count, 3);
}

TEST_F(ManifestMergeManagerTest, PackEndOlderManifestsMergedNotNewest) {
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m2, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));

  ICEBERG_UNWRAP_OR_FAIL(auto mgr, MakeMergeManager(/*target=*/250, /*min_count=*/3,
                                                    /*enabled=*/true));
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr->MergeManifests({m1, m2}, {m0}, *metadata_, MakeWriterFactory()));
  ASSERT_EQ(result.size(), 2U);
  EXPECT_EQ(result[0].manifest_length, m0.manifest_length);
  EXPECT_NE(result[1].manifest_path, m1.manifest_path);
  EXPECT_NE(result[1].manifest_path, m2.manifest_path);
  ICEBERG_UNWRAP_OR_FAIL(auto count, CountEntries(result));
  EXPECT_EQ(count, 3);
}

}  // namespace iceberg
