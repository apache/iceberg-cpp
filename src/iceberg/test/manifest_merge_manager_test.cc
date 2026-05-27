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

  ManifestWriterFactory MakeWriterFactory() {
    return [this](int32_t spec_id,
                  ManifestContent content) -> Result<std::unique_ptr<ManifestWriter>> {
      ++factory_call_count_;
      auto spec = spec_id == kSpecId0 ? spec0_ : spec1_;
      auto path = std::format("merged-{}.avro", manifest_counter_++);
      return ManifestWriter::MakeWriter(kFormatVersion, kSnapshotId, path, file_io_, spec,
                                        schema_, content);
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

  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/2, /*enabled=*/false);
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr.MergeManifests({m0, m1}, {m2}, kSnapshotId, *metadata_, file_io_,
                                      MakeWriterFactory()));
  // merge disabled → all 3 manifests returned, factory never called
  EXPECT_EQ(result.size(), 3U);
  EXPECT_EQ(factory_call_count_, 0);
}

TEST_F(ManifestMergeManagerTest, BelowMinCountThreshold) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1));

  // min_count=3, only 2 manifests total → no merge
  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/3, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr.MergeManifests({m0}, {m1}, kSnapshotId, *metadata_, file_io_,
                                            MakeWriterFactory()));
  EXPECT_EQ(result.size(), 2U);
  EXPECT_EQ(factory_call_count_, 0);
}

TEST_F(ManifestMergeManagerTest, MergeOccursAtThreshold) {
  // 3 small manifests (each 100 bytes), target=1024 → all fit in one bin
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m2, WriteManifest(kSpecId0, 1, /*size=*/100));

  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/3, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr.MergeManifests({m0, m1}, {m2}, kSnapshotId, *metadata_, file_io_,
                                      MakeWriterFactory()));
  // All 3 merged into 1 manifest (total 3 entries)
  EXPECT_EQ(result.size(), 1U);
  ICEBERG_UNWRAP_OR_FAIL(auto count1, CountEntries(result));
  EXPECT_EQ(count1, 3);
}

TEST_F(ManifestMergeManagerTest, ReplacedManifestCountTracksPreviousSnapshotInputs) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  m0.added_snapshot_id = kSnapshotId - 1;
  m1.added_snapshot_id = kSnapshotId - 2;

  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/2, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr.MergeManifests({m0, m1}, {}, kSnapshotId, *metadata_, file_io_,
                                            MakeWriterFactory()));

  EXPECT_EQ(result.size(), 1U);
  EXPECT_EQ(mgr.ReplacedManifestsCount(), 2);
}

TEST_F(ManifestMergeManagerTest, ReplacedManifestCountIgnoresCurrentSnapshotInputs) {
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));

  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/2, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr.MergeManifests({}, {m0, m1}, kSnapshotId, *metadata_, file_io_,
                                            MakeWriterFactory()));

  EXPECT_EQ(result.size(), 1U);
  EXPECT_EQ(mgr.ReplacedManifestsCount(), 0);
}

TEST_F(ManifestMergeManagerTest, OversizedManifestPassedThrough) {
  // m_large exceeds target → must not be merged; m_small fits
  ICEBERG_UNWRAP_OR_FAIL(auto m_large, WriteManifest(kSpecId0, 2, /*size=*/2000));
  ICEBERG_UNWRAP_OR_FAIL(auto m_small, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m_small2, WriteManifest(kSpecId0, 1, /*size=*/100));

  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/2, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr.MergeManifests({m_large, m_small}, {m_small2}, kSnapshotId,
                                            *metadata_, file_io_, MakeWriterFactory()));
  // m_large is oversized and acts as a bin boundary — the two small manifests on either
  // side of it are never merged together.  m_small2 (the newest) is also protected by
  // minCountToMerge (size 1 < 2).  All three remain separate.
  EXPECT_EQ(result.size(), 3U);
  ICEBERG_UNWRAP_OR_FAIL(auto count2, CountEntries(result));
  EXPECT_EQ(count2, 4);  // 2 + 1 + 1
}

TEST_F(ManifestMergeManagerTest, CrossSpecManifestsNotMerged) {
  // Manifests with different spec IDs must never be merged together
  ICEBERG_UNWRAP_OR_FAIL(auto m_spec0a, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m_spec0b, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m_spec1a, WriteManifest(kSpecId1, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m_spec1b, WriteManifest(kSpecId1, 1, /*size=*/100));

  // With 4 manifests (target large enough for each pair), we get 2 merged outputs
  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/2, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(
      auto result,
      mgr.MergeManifests({m_spec0a, m_spec1a}, {m_spec0b, m_spec1b}, kSnapshotId,
                         *metadata_, file_io_, MakeWriterFactory()));
  EXPECT_EQ(result.size(), 2U);
  // Verify spec IDs are preserved per output manifest
  for (const auto& m : result) {
    EXPECT_THAT(m.partition_spec_id, ::testing::AnyOf(kSpecId0, kSpecId1));
  }
}

TEST_F(ManifestMergeManagerTest, WriterFactoryCalledOncePerMergedManifest) {
  // 4 small manifests in two groups → 2 merged outputs → factory called twice
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m2, WriteManifest(kSpecId1, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m3, WriteManifest(kSpecId1, 1, /*size=*/100));

  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/2, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr.MergeManifests({m0, m2}, {m1, m3}, kSnapshotId, *metadata_,
                                            file_io_, MakeWriterFactory()));
  EXPECT_EQ(result.size(), 2U);
  EXPECT_EQ(factory_call_count_, 2);
}

TEST_F(ManifestMergeManagerTest, MixedContentManifestsNotMerged) {
  // Data and delete manifests sharing the same spec_id must never be merged together.
  // The grouping key is (spec_id, content), so they land in separate bins.
  ICEBERG_UNWRAP_OR_FAIL(
      auto d0, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kData));
  ICEBERG_UNWRAP_OR_FAIL(
      auto d1, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kData));
  ICEBERG_UNWRAP_OR_FAIL(
      auto del0, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kDeletes));
  ICEBERG_UNWRAP_OR_FAIL(
      auto del1, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kDeletes));

  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/2, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr.MergeManifests({d0, del0}, {d1, del1}, kSnapshotId, *metadata_,
                                      file_io_, MakeWriterFactory()));
  // 2 data → 1 merged data manifest; 2 delete → 1 merged delete manifest
  EXPECT_EQ(result.size(), 2U);
  int data_count = 0;
  int delete_count = 0;
  for (const auto& m : result) {
    if (m.content == ManifestContent::kData) ++data_count;
    if (m.content == ManifestContent::kDeletes) ++delete_count;
  }
  EXPECT_EQ(data_count, 1);
  EXPECT_EQ(delete_count, 1);
}

TEST_F(ManifestMergeManagerTest, MixedContentUsesFirstManifestPerContent) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto d0, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kData));
  ICEBERG_UNWRAP_OR_FAIL(
      auto d1, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kData));
  ICEBERG_UNWRAP_OR_FAIL(
      auto del0, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kDeletes));
  ICEBERG_UNWRAP_OR_FAIL(
      auto del1, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kDeletes));

  // Each content type's newest manifest must be protected by the threshold
  // independently.
  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/3, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr.MergeManifests({d0, del0}, {d1, del1}, kSnapshotId, *metadata_,
                                      file_io_, MakeWriterFactory()));

  // Each content type has exactly two manifests, below min_count=3, so neither pair
  // should be merged.
  ASSERT_EQ(result.size(), 4U);
  int data_count = 0;
  int delete_count = 0;
  for (const auto& manifest : result) {
    if (manifest.content == ManifestContent::kData) {
      ++data_count;
    } else if (manifest.content == ManifestContent::kDeletes) {
      ++delete_count;
    }
  }
  EXPECT_EQ(data_count, 2);
  EXPECT_EQ(delete_count, 2);
}

TEST_F(ManifestMergeManagerTest, DeleteManifestsMerged) {
  // Delete manifests are bin-packed and merged just like data manifests.
  ICEBERG_UNWRAP_OR_FAIL(
      auto del0, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kDeletes));
  ICEBERG_UNWRAP_OR_FAIL(
      auto del1, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kDeletes));
  ICEBERG_UNWRAP_OR_FAIL(
      auto del2, WriteManifest(kSpecId0, 1, /*size=*/100, ManifestContent::kDeletes));

  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/3, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(auto result,
                         mgr.MergeManifests({del0, del1}, {del2}, kSnapshotId, *metadata_,
                                            file_io_, MakeWriterFactory()));
  EXPECT_EQ(result.size(), 1U);
  EXPECT_EQ(result[0].content, ManifestContent::kDeletes);
  ICEBERG_UNWRAP_OR_FAIL(auto count, CountEntries(result));
  EXPECT_EQ(count, 3);
}

TEST_F(ManifestMergeManagerTest, PackEndOlderManifestsMergedNotNewest) {
  // packEnd semantics: for [m0_new, m1_old, m2_old] with target=250 (pairs fit but
  // triples don't), packing from the end merges m1+m2 (the older pair) and leaves
  // m0 (the newest) in its own under-filled bin at the front of the output.
  // This is the opposite of naive forward packing, which would merge m0+m1.
  ICEBERG_UNWRAP_OR_FAIL(auto m1, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m2, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m0, WriteManifest(kSpecId0, 1, /*size=*/100));

  // target=250 fits two 100-byte manifests but not three.
  // min_count=3 so m0's single-element bin is kept as-is (below threshold).
  ManifestMergeManager mgr(/*target=*/250, /*min_count=*/3, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr.MergeManifests({m1, m2}, {m0}, kSnapshotId, *metadata_, file_io_,
                                      MakeWriterFactory()));
  // Expected: [m0 (pass-through), merged(m1+m2)]
  ASSERT_EQ(result.size(), 2U);
  // First output is the newest manifest m0, passed through unchanged (under-filled bin).
  EXPECT_EQ(result[0].manifest_length, m0.manifest_length);
  // Second output is the merged older pair — it must be a newly written manifest
  // (different path than either original).
  EXPECT_NE(result[1].manifest_path, m1.manifest_path);
  EXPECT_NE(result[1].manifest_path, m2.manifest_path);
  ICEBERG_UNWRAP_OR_FAIL(auto count, CountEntries(result));
  EXPECT_EQ(count, 3);
}

}  // namespace iceberg
