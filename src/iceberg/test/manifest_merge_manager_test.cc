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
#include "iceberg/manifest/manifest_filter_manager.h"
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
    spec0_ = PartitionSpec::Make(
                 kSpecId0,
                 {PartitionField(1, 1000, "x", Transform::Identity())})
                 .value();
    spec1_ = PartitionSpec::Make(
                 kSpecId1,
                 {PartitionField(1, 1001, "x_bucket", Transform::Bucket(8))})
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
                                     int64_t file_size_override = 512) {
    auto path = std::format("manifest-{}.avro", manifest_counter_++);
    auto spec = spec_id == kSpecId0 ? spec0_ : spec1_;
    ICEBERG_ASSIGN_OR_RAISE(auto writer,
                             ManifestWriter::MakeWriter(kFormatVersion, kSnapshotId, path,
                                                        file_io_, spec, schema_,
                                                        ManifestContent::kData));
    for (int i = 0; i < num_files; ++i) {
      auto f = std::make_shared<DataFile>();
      f->content = DataFile::Content::kData;
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
    return [this](int32_t spec_id, ManifestContent content)
               -> Result<std::unique_ptr<ManifestWriter>> {
      ++factory_call_count_;
      auto spec = spec_id == kSpecId0 ? spec0_ : spec1_;
      auto path = std::format("merged-{}.avro", manifest_counter_++);
      return ManifestWriter::MakeWriter(kFormatVersion, kSnapshotId, path, file_io_,
                                        spec, schema_, content);
    };
  }

  // Count total entries across all manifests.
  Result<int> CountEntries(const std::vector<ManifestFile>& manifests) {
    int total = 0;
    for (const auto& m : manifests) {
      auto spec = m.partition_spec_id == kSpecId0 ? spec0_ : spec1_;
      ICEBERG_ASSIGN_OR_RAISE(auto reader, ManifestReader::Make(m, file_io_, schema_, spec));
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
      auto result, mgr.MergeManifests({m0, m1}, {m2}, *metadata_, file_io_,
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
  ICEBERG_UNWRAP_OR_FAIL(
      auto result, mgr.MergeManifests({m0}, {m1}, *metadata_, file_io_, MakeWriterFactory()));
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
      auto result, mgr.MergeManifests({m0, m1}, {m2}, *metadata_, file_io_,
                                       MakeWriterFactory()));
  // All 3 merged into 1 manifest (total 3 entries)
  EXPECT_EQ(result.size(), 1U);
  ICEBERG_UNWRAP_OR_FAIL(auto count1, CountEntries(result));
  EXPECT_EQ(count1, 3);
}

TEST_F(ManifestMergeManagerTest, OversizedManifestPassedThrough) {
  // m_large exceeds target → must not be merged; m_small fits
  ICEBERG_UNWRAP_OR_FAIL(auto m_large, WriteManifest(kSpecId0, 2, /*size=*/2000));
  ICEBERG_UNWRAP_OR_FAIL(auto m_small, WriteManifest(kSpecId0, 1, /*size=*/100));
  ICEBERG_UNWRAP_OR_FAIL(auto m_small2, WriteManifest(kSpecId0, 1, /*size=*/100));

  ManifestMergeManager mgr(/*target=*/1024, /*min_count=*/2, /*enabled=*/true);
  ICEBERG_UNWRAP_OR_FAIL(
      auto result,
      mgr.MergeManifests({m_large, m_small}, {m_small2}, *metadata_, file_io_,
                          MakeWriterFactory()));
  // m_large passes through; m_small and m_small2 merge into 1
  EXPECT_EQ(result.size(), 2U);
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
      mgr.MergeManifests({m_spec0a, m_spec1a}, {m_spec0b, m_spec1b}, *metadata_,
                          file_io_, MakeWriterFactory()));
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
  ICEBERG_UNWRAP_OR_FAIL(
      auto result,
      mgr.MergeManifests({m0, m2}, {m1, m3}, *metadata_, file_io_, MakeWriterFactory()));
  EXPECT_EQ(result.size(), 2U);
  EXPECT_EQ(factory_call_count_, 2);
}

}  // namespace iceberg
