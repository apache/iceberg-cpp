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

#include "iceberg/delete_file_index.h"

#include <chrono>
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

class DeleteFileIndexTest : public testing::TestWithParam<int> {
 protected:
  void SetUp() override {
    avro::RegisterAll();

    file_io_ = arrow::MakeMockFileIO();

    // Schema with id and data fields
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(/*field_id=*/1, "id", int32()),
        SchemaField::MakeRequired(/*field_id=*/2, "data", string())});

    // Partitioned spec: bucket by data
    ICEBERG_UNWRAP_OR_FAIL(
        partitioned_spec_,
        PartitionSpec::Make(
            /*spec_id=*/1, {PartitionField(/*source_id=*/2, /*field_id=*/1000,
                                           "data_bucket", Transform::Bucket(16))}));

    // Unpartitioned spec
    unpartitioned_spec_ = PartitionSpec::Unpartitioned();

    // Create sample data files
    file_a_ = MakeDataFile("/path/to/data-a.parquet", PartitionValues({Literal::Int(0)}),
                           partitioned_spec_->spec_id());
    file_b_ = MakeDataFile("/path/to/data-b.parquet", PartitionValues({Literal::Int(1)}),
                           partitioned_spec_->spec_id());
    file_c_ = MakeDataFile("/path/to/data-c.parquet", PartitionValues({Literal::Int(2)}),
                           partitioned_spec_->spec_id());
    unpartitioned_file_ = MakeDataFile("/path/to/data-unpartitioned.parquet",
                                       PartitionValues(std::vector<Literal>{}),
                                       unpartitioned_spec_->spec_id());
  }

  std::string MakeManifestPath() {
    static int counter = 0;
    return std::format("manifest-{}-{}.avro", counter++,
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& path,
                                         const PartitionValues& partition,
                                         int32_t spec_id, int64_t record_count = 1) {
    return std::make_shared<DataFile>(DataFile{
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = record_count,
        .file_size_in_bytes = 10,
        .sort_order_id = 0,
        .partition_spec_id = spec_id,
    });
  }

  std::shared_ptr<DataFile> MakePositionDeleteFile(
      const std::string& path, const PartitionValues& partition, int32_t spec_id,
      std::optional<std::string> referenced_file = std::nullopt) {
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kPositionDeletes,
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = 1,
        .file_size_in_bytes = 10,
        .partition_spec_id = spec_id,
        .referenced_data_file = referenced_file,
    });
  }

  std::shared_ptr<DataFile> MakeEqualityDeleteFile(const std::string& path,
                                                   const PartitionValues& partition,
                                                   int32_t spec_id,
                                                   std::vector<int> equality_ids = {1}) {
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kEqualityDeletes,
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = 1,
        .file_size_in_bytes = 10,
        .equality_ids = std::move(equality_ids),
        .partition_spec_id = spec_id,
    });
  }

  std::shared_ptr<DataFile> MakeDV(const std::string& path,
                                   const PartitionValues& partition, int32_t spec_id,
                                   const std::string& referenced_file,
                                   int64_t content_offset = 4L,
                                   int64_t content_size = 6L) {
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kPositionDeletes,
        .file_path = path,
        .file_format = FileFormatType::kPuffin,
        .partition = partition,
        .record_count = 1,
        .file_size_in_bytes = 10,
        .partition_spec_id = spec_id,
        .referenced_data_file = referenced_file,
        .content_offset = content_offset,
        .content_size_in_bytes = content_size,
    });
  }

  ManifestEntry MakeDeleteEntry(int64_t snapshot_id, int64_t sequence_number,
                                std::shared_ptr<DataFile> file,
                                ManifestStatus status = ManifestStatus::kAdded) {
    return ManifestEntry{
        .status = status,
        .snapshot_id = snapshot_id,
        .sequence_number = sequence_number,
        .file_sequence_number = sequence_number,
        .data_file = std::move(file),
    };
  }

  ManifestFile WriteDeleteManifest(int format_version, int64_t snapshot_id,
                                   std::vector<ManifestEntry> entries,
                                   std::shared_ptr<PartitionSpec> spec) {
    const std::string manifest_path = MakeManifestPath();

    Result<std::unique_ptr<ManifestWriter>> writer_result =
        NotSupported("Format version: {}", format_version);

    if (format_version == 2) {
      writer_result = ManifestWriter::MakeV2Writer(
          snapshot_id, manifest_path, file_io_, spec, schema_, ManifestContent::kDeletes);
    } else if (format_version == 3) {
      writer_result = ManifestWriter::MakeV3Writer(
          snapshot_id, /*first_row_id=*/std::nullopt, manifest_path, file_io_, spec,
          schema_, ManifestContent::kDeletes);
    }

    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    for (const auto& entry : entries) {
      EXPECT_THAT(writer->WriteEntry(entry), IsOk());
    }

    EXPECT_THAT(writer->Close(), IsOk());
    auto manifest_result = writer->ToManifestFile();
    EXPECT_THAT(manifest_result, IsOk());
    return std::move(manifest_result.value());
  }

  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> GetSpecsById() {
    return {{partitioned_spec_->spec_id(), partitioned_spec_},
            {unpartitioned_spec_->spec_id(), unpartitioned_spec_}};
  }

  Result<std::unique_ptr<DeleteFileIndex>> BuildIndex(
      std::vector<ManifestFile> delete_manifests,
      std::optional<int64_t> after_sequence_number = std::nullopt) {
    ICEBERG_ASSIGN_OR_RAISE(auto builder,
                            DeleteFileIndex::BuilderFor(file_io_, schema_, GetSpecsById(),
                                                        std::move(delete_manifests)));
    if (after_sequence_number.has_value()) {
      builder.AfterSequenceNumber(after_sequence_number.value());
    }
    return builder.Build();
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> partitioned_spec_;
  std::shared_ptr<PartitionSpec> unpartitioned_spec_;

  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
  std::shared_ptr<DataFile> file_c_;
  std::shared_ptr<DataFile> unpartitioned_file_;

  // Helper to extract paths from delete files for comparison
  static std::vector<std::string> GetPaths(
      const std::vector<std::shared_ptr<DataFile>>& files) {
    return std::ranges::transform_view(files,
                                       [](const auto& f) { return f->file_path; }) |
           std::ranges::to<std::vector<std::string>>();
  }
};

TEST_P(DeleteFileIndexTest, TestEmptyIndex) {
  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({}));

  EXPECT_TRUE(index->empty());
  EXPECT_FALSE(index->has_equality_deletes());
  EXPECT_FALSE(index->has_position_deletes());

  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *file_a_));
  EXPECT_TRUE(deletes.empty());
}

TEST_P(DeleteFileIndexTest, TestMinSequenceNumberFilteringForFiles) {
  int version = GetParam();

  auto eq_delete_1 = MakeEqualityDeleteFile("/path/to/eq-delete-1.parquet",
                                            PartitionValues(std::vector<Literal>{}),
                                            unpartitioned_spec_->spec_id());
  auto eq_delete_2 = MakeEqualityDeleteFile("/path/to/eq-delete-2.parquet",
                                            PartitionValues(std::vector<Literal>{}),
                                            unpartitioned_spec_->spec_id());

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/4, eq_delete_1));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/6, eq_delete_2));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      unpartitioned_spec_);

  // Build index with afterSequenceNumber = 4
  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}, /*after_sequence_number=*/4));

  EXPECT_TRUE(index->has_equality_deletes());
  EXPECT_FALSE(index->has_position_deletes());

  // Only delete file with seq > 4 should be included (seq=6)
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *unpartitioned_file_));
  EXPECT_EQ(deletes.size(), 1);
  EXPECT_EQ(deletes[0]->file_path, "/path/to/eq-delete-2.parquet");
}

TEST_P(DeleteFileIndexTest, TestUnpartitionedDeletes) {
  int version = GetParam();

  auto eq_delete_1 = MakeEqualityDeleteFile("/path/to/eq-delete-1.parquet",
                                            PartitionValues(std::vector<Literal>{}),
                                            unpartitioned_spec_->spec_id());
  auto eq_delete_2 = MakeEqualityDeleteFile("/path/to/eq-delete-2.parquet",
                                            PartitionValues(std::vector<Literal>{}),
                                            unpartitioned_spec_->spec_id());
  auto pos_delete_1 = MakePositionDeleteFile("/path/to/pos-delete-1.parquet",
                                             PartitionValues(std::vector<Literal>{}),
                                             unpartitioned_spec_->spec_id());
  auto pos_delete_2 = MakePositionDeleteFile("/path/to/pos-delete-2.parquet",
                                             PartitionValues(std::vector<Literal>{}),
                                             unpartitioned_spec_->spec_id());

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/4, eq_delete_1));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/6, eq_delete_2));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/5, pos_delete_1));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/6, pos_delete_2));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      unpartitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  EXPECT_TRUE(index->has_equality_deletes());
  EXPECT_TRUE(index->has_position_deletes());

  // All deletes should apply to seq 0
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *unpartitioned_file_));
    EXPECT_EQ(deletes.size(), 4);
    EXPECT_THAT(GetPaths(deletes),
                testing::UnorderedElementsAre(
                    "/path/to/eq-delete-1.parquet", "/path/to/eq-delete-2.parquet",
                    "/path/to/pos-delete-1.parquet", "/path/to/pos-delete-2.parquet"));
  }

  // All deletes should apply to seq 3
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(3, *unpartitioned_file_));
    EXPECT_EQ(deletes.size(), 4);
    EXPECT_THAT(GetPaths(deletes),
                testing::UnorderedElementsAre(
                    "/path/to/eq-delete-1.parquet", "/path/to/eq-delete-2.parquet",
                    "/path/to/pos-delete-1.parquet", "/path/to/pos-delete-2.parquet"));
  }

  // Last 3 deletes should apply to seq 4 (eq_delete_2, pos_delete_1, pos_delete_2)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(4, *unpartitioned_file_));
    EXPECT_EQ(deletes.size(), 3);
    EXPECT_THAT(GetPaths(deletes),
                testing::UnorderedElementsAre("/path/to/eq-delete-2.parquet",
                                              "/path/to/pos-delete-1.parquet",
                                              "/path/to/pos-delete-2.parquet"));
  }

  // Last 3 deletes should apply to seq 5
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(5, *unpartitioned_file_));
    EXPECT_EQ(deletes.size(), 3);
    EXPECT_THAT(GetPaths(deletes),
                testing::UnorderedElementsAre("/path/to/eq-delete-2.parquet",
                                              "/path/to/pos-delete-1.parquet",
                                              "/path/to/pos-delete-2.parquet"));
  }

  // Last delete should apply to seq 6 (only pos_delete_2)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(6, *unpartitioned_file_));
    EXPECT_EQ(deletes.size(), 1);
    EXPECT_EQ(deletes[0]->file_path, "/path/to/pos-delete-2.parquet");
  }

  // No deletes should apply to seq 7
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(7, *unpartitioned_file_));
    EXPECT_TRUE(deletes.empty());
  }

  // Global equality deletes should apply to a partitioned file
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *file_a_));
    // Only equality deletes are global, position deletes are not
    EXPECT_EQ(deletes.size(), 2);
    EXPECT_THAT(GetPaths(deletes),
                testing::UnorderedElementsAre("/path/to/eq-delete-1.parquet",
                                              "/path/to/eq-delete-2.parquet"));
  }
}

TEST_P(DeleteFileIndexTest, TestPartitionedDeleteIndex) {
  int version = GetParam();

  auto partition_a = PartitionValues({Literal::Int(0)});
  auto eq_delete_1 = MakeEqualityDeleteFile("/path/to/eq-delete-1.parquet", partition_a,
                                            partitioned_spec_->spec_id());
  auto eq_delete_2 = MakeEqualityDeleteFile("/path/to/eq-delete-2.parquet", partition_a,
                                            partitioned_spec_->spec_id());
  auto pos_delete_1 = MakePositionDeleteFile("/path/to/pos-delete-1.parquet", partition_a,
                                             partitioned_spec_->spec_id());
  auto pos_delete_2 = MakePositionDeleteFile("/path/to/pos-delete-2.parquet", partition_a,
                                             partitioned_spec_->spec_id());

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/4, eq_delete_1));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/6, eq_delete_2));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/5, pos_delete_1));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/6, pos_delete_2));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  EXPECT_TRUE(index->has_equality_deletes());
  EXPECT_TRUE(index->has_position_deletes());

  // All deletes should apply to file_a_ at seq 0
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *file_a_));
    EXPECT_EQ(deletes.size(), 4);
    EXPECT_THAT(GetPaths(deletes),
                testing::UnorderedElementsAre(
                    "/path/to/eq-delete-1.parquet", "/path/to/eq-delete-2.parquet",
                    "/path/to/pos-delete-1.parquet", "/path/to/pos-delete-2.parquet"));
  }

  // All deletes should apply to file_a_ at seq 3
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(3, *file_a_));
    EXPECT_EQ(deletes.size(), 4);
    EXPECT_THAT(GetPaths(deletes),
                testing::UnorderedElementsAre(
                    "/path/to/eq-delete-1.parquet", "/path/to/eq-delete-2.parquet",
                    "/path/to/pos-delete-1.parquet", "/path/to/pos-delete-2.parquet"));
  }

  // Last 3 deletes should apply to file_a_ at seq 4
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(4, *file_a_));
    EXPECT_EQ(deletes.size(), 3);
    EXPECT_THAT(GetPaths(deletes),
                testing::UnorderedElementsAre("/path/to/eq-delete-2.parquet",
                                              "/path/to/pos-delete-1.parquet",
                                              "/path/to/pos-delete-2.parquet"));
  }

  // Last 3 deletes should apply to file_a_ at seq 5
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(5, *file_a_));
    EXPECT_EQ(deletes.size(), 3);
    EXPECT_THAT(GetPaths(deletes),
                testing::UnorderedElementsAre("/path/to/eq-delete-2.parquet",
                                              "/path/to/pos-delete-1.parquet",
                                              "/path/to/pos-delete-2.parquet"));
  }

  // Last delete should apply to file_a_ at seq 6
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(6, *file_a_));
    EXPECT_EQ(deletes.size(), 1);
    EXPECT_EQ(deletes[0]->file_path, "/path/to/pos-delete-2.parquet");
  }

  // No deletes should apply to file_a_ at seq 7
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(7, *file_a_));
    EXPECT_TRUE(deletes.empty());
  }

  // No deletes should apply to file_b_ (different partition)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *file_b_));
    EXPECT_TRUE(deletes.empty());
  }

  // No deletes should apply to file_c_ (different partition)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *file_c_));
    EXPECT_TRUE(deletes.empty());
  }

  // No deletes should apply to unpartitioned file (different spec)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *unpartitioned_file_));
    EXPECT_TRUE(deletes.empty());
  }
}

TEST_P(DeleteFileIndexTest, TestPartitionedTableWithPartitionPosDeletes) {
  int version = GetParam();

  auto partition_a = PartitionValues({Literal::Int(0)});
  auto pos_delete = MakePositionDeleteFile("/path/to/pos-delete.parquet", partition_a,
                                           partitioned_spec_->spec_id());

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/2, pos_delete));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  EXPECT_FALSE(index->has_equality_deletes());
  EXPECT_TRUE(index->has_position_deletes());

  // Position delete should apply to file_a_ at seq 1
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(1, *file_a_));
  EXPECT_EQ(deletes.size(), 1);
  EXPECT_EQ(deletes[0]->file_path, "/path/to/pos-delete.parquet");
}

TEST_P(DeleteFileIndexTest, TestPartitionedTableWithPartitionEqDeletes) {
  int version = GetParam();

  auto partition_a = PartitionValues({Literal::Int(0)});
  auto eq_delete = MakeEqualityDeleteFile("/path/to/eq-delete.parquet", partition_a,
                                          partitioned_spec_->spec_id());

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/2, eq_delete));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  EXPECT_TRUE(index->has_equality_deletes());
  EXPECT_FALSE(index->has_position_deletes());

  // Equality delete should apply to file_a_ at seq 1
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(1, *file_a_));
  EXPECT_EQ(deletes.size(), 1);
  EXPECT_EQ(deletes[0]->file_path, "/path/to/eq-delete.parquet");
}

TEST_P(DeleteFileIndexTest, TestPartitionedTableWithUnrelatedPartitionDeletes) {
  int version = GetParam();

  // Create deletes for partition A
  auto partition_a = PartitionValues({Literal::Int(0)});
  auto pos_delete = MakePositionDeleteFile("/path/to/pos-delete.parquet", partition_a,
                                           partitioned_spec_->spec_id());
  auto eq_delete = MakeEqualityDeleteFile("/path/to/eq-delete.parquet", partition_a,
                                          partitioned_spec_->spec_id());

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/2, pos_delete));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/2, eq_delete));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  // No deletes should apply to file_b_ (different partition)
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(1, *file_b_));
  EXPECT_TRUE(deletes.empty());
}

TEST_P(DeleteFileIndexTest, TestPartitionedTableWithOlderPartitionDeletes) {
  int version = GetParam();
  if (version >= 3) {
    GTEST_SKIP() << "DVs are not filtered using sequence numbers in V3+";
  }

  auto partition_a = PartitionValues({Literal::Int(0)});
  auto pos_delete = MakePositionDeleteFile("/path/to/pos-delete.parquet", partition_a,
                                           partitioned_spec_->spec_id());
  auto eq_delete = MakeEqualityDeleteFile("/path/to/eq-delete.parquet", partition_a,
                                          partitioned_spec_->spec_id());

  // Delete files have sequence number 1
  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, pos_delete));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, eq_delete));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  // Data file with sequence number 2 should not have any deletes applied
  // (deletes were committed before the data file)
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(2, *file_a_));
  EXPECT_TRUE(deletes.empty());
}

TEST_P(DeleteFileIndexTest, TestPartitionedTableScanWithGlobalDeletes) {
  int version = GetParam();
  if (version >= 3) {
    GTEST_SKIP() << "Different behavior for position deletes in V3";
  }

  // Create unpartitioned equality and position deletes
  auto eq_delete = MakeEqualityDeleteFile("/path/to/eq-delete.parquet",
                                          PartitionValues(std::vector<Literal>{}),
                                          unpartitioned_spec_->spec_id());
  auto pos_delete = MakePositionDeleteFile("/path/to/pos-delete.parquet",
                                           PartitionValues(std::vector<Literal>{}),
                                           unpartitioned_spec_->spec_id());

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/2, eq_delete));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/2, pos_delete));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      unpartitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  // Only global equality deletes should apply to partitioned file_a_
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(1, *file_a_));
  EXPECT_EQ(deletes.size(), 1);
  EXPECT_EQ(deletes[0]->file_path, "/path/to/eq-delete.parquet");
}

TEST_P(DeleteFileIndexTest, TestPartitionedTableScanWithGlobalAndPartitionDeletes) {
  int version = GetParam();
  if (version >= 3) {
    GTEST_SKIP() << "Different behavior for position deletes in V3";
  }

  // Create partition-scoped equality delete
  auto partition_a = PartitionValues({Literal::Int(0)});
  auto partition_eq_delete = MakeEqualityDeleteFile(
      "/path/to/partition-eq-delete.parquet", partition_a, partitioned_spec_->spec_id());

  std::vector<ManifestEntry> partition_entries;
  partition_entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/2, partition_eq_delete));

  auto partition_manifest = WriteDeleteManifest(
      version, /*snapshot_id=*/1000L, std::move(partition_entries), partitioned_spec_);

  // Create unpartitioned equality and position deletes
  auto global_eq_delete = MakeEqualityDeleteFile("/path/to/global-eq-delete.parquet",
                                                 PartitionValues(std::vector<Literal>{}),
                                                 unpartitioned_spec_->spec_id());
  auto global_pos_delete = MakePositionDeleteFile("/path/to/global-pos-delete.parquet",
                                                  PartitionValues(std::vector<Literal>{}),
                                                  unpartitioned_spec_->spec_id());

  std::vector<ManifestEntry> global_entries;
  global_entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1001L, /*sequence_number=*/3, global_eq_delete));
  global_entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1001L, /*sequence_number=*/3, global_pos_delete));

  auto global_manifest = WriteDeleteManifest(
      version, /*snapshot_id=*/1001L, std::move(global_entries), unpartitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({partition_manifest, global_manifest}));

  // Both partition-scoped and global equality deletes should apply to file_a_
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(1, *file_a_));
  EXPECT_EQ(deletes.size(), 2);
  EXPECT_THAT(GetPaths(deletes),
              testing::UnorderedElementsAre("/path/to/partition-eq-delete.parquet",
                                            "/path/to/global-eq-delete.parquet"));
}

TEST_P(DeleteFileIndexTest, TestPartitionedTableSequenceNumbers) {
  int version = GetParam();

  auto partition_a = PartitionValues({Literal::Int(0)});
  auto eq_delete = MakeEqualityDeleteFile("/path/to/eq-delete.parquet", partition_a,
                                          partitioned_spec_->spec_id());
  auto pos_delete = MakePositionDeleteFile("/path/to/pos-delete.parquet", partition_a,
                                           partitioned_spec_->spec_id());

  // Both data and deletes have same sequence number (same commit)
  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, eq_delete));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, pos_delete));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  // Data file with sequence number 1 should only have position deletes applied
  // (equality deletes apply to data with seq < delete seq)
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(1, *file_a_));
  EXPECT_EQ(deletes.size(), 1);
  EXPECT_EQ(deletes[0]->file_path, "/path/to/pos-delete.parquet");
}

TEST_P(DeleteFileIndexTest, TestUnpartitionedTableSequenceNumbers) {
  int version = GetParam();
  if (version >= 3) {
    GTEST_SKIP() << "Different behavior in V3";
  }

  auto eq_delete = MakeEqualityDeleteFile("/path/to/eq-delete.parquet",
                                          PartitionValues(std::vector<Literal>{}),
                                          unpartitioned_spec_->spec_id());
  auto pos_delete = MakePositionDeleteFile("/path/to/pos-delete.parquet",
                                           PartitionValues(std::vector<Literal>{}),
                                           unpartitioned_spec_->spec_id());

  // Both have same sequence number
  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, eq_delete));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, pos_delete));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      unpartitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  // Data file with sequence number 1 should only have position deletes applied
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(1, *unpartitioned_file_));
  EXPECT_EQ(deletes.size(), 1);
  EXPECT_EQ(deletes[0]->file_path, "/path/to/pos-delete.parquet");
}

TEST_P(DeleteFileIndexTest, TestPositionDeletesGroup) {
  internal::PositionDeletes group;

  auto partition_a = PartitionValues({Literal::Int(0)});
  auto file1 = MakePositionDeleteFile("/path/to/pos-delete-1.parquet", partition_a,
                                      partitioned_spec_->spec_id());
  auto file2 = MakePositionDeleteFile("/path/to/pos-delete-2.parquet", partition_a,
                                      partitioned_spec_->spec_id());
  auto file3 = MakePositionDeleteFile("/path/to/pos-delete-3.parquet", partition_a,
                                      partitioned_spec_->spec_id());
  auto file4 = MakePositionDeleteFile("/path/to/pos-delete-4.parquet", partition_a,
                                      partitioned_spec_->spec_id());

  // Add files out of order
  EXPECT_THAT(group.Add(MakeDeleteEntry(1000L, 4, file4)), IsOk());
  EXPECT_THAT(group.Add(MakeDeleteEntry(1000L, 2, file2)), IsOk());
  EXPECT_THAT(group.Add(MakeDeleteEntry(1000L, 1, file1)), IsOk());
  EXPECT_THAT(group.Add(MakeDeleteEntry(1000L, 3, file3)), IsOk());

  // Group must not be empty
  EXPECT_FALSE(group.empty());

  // All files must be reported as referenced
  auto referenced = group.ReferencedDeleteFiles();
  EXPECT_EQ(referenced.size(), 4);
  EXPECT_THAT(GetPaths(referenced),
              testing::UnorderedElementsAre(
                  "/path/to/pos-delete-1.parquet", "/path/to/pos-delete-2.parquet",
                  "/path/to/pos-delete-3.parquet", "/path/to/pos-delete-4.parquet"));

  // Position deletes are indexed by their data sequence numbers
  {
    auto filtered = group.Filter(0);
    EXPECT_EQ(filtered.size(), 4);
    EXPECT_THAT(GetPaths(filtered),
                testing::UnorderedElementsAre(
                    "/path/to/pos-delete-1.parquet", "/path/to/pos-delete-2.parquet",
                    "/path/to/pos-delete-3.parquet", "/path/to/pos-delete-4.parquet"));
  }
  {
    auto filtered = group.Filter(1);
    EXPECT_EQ(filtered.size(), 4);
    EXPECT_THAT(GetPaths(filtered),
                testing::UnorderedElementsAre(
                    "/path/to/pos-delete-1.parquet", "/path/to/pos-delete-2.parquet",
                    "/path/to/pos-delete-3.parquet", "/path/to/pos-delete-4.parquet"));
  }
  {
    auto filtered = group.Filter(2);
    EXPECT_EQ(filtered.size(), 3);
    EXPECT_THAT(GetPaths(filtered),
                testing::UnorderedElementsAre("/path/to/pos-delete-2.parquet",
                                              "/path/to/pos-delete-3.parquet",
                                              "/path/to/pos-delete-4.parquet"));
  }
  {
    auto filtered = group.Filter(3);
    EXPECT_EQ(filtered.size(), 2);
    EXPECT_THAT(GetPaths(filtered),
                testing::UnorderedElementsAre("/path/to/pos-delete-3.parquet",
                                              "/path/to/pos-delete-4.parquet"));
  }
  {
    auto filtered = group.Filter(4);
    EXPECT_EQ(filtered.size(), 1);
    EXPECT_EQ(filtered[0]->file_path, "/path/to/pos-delete-4.parquet");
  }
  {
    auto filtered = group.Filter(5);
    EXPECT_EQ(filtered.size(), 0);
  }
}

TEST_P(DeleteFileIndexTest, TestEqualityDeletesGroup) {
  internal::EqualityDeletes group(*schema_);

  auto partition_a = PartitionValues({Literal::Int(0)});
  auto file1 = MakeEqualityDeleteFile("/path/to/eq-delete-1.parquet", partition_a,
                                      partitioned_spec_->spec_id());
  auto file2 = MakeEqualityDeleteFile("/path/to/eq-delete-2.parquet", partition_a,
                                      partitioned_spec_->spec_id());
  auto file3 = MakeEqualityDeleteFile("/path/to/eq-delete-3.parquet", partition_a,
                                      partitioned_spec_->spec_id());
  auto file4 = MakeEqualityDeleteFile("/path/to/eq-delete-4.parquet", partition_a,
                                      partitioned_spec_->spec_id());

  // Add files out of order
  EXPECT_THAT(group.Add(MakeDeleteEntry(1000L, 4, file4)), IsOk());
  EXPECT_THAT(group.Add(MakeDeleteEntry(1000L, 2, file2)), IsOk());
  EXPECT_THAT(group.Add(MakeDeleteEntry(1000L, 1, file1)), IsOk());
  EXPECT_THAT(group.Add(MakeDeleteEntry(1000L, 3, file3)), IsOk());

  // Group must not be empty
  EXPECT_FALSE(group.empty());

  // All files must be reported as referenced
  auto referenced = group.ReferencedDeleteFiles();
  EXPECT_EQ(referenced.size(), 4);
  EXPECT_THAT(GetPaths(referenced),
              testing::UnorderedElementsAre(
                  "/path/to/eq-delete-1.parquet", "/path/to/eq-delete-2.parquet",
                  "/path/to/eq-delete-3.parquet", "/path/to/eq-delete-4.parquet"));

  // Equality deletes are indexed by data sequence number - 1 to apply to next snapshots
  {
    ICEBERG_UNWRAP_OR_FAIL(auto filtered, group.Filter(0, *file_a_));
    EXPECT_EQ(filtered.size(), 4);
    EXPECT_THAT(GetPaths(filtered),
                testing::UnorderedElementsAre(
                    "/path/to/eq-delete-1.parquet", "/path/to/eq-delete-2.parquet",
                    "/path/to/eq-delete-3.parquet", "/path/to/eq-delete-4.parquet"));
  }
  {
    ICEBERG_UNWRAP_OR_FAIL(auto filtered, group.Filter(1, *file_a_));
    EXPECT_EQ(filtered.size(), 3);
    EXPECT_THAT(GetPaths(filtered),
                testing::UnorderedElementsAre("/path/to/eq-delete-2.parquet",
                                              "/path/to/eq-delete-3.parquet",
                                              "/path/to/eq-delete-4.parquet"));
  }
  {
    ICEBERG_UNWRAP_OR_FAIL(auto filtered, group.Filter(2, *file_a_));
    EXPECT_EQ(filtered.size(), 2);
    EXPECT_THAT(GetPaths(filtered),
                testing::UnorderedElementsAre("/path/to/eq-delete-3.parquet",
                                              "/path/to/eq-delete-4.parquet"));
  }
  {
    ICEBERG_UNWRAP_OR_FAIL(auto filtered, group.Filter(3, *file_a_));
    EXPECT_EQ(filtered.size(), 1);
    EXPECT_EQ(filtered[0]->file_path, "/path/to/eq-delete-4.parquet");
  }
  {
    ICEBERG_UNWRAP_OR_FAIL(auto filtered, group.Filter(4, *file_a_));
    EXPECT_EQ(filtered.size(), 0);
  }
}

TEST_P(DeleteFileIndexTest, TestMixDeleteFilesAndDVs) {
  int version = GetParam();
  if (version < 3) {
    GTEST_SKIP() << "DVs only supported in V3+";
  }

  auto partition_a = PartitionValues({Literal::Int(0)});
  auto partition_b = PartitionValues({Literal::Int(1)});

  // Position delete for file_a_
  auto pos_delete_a = MakePositionDeleteFile("/path/to/pos-delete-a.parquet", partition_a,
                                             partitioned_spec_->spec_id());
  // DV for file_a_ (should take precedence)
  auto dv_a = MakeDV("/path/to/dv-a.puffin", partition_a, partitioned_spec_->spec_id(),
                     file_a_->file_path);
  // Position deletes for file_b_ (no DV)
  auto pos_delete_b1 = MakePositionDeleteFile("/path/to/pos-delete-b1.parquet",
                                              partition_b, partitioned_spec_->spec_id());
  auto pos_delete_b2 = MakePositionDeleteFile("/path/to/pos-delete-b2.parquet",
                                              partition_b, partitioned_spec_->spec_id());

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, pos_delete_a));
  entries.push_back(MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/2, dv_a));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, pos_delete_b1));
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/2, pos_delete_b2));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  // Only DV should apply to file_a_
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *file_a_));
    EXPECT_EQ(deletes.size(), 1);
    EXPECT_TRUE(deletes[0]->content_offset.has_value());  // DV has content_offset
    EXPECT_EQ(deletes[0]->referenced_data_file, file_a_->file_path);
    EXPECT_EQ(deletes[0]->file_path, "/path/to/dv-a.puffin");
  }

  // Two delete files should apply to file_b_
  {
    ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *file_b_));
    EXPECT_EQ(deletes.size(), 2);
    EXPECT_FALSE(deletes[0]->content_offset.has_value());  // Not DVs
    EXPECT_FALSE(deletes[1]->content_offset.has_value());
    EXPECT_THAT(GetPaths(deletes),
                testing::UnorderedElementsAre("/path/to/pos-delete-b1.parquet",
                                              "/path/to/pos-delete-b2.parquet"));
  }
}

TEST_P(DeleteFileIndexTest, TestMultipleDVs) {
  int version = GetParam();
  if (version < 3) {
    GTEST_SKIP() << "DVs only supported in V3+";
  }

  auto partition_a = PartitionValues({Literal::Int(0)});

  auto dv1 = MakeDV("/path/to/dv1.puffin", partition_a, partitioned_spec_->spec_id(),
                    file_a_->file_path);
  auto dv2 = MakeDV("/path/to/dv2.puffin", partition_a, partitioned_spec_->spec_id(),
                    file_a_->file_path);

  std::vector<ManifestEntry> entries;
  entries.push_back(MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, dv1));
  entries.push_back(MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/2, dv2));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  auto index_result = BuildIndex({manifest});
  EXPECT_THAT(index_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(index_result, HasErrorMessage("Can't index multiple DVs"));
  EXPECT_THAT(index_result, HasErrorMessage(file_a_->file_path));
}

TEST_P(DeleteFileIndexTest, TestInvalidDVSequenceNumber) {
  int version = GetParam();
  if (version < 3) {
    GTEST_SKIP() << "DVs only supported in V3+";
  }

  auto partition_a = PartitionValues({Literal::Int(0)});

  auto dv = MakeDV("/path/to/dv.puffin", partition_a, partitioned_spec_->spec_id(),
                   file_a_->file_path);

  std::vector<ManifestEntry> entries;
  entries.push_back(MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, dv));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  // Querying with sequence number > DV sequence number should fail
  auto result = index->ForDataFile(2, *file_a_);
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage(
                          "must be greater than or equal to data file sequence number"));
}

TEST_P(DeleteFileIndexTest, TestReferencedDeleteFiles) {
  int version = GetParam();

  auto partition_a = PartitionValues({Literal::Int(0)});
  auto eq_delete = MakeEqualityDeleteFile("/path/to/eq-delete.parquet", partition_a,
                                          partitioned_spec_->spec_id());
  auto pos_delete = MakePositionDeleteFile("/path/to/pos-delete.parquet", partition_a,
                                           partitioned_spec_->spec_id());
  auto global_eq_delete = MakeEqualityDeleteFile("/path/to/global-eq-delete.parquet",
                                                 PartitionValues(std::vector<Literal>{}),
                                                 unpartitioned_spec_->spec_id());

  std::vector<ManifestEntry> partition_entries;
  partition_entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, eq_delete));
  partition_entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, pos_delete));

  auto partition_manifest = WriteDeleteManifest(
      version, /*snapshot_id=*/1000L, std::move(partition_entries), partitioned_spec_);

  std::vector<ManifestEntry> global_entries;
  global_entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1001L, /*sequence_number=*/2, global_eq_delete));

  auto global_manifest = WriteDeleteManifest(
      version, /*snapshot_id=*/1001L, std::move(global_entries), unpartitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({partition_manifest, global_manifest}));

  auto referenced = index->ReferencedDeleteFiles();
  EXPECT_EQ(referenced.size(), 3);
  EXPECT_THAT(GetPaths(referenced),
              testing::UnorderedElementsAre("/path/to/eq-delete.parquet",
                                            "/path/to/pos-delete.parquet",
                                            "/path/to/global-eq-delete.parquet"));
}

TEST_P(DeleteFileIndexTest, TestExistingDeleteFiles) {
  int version = GetParam();

  auto partition_a = PartitionValues({Literal::Int(0)});
  auto eq_delete = MakeEqualityDeleteFile("/path/to/eq-delete.parquet", partition_a,
                                          partitioned_spec_->spec_id());
  auto pos_delete = MakePositionDeleteFile("/path/to/pos-delete.parquet", partition_a,
                                           partitioned_spec_->spec_id());

  std::vector<ManifestEntry> entries;
  // Use ManifestStatus::kExisting to simulate files that were merged from a previous
  // manifest
  entries.push_back(MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1,
                                    eq_delete, ManifestStatus::kExisting));
  entries.push_back(MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1,
                                    pos_delete, ManifestStatus::kExisting));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  EXPECT_TRUE(index->has_equality_deletes());
  EXPECT_TRUE(index->has_position_deletes());

  // Both delete files should be correctly loaded and applied to file_a_
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *file_a_));
  EXPECT_EQ(deletes.size(), 2);
  EXPECT_THAT(GetPaths(deletes),
              testing::UnorderedElementsAre("/path/to/eq-delete.parquet",
                                            "/path/to/pos-delete.parquet"));
}

TEST_P(DeleteFileIndexTest, TestDeletedStatusExcluded) {
  int version = GetParam();

  auto partition_a = PartitionValues({Literal::Int(0)});
  auto eq_delete_added = MakeEqualityDeleteFile(
      "/path/to/eq-delete-added.parquet", partition_a, partitioned_spec_->spec_id());
  auto eq_delete_deleted = MakeEqualityDeleteFile(
      "/path/to/eq-delete-deleted.parquet", partition_a, partitioned_spec_->spec_id());
  auto pos_delete_added = MakePositionDeleteFile(
      "/path/to/pos-delete-added.parquet", partition_a, partitioned_spec_->spec_id());
  auto pos_delete_deleted = MakePositionDeleteFile(
      "/path/to/pos-delete-deleted.parquet", partition_a, partitioned_spec_->spec_id());

  std::vector<ManifestEntry> entries;
  entries.push_back(MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1,
                                    eq_delete_added, ManifestStatus::kAdded));
  entries.push_back(MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1,
                                    eq_delete_deleted, ManifestStatus::kDeleted));
  entries.push_back(MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1,
                                    pos_delete_added, ManifestStatus::kAdded));
  entries.push_back(MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1,
                                    pos_delete_deleted, ManifestStatus::kDeleted));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  EXPECT_TRUE(index->has_equality_deletes());
  EXPECT_TRUE(index->has_position_deletes());

  // Only the non-deleted (ADDED) delete files should be loaded
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *file_a_));
  EXPECT_EQ(deletes.size(), 2);
  EXPECT_THAT(GetPaths(deletes),
              testing::UnorderedElementsAre("/path/to/eq-delete-added.parquet",
                                            "/path/to/pos-delete-added.parquet"));
}

TEST_P(DeleteFileIndexTest, TestPositionDeleteDiscardMetrics) {
  int version = GetParam();

  auto partition_a = PartitionValues({Literal::Int(0)});

  constexpr int32_t kDeleteFilePathFieldId = MetadataColumns::kDeleteFilePathColumnId;
  constexpr int32_t kPositionFieldId = MetadataColumns::kFilePositionColumnId;

  // Create a position delete file with full metrics
  auto pos_delete = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "/path/to/pos-delete-with-metrics.parquet",
      .file_format = FileFormatType::kParquet,
      .partition = partition_a,
      .record_count = 100,
      .file_size_in_bytes = 1024,
      // Add stats for multiple columns
      .column_sizes = {{kDeleteFilePathFieldId, 100}, {kPositionFieldId, 200}},
      .value_counts = {{kDeleteFilePathFieldId, 10}, {kPositionFieldId, 20}},
      .null_value_counts = {{kDeleteFilePathFieldId, 1}, {kPositionFieldId, 2}},
      .nan_value_counts = {{kDeleteFilePathFieldId, 0}, {kPositionFieldId, 0}},
      .lower_bounds = {{kDeleteFilePathFieldId, {0x01}}, {kPositionFieldId, {0x02}}},
      .upper_bounds = {{kDeleteFilePathFieldId, {0xFF}}, {kPositionFieldId, {0xFE}}},
      .partition_spec_id = partitioned_spec_->spec_id(),
  });

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, pos_delete));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  EXPECT_TRUE(index->has_position_deletes());

  // Get the delete files from the index
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *file_a_));
  ASSERT_EQ(deletes.size(), 1);

  const auto& returned_file = deletes[0];
  EXPECT_EQ(returned_file->file_path, "/path/to/pos-delete-with-metrics.parquet");
  // record_count should be preserved
  EXPECT_EQ(returned_file->record_count, 100);
  // Stats maps should only contain entries for delete file path.
  EXPECT_EQ(returned_file->column_sizes.size(), 1);
  EXPECT_EQ(returned_file->value_counts.size(), 1);
  EXPECT_EQ(returned_file->null_value_counts.size(), 1);
  EXPECT_EQ(returned_file->nan_value_counts.size(), 1);
  EXPECT_EQ(returned_file->lower_bounds.size(), 1);
  EXPECT_EQ(returned_file->upper_bounds.size(), 1);
  EXPECT_TRUE(returned_file->column_sizes.contains(kDeleteFilePathFieldId));
  EXPECT_TRUE(returned_file->value_counts.contains(kDeleteFilePathFieldId));
  EXPECT_TRUE(returned_file->null_value_counts.contains(kDeleteFilePathFieldId));
  EXPECT_TRUE(returned_file->nan_value_counts.contains(kDeleteFilePathFieldId));
  EXPECT_TRUE(returned_file->lower_bounds.contains(kDeleteFilePathFieldId));
  EXPECT_TRUE(returned_file->upper_bounds.contains(kDeleteFilePathFieldId));
}

TEST_P(DeleteFileIndexTest, TestEqualityDeleteDiscardMetrics) {
  int version = GetParam();

  auto partition_a = PartitionValues({Literal::Int(0)});

  // Create an equality delete file with full metrics
  auto eq_delete = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "/path/to/eq-delete-with-metrics.parquet",
      .file_format = FileFormatType::kParquet,
      .partition = partition_a,
      .record_count = 50,
      .file_size_in_bytes = 512,
      // Add stats for multiple columns
      .column_sizes = {{1, 100}, {2, 200}, {3, 300}},
      .value_counts = {{1, 10}, {2, 20}, {3, 30}},
      .null_value_counts = {{1, 1}, {2, 2}, {3, 3}},
      .nan_value_counts = {{1, 0}, {2, 0}, {3, 0}},
      .lower_bounds = {{1, {0x01}}, {2, {0x02}}, {3, {0x03}}},
      .upper_bounds = {{1, {0xFF}}, {2, {0xFE}}, {3, {0xFD}}},
      .equality_ids = {1},  // equality field IDs
      .partition_spec_id = partitioned_spec_->spec_id(),
  });

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeDeleteEntry(/*snapshot_id=*/1000L, /*sequence_number=*/1, eq_delete));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries),
                                      partitioned_spec_);

  ICEBERG_UNWRAP_OR_FAIL(auto index, BuildIndex({manifest}));

  EXPECT_TRUE(index->has_equality_deletes());

  // Get the delete files from the index
  ICEBERG_UNWRAP_OR_FAIL(auto deletes, index->ForDataFile(0, *file_a_));
  ASSERT_EQ(deletes.size(), 1);

  const auto& returned_file = deletes[0];
  EXPECT_EQ(returned_file->file_path, "/path/to/eq-delete-with-metrics.parquet");
  // record_count should be preserved
  EXPECT_EQ(returned_file->record_count, 50);
  // Stats maps should only contain entries for equality field IDs.
  EXPECT_EQ(returned_file->column_sizes.size(), 1);
  EXPECT_EQ(returned_file->value_counts.size(), 1);
  EXPECT_EQ(returned_file->null_value_counts.size(), 1);
  EXPECT_EQ(returned_file->nan_value_counts.size(), 1);
  EXPECT_EQ(returned_file->lower_bounds.size(), 1);
  EXPECT_EQ(returned_file->upper_bounds.size(), 1);
  EXPECT_TRUE(returned_file->column_sizes.contains(1));
  EXPECT_TRUE(returned_file->value_counts.contains(1));
  EXPECT_TRUE(returned_file->null_value_counts.contains(1));
  EXPECT_TRUE(returned_file->nan_value_counts.contains(1));
  EXPECT_TRUE(returned_file->lower_bounds.contains(1));
  EXPECT_TRUE(returned_file->upper_bounds.contains(1));
}

INSTANTIATE_TEST_SUITE_P(DeleteFileIndexVersions, DeleteFileIndexTest,
                         testing::Values(2, 3));

}  // namespace iceberg
