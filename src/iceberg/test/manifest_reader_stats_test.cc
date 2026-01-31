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

#include <chrono>
#include <format>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

class TestManifestReaderStats : public testing::TestWithParam<int8_t> {
 protected:
  void SetUp() override {
    avro::RegisterAll();
    file_io_ = arrow::MakeMockFileIO();

    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(/*field_id=*/3, "id", int32()),
        SchemaField::MakeRequired(/*field_id=*/4, "data", string())});

    ICEBERG_UNWRAP_OR_FAIL(
        spec_,
        PartitionSpec::Make(
            /*spec_id=*/0, {PartitionField(/*source_id=*/4, /*field_id=*/1000,
                                           "data_bucket", Transform::Bucket(16))}));
  }

  std::string MakeManifestPath() {
    return std::format("manifest-{}.avro",
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  inline static const std::map<int32_t, int64_t> kValueCounts = {{3, 3L}};
  inline static const std::map<int32_t, int64_t> kNullValueCounts = {{3, 0L}};
  inline static const std::map<int32_t, int64_t> kNanValueCounts = {{3, 1L}};
  inline static const std::map<int32_t, std::vector<uint8_t>> kLowerBounds = {
      {3, Literal::Int(2).Serialize().value()}};
  inline static const std::map<int32_t, std::vector<uint8_t>> kUpperBounds = {
      {3, Literal::Int(4).Serialize().value()}};

  std::unique_ptr<DataFile> MakeDataFileWithStats() {
    return std::make_unique<DataFile>(DataFile{
        .file_path = "/path/to/data-with-stats.parquet",
        .file_format = FileFormatType::kParquet,
        .partition = PartitionValues({Literal::Int(0)}),
        .record_count = 3,
        .file_size_in_bytes = 10,
        .value_counts = kValueCounts,
        .null_value_counts = kNullValueCounts,
        .nan_value_counts = kNanValueCounts,
        .lower_bounds = kLowerBounds,
        .upper_bounds = kUpperBounds,
        .sort_order_id = 0,
    });
  }

  ManifestFile WriteManifest(int8_t format_version, std::unique_ptr<DataFile> data_file) {
    const std::string manifest_path = MakeManifestPath();

    auto writer_result = ManifestWriter::MakeWriter(
        format_version, /*snapshot_id=*/1000L, manifest_path, file_io_, spec_, schema_,
        ManifestContent::kData, /*first_row_id=*/0L);
    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    EXPECT_THAT(writer->WriteAddedEntry(std::move(data_file)), IsOk());
    EXPECT_THAT(writer->Close(), IsOk());

    auto manifest_result = writer->ToManifestFile();
    EXPECT_THAT(manifest_result, IsOk());
    return std::move(manifest_result.value());
  }

  void AssertFullStats(const DataFile& file) {
    EXPECT_EQ(file.record_count, 3);
    EXPECT_EQ(file.value_counts, kValueCounts);
    EXPECT_EQ(file.null_value_counts, kNullValueCounts);
    EXPECT_EQ(file.nan_value_counts, kNanValueCounts);
    EXPECT_EQ(file.lower_bounds, kLowerBounds);
    EXPECT_EQ(file.upper_bounds, kUpperBounds);
  }

  void AssertStatsDropped(const DataFile& file) {
    EXPECT_EQ(file.record_count, 3);  // record count is not dropped
    EXPECT_TRUE(file.value_counts.empty());
    EXPECT_TRUE(file.null_value_counts.empty());
    EXPECT_TRUE(file.nan_value_counts.empty());
    EXPECT_TRUE(file.lower_bounds.empty());
    EXPECT_TRUE(file.upper_bounds.empty());
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
};

TEST_P(TestManifestReaderStats, TestReadIncludesFullStats) {
  auto version = GetParam();
  auto manifest = WriteManifest(version, MakeDataFileWithStats());

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  auto entries_result = reader->Entries();
  ASSERT_THAT(entries_result, IsOk());
  auto entries = entries_result.value();
  ASSERT_EQ(entries.size(), 1);
  AssertFullStats(*entries[0].data_file);
}

TEST_P(TestManifestReaderStats, TestReadEntriesWithFilterIncludesFullStats) {
  auto version = GetParam();
  auto manifest = WriteManifest(version, MakeDataFileWithStats());

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  reader->FilterRows(Expressions::Equal("id", Literal::Int(3)));

  auto entries_result = reader->Entries();
  ASSERT_THAT(entries_result, IsOk());
  auto entries = entries_result.value();
  ASSERT_EQ(entries.size(), 1);
  AssertFullStats(*entries[0].data_file);
}

TEST_P(TestManifestReaderStats, TestReadEntriesWithFilterAndSelectIncludesFullStats) {
  auto version = GetParam();
  auto manifest = WriteManifest(version, MakeDataFileWithStats());

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  reader->Select({"file_path"});
  reader->FilterRows(Expressions::Equal("id", Literal::Int(3)));

  auto entries_result = reader->Entries();
  ASSERT_THAT(entries_result, IsOk());
  auto entries = entries_result.value();
  ASSERT_EQ(entries.size(), 1);
  AssertFullStats(*entries[0].data_file);
}

TEST_P(TestManifestReaderStats, TestReadEntriesWithSelectNotProjectStats) {
  auto version = GetParam();
  auto manifest = WriteManifest(version, MakeDataFileWithStats());

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  reader->Select({"file_path"});

  auto entries_result = reader->Entries();
  ASSERT_THAT(entries_result, IsOk());
  auto entries = entries_result.value();
  ASSERT_EQ(entries.size(), 1);
  AssertStatsDropped(*entries[0].data_file);
}

TEST_P(TestManifestReaderStats, TestReadEntriesWithSelectCertainStatNotProjectStats) {
  auto version = GetParam();
  auto manifest = WriteManifest(version, MakeDataFileWithStats());

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  reader->Select({"file_path", "value_counts"});

  auto entries_result = reader->Entries();
  ASSERT_THAT(entries_result, IsOk());
  auto entries = entries_result.value();
  ASSERT_EQ(entries.size(), 1);

  const auto& file = *entries[0].data_file;
  EXPECT_FALSE(file.value_counts.empty());
  EXPECT_TRUE(file.null_value_counts.empty());
  EXPECT_TRUE(file.nan_value_counts.empty());
  EXPECT_TRUE(file.lower_bounds.empty());
  EXPECT_TRUE(file.upper_bounds.empty());
}

INSTANTIATE_TEST_SUITE_P(ManifestReaderStatsVersions, TestManifestReaderStats,
                         testing::Values(1, 2, 3));

}  // namespace iceberg
