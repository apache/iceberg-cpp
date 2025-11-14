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

#include <cstddef>

#include <arrow/filesystem/localfs.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_reader.h"
#include "iceberg/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"
#include "iceberg/test/test_common.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

class ManifestReaderWriterTestBase : public TempFileTestBase {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  void SetUp() override {
    TempFileTestBase::SetUp();
    local_fs_ = std::make_shared<::arrow::fs::LocalFileSystem>();
    file_io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(local_fs_);
  }

  void TestManifestReading(const std::string& resource_name,
                           const std::vector<ManifestEntry>& expected_entries,
                           std::shared_ptr<Schema> partition_schema = nullptr) {
    std::string path = GetResourcePath(resource_name);
    TestManifestReadingByPath(path, expected_entries, partition_schema);
  }

  void TestManifestReadingByPath(const std::string& path,
                                 const std::vector<ManifestEntry>& expected_entries,
                                 std::shared_ptr<Schema> partition_schema = nullptr) {
    auto manifest_reader_result = ManifestReader::Make(path, file_io_, partition_schema);
    ASSERT_TRUE(manifest_reader_result.has_value())
        << manifest_reader_result.error().message;

    auto manifest_reader = std::move(manifest_reader_result.value());
    auto read_result = manifest_reader->Entries();
    ASSERT_TRUE(read_result.has_value()) << read_result.error().message;
    ASSERT_EQ(read_result.value().size(), expected_entries.size());
    ASSERT_EQ(read_result.value(), expected_entries);
  }

  void TestManifestReadingWithManifestFile(
      const ManifestFile& manifest_file,
      const std::vector<ManifestEntry>& expected_entries,
      std::shared_ptr<Schema> partition_schema = nullptr) {
    auto manifest_reader_result =
        ManifestReader::Make(manifest_file, file_io_, partition_schema);
    ASSERT_TRUE(manifest_reader_result.has_value())
        << manifest_reader_result.error().message;

    auto manifest_reader = std::move(manifest_reader_result.value());
    auto read_result = manifest_reader->Entries();
    ASSERT_TRUE(read_result.has_value()) << read_result.error().message;
    ASSERT_EQ(read_result.value().size(), expected_entries.size());
    ASSERT_EQ(read_result.value(), expected_entries);
  }

  std::shared_ptr<::arrow::fs::LocalFileSystem> local_fs_;
  std::shared_ptr<FileIO> file_io_;
};

class ManifestV1Test : public ManifestReaderWriterTestBase {
 protected:
  std::vector<ManifestEntry> PreparePartitionedTestData() {
    std::vector<ManifestEntry> manifest_entries;
    std::string test_dir_prefix = "/tmp/db/db/iceberg_test/data/";
    std::vector<std::string> paths = {
        "order_ts_hour=2021-01-27-00/"
        "00000-2-d5ae78b7-4449-45ec-adb7-c0e9c0bdb714-0-00001.parquet",
        "order_ts_hour=2024-01-27-00/"
        "00000-2-d5ae78b7-4449-45ec-adb7-c0e9c0bdb714-0-00002.parquet",
        "order_ts_hour=2023-01-26-00/"
        "00000-2-d5ae78b7-4449-45ec-adb7-c0e9c0bdb714-0-00003.parquet",
        "order_ts_hour=2021-01-26-00/"
        "00000-2-d5ae78b7-4449-45ec-adb7-c0e9c0bdb714-0-00004.parquet"};
    std::vector<int64_t> partitions = {447696, 473976, 465192, 447672};

    // Note: The precision and scale for decimal literals are chosen arbitrarily here,
    // since the lower and upper bounds for decimal values are stored as unscaled int128_t
    // values in manifest files.
    std::vector<std::map<int32_t, std::vector<uint8_t>>> bounds = {
        {{1, Literal::Long(1234).Serialize().value()},
         {2, Literal::Long(5678).Serialize().value()},
         {3, Literal::Decimal(4834, 10, 2).Serialize().value()},
         {4, Literal::Timestamp(1611706223000000LL).Serialize().value()}},

        {{1, Literal::Long(1234).Serialize().value()},
         {2, Literal::Long(5678).Serialize().value()},
         {3, Literal::Decimal(4835, 10, 2).Serialize().value()},
         {4, Literal::Timestamp(1706314223000000LL).Serialize().value()}},

        {{1, Literal::Long(123).Serialize().value()},
         {2, Literal::Long(456).Serialize().value()},
         {3, Literal::Decimal(3618, 10, 2).Serialize().value()},
         {4, Literal::Timestamp(1674691823000000LL).Serialize().value()}},

        {{1, Literal::Long(123).Serialize().value()},
         {2, Literal::Long(456).Serialize().value()},
         {3, Literal::Decimal(3617, 10, 2).Serialize().value()},
         {4, Literal::Timestamp(1611619823000000LL).Serialize().value()}},
    };

    for (int i = 0; i < 4; ++i) {
      ManifestEntry entry;
      entry.status = ManifestStatus::kAdded;
      entry.snapshot_id = 6387266376565973956;
      entry.data_file = std::make_shared<DataFile>();
      entry.data_file->file_path = test_dir_prefix + paths[i];
      entry.data_file->file_format = FileFormatType::kParquet;
      entry.data_file->partition.emplace_back(Literal::Int(partitions[i]));
      entry.data_file->record_count = 1;
      entry.data_file->file_size_in_bytes = 1375;
      entry.data_file->column_sizes = {{1, 49}, {2, 49}, {3, 49}, {4, 49}};
      entry.data_file->value_counts = {{1, 1}, {2, 1}, {3, 1}, {4, 1}};
      entry.data_file->null_value_counts = {{1, 0}, {2, 0}, {3, 0}, {4, 0}};
      entry.data_file->split_offsets = {4};
      entry.data_file->sort_order_id = 0;
      entry.data_file->upper_bounds = bounds[i];
      entry.data_file->lower_bounds = bounds[i];
      manifest_entries.emplace_back(entry);
    }
    return manifest_entries;
  }

  void TestWriteManifest(const std::string& manifest_list_path,
                         std::shared_ptr<PartitionSpec> partition_spec,
                         const std::vector<ManifestEntry>& manifest_entries,
                         std::shared_ptr<Schema> table_schema) {
    auto result =
        ManifestWriter::MakeV1Writer(1, manifest_list_path, file_io_,
                                     std::move(partition_spec), std::move(table_schema));
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto writer = std::move(result.value());
    auto status = writer->AddAll(manifest_entries);
    EXPECT_THAT(status, IsOk());
    status = writer->Close();
    EXPECT_THAT(status, IsOk());
  }
};

TEST_F(ManifestV1Test, ReadPartitionedTest) {
  // TODO(xiao.dong) we need to add more cases for different partition types
  SchemaField partition_field(1000, "order_ts_hour", int32(), true);
  auto partition_schema =
      std::make_shared<Schema>(std::vector<SchemaField>({partition_field}));
  auto expected_entries = PreparePartitionedTestData();
  TestManifestReading("56357cd7-391f-4df8-aa24-e7e667da8870-m4.avro", expected_entries,
                      partition_schema);
}

TEST_F(ManifestV1Test, WritePartitionedTest) {
  SchemaField table_field(1, "order_ts_hour_source", int32(), true);
  SchemaField partition_field(1000, "order_ts_hour", int32(), true);
  auto table_schema = std::make_shared<Schema>(std::vector<SchemaField>({table_field}));
  auto partition_schema =
      std::make_shared<Schema>(std::vector<SchemaField>({partition_field}));
  auto identity_transform = Transform::Identity();
  std::vector<PartitionField> fields{
      PartitionField(1, 1000, "order_ts_hour", identity_transform)};
  ICEBERG_UNWRAP_OR_FAIL(std::shared_ptr<PartitionSpec> partition_spec,
                         PartitionSpec::Make(*table_schema, 1, fields, false));

  auto expected_entries = PreparePartitionedTestData();
  auto write_manifest_path = CreateNewTempFilePath();
  TestWriteManifest(write_manifest_path, partition_spec, expected_entries, table_schema);
  TestManifestReadingByPath(write_manifest_path, expected_entries, partition_schema);
}

class ManifestV2Test : public ManifestReaderWriterTestBase {
 protected:
  std::vector<ManifestEntry> CreateV2TestData(
      std::optional<int64_t> sequence_number = std::nullopt,
      std::optional<int32_t> partition_spec_id = std::nullopt) {
    std::vector<ManifestEntry> manifest_entries;
    std::string test_dir_prefix = "/tmp/db/db/v2_manifest_non_partitioned/data/";

    std::vector<std::string> paths = {
        "00000-0-b0f98903-6d21-45fd-9e0b-afbd4963e365-0-00001.parquet"};

    std::vector<int64_t> file_sizes = {1344};
    std::vector<int64_t> record_counts = {4};

    std::vector<std::map<int32_t, std::vector<uint8_t>>> lower_bounds = {
        {{1, Literal::Long(1).Serialize().value()},
         {2, Literal::String("record_four").Serialize().value()},
         {3, Literal::String("data_content_1").Serialize().value()},
         {4, Literal::Double(123.45).Serialize().value()}}};

    std::vector<std::map<int32_t, std::vector<uint8_t>>> upper_bounds = {
        {{1, Literal::Long(4).Serialize().value()},
         {2, Literal::String("record_two").Serialize().value()},
         {3, Literal::String("data_content_4").Serialize().value()},
         {4, Literal::Double(456.78).Serialize().value()}}};

    DataFile data_file{.file_path = test_dir_prefix + paths[0],
                       .file_format = FileFormatType::kParquet,
                       .record_count = record_counts[0],
                       .file_size_in_bytes = file_sizes[0],
                       .column_sizes = {{1, 56}, {2, 73}, {3, 66}, {4, 67}},
                       .value_counts = {{1, 4}, {2, 4}, {3, 4}, {4, 4}},
                       .null_value_counts = {{1, 0}, {2, 0}, {3, 0}, {4, 0}},
                       .nan_value_counts = {{4, 0}},
                       .lower_bounds = lower_bounds[0],
                       .upper_bounds = upper_bounds[0],
                       .key_metadata = {},
                       .split_offsets = {4},
                       .equality_ids = {},
                       .sort_order_id = 0,
                       .first_row_id = std::nullopt,
                       .referenced_data_file = std::nullopt,
                       .content_offset = std::nullopt,
                       .content_size_in_bytes = std::nullopt};

    if (partition_spec_id.has_value()) {
      data_file.partition_spec_id = partition_spec_id.value();
    }

    manifest_entries.emplace_back(
        ManifestEntry{.status = ManifestStatus::kAdded,
                      .snapshot_id = 679879563479918846LL,
                      .sequence_number = sequence_number,
                      .file_sequence_number = sequence_number,
                      .data_file = std::make_shared<DataFile>(data_file)});
    return manifest_entries;
  }

  std::vector<ManifestEntry> PrepareNonPartitionedTestData() {
    return CreateV2TestData();
  }

  std::vector<ManifestEntry> PrepareMetadataInheritanceTestData() {
    return CreateV2TestData(/*sequence_number=*/15, /*partition_spec_id*/ 12);
  }

  void TestWriteManifest(int64_t snapshot_id, const std::string& manifest_list_path,
                         std::shared_ptr<PartitionSpec> partition_spec,
                         const std::vector<ManifestEntry>& manifest_entries,
                         std::shared_ptr<Schema> table_schema) {
    auto result = ManifestWriter::MakeV2Writer(
        snapshot_id, manifest_list_path, file_io_, std::move(partition_spec),
        std::move(table_schema), ManifestContent::kData);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto writer = std::move(result.value());
    auto status = writer->AddAll(manifest_entries);
    EXPECT_THAT(status, IsOk());
    status = writer->Close();
    EXPECT_THAT(status, IsOk());
  }
};

TEST_F(ManifestV2Test, ReadNonPartitionedTest) {
  auto expected_entries = PrepareNonPartitionedTestData();
  TestManifestReading("2ddf1bc9-830b-4015-aced-c060df36f150-m0.avro", expected_entries);
}

TEST_F(ManifestV2Test, ReadMetadataInheritanceTest) {
  std::string path = GetResourcePath("2ddf1bc9-830b-4015-aced-c060df36f150-m0.avro");
  ManifestFile manifest_file{
      .manifest_path = path,
      .manifest_length = 100,
      .partition_spec_id = 12,
      .content = ManifestFile::Content::kData,
      .sequence_number = 15,
      .added_snapshot_id = 679879563479918846LL,
  };
  auto expected_entries = PrepareMetadataInheritanceTestData();
  TestManifestReadingWithManifestFile(manifest_file, expected_entries);
}

TEST_F(ManifestV2Test, WriteNonPartitionedTest) {
  SchemaField table_field(1, "order_ts_hour_source", int32(), true);
  SchemaField partition_field(1000, "order_ts_hour", int32(), true);
  auto table_schema = std::make_shared<Schema>(std::vector<SchemaField>({table_field}));
  auto expected_entries = PrepareNonPartitionedTestData();
  auto write_manifest_path = CreateNewTempFilePath();
  TestWriteManifest(679879563479918846LL, write_manifest_path,
                    PartitionSpec::Unpartitioned(), expected_entries, table_schema);
  TestManifestReadingByPath(write_manifest_path, expected_entries);
}

TEST_F(ManifestV2Test, WriteInheritancePartitionedTest) {
  SchemaField table_field(1, "order_ts_hour_source", int32(), true);
  SchemaField partition_field(1000, "order_ts_hour", int32(), true);
  auto table_schema = std::make_shared<Schema>(std::vector<SchemaField>({table_field}));
  auto expected_entries = PrepareMetadataInheritanceTestData();
  auto write_manifest_path = CreateNewTempFilePath();
  TestWriteManifest(679879563479918846LL, write_manifest_path,
                    PartitionSpec::Unpartitioned(), expected_entries, table_schema);
  ManifestFile manifest_file{
      .manifest_path = write_manifest_path,
      .manifest_length = 100,
      .partition_spec_id = 12,
      .content = ManifestFile::Content::kData,
      .sequence_number = 15,
      .added_snapshot_id = 679879563479918846LL,
  };
  TestManifestReadingWithManifestFile(manifest_file, expected_entries);
}

}  // namespace iceberg
