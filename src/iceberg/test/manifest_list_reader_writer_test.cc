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

#include <arrow/filesystem/localfs.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/expression/literal.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"
#include "iceberg/test/test_common.h"

namespace iceberg {

class ManifestListReaderWriterTestBase : public TempFileTestBase {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  void SetUp() override {
    TempFileTestBase::SetUp();
    local_fs_ = std::make_shared<::arrow::fs::LocalFileSystem>();
    file_io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(local_fs_);
  }

  void TestManifestListReading(const std::string& resource_name,
                               const std::vector<ManifestFile>& expected_manifest_list) {
    std::string path = GetResourcePath(resource_name);
    TestManifestListReadingByPath(path, expected_manifest_list);
  }

  void TestManifestListReadingByPath(
      const std::string& path, const std::vector<ManifestFile>& expected_manifest_list) {
    auto manifest_reader_result = ManifestListReader::Make(path, file_io_);
    ASSERT_EQ(manifest_reader_result.has_value(), true);

    auto manifest_reader = std::move(manifest_reader_result.value());
    auto read_result = manifest_reader->Files();
    ASSERT_EQ(read_result.has_value(), true);
    ASSERT_EQ(read_result.value().size(), expected_manifest_list.size());
    ASSERT_EQ(read_result.value(), expected_manifest_list);
  }

  void TestNonPartitionedManifests(const std::vector<ManifestFile>& manifest_files) {
    for (const auto& manifest : manifest_files) {
      ASSERT_EQ(manifest.partition_spec_id, 0);
      ASSERT_TRUE(manifest.partitions.empty());
      ASSERT_EQ(manifest.content, ManifestContent::kData);
    }
  }

  std::shared_ptr<::arrow::fs::LocalFileSystem> local_fs_;
  std::shared_ptr<FileIO> file_io_;
};

class ManifestListReaderWriterV1Test : public ManifestListReaderWriterTestBase {
 protected:
  std::vector<ManifestFile> PreparePartitionedTestData() {
    std::vector<std::string> paths = {
        "iceberg-warehouse/db/v1_partition_test/metadata/"
        "eafd2972-f58e-4185-9237-6378f564787e-m1.avro",
        "iceberg-warehouse/db/v1_partition_test/metadata/"
        "eafd2972-f58e-4185-9237-6378f564787e-m0.avro"};
    std::vector<int64_t> file_size = {6185, 6113};
    std::vector<int64_t> snapshot_id = {7532614258660258098, 7532614258660258098};

    return {
        {.manifest_path = paths[0],
         .manifest_length = file_size[0],
         .partition_spec_id = 0,
         .added_snapshot_id = snapshot_id[0],
         .added_files_count = 4,
         .existing_files_count = 0,
         .deleted_files_count = 0,
         .added_rows_count = 6,
         .existing_rows_count = 0,
         .deleted_rows_count = 0,
         .partitions = {{.contains_null = false,
                         .contains_nan = false,
                         .lower_bound = Literal::String("2022-02-22").Serialize().value(),
                         .upper_bound =
                             Literal::String("2022-2-23").Serialize().value()}}},

        {.manifest_path = paths[1],
         .manifest_length = file_size[1],
         .partition_spec_id = 0,
         .added_snapshot_id = snapshot_id[1],
         .added_files_count = 0,
         .existing_files_count = 0,
         .deleted_files_count = 2,
         .added_rows_count = 0,
         .existing_rows_count = 0,
         .deleted_rows_count = 6,
         .partitions = {
             {.contains_null = false,
              .contains_nan = false,
              .lower_bound = Literal::String("2022-2-22").Serialize().value(),
              .upper_bound = Literal::String("2022-2-23").Serialize().value()}}}};
  }

  std::vector<ManifestFile> PrepareComplexTypeTestData() {
    std::vector<std::string> paths = {
        "iceberg-warehouse/db/v1_type_test/metadata/"
        "aeffe099-3bac-4011-bc17-5875210d8dc0-m1.avro",
        "iceberg-warehouse/db/v1_type_test/metadata/"
        "aeffe099-3bac-4011-bc17-5875210d8dc0-m0.avro"};
    std::vector<int64_t> file_size = {6498, 6513};
    std::vector<int64_t> snapshot_id = {4134160420377642835, 4134160420377642835};

    return {{.manifest_path = paths[0],
             .manifest_length = file_size[0],
             .partition_spec_id = 0,
             .added_snapshot_id = snapshot_id[0],
             .added_files_count = 1,
             .existing_files_count = 0,
             .deleted_files_count = 0,
             .added_rows_count = 2,
             .existing_rows_count = 0,
             .deleted_rows_count = 0},

            {.manifest_path = paths[1],
             .manifest_length = file_size[1],
             .partition_spec_id = 0,
             .added_snapshot_id = snapshot_id[1],
             .added_files_count = 0,
             .existing_files_count = 0,
             .deleted_files_count = 1,
             .added_rows_count = 0,
             .existing_rows_count = 0,
             .deleted_rows_count = 3}};
  }

  std::vector<ManifestFile> PrepareComplexPartitionedTestData() {
    std::vector<std::string> paths = {
        "iceberg-warehouse/db2/v1_complex_partition_test/metadata/"
        "5d690750-8fb4-4cd1-8ae7-85c7b39abe14-m0.avro",
        "iceberg-warehouse/db2/v1_complex_partition_test/metadata/"
        "5d690750-8fb4-4cd1-8ae7-85c7b39abe14-m1.avro"};
    std::vector<int64_t> file_size = {6402, 6318};
    std::vector<int64_t> snapshot_id = {7522296285847100621, 7522296285847100621};

    std::vector<std::vector<std::uint8_t>> lower_bounds = {
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x32},
        {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x32},
        {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

    std::vector<std::vector<std::uint8_t>> upper_bounds = {
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x34},
        {0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
        {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x33},
        {0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

    return {{.manifest_path = paths[0],
             .manifest_length = file_size[0],
             .partition_spec_id = 0,
             .added_snapshot_id = snapshot_id[0],
             .added_files_count = 0,
             .existing_files_count = 3,
             .deleted_files_count = 1,
             .added_rows_count = 0,
             .existing_rows_count = 4,
             .deleted_rows_count = 2,
             .partitions = {{.contains_null = false,
                             .contains_nan = false,
                             .lower_bound = lower_bounds[0],
                             .upper_bound = upper_bounds[0]},
                            {.contains_null = false,
                             .contains_nan = false,
                             .lower_bound = lower_bounds[1],
                             .upper_bound = upper_bounds[1]}}},

            {.manifest_path = paths[1],
             .manifest_length = file_size[1],
             .partition_spec_id = 0,
             .added_snapshot_id = snapshot_id[1],
             .added_files_count = 0,
             .existing_files_count = 1,
             .deleted_files_count = 1,
             .added_rows_count = 0,
             .existing_rows_count = 1,
             .deleted_rows_count = 1,
             .partitions = {{.contains_null = false,
                             .contains_nan = false,
                             .lower_bound = lower_bounds[2],
                             .upper_bound = upper_bounds[2]},
                            {.contains_null = false,
                             .contains_nan = false,
                             .lower_bound = lower_bounds[3],
                             .upper_bound = upper_bounds[3]}}}};
  }

  void TestWriteManifestList(const std::string& manifest_list_path,
                             const std::vector<ManifestFile>& manifest_files) {
    auto result = ManifestListWriter::MakeV1Writer(1, 0, manifest_list_path, file_io_);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto writer = std::move(result.value());
    auto status = writer->AddAll(manifest_files);
    EXPECT_THAT(status, IsOk());
    status = writer->Close();
    EXPECT_THAT(status, IsOk());
  }
};

class ManifestListReaderWriterV2Test : public ManifestListReaderWriterTestBase {
 protected:
  std::vector<ManifestFile> PreparePartitionedTestData() {
    std::vector<ManifestFile> manifest_files;
    std::string test_dir_prefix = "/tmp/db/db/iceberg_test/metadata/";
    std::vector<std::string> paths = {"2bccd69e-d642-4816-bba0-261cd9bd0d93-m0.avro",
                                      "9b6ffacd-ef10-4abf-a89c-01c733696796-m0.avro",
                                      "2541e6b5-4923-4bd5-886d-72c6f7228400-m0.avro",
                                      "3118c801-d2e0-4df6-8c7a-7d4eaade32f8-m0.avro"};
    std::vector<int64_t> file_size = {7433, 7431, 7433, 7431};
    std::vector<int64_t> snapshot_id = {7412193043800610213, 5485972788975780755,
                                        1679468743751242972, 1579605567338877265};
    std::vector<std::vector<uint8_t>> bounds = {{'x', ';', 0x07, 0x00},
                                                {'(', 0x19, 0x07, 0x00},
                                                {0xd0, 0xd4, 0x06, 0x00},
                                                {0xb8, 0xd4, 0x06, 0x00}};
    for (int i = 0; i < 4; ++i) {
      ManifestFile manifest_file;
      manifest_file.manifest_path = test_dir_prefix + paths[i];
      manifest_file.manifest_length = file_size[i];
      manifest_file.partition_spec_id = 0;
      manifest_file.content = ManifestContent::kData;
      manifest_file.sequence_number = 4 - i;
      manifest_file.min_sequence_number = 4 - i;
      manifest_file.added_snapshot_id = snapshot_id[i];
      manifest_file.added_files_count = 1;
      manifest_file.existing_files_count = 0;
      manifest_file.deleted_files_count = 0;
      manifest_file.added_rows_count = 1;
      manifest_file.existing_rows_count = 0;
      manifest_file.deleted_rows_count = 0;
      PartitionFieldSummary partition;
      partition.contains_null = false;
      partition.contains_nan = false;
      partition.lower_bound = bounds[i];
      partition.upper_bound = bounds[i];
      manifest_file.partitions.emplace_back(partition);
      manifest_files.emplace_back(manifest_file);
    }
    return manifest_files;
  }

  std::vector<ManifestFile> PrepareNonPartitionedTestData() {
    std::vector<ManifestFile> manifest_files;
    std::string test_dir_prefix = "/tmp/db/db/v2_non_partitioned_test/metadata/";

    std::vector<std::string> paths = {"ccb6dbcb-0611-48da-be68-bd506ea63188-m0.avro",
                                      "b89a10c9-a7a8-4526-99c5-5587a4ea7527-m0.avro",
                                      "a74d20fa-c800-4706-9ddb-66be15a5ecb0-m0.avro",
                                      "ae7d5fce-7245-4335-9b57-bc598c595c84-m0.avro"};

    std::vector<int64_t> file_size = {7169, 7170, 7169, 7170};

    std::vector<int64_t> snapshot_id = {251167482216575399, 4248697313956014690,
                                        281757490425433194, 5521202581490753283};

    for (int i = 0; i < 4; ++i) {
      ManifestFile manifest_file;
      manifest_file.manifest_path = test_dir_prefix + paths[i];
      manifest_file.manifest_length = file_size[i];
      manifest_file.partition_spec_id = 0;
      manifest_file.content = ManifestContent::kData;
      manifest_file.sequence_number = 4 - i;
      manifest_file.min_sequence_number = 4 - i;
      manifest_file.added_snapshot_id = snapshot_id[i];
      manifest_file.added_files_count = 1;
      manifest_file.existing_files_count = 0;
      manifest_file.deleted_files_count = 0;
      manifest_file.added_rows_count = 1;
      manifest_file.existing_rows_count = 0;
      manifest_file.deleted_rows_count = 0;
      // Note: no partitions for non-partitioned test
      manifest_files.emplace_back(manifest_file);
    }
    return manifest_files;
  }

  void TestWriteManifestList(const std::string& manifest_list_path,
                             const std::vector<ManifestFile>& manifest_files) {
    auto result = ManifestListWriter::MakeV2Writer(1, 0, 4, manifest_list_path, file_io_);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto writer = std::move(result.value());
    auto status = writer->AddAll(manifest_files);
    EXPECT_THAT(status, IsOk());
    status = writer->Close();
    EXPECT_THAT(status, IsOk());
  }
};

// V1 Tests
TEST_F(ManifestListReaderWriterV1Test, PartitionedTest) {
  auto expected_manifest_list = PreparePartitionedTestData();
  TestManifestListReading(
      "snap-7532614258660258098-1-eafd2972-f58e-4185-9237-6378f564787e.avro",
      expected_manifest_list);
}

TEST_F(ManifestListReaderWriterV1Test, ComplexTypeTest) {
  auto expected_manifest_list = PrepareComplexTypeTestData();
  TestManifestListReading(
      "snap-4134160420377642835-1-aeffe099-3bac-4011-bc17-5875210d8dc0.avro",
      expected_manifest_list);
}

TEST_F(ManifestListReaderWriterV1Test, ComplexPartitionedTest) {
  auto expected_manifest_list = PrepareComplexPartitionedTestData();
  TestManifestListReading(
      "snap-7522296285847100621-1-5d690750-8fb4-4cd1-8ae7-85c7b39abe14.avro",
      expected_manifest_list);
}

TEST_F(ManifestListReaderWriterV1Test, WritePartitionedTest) {
  auto expected_manifest_list = PreparePartitionedTestData();
  auto write_manifest_list_path = CreateNewTempFilePath();
  TestWriteManifestList(write_manifest_list_path, expected_manifest_list);
  TestManifestListReadingByPath(write_manifest_list_path, expected_manifest_list);
}

TEST_F(ManifestListReaderWriterV1Test, WriteComplexTypeTest) {
  auto expected_manifest_list = PrepareComplexTypeTestData();
  auto write_manifest_list_path = CreateNewTempFilePath();
  TestWriteManifestList(write_manifest_list_path, expected_manifest_list);
  TestManifestListReadingByPath(write_manifest_list_path, expected_manifest_list);
}

TEST_F(ManifestListReaderWriterV1Test, WriteComplexPartitionedTest) {
  auto expected_manifest_list = PrepareComplexPartitionedTestData();
  auto write_manifest_list_path = CreateNewTempFilePath();
  TestWriteManifestList(write_manifest_list_path, expected_manifest_list);
  TestManifestListReadingByPath(write_manifest_list_path, expected_manifest_list);
}

// V2 Tests
TEST_F(ManifestListReaderWriterV2Test, PartitionedTest) {
  auto expected_manifest_list = PreparePartitionedTestData();
  TestManifestListReading(
      "snap-7412193043800610213-1-2bccd69e-d642-4816-bba0-261cd9bd0d93.avro",
      expected_manifest_list);
}

TEST_F(ManifestListReaderWriterV2Test, NonPartitionedTest) {
  auto expected_manifest_list = PrepareNonPartitionedTestData();
  TestManifestListReading(
      "snap-251167482216575399-1-ccb6dbcb-0611-48da-be68-bd506ea63188.avro",
      expected_manifest_list);

  // Additional verification: ensure all manifests are truly non-partitioned
  TestNonPartitionedManifests(expected_manifest_list);
}

TEST_F(ManifestListReaderWriterV2Test, WritePartitionedTest) {
  auto expected_manifest_list = PreparePartitionedTestData();
  auto write_manifest_list_path = CreateNewTempFilePath();
  TestWriteManifestList(write_manifest_list_path, expected_manifest_list);
  TestManifestListReadingByPath(write_manifest_list_path, expected_manifest_list);
}

TEST_F(ManifestListReaderWriterV2Test, WriteNonPartitionedTest) {
  auto expected_manifest_list = PrepareNonPartitionedTestData();
  auto write_manifest_list_path = CreateNewTempFilePath();
  TestWriteManifestList(write_manifest_list_path, expected_manifest_list);
  TestManifestListReadingByPath(write_manifest_list_path, expected_manifest_list);

  // Additional verification: ensure all manifests are truly non-partitioned
  TestNonPartitionedManifests(expected_manifest_list);
}

}  // namespace iceberg
