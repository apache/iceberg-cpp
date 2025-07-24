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

#include "iceberg/manifest_reader.h"

#include <arrow/filesystem/localfs.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io.h"
#include "iceberg/avro/avro_reader.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/schema.h"
#include "temp_file_test_base.h"
#include "test_common.h"

namespace iceberg {

class ManifestReaderTest : public TempFileTestBase {
 protected:
  static void SetUpTestSuite() { avro::AvroReader::Register(); }

  void SetUp() override {
    TempFileTestBase::SetUp();
    local_fs_ = std::make_shared<::arrow::fs::LocalFileSystem>();
    file_io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(local_fs_);

    avro::RegisterLogicalTypes();
  }

  std::vector<ManifestEntry> prepare_manifest_entries() {
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
    std::vector<std::map<int32_t, std::vector<uint8_t>>> bounds = {
        {{1, {0xd2, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
         {2, {'.', 0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
         {3, {0x12, 0xe2}},
         {4, {0xc0, 'y', 0xe7, 0x98, 0xd6, 0xb9, 0x05, 0x00}}},
        {{1, {0xd2, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
         {2, {'.', 0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
         {3, {0x12, 0xe3}},
         {4, {0xc0, 0x19, '#', '=', 0xe2, 0x0f, 0x06, 0x00}}},
        {{1, {'{', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
         {2, {0xc8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
         {3, {0x0e, '"'}},
         {4, {0xc0, 0xd9, '7', 0x93, 0x1f, 0xf3, 0x05, 0x00}}},
        {{1, {'{', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
         {2, {0xc8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
         {3, {0x0e, '!'}},
         {4, {0xc0, 0x19, 0x10, '{', 0xc2, 0xb9, 0x05, 0x00}}},
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

  std::shared_ptr<::arrow::fs::LocalFileSystem> local_fs_;
  std::shared_ptr<FileIO> file_io_;
};

TEST_F(ManifestReaderTest, BasicTest) {
  iceberg::SchemaField partition_field(1000, "order_ts_hour", iceberg::int32(), true);
  auto partition_schema =
      std::make_shared<Schema>(std::vector<SchemaField>({partition_field}));
  std::string path = GetResourcePath("56357cd7-391f-4df8-aa24-e7e667da8870-m4.avro");
  auto manifest_reader_result =
      ManifestReader::MakeReader(path, file_io_, partition_schema);
  ASSERT_EQ(manifest_reader_result.has_value(), true)
      << manifest_reader_result.error().message;
  auto manifest_reader = std::move(manifest_reader_result.value());
  auto read_result = manifest_reader->Entries();
  ASSERT_EQ(read_result.has_value(), true) << read_result.error().message;

  auto expected_entries = prepare_manifest_entries();
  ASSERT_EQ(read_result.value(), expected_entries);
}

}  // namespace iceberg
