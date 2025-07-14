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
#include <avro/GenericDatum.hh>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io.h"
#include "iceberg/avro/avro_reader.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_reader.h"
#include "iceberg/schema.h"
#include "matchers.h"
#include "temp_file_test_base.h"
#include "test_common.h"

namespace iceberg {

class ManifestListReaderTest : public TempFileTestBase {
 protected:
  static void SetUpTestSuite() { avro::AvroReader::Register(); }

  void SetUp() override {
    TempFileTestBase::SetUp();
    local_fs_ = std::make_shared<::arrow::fs::LocalFileSystem>();
    file_io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(local_fs_);
  }

  std::shared_ptr<::arrow::fs::LocalFileSystem> local_fs_;
  std::shared_ptr<FileIO> file_io_;
};

TEST_F(ManifestListReaderTest, BasicTest) {
  std::string path = GetResourcePath(
      "snap-7412193043800610213-1-2bccd69e-d642-4816-bba0-261cd9bd0d93.avro");
  auto manifest_reader_result = ManifestListReader::MakeReader(path, file_io_);
  ASSERT_EQ(manifest_reader_result.has_value(), true);
  auto manifest_reader = std::move(manifest_reader_result.value());
  auto read_result = manifest_reader->Files();
  ASSERT_EQ(read_result.has_value(), true);
  ASSERT_EQ(read_result.value().size(), 4);
  std::string test_dir_prefix = "/tmp/db/db/iceberg_test/metadata/";
  for (const auto& file : read_result.value()) {
    auto manifest_path = file.manifest_path.substr(test_dir_prefix.size());
    if (manifest_path == "2bccd69e-d642-4816-bba0-261cd9bd0d93-m0.avro") {
      ASSERT_EQ(file.added_snapshot_id, 7412193043800610213);
      ASSERT_EQ(file.manifest_length, 7433);
      ASSERT_EQ(file.sequence_number, 4);
      ASSERT_EQ(file.min_sequence_number, 4);
      ASSERT_EQ(file.partitions.size(), 1);
      const auto& partition = file.partitions[0];
      ASSERT_EQ(partition.contains_null, false);
      ASSERT_EQ(partition.contains_nan.value(), false);
      ASSERT_EQ(partition.lower_bound.value(),
                std::vector<uint8_t>({'x', ';', 0x07, 0x00}));
      ASSERT_EQ(partition.upper_bound.value(),
                std::vector<uint8_t>({'x', ';', 0x07, 0x00}));
    } else if (manifest_path == "9b6ffacd-ef10-4abf-a89c-01c733696796-m0.avro") {
      ASSERT_EQ(file.added_snapshot_id, 5485972788975780755);
      ASSERT_EQ(file.manifest_length, 7431);
      ASSERT_EQ(file.sequence_number, 3);
      ASSERT_EQ(file.min_sequence_number, 3);
      ASSERT_EQ(file.partitions.size(), 1);
      const auto& partition = file.partitions[0];
      ASSERT_EQ(partition.contains_null, false);
      ASSERT_EQ(partition.contains_nan.value(), false);
      ASSERT_EQ(partition.lower_bound.value(),
                std::vector<uint8_t>({'(', 0x19, 0x07, 0x00}));
      ASSERT_EQ(partition.upper_bound.value(),
                std::vector<uint8_t>({'(', 0x19, 0x07, 0x00}));
    } else if (manifest_path == "2541e6b5-4923-4bd5-886d-72c6f7228400-m0.avro") {
      ASSERT_EQ(file.added_snapshot_id, 1679468743751242972);
      ASSERT_EQ(file.manifest_length, 7433);
      ASSERT_EQ(file.sequence_number, 2);
      ASSERT_EQ(file.min_sequence_number, 2);
      ASSERT_EQ(file.partitions.size(), 1);
      const auto& partition = file.partitions[0];
      ASSERT_EQ(partition.contains_null, false);
      ASSERT_EQ(partition.contains_nan.value(), false);
      ASSERT_EQ(partition.lower_bound.value(),
                std::vector<uint8_t>({0xd0, 0xd4, 0x06, 0x00}));
      ASSERT_EQ(partition.upper_bound.value(),
                std::vector<uint8_t>({0xd0, 0xd4, 0x06, 0x00}));
    } else if (manifest_path == "3118c801-d2e0-4df6-8c7a-7d4eaade32f8-m0.avro") {
      ASSERT_EQ(file.added_snapshot_id, 1579605567338877265);
      ASSERT_EQ(file.manifest_length, 7431);
      ASSERT_EQ(file.sequence_number, 1);
      ASSERT_EQ(file.min_sequence_number, 1);
      ASSERT_EQ(file.partitions.size(), 1);
      const auto& partition = file.partitions[0];
      ASSERT_EQ(partition.contains_null, false);
      ASSERT_EQ(partition.contains_nan.value(), false);
      ASSERT_EQ(partition.lower_bound.value(),
                std::vector<uint8_t>({0xb8, 0xd4, 0x06, 0x00}));
      ASSERT_EQ(partition.upper_bound.value(),
                std::vector<uint8_t>({0xb8, 0xd4, 0x06, 0x00}));
    } else {
      ASSERT_TRUE(false) << "Unexpected manifest file: " << manifest_path;
    }
    ASSERT_EQ(file.partition_spec_id, 0);
    ASSERT_EQ(file.content, ManifestFile::Content::kData);
    ASSERT_EQ(file.added_files_count, 1);
    ASSERT_EQ(file.existing_files_count, 0);
    ASSERT_EQ(file.deleted_files_count, 0);
    ASSERT_EQ(file.added_rows_count, 1);
    ASSERT_EQ(file.existing_rows_count, 0);
    ASSERT_EQ(file.deleted_rows_count, 0);
    ASSERT_EQ(file.key_metadata.empty(), true);
  }
}

}  // namespace iceberg
