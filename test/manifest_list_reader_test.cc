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

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
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

  std::vector<ManifestFile> PrepareTestManifestList() {
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
      manifest_file.content = ManifestFile::Content::kData;
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

  auto expected_manifest_list = PrepareTestManifestList();
  ASSERT_EQ(read_result.value(), expected_manifest_list);
}

}  // namespace iceberg
