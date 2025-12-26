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

#include <filesystem>

#include <arrow/filesystem/localfs.h>
#include <arrow/io/compressed.h>
#include <arrow/io/file.h>
#include <arrow/util/compression.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/file_io.h"
#include "iceberg/json_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"

namespace iceberg {

class MetadataIOTest : public TempFileTestBase {
 protected:
  void SetUp() override {
    TempFileTestBase::SetUp();
    io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    location_ = CreateTempDirectory();
    temp_filepath_ = std::format("{}/{}", location_, "metadata/00000-xxx.metadata.json");
    ASSERT_TRUE(
        std::filesystem::create_directories(std::format("{}/metadata", location_)));
  }

  TableMetadata PrepareMetadata() {
    std::vector<SchemaField> schema_fields;
    schema_fields.emplace_back(/*field_id=*/1, "x", iceberg::int64(),
                               /*optional=*/false);
    auto schema = std::make_shared<Schema>(std::move(schema_fields), /*schema_id=*/1);

    return TableMetadata{.format_version = 1,
                         .table_uuid = "1234567890",
                         .location = location_,
                         .last_sequence_number = 0,
                         .schemas = {schema},
                         .current_schema_id = 1,
                         .partition_specs = {PartitionSpec::Unpartitioned()},
                         .default_spec_id = 0,
                         .last_partition_id = 0,
                         .properties = TableProperties::FromMap({{"key", "value"}}),
                         .current_snapshot_id = 3051729675574597004,
                         .snapshots = {std::make_shared<Snapshot>(Snapshot{
                             .snapshot_id = 3051729675574597004,
                             .sequence_number = 0,
                             .timestamp_ms = TimePointMsFromUnixMs(1515100955770).value(),
                             .manifest_list = "s3://a/b/1.avro",
                             .summary = {{"operation", "append"}},
                         })},
                         .sort_orders = {SortOrder::Unsorted()},
                         .default_sort_order_id = 0,
                         .next_row_id = 0};
  }

  std::shared_ptr<iceberg::FileIO> io_;
  std::string location_;
  std::string temp_filepath_;
};

TEST_F(MetadataIOTest, ReadWriteMetadata) {
  TableMetadata metadata = PrepareMetadata();

  EXPECT_THAT(TableMetadataUtil::Write(*io_, temp_filepath_, metadata), IsOk());

  auto result = TableMetadataUtil::Read(*io_, temp_filepath_);
  EXPECT_THAT(result, IsOk());

  auto metadata_read = std::move(result.value());
  EXPECT_EQ(*metadata_read, metadata);
}

TEST_F(MetadataIOTest, ReadWriteCompressedMetadata) {
  TableMetadata metadata = PrepareMetadata();

  auto ret = ToJsonString(metadata);
  ASSERT_TRUE(ret.has_value());
  auto json_string = ret.value();

#define ARROW_ASSIGN_OR_THROW(var, expr)   \
  {                                        \
    auto result = expr;                    \
    ASSERT_TRUE(result.ok());              \
    var = std::move(result.ValueUnsafe()); \
  }

  auto file_path = CreateNewTempFilePathWithSuffix(".metadata.json.gz");
  std::shared_ptr<::arrow::io::FileOutputStream> out_stream;
  ARROW_ASSIGN_OR_THROW(out_stream, ::arrow::io::FileOutputStream::Open(file_path));

  std::shared_ptr<::arrow::util::Codec> gzip_codec;
  ARROW_ASSIGN_OR_THROW(gzip_codec,
                        ::arrow::util::Codec::Create(::arrow::Compression::GZIP));

  std::shared_ptr<::arrow::io::OutputStream> compressed_stream;
  ARROW_ASSIGN_OR_THROW(compressed_stream, ::arrow::io::CompressedOutputStream::Make(
                                               gzip_codec.get(), out_stream));
#undef ARROW_ASSIGN_OR_THROW

  ASSERT_TRUE(compressed_stream->Write(json_string).ok());
  ASSERT_TRUE(compressed_stream->Flush().ok());
  ASSERT_TRUE(compressed_stream->Close().ok());

  auto result = TableMetadataUtil::Read(*io_, file_path);
  EXPECT_THAT(result, IsOk());

  auto metadata_read = std::move(result.value());
  EXPECT_EQ(*metadata_read, metadata);
}

TEST_F(MetadataIOTest, WriteMetadataWithBase) {
  TableMetadata base = PrepareMetadata();

  {
    // Invalid base metadata_file_location, set version to 0
    TableMetadata new_metadata = PrepareMetadata();
    ICEBERG_UNWRAP_OR_FAIL(
        auto new_metadata_location,
        TableMetadataUtil::Write(*io_, &base, "invalid_location", new_metadata));
    EXPECT_THAT(new_metadata_location, testing::HasSubstr("/metadata/00000-"));
  }

  // Reset base metadata_file_location
  // base.metadata_file_location = temp_filepath_;

  {
    // Specify write location property
    TableMetadata new_metadata = PrepareMetadata();
    new_metadata.properties.Set(TableProperties::kWriteMetadataLocation, location_);
    ICEBERG_UNWRAP_OR_FAIL(
        auto new_metadata_location,
        TableMetadataUtil::Write(*io_, &base, temp_filepath_, new_metadata));
    EXPECT_THAT(new_metadata_location,
                testing::HasSubstr(std::format("{}/00001-", location_)));
  }

  {
    // Default write location
    TableMetadata new_metadata = PrepareMetadata();
    ICEBERG_UNWRAP_OR_FAIL(
        auto new_metadata_location,
        TableMetadataUtil::Write(*io_, &base, temp_filepath_, new_metadata));
    EXPECT_THAT(new_metadata_location,
                testing::HasSubstr(std::format("{}/metadata/00001-", location_)));
  }
}

TEST_F(MetadataIOTest, RemoveDeletedMetadataFiles) {
  TableMetadata base1 = PrepareMetadata();
  base1.properties.Set(TableProperties::kMetadataPreviousVersionsMax, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto base1_metadata_location,
                         TableMetadataUtil::Write(*io_, nullptr, "", base1));

  ICEBERG_UNWRAP_OR_FAIL(auto base2,
                         TableMetadataBuilder::BuildFrom(&base1)
                             ->SetPreviousMetadataLocation(base1_metadata_location)
                             .Build());
  ICEBERG_UNWRAP_OR_FAIL(
      auto base2_metadata_location,
      TableMetadataUtil::Write(*io_, &base1, base1_metadata_location, *base2));

  ICEBERG_UNWRAP_OR_FAIL(auto new_metadata,
                         TableMetadataBuilder::BuildFrom(base2.get())
                             ->SetPreviousMetadataLocation(base2_metadata_location)
                             .Build());
  ICEBERG_UNWRAP_OR_FAIL(auto new_metadata_location,
                         TableMetadataUtil::Write(
                             *io_, base2.get(), base2_metadata_location, *new_metadata));

  // The first metadata file should not be deleted
  new_metadata->properties.Set(TableProperties::kMetadataDeleteAfterCommitEnabled, false);
  TableMetadataUtil::DeleteRemovedMetadataFiles(*io_, base2.get(), *new_metadata);
  EXPECT_TRUE(std::filesystem::exists(base1_metadata_location));

  // The first metadata file should be deleted
  new_metadata->properties.Set(TableProperties::kMetadataDeleteAfterCommitEnabled, true);
  TableMetadataUtil::DeleteRemovedMetadataFiles(*io_, base2.get(), *new_metadata);
  EXPECT_FALSE(std::filesystem::exists(base1_metadata_location));
}

}  // namespace iceberg
