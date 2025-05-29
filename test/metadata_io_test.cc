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
#include <arrow/io/compressed.h>
#include <arrow/io/file.h>
#include <arrow/util/compression.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/arrow/arrow_fs_file_io.h"
#include "iceberg/file_io.h"
#include "iceberg/json_internal.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "matchers.h"
#include "temp_file_test_base.h"

namespace iceberg {

class MetadataIOTest : public TempFileTestBase {
 protected:
  void SetUp() override {
    TempFileTestBase::SetUp();
    io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    temp_filepath_ = CreateNewTempFilePathWithSuffix(".metadata.json");
  }

  TableMetadata PrepareMetadata() {
    std::vector<SchemaField> schema_fields;
    schema_fields.emplace_back(/*field_id=*/1, "x", std::make_shared<LongType>(),
                               /*optional=*/false);
    auto schema = std::make_shared<Schema>(std::move(schema_fields), /*schema_id=*/1);

    TableMetadata metadata{
        .format_version = 1,
        .table_uuid = "1234567890",
        .location = "s3://bucket/path",
        .last_sequence_number = 0,
        .schemas = {schema},
        .current_schema_id = 1,
        .default_spec_id = 0,
        .last_partition_id = 0,
        .properties = {{"key", "value"}},
        .current_snapshot_id = 3051729675574597004,
        .snapshots = {std::make_shared<Snapshot>(Snapshot{
            .snapshot_id = 3051729675574597004,
            .sequence_number = 0,
            .timestamp_ms = TimePointMsFromUnixMs(1515100955770).value(),
            .manifest_list = "s3://a/b/1.avro",
            .summary = {{"operation", "append"}},
        })},
        .default_sort_order_id = 0,
        .next_row_id = 0};
    return metadata;
  }

  std::shared_ptr<iceberg::FileIO> io_;
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

  auto json = ToJson(metadata);
  auto ret = ToJsonString(json);
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

}  // namespace iceberg
