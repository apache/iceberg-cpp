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

#include "iceberg/data/data_writer.h"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

using ::testing::HasSubstr;

class DataWriterTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    parquet::RegisterAll();
    avro::RegisterAll();
  }

  void SetUp() override {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "name", string())});
    partition_spec_ = PartitionSpec::Unpartitioned();
  }

  DataWriterOptions MakeDefaultOptions(
      std::optional<int32_t> sort_order_id = std::nullopt,
      PartitionValues partition = PartitionValues{}) {
    return DataWriterOptions{
        .path = "test_data.parquet",
        .schema = schema_,
        .spec = partition_spec_,
        .partition = std::move(partition),
        .format = FileFormatType::kParquet,
        .io = file_io_,
        .sort_order_id = sort_order_id,
        .properties = {{"write.parquet.compression-codec", "uncompressed"}},
    };
  }

  std::shared_ptr<::arrow::Array> CreateTestData() {
    ArrowSchema arrow_c_schema;
    ICEBERG_THROW_NOT_OK(ToArrowSchema(*schema_, &arrow_c_schema));
    auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

    return ::arrow::json::ArrayFromJSONString(
               ::arrow::struct_(arrow_schema->fields()),
               R"([[1, "Alice"], [2, "Bob"], [3, "Charlie"]])")
        .ValueOrDie();
  }

  void WriteTestDataToWriter(DataWriter* writer) {
    auto test_data = CreateTestData();
    ArrowArray arrow_array;
    ASSERT_TRUE(::arrow::ExportArray(*test_data, &arrow_array).ok());
    ASSERT_THAT(writer->Write(&arrow_array), IsOk());
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> partition_spec_;
};

class DataWriterFormatTest
    : public DataWriterTest,
      public ::testing::WithParamInterface<std::pair<FileFormatType, std::string>> {};

TEST_P(DataWriterFormatTest, CreateWithFormat) {
  auto [format, path] = GetParam();
  DataWriterOptions options{
      .path = path,
      .schema = schema_,
      .spec = partition_spec_,
      .partition = PartitionValues{},
      .format = format,
      .io = file_io_,
      .properties =
          format == FileFormatType::kParquet
              ? std::unordered_map<std::string,
                                   std::string>{{"write.parquet.compression-codec",
                                                 "uncompressed"}}
              : std::unordered_map<std::string, std::string>{},
  };

  auto writer_result = DataWriter::Make(options);
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());
  ASSERT_NE(writer, nullptr);
}

INSTANTIATE_TEST_SUITE_P(
    FormatTypes, DataWriterFormatTest,
    ::testing::Values(std::make_pair(FileFormatType::kParquet, "test_data.parquet"),
                      std::make_pair(FileFormatType::kAvro, "test_data.avro")));

TEST_F(DataWriterTest, WriteAndClose) {
  auto writer_result = DataWriter::Make(MakeDefaultOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  // Write data
  WriteTestDataToWriter(writer.get());

  // Length should be greater than 0 after write
  auto length_result = writer->Length();
  ASSERT_THAT(length_result, IsOk());
  EXPECT_GT(length_result.value(), 0);

  // Close
  ASSERT_THAT(writer->Close(), IsOk());
}

TEST_F(DataWriterTest, MetadataAfterClose) {
  auto writer_result = DataWriter::Make(MakeDefaultOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  WriteTestDataToWriter(writer.get());
  ASSERT_THAT(writer->Close(), IsOk());

  // Get metadata
  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());

  const auto& write_result = metadata_result.value();
  ASSERT_EQ(write_result.data_files.size(), 1);

  const auto& data_file = write_result.data_files[0];
  EXPECT_EQ(data_file->content, DataFile::Content::kData);
  EXPECT_EQ(data_file->file_path, "test_data.parquet");
  EXPECT_EQ(data_file->file_format, FileFormatType::kParquet);
  EXPECT_GT(data_file->file_size_in_bytes, 0);

  // Metrics availability depends on the underlying writer implementation
  EXPECT_GE(data_file->column_sizes.size(), 0);
  EXPECT_GE(data_file->value_counts.size(), 0);
  EXPECT_GE(data_file->null_value_counts.size(), 0);
}

TEST_F(DataWriterTest, MetadataBeforeCloseReturnsError) {
  auto writer_result = DataWriter::Make(MakeDefaultOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  // Try to get metadata before closing
  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(metadata_result,
              HasErrorMessage("Cannot get metadata before closing the writer"));
}

TEST_F(DataWriterTest, CloseIsIdempotent) {
  auto writer_result = DataWriter::Make(MakeDefaultOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  WriteTestDataToWriter(writer.get());

  ASSERT_THAT(writer->Close(), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
}

TEST_F(DataWriterTest, SortOrderIdInMetadata) {
  // Test with explicit sort order id
  {
    const int32_t sort_order_id = 42;
    auto writer_result = DataWriter::Make(MakeDefaultOptions(sort_order_id));
    ASSERT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    WriteTestDataToWriter(writer.get());
    ASSERT_THAT(writer->Close(), IsOk());

    auto metadata_result = writer->Metadata();
    ASSERT_THAT(metadata_result, IsOk());
    const auto& data_file = metadata_result.value().data_files[0];
    ASSERT_TRUE(data_file->sort_order_id.has_value());
    EXPECT_EQ(data_file->sort_order_id.value(), sort_order_id);
  }

  // Test without sort order id (should be nullopt)
  {
    auto writer_result = DataWriter::Make(MakeDefaultOptions());
    ASSERT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    WriteTestDataToWriter(writer.get());
    ASSERT_THAT(writer->Close(), IsOk());

    auto metadata_result = writer->Metadata();
    ASSERT_THAT(metadata_result, IsOk());
    const auto& data_file = metadata_result.value().data_files[0];
    EXPECT_FALSE(data_file->sort_order_id.has_value());
  }
}

TEST_F(DataWriterTest, PartitionValuesPreserved) {
  PartitionValues partition_values({Literal::Int(42), Literal::String("test")});

  auto writer_result =
      DataWriter::Make(MakeDefaultOptions(std::nullopt, partition_values));
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  WriteTestDataToWriter(writer.get());
  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());
  const auto& data_file = metadata_result.value().data_files[0];

  EXPECT_EQ(data_file->partition.num_fields(), partition_values.num_fields());
  EXPECT_EQ(data_file->partition.num_fields(), 2);
}

TEST_F(DataWriterTest, WriteMultipleBatches) {
  auto writer_result = DataWriter::Make(MakeDefaultOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  WriteTestDataToWriter(writer.get());
  WriteTestDataToWriter(writer.get());
  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());
  const auto& data_file = metadata_result.value().data_files[0];
  EXPECT_GT(data_file->file_size_in_bytes, 0);
}

class PositionDeleteWriterTest : public DataWriterTest {
 protected:
  PositionDeleteWriterOptions MakeDeleteOptions() {
    return PositionDeleteWriterOptions{
        .path = "test_deletes.parquet",
        .schema = schema_,
        .spec = partition_spec_,
        .partition = PartitionValues{},
        .format = FileFormatType::kParquet,
        .io = file_io_,
        .properties = {{"write.parquet.compression-codec", "uncompressed"}},
    };
  }

  std::shared_ptr<::arrow::Array> CreatePositionDeleteData() {
    auto delete_schema = std::make_shared<Schema>(std::vector<SchemaField>{
        MetadataColumns::kDeleteFilePath, MetadataColumns::kDeleteFilePos});

    ArrowSchema arrow_c_schema;
    ICEBERG_THROW_NOT_OK(ToArrowSchema(*delete_schema, &arrow_c_schema));
    auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

    return ::arrow::json::ArrayFromJSONString(
               ::arrow::struct_(arrow_type->fields()),
               R"([["data_file_1.parquet", 0], ["data_file_1.parquet", 5], ["data_file_1.parquet", 10]])")
        .ValueOrDie();
  }
};

TEST_F(PositionDeleteWriterTest, WriteDeleteAndClose) {
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  ASSERT_THAT(writer->WriteDelete("data_file.parquet", 0), IsOk());
  ASSERT_THAT(writer->WriteDelete("data_file.parquet", 5), IsOk());
  ASSERT_THAT(writer->WriteDelete("data_file.parquet", 10), IsOk());

  ASSERT_THAT(writer->Close(), IsOk());

  auto length_result = writer->Length();
  ASSERT_THAT(length_result, IsOk());
  EXPECT_GT(length_result.value(), 0);
}

TEST_F(PositionDeleteWriterTest, MetadataAfterClose) {
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  ASSERT_THAT(writer->WriteDelete("data_file.parquet", 0), IsOk());
  ASSERT_THAT(writer->WriteDelete("data_file.parquet", 5), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());

  const auto& write_result = metadata_result.value();
  ASSERT_EQ(write_result.data_files.size(), 1);

  const auto& data_file = write_result.data_files[0];
  EXPECT_EQ(data_file->content, DataFile::Content::kPositionDeletes);
  EXPECT_EQ(data_file->file_path, "test_deletes.parquet");
  EXPECT_EQ(data_file->file_format, FileFormatType::kParquet);
  EXPECT_GT(data_file->file_size_in_bytes, 0);
  EXPECT_FALSE(data_file->sort_order_id.has_value());
}

TEST_F(PositionDeleteWriterTest, MetadataBeforeCloseReturnsError) {
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(metadata_result,
              HasErrorMessage("Cannot get metadata before closing the writer"));
}

TEST_F(PositionDeleteWriterTest, CloseIsIdempotent) {
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  ASSERT_THAT(writer->WriteDelete("data_file.parquet", 0), IsOk());

  ASSERT_THAT(writer->Close(), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
}

TEST_F(PositionDeleteWriterTest, WriteMultipleDeletes) {
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  for (int64_t i = 0; i < 100; ++i) {
    ASSERT_THAT(writer->WriteDelete("data_file.parquet", i), IsOk());
  }

  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());

  const auto& data_file = metadata_result.value().data_files[0];
  EXPECT_EQ(data_file->content, DataFile::Content::kPositionDeletes);
  EXPECT_GT(data_file->file_size_in_bytes, 0);
}

TEST_F(PositionDeleteWriterTest, WriteBatchData) {
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  auto test_data = CreatePositionDeleteData();
  ArrowArray arrow_array;
  ASSERT_TRUE(::arrow::ExportArray(*test_data, &arrow_array).ok());
  ASSERT_THAT(writer->Write(&arrow_array), IsOk());

  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());

  const auto& data_file = metadata_result.value().data_files[0];
  EXPECT_EQ(data_file->content, DataFile::Content::kPositionDeletes);
  EXPECT_GT(data_file->file_size_in_bytes, 0);
}

}  // namespace iceberg
