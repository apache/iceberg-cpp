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

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/data/equality_delete_writer.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/file_format.h"
#include "iceberg/file_reader.h"
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

  std::shared_ptr<::arrow::Array> CreateArray(const Schema& schema,
                                              std::string_view json) {
    ArrowSchema arrow_c_schema;
    ICEBERG_THROW_NOT_OK(ToArrowSchema(schema, &arrow_c_schema));
    auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

    return ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()),
                                              std::string(json))
        .ValueOrDie();
  }

  std::shared_ptr<::arrow::Array> CreateTestData() {
    return CreateArray(*schema_, R"([[1, "Alice"], [2, "Bob"], [3, "Charlie"]])");
  }

  std::shared_ptr<Schema> RowLineageSchema() {
    return std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(1, "id", int32()), MetadataColumns::kRowId,
        MetadataColumns::kLastUpdatedSequenceNumber});
  }

  std::unordered_map<std::string, std::string> FormatProperties(FileFormatType format) {
    if (format == FileFormatType::kParquet) {
      return {{"write.parquet.compression-codec", "uncompressed"}};
    }
    return {};
  }

  void WriteTestDataToWriter(DataWriter* writer) {
    auto test_data = CreateTestData();
    ArrowArray arrow_array;
    ASSERT_TRUE(::arrow::ExportArray(*test_data, &arrow_array).ok());
    ASSERT_THAT(writer->Write(&arrow_array), IsOk());
  }

  void VerifyNextBatch(Reader& reader, const Schema& schema,
                       std::string_view expected_json) {
    auto data = reader.Next();
    ASSERT_THAT(data, IsOk()) << "Reader.Next() failed: " << data.error().message;
    ASSERT_TRUE(data.value().has_value()) << "Reader.Next() returned no data";

    auto expected_array = CreateArray(schema, expected_json);
    auto actual_array =
        ::arrow::ImportArray(&data.value().value(), expected_array->type()).ValueOrDie();
    ASSERT_TRUE(actual_array->Equals(expected_array))
        << "Expected: " << expected_array->ToString()
        << "\nActual: " << actual_array->ToString();
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
      .properties = FormatProperties(format),
  };

  auto writer_result = DataWriter::Make(options);
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());
  ASSERT_NE(writer, nullptr);
}

TEST_P(DataWriterFormatTest, WriteRowLineage) {
  auto [format, path] = GetParam();
  auto schema = RowLineageSchema();
  DataWriterOptions options{
      .path = path,
      .schema = schema,
      .spec = partition_spec_,
      .partition = PartitionValues{},
      .format = format,
      .io = file_io_,
      .properties = FormatProperties(format),
  };

  ICEBERG_UNWRAP_OR_FAIL(auto writer, DataWriter::Make(options));

  auto array = CreateArray(*schema, R"([[1, null, null],
                                       [2, 777, 8],
                                       [3, null, null]])");
  ArrowArray arrow_array;
  ASSERT_TRUE(::arrow::ExportArray(*array, &arrow_array).ok());
  ASSERT_THAT(writer->Write(&arrow_array), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, ReaderFactoryRegistry::Open(format, {.path = path,
                                                        .io = file_io_,
                                                        .projection = schema,
                                                        .first_row_id = 100L,
                                                        .data_sequence_number = 7L}));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, *schema, R"([[1, 100, 7],
                                                               [2, 777, 8],
                                                               [3, 102, 7]])"));
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
  PositionDeleteWriterOptions MakeDeleteOptions(int64_t flush_threshold = 1000) {
    return PositionDeleteWriterOptions{
        .path = "test_deletes.parquet",
        .schema = schema_,
        .spec = partition_spec_,
        .partition = PartitionValues{},
        .format = FileFormatType::kParquet,
        .io = file_io_,
        .flush_threshold = flush_threshold,
        .properties = {{"write.parquet.compression-codec", "uncompressed"}},
    };
  }

  std::shared_ptr<::arrow::Array> CreatePositionDeleteData(
      std::string_view json =
          R"([["data_file_1.parquet", 0], ["data_file_1.parquet", 5], ["data_file_1.parquet", 10]])") {
    auto delete_schema = std::make_shared<Schema>(std::vector<SchemaField>{
        MetadataColumns::kDeleteFilePath, MetadataColumns::kDeleteFilePos});
    return CreateArray(*delete_schema, json);
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
  ASSERT_TRUE(data_file->referenced_data_file.has_value());
  EXPECT_EQ(data_file->referenced_data_file.value(), "data_file_1.parquet");
}

TEST_F(PositionDeleteWriterTest, WriteBatchDataForMultipleFiles) {
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  auto test_data = CreatePositionDeleteData(
      R"([["data_file_1.parquet", 0], ["data_file_2.parquet", 5]])");
  ArrowArray arrow_array;
  ASSERT_TRUE(::arrow::ExportArray(*test_data, &arrow_array).ok());
  ASSERT_THAT(writer->Write(&arrow_array), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());

  const auto& data_file = metadata_result.value().data_files[0];
  EXPECT_FALSE(data_file->referenced_data_file.has_value());
  EXPECT_FALSE(
      data_file->lower_bounds.contains(MetadataColumns::kDeleteFilePathColumnId));
  EXPECT_FALSE(data_file->lower_bounds.contains(MetadataColumns::kDeleteFilePosColumnId));
  EXPECT_FALSE(
      data_file->upper_bounds.contains(MetadataColumns::kDeleteFilePathColumnId));
  EXPECT_FALSE(data_file->upper_bounds.contains(MetadataColumns::kDeleteFilePosColumnId));
}

TEST_F(PositionDeleteWriterTest, WriteBatchThenDeleteTracksAllReferencedFiles) {
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  auto test_data = CreatePositionDeleteData(R"([["data_file_1.parquet", 0]])");
  ArrowArray arrow_array;
  ASSERT_TRUE(::arrow::ExportArray(*test_data, &arrow_array).ok());
  ASSERT_THAT(writer->Write(&arrow_array), IsOk());
  ASSERT_THAT(writer->WriteDelete("data_file_2.parquet", 5), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());
  EXPECT_FALSE(metadata_result.value().data_files[0]->referenced_data_file.has_value());
}

TEST_F(PositionDeleteWriterTest, WriteBatchRejectsNullFilePath) {
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  auto test_data = CreatePositionDeleteData(R"([[null, 0]])");
  ArrowArray arrow_array;
  ASSERT_TRUE(::arrow::ExportArray(*test_data, &arrow_array).ok());
  internal::ArrowArrayGuard array_guard(&arrow_array);

  auto result = writer->Write(&arrow_array);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArrowData));
  EXPECT_THAT(result,
              HasErrorMessage("Position delete file paths must not contain null values"));
}

TEST_F(PositionDeleteWriterTest, WriteBatchRejectsNullData) {
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  auto result = writer->Write(nullptr);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Position delete data must not be null"));
}

TEST_F(PositionDeleteWriterTest, WriteEmptyBatchDoesNotAddReferencedFiles) {
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  auto test_data = CreatePositionDeleteData("[]");
  ArrowArray arrow_array;
  ASSERT_TRUE(::arrow::ExportArray(*test_data, &arrow_array).ok());
  ASSERT_THAT(writer->Write(&arrow_array), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());
  EXPECT_FALSE(metadata_result.value().data_files[0]->referenced_data_file.has_value());
}

TEST_F(PositionDeleteWriterTest, AutoFlushOnThreshold) {
  // Use a small flush threshold to trigger automatic flush
  const int64_t flush_threshold = 5;
  auto writer_result = PositionDeleteWriter::Make(MakeDeleteOptions(flush_threshold));
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  // Write more deletes than the threshold to trigger auto-flush
  for (int64_t i = 0; i < 12; ++i) {
    ASSERT_THAT(writer->WriteDelete("data_file.parquet", i), IsOk());
  }

  // Length should be > 0 since auto-flush should have written data
  auto length_result = writer->Length();
  ASSERT_THAT(length_result, IsOk());
  EXPECT_GT(length_result.value(), 0);

  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());
  const auto& data_file = metadata_result.value().data_files[0];
  EXPECT_EQ(data_file->content, DataFile::Content::kPositionDeletes);
  EXPECT_GT(data_file->file_size_in_bytes, 0);
}

class EqualityDeleteWriterTest : public DataWriterTest {
 protected:
  EqualityDeleteWriterOptions MakeDeleteOptions(
      std::vector<int32_t> equality_field_ids = {1, 2},
      std::optional<int32_t> sort_order_id = std::nullopt) {
    return EqualityDeleteWriterOptions{
        .path = "test_eq_deletes.parquet",
        .schema = schema_,
        .spec = partition_spec_,
        .partition = PartitionValues{},
        .format = FileFormatType::kParquet,
        .io = file_io_,
        .equality_field_ids = std::move(equality_field_ids),
        .sort_order_id = sort_order_id,
        .properties = {{"write.parquet.compression-codec", "uncompressed"}},
    };
  }

  void WriteTestDataToEqualityWriter(EqualityDeleteWriter* writer) {
    auto test_data = CreateTestData();
    ArrowArray arrow_array;
    ASSERT_TRUE(::arrow::ExportArray(*test_data, &arrow_array).ok());
    ASSERT_THAT(writer->Write(&arrow_array), IsOk());
  }
};

TEST_F(EqualityDeleteWriterTest, WriteAndClose) {
  auto writer_result = EqualityDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  WriteTestDataToEqualityWriter(writer.get());

  auto length_result = writer->Length();
  ASSERT_THAT(length_result, IsOk());
  EXPECT_GT(length_result.value(), 0);

  ASSERT_THAT(writer->Close(), IsOk());
}

TEST_F(EqualityDeleteWriterTest, MetadataAfterClose) {
  auto writer_result = EqualityDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  WriteTestDataToEqualityWriter(writer.get());
  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());

  const auto& write_result = metadata_result.value();
  ASSERT_EQ(write_result.data_files.size(), 1);

  const auto& data_file = write_result.data_files[0];
  EXPECT_EQ(data_file->content, DataFile::Content::kEqualityDeletes);
  EXPECT_EQ(data_file->file_path, "test_eq_deletes.parquet");
  EXPECT_EQ(data_file->file_format, FileFormatType::kParquet);
  EXPECT_GT(data_file->file_size_in_bytes, 0);

  // Partition spec id must be set
  ASSERT_TRUE(data_file->partition_spec_id.has_value());
  EXPECT_EQ(data_file->partition_spec_id.value(), PartitionSpec::kInitialSpecId);

  // Equality field ids must be set
  ASSERT_EQ(data_file->equality_ids.size(), 2);
  EXPECT_EQ(data_file->equality_ids[0], 1);
  EXPECT_EQ(data_file->equality_ids[1], 2);
}

TEST_F(EqualityDeleteWriterTest, MetadataBeforeCloseReturnsError) {
  auto writer_result = EqualityDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(metadata_result,
              HasErrorMessage("Cannot get metadata before closing the writer"));
}

TEST_F(EqualityDeleteWriterTest, CloseIsIdempotent) {
  auto writer_result = EqualityDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  WriteTestDataToEqualityWriter(writer.get());

  ASSERT_THAT(writer->Close(), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
}

TEST_F(EqualityDeleteWriterTest, SortOrderIdInMetadata) {
  const int32_t sort_order_id = 7;
  auto writer_result = EqualityDeleteWriter::Make(MakeDeleteOptions({1}, sort_order_id));
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  WriteTestDataToEqualityWriter(writer.get());
  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());
  const auto& data_file = metadata_result.value().data_files[0];
  ASSERT_TRUE(data_file->sort_order_id.has_value());
  EXPECT_EQ(data_file->sort_order_id.value(), sort_order_id);
}

TEST_F(EqualityDeleteWriterTest, EqualityFieldIdsAccessor) {
  std::vector<int32_t> field_ids = {1, 2, 3};
  auto writer_result = EqualityDeleteWriter::Make(MakeDeleteOptions(field_ids));
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  auto ids = writer->equality_field_ids();
  ASSERT_EQ(ids.size(), 3);
  EXPECT_EQ(ids[0], 1);
  EXPECT_EQ(ids[1], 2);
  EXPECT_EQ(ids[2], 3);
}

TEST_F(EqualityDeleteWriterTest, WriteMultipleBatches) {
  auto writer_result = EqualityDeleteWriter::Make(MakeDeleteOptions());
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());

  WriteTestDataToEqualityWriter(writer.get());
  WriteTestDataToEqualityWriter(writer.get());
  ASSERT_THAT(writer->Close(), IsOk());

  auto metadata_result = writer->Metadata();
  ASSERT_THAT(metadata_result, IsOk());
  const auto& data_file = metadata_result.value().data_files[0];
  EXPECT_EQ(data_file->content, DataFile::Content::kEqualityDeletes);
  EXPECT_GT(data_file->file_size_in_bytes, 0);
}

}  // namespace iceberg
