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

#include <sstream>

#include <arrow/array.h>
#include <arrow/array/array_base.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/avro/avro_writer.h"
#include "iceberg/file_reader.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg::avro {

class AvroReaderTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { RegisterAll(); }

  void SetUp() override {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
    temp_avro_file_ = "avro_reader_test.avro";
  }

  bool skip_datum_{true};

  void CreateSimpleAvroFile() {
    // Create simple avro file using the writer API instead of direct Avro library
    auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
        SchemaField::MakeOptional(2, "name", std::make_shared<StringType>())});

    ArrowSchema arrow_c_schema;
    ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
    auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

    auto array = ::arrow::json::ArrayFromJSONString(
                     ::arrow::struct_(arrow_schema->fields()),
                     R"([[1, "Alice"], [2, "Bob"], [3, "Charlie"]])")
                     .ValueOrDie();

    struct ArrowArray arrow_array;
    auto export_result = ::arrow::ExportArray(*array, &arrow_array);
    ASSERT_TRUE(export_result.ok());

    auto writer_result =
        WriterFactoryRegistry::Open(FileFormatType::kAvro, {
                                                               .path = temp_avro_file_,
                                                               .schema = schema,
                                                               .io = file_io_,
                                                           });
    ASSERT_TRUE(writer_result.has_value());
    auto writer = std::move(writer_result.value());
    ASSERT_THAT(writer->Write(&arrow_array), IsOk());
    ASSERT_THAT(writer->Close(), IsOk());
  }

  void VerifyNextBatch(Reader& reader, std::string_view expected_json) {
    // Boilerplate to get Arrow schema
    auto schema_result = reader.Schema();
    ASSERT_THAT(schema_result, IsOk());
    auto arrow_c_schema = std::move(schema_result.value());
    auto import_schema_result = ::arrow::ImportType(&arrow_c_schema);
    auto arrow_schema = import_schema_result.ValueOrDie();

    // Boilerplate to get Arrow array
    auto data = reader.Next();
    ASSERT_THAT(data, IsOk());
    ASSERT_TRUE(data.value().has_value());
    auto arrow_c_array = data.value().value();
    auto data_result = ::arrow::ImportArray(&arrow_c_array, arrow_schema);
    auto arrow_array = data_result.ValueOrDie();

    // Verify data
    auto expected_array =
        ::arrow::json::ArrayFromJSONString(arrow_schema, expected_json).ValueOrDie();
    ASSERT_TRUE(arrow_array->Equals(*expected_array));
  }

  void VerifyExhausted(Reader& reader) {
    auto data = reader.Next();
    ASSERT_THAT(data, IsOk());
    ASSERT_FALSE(data.value().has_value());
  }

  void WriteAndVerify(std::shared_ptr<Schema> schema,
                      const std::string& expected_string) {
    ArrowSchema arrow_c_schema;
    ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());

    auto arrow_schema_result = ::arrow::ImportType(&arrow_c_schema);
    ASSERT_TRUE(arrow_schema_result.ok());
    auto arrow_schema = arrow_schema_result.ValueOrDie();

    auto array_result = ::arrow::json::ArrayFromJSONString(arrow_schema, expected_string);
    ASSERT_TRUE(array_result.ok());
    auto array = array_result.ValueOrDie();

    struct ArrowArray arrow_array;
    auto export_result = ::arrow::ExportArray(*array, &arrow_array);
    ASSERT_TRUE(export_result.ok());

    std::unordered_map<std::string, std::string> metadata = {{"k1", "v1"}, {"k2", "v2"}};

    auto writer_result =
        WriterFactoryRegistry::Open(FileFormatType::kAvro, {.path = temp_avro_file_,
                                                            .schema = schema,
                                                            .io = file_io_,
                                                            .metadata = metadata});
    ASSERT_TRUE(writer_result.has_value());
    auto writer = std::move(writer_result.value());
    ASSERT_THAT(writer->Write(&arrow_array), IsOk());
    ASSERT_THAT(writer->Close(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto written_length, writer->length());

    auto reader_properties = ReaderProperties::default_properties();
    reader_properties->Set(ReaderProperties::kAvroSkipDatum, skip_datum_);

    auto reader_result = ReaderFactoryRegistry::Open(
        FileFormatType::kAvro, {.path = temp_avro_file_,
                                .length = written_length,
                                .io = file_io_,
                                .projection = schema,
                                .properties = std::move(reader_properties)});
    ASSERT_THAT(reader_result, IsOk());
    auto reader = std::move(reader_result.value());
    ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, expected_string));
    ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));

    auto metadata_result = reader->Metadata();
    ASSERT_THAT(metadata_result, IsOk());
    auto read_metadata = std::move(metadata_result.value());
    for (const auto& [key, value] : metadata) {
      auto it = read_metadata.find(key);
      ASSERT_NE(it, read_metadata.end());
      ASSERT_EQ(it->second, value);
    }
  }

  std::shared_ptr<FileIO> file_io_;
  std::string temp_avro_file_;
};

TEST_F(AvroReaderTest, ReadTwoFields) {
  CreateSimpleAvroFile();
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeOptional(2, "name", std::make_shared<StringType>())});

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kAvro,
      {.path = temp_avro_file_, .io = file_io_, .projection = schema});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(
      VerifyNextBatch(*reader, R"([[1, "Alice"], [2, "Bob"], [3, "Charlie"]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(AvroReaderTest, ReadReorderedFieldsWithNulls) {
  CreateSimpleAvroFile();
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(2, "name", std::make_shared<StringType>()),
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeOptional(3, "score", std::make_shared<DoubleType>())});

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kAvro,
      {.path = temp_avro_file_, .io = file_io_, .projection = schema});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(
      *reader, R"([["Alice", 1, null], ["Bob", 2, null], ["Charlie", 3, null]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(AvroReaderTest, ReadWithBatchSize) {
  CreateSimpleAvroFile();
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>())});

  auto reader_properties = ReaderProperties::default_properties();
  reader_properties->Set(ReaderProperties::kBatchSize, int64_t{2});

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kAvro, {.path = temp_avro_file_,
                              .io = file_io_,
                              .projection = schema,
                              .properties = std::move(reader_properties)});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[1], [2]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[3]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(AvroReaderTest, BufferSizeConfiguration) {
  // Test default buffer size
  auto properties1 = ReaderProperties::default_properties();
  ASSERT_EQ(properties1->Get(ReaderProperties::kAvroBufferSize), 1024 * 1024);

  // Test setting custom buffer size
  auto properties2 = ReaderProperties::default_properties();
  constexpr int64_t kCustomBufferSize = 2 * 1024 * 1024;  // 2MB
  properties2->Set(ReaderProperties::kAvroBufferSize, kCustomBufferSize);
  ASSERT_EQ(properties2->Get(ReaderProperties::kAvroBufferSize), kCustomBufferSize);

  // Test setting via FromMap
  std::unordered_map<std::string, std::string> config_map = {
      {"read.avro.buffer-size", "4194304"}  // 4MB
  };
  auto properties3 = ReaderProperties::FromMap(config_map);
  ASSERT_EQ(properties3->Get(ReaderProperties::kAvroBufferSize), 4194304);

  // Test that unset returns to default
  properties2->Unset(ReaderProperties::kAvroBufferSize);
  ASSERT_EQ(properties2->Get(ReaderProperties::kAvroBufferSize), 1024 * 1024);
}

// Parameterized test fixture for testing both DirectDecoder and GenericDatum modes
class AvroReaderParameterizedTest : public AvroReaderTest,
                                    public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    AvroReaderTest::SetUp();
    skip_datum_ = GetParam();
  }
};

TEST_P(AvroReaderParameterizedTest, AvroWriterBasicType) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "name", std::make_shared<StringType>())});

  std::string expected_string = R"([["Hello"], ["世界"], ["nanoarrow"]])";

  WriteAndVerify(schema, expected_string);
}

TEST_P(AvroReaderParameterizedTest, AvroWriterNestedType) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeRequired(
          2, "info",
          std::make_shared<iceberg::StructType>(std::vector<SchemaField>{
              SchemaField::MakeRequired(3, "name", std::make_shared<StringType>()),
              SchemaField::MakeRequired(4, "age", std::make_shared<IntType>())}))});

  std::string expected_string =
      R"([[1, ["Alice", 25]], [2, ["Bob", 30]], [3, ["Ivy", 35]]])";

  WriteAndVerify(schema, expected_string);
}

TEST_P(AvroReaderParameterizedTest, AllPrimitiveTypes) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "bool_col", std::make_shared<BooleanType>()),
      SchemaField::MakeRequired(2, "int_col", std::make_shared<IntType>()),
      SchemaField::MakeRequired(3, "long_col", std::make_shared<LongType>()),
      SchemaField::MakeRequired(4, "float_col", std::make_shared<FloatType>()),
      SchemaField::MakeRequired(5, "double_col", std::make_shared<DoubleType>()),
      SchemaField::MakeRequired(6, "string_col", std::make_shared<StringType>()),
      SchemaField::MakeRequired(7, "binary_col", std::make_shared<BinaryType>())});

  std::string expected_string = R"([
    [true, 42, 1234567890, 3.14, 2.71828, "test", "AQID"],
    [false, -100, -9876543210, -1.5, 0.0, "hello", "BAUG"]
  ])";

  WriteAndVerify(schema, expected_string);
}

// Skipping DecimalType test - requires specific decimal encoding in JSON

TEST_P(AvroReaderParameterizedTest, DateTimeTypes) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "date_col", std::make_shared<DateType>()),
      SchemaField::MakeRequired(2, "time_col", std::make_shared<TimeType>()),
      SchemaField::MakeRequired(3, "timestamp_col", std::make_shared<TimestampType>())});

  // Dates as days since epoch, time/timestamps as microseconds
  std::string expected_string = R"([
    [18628, 43200000000, 1640995200000000],
    [18629, 86399000000, 1641081599000000]
  ])";

  WriteAndVerify(schema, expected_string);
}

TEST_P(AvroReaderParameterizedTest, NestedStruct) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeRequired(
          2, "person",
          std::make_shared<iceberg::StructType>(std::vector<SchemaField>{
              SchemaField::MakeRequired(3, "name", std::make_shared<StringType>()),
              SchemaField::MakeRequired(4, "age", std::make_shared<IntType>()),
              SchemaField::MakeOptional(
                  5, "address",
                  std::make_shared<iceberg::StructType>(std::vector<SchemaField>{
                      SchemaField::MakeRequired(6, "street",
                                                std::make_shared<StringType>()),
                      SchemaField::MakeRequired(7, "city",
                                                std::make_shared<StringType>())}))}))});

  std::string expected_string = R"([
    [1, ["Alice", 30, ["123 Main St", "NYC"]]],
    [2, ["Bob", 25, ["456 Oak Ave", "LA"]]]
  ])";

  WriteAndVerify(schema, expected_string);
}

TEST_P(AvroReaderParameterizedTest, ListType) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeRequired(2, "tags",
                                std::make_shared<ListType>(SchemaField::MakeRequired(
                                    3, "element", std::make_shared<StringType>())))});

  std::string expected_string = R"([
    [1, ["tag1", "tag2", "tag3"]],
    [2, ["foo", "bar"]],
    [3, []]
  ])";

  WriteAndVerify(schema, expected_string);
}

TEST_P(AvroReaderParameterizedTest, MapType) {
  auto schema = std::make_shared<iceberg::Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(
          1, "properties",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(2, "key", std::make_shared<StringType>()),
              SchemaField::MakeRequired(3, "value", std::make_shared<IntType>())))});

  std::string expected_string = R"([
    [[["key1", 100], ["key2", 200]]],
    [[["a", 1], ["b", 2], ["c", 3]]]
  ])";

  WriteAndVerify(schema, expected_string);
}

TEST_P(AvroReaderParameterizedTest, MapTypeWithNonStringKey) {
  auto schema = std::make_shared<iceberg::Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(
          1, "int_map",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(2, "key", std::make_shared<IntType>()),
              SchemaField::MakeRequired(3, "value", std::make_shared<StringType>())))});

  std::string expected_string = R"([
    [[[1, "one"], [2, "two"], [3, "three"]]],
    [[[10, "ten"], [20, "twenty"]]]
  ])";

  WriteAndVerify(schema, expected_string);
}

TEST_F(AvroReaderTest, ProjectionSubsetAndReorder) {
  // Write file with full schema
  auto write_schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeRequired(2, "name", std::make_shared<StringType>()),
      SchemaField::MakeRequired(3, "age", std::make_shared<IntType>()),
      SchemaField::MakeRequired(4, "city", std::make_shared<StringType>())});

  std::string write_data = R"([
    [1, "Alice", 25, "NYC"],
    [2, "Bob", 30, "SF"],
    [3, "Charlie", 35, "LA"]
  ])";

  // Write with full schema
  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(*write_schema, &arrow_c_schema), IsOk());
  auto arrow_schema_result = ::arrow::ImportType(&arrow_c_schema);
  ASSERT_TRUE(arrow_schema_result.ok());
  auto arrow_schema = arrow_schema_result.ValueOrDie();

  auto array_result = ::arrow::json::ArrayFromJSONString(arrow_schema, write_data);
  ASSERT_TRUE(array_result.ok());
  auto array = array_result.ValueOrDie();

  struct ArrowArray arrow_array;
  auto export_result = ::arrow::ExportArray(*array, &arrow_array);
  ASSERT_TRUE(export_result.ok());

  std::unordered_map<std::string, std::string> metadata = {{"k1", "v1"}};
  auto writer_result =
      WriterFactoryRegistry::Open(FileFormatType::kAvro, {.path = temp_avro_file_,
                                                          .schema = write_schema,
                                                          .io = file_io_,
                                                          .metadata = metadata});
  ASSERT_TRUE(writer_result.has_value());
  auto writer = std::move(writer_result.value());
  ASSERT_THAT(writer->Write(&arrow_array), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto written_length, writer->length());

  // Read with projected schema: subset of columns (city, id) in different order
  auto read_schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(4, "city", std::make_shared<StringType>()),
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>())});

  auto reader_result =
      ReaderFactoryRegistry::Open(FileFormatType::kAvro, {.path = temp_avro_file_,
                                                          .length = written_length,
                                                          .io = file_io_,
                                                          .projection = read_schema});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  // Verify reordered subset
  ASSERT_NO_FATAL_FAILURE(
      VerifyNextBatch(*reader, R"([["NYC", 1], ["SF", 2], ["LA", 3]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_P(AvroReaderParameterizedTest, ComplexNestedTypes) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeRequired(2, "nested_list",
                                std::make_shared<ListType>(SchemaField::MakeRequired(
                                    3, "element",
                                    std::make_shared<ListType>(SchemaField::MakeRequired(
                                        4, "element", std::make_shared<IntType>())))))});

  std::string expected_string = R"([
    [1, [[1, 2], [3, 4]]],
    [2, [[5], [6, 7, 8]]]
  ])";

  WriteAndVerify(schema, expected_string);
}

TEST_P(AvroReaderParameterizedTest, OptionalFieldsWithNulls) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeOptional(2, "name", std::make_shared<StringType>()),
      SchemaField::MakeOptional(3, "age", std::make_shared<IntType>())});

  std::string expected_string = R"([
    [1, "Alice", 30],
    [2, null, 25],
    [3, "Charlie", null],
    [4, null, null]
  ])";

  WriteAndVerify(schema, expected_string);
}

TEST_P(AvroReaderParameterizedTest, LargeDataset) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<LongType>()),
      SchemaField::MakeRequired(2, "value", std::make_shared<DoubleType>())});

  // Generate large dataset JSON
  std::ostringstream json;
  json << "[";
  for (int i = 0; i < 1000; ++i) {
    if (i > 0) json << ", ";
    json << "[" << i << ", " << (i * 1.5) << "]";
  }
  json << "]";

  WriteAndVerify(schema, json.str());
}

TEST_P(AvroReaderParameterizedTest, EmptyCollections) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeRequired(2, "list_col",
                                std::make_shared<ListType>(SchemaField::MakeRequired(
                                    3, "element", std::make_shared<IntType>())))});

  std::string expected_string = R"([
    [1, []],
    [2, [10, 20, 30]]
  ])";

  WriteAndVerify(schema, expected_string);
}

TEST_P(AvroReaderParameterizedTest, ReadFilePathColumn) {
  temp_avro_file_ = "avro_metadata_path.avro";
  CreateSimpleAvroFile();

  // Create schema with _path metadata column
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      MetadataColumns::kFilePath,
  });

  const std::string expected_string = R"([
    [1, "avro_metadata_path.avro"],
    [2, "avro_metadata_path.avro"],
    [3, "avro_metadata_path.avro"]
  ])";

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, ReaderFactoryRegistry::Open(
                       FileFormatType::kAvro,
                       {.path = temp_avro_file_, .io = file_io_, .projection = schema}));

  VerifyNextBatch(*reader, expected_string);
}

TEST_P(AvroReaderParameterizedTest, ReadRowPositionColumn) {
  temp_avro_file_ = "avro_metadata_pos.avro";
  CreateSimpleAvroFile();

  // Create schema with _pos metadata column
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      MetadataColumns::kRowPosition,
  });

  const std::string expected_string = R"([[1, 0], [2, 1], [3, 2]])";

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, ReaderFactoryRegistry::Open(
                       FileFormatType::kAvro,
                       {.path = temp_avro_file_, .io = file_io_, .projection = schema}));
}

TEST_P(AvroReaderParameterizedTest, ReadBothMetadataColumns) {
  temp_avro_file_ = "avro_metadata_path_pos.avro";
  CreateSimpleAvroFile();

  // Create schema with both _file and _pos metadata columns
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      MetadataColumns::kFilePath,
      MetadataColumns::kRowPosition,
  });

  const std::string expected_string = R"([
    [1, "avro_metadata_path_pos.avro", 0],
    [2, "avro_metadata_path_pos.avro", 1],
    [3, "avro_metadata_path_pos.avro", 2]
  ])";

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, ReaderFactoryRegistry::Open(
                       FileFormatType::kAvro,
                       {.path = temp_avro_file_, .io = file_io_, .projection = schema}));

  VerifyNextBatch(*reader, expected_string);
}

TEST_P(AvroReaderParameterizedTest, ReadMetadataOnlyProjection) {
  temp_avro_file_ = "avro_metadata_only.avro";
  CreateSimpleAvroFile();

  // Create schema with only metadata columns (no data columns)
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      MetadataColumns::kFilePath,
      MetadataColumns::kRowPosition,
  });

  const std::string expected_string = R"([
    ["avro_metadata_only.avro", 0],
    ["avro_metadata_only.avro", 1],
    ["avro_metadata_only.avro", 2]
  ])";

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, ReaderFactoryRegistry::Open(
                       FileFormatType::kAvro,
                       {.path = temp_avro_file_, .io = file_io_, .projection = schema}));

  VerifyNextBatch(*reader, expected_string);
}

TEST_P(AvroReaderParameterizedTest, SplitWithRowPositionNotSupported) {
  CreateSimpleAvroFile();

  // Create schema with _pos metadata column
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      MetadataColumns::kRowPosition,
  });

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kAvro, {.path = temp_avro_file_,
                              .split = Split{.offset = 100, .length = 200},
                              .io = file_io_,
                              .projection = schema});

  ASSERT_THAT(reader_result, IsError(ErrorKind::kNotSupported));
  EXPECT_THAT(reader_result,
              HasErrorMessage("'_pos' metadata column with split is not supported"));
}

INSTANTIATE_TEST_SUITE_P(DirectDecoderModes, AvroReaderParameterizedTest,
                         ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                           return info.param ? "DirectDecoder" : "GenericDatum";
                         });

// Parameterized test fixture for testing both direct encoder and GenericDatum modes
class AvroWriterTest : public ::testing::Test,
                       public ::testing::WithParamInterface<bool> {
 protected:
  static void SetUpTestSuite() { RegisterAll(); }

  void SetUp() override {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
    temp_avro_file_ = "avro_writer_test.avro";
    skip_datum_ = GetParam();
  }

  void WriteAvroFile(std::shared_ptr<Schema> schema, const std::string& json_data) {
    ArrowSchema arrow_c_schema;
    ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());

    auto arrow_schema_result = ::arrow::ImportType(&arrow_c_schema);
    ASSERT_TRUE(arrow_schema_result.ok());
    auto arrow_schema = arrow_schema_result.ValueOrDie();

    auto array_result = ::arrow::json::ArrayFromJSONString(arrow_schema, json_data);
    ASSERT_TRUE(array_result.ok());
    auto array = array_result.ValueOrDie();

    struct ArrowArray arrow_array;
    auto export_result = ::arrow::ExportArray(*array, &arrow_array);
    ASSERT_TRUE(export_result.ok());

    std::unordered_map<std::string, std::string> metadata = {
        {"writer_test", "direct_encoder"}};

    auto writer_properties = WriterProperties::default_properties();
    writer_properties->Set(WriterProperties::kAvroSkipDatum, skip_datum_);

    auto writer_result = WriterFactoryRegistry::Open(
        FileFormatType::kAvro, {.path = temp_avro_file_,
                                .schema = schema,
                                .io = file_io_,
                                .metadata = metadata,
                                .properties = std::move(writer_properties)});
    ASSERT_TRUE(writer_result.has_value());
    writer_ = std::move(writer_result.value());
    ASSERT_THAT(writer_->Write(&arrow_array), IsOk());
    ASSERT_THAT(writer_->Close(), IsOk());
    write_schema_ = schema;
  }

  void VerifyNextBatch(Reader& reader, std::string_view expected_json) {
    // Boilerplate to get Arrow schema
    auto schema_result = reader.Schema();
    ASSERT_THAT(schema_result, IsOk());
    auto arrow_c_schema = std::move(schema_result.value());
    auto import_schema_result = ::arrow::ImportType(&arrow_c_schema);
    auto arrow_schema = import_schema_result.ValueOrDie();

    // Boilerplate to get Arrow array
    auto data = reader.Next();
    ASSERT_THAT(data, IsOk());
    ASSERT_TRUE(data.value().has_value());
    auto arrow_c_array = data.value().value();
    auto data_result = ::arrow::ImportArray(&arrow_c_array, arrow_schema);
    auto arrow_array = data_result.ValueOrDie();

    // Verify data
    auto expected_array =
        ::arrow::json::ArrayFromJSONString(arrow_schema, expected_json).ValueOrDie();
    ASSERT_TRUE(arrow_array->Equals(*expected_array));
  }

  void VerifyExhausted(Reader& reader) {
    auto data = reader.Next();
    ASSERT_THAT(data, IsOk());
    ASSERT_FALSE(data.value().has_value());
  }

  void VerifyWrittenData(const std::string& expected_json) {
    ICEBERG_UNWRAP_OR_FAIL(auto written_length, writer_->length());

    auto reader_result =
        ReaderFactoryRegistry::Open(FileFormatType::kAvro, {.path = temp_avro_file_,
                                                            .length = written_length,
                                                            .io = file_io_,
                                                            .projection = write_schema_});
    ASSERT_THAT(reader_result, IsOk());
    auto reader = std::move(reader_result.value());
    ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, expected_json));
    ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
  }

  std::shared_ptr<FileIO> file_io_;
  std::string temp_avro_file_;
  bool skip_datum_{true};
  std::shared_ptr<Schema> write_schema_;
  std::unique_ptr<Writer> writer_;
};

TEST_P(AvroWriterTest, WritePrimitiveTypes) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "bool_col", std::make_shared<BooleanType>()),
      SchemaField::MakeRequired(2, "int_col", std::make_shared<IntType>()),
      SchemaField::MakeRequired(3, "long_col", std::make_shared<LongType>()),
      SchemaField::MakeRequired(4, "float_col", std::make_shared<FloatType>()),
      SchemaField::MakeRequired(5, "double_col", std::make_shared<DoubleType>()),
      SchemaField::MakeRequired(6, "string_col", std::make_shared<StringType>())});

  std::string test_data = R"([
    [true, 42, 1234567890, 3.14, 2.71828, "hello"],
    [false, -100, -9876543210, -1.5, 0.0, "world"]
  ])";

  WriteAvroFile(schema, test_data);
  VerifyWrittenData(test_data);
}

TEST_P(AvroWriterTest, WriteTemporalTypes) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "date_col", std::make_shared<DateType>()),
      SchemaField::MakeRequired(2, "time_col", std::make_shared<TimeType>()),
      SchemaField::MakeRequired(3, "timestamp_col", std::make_shared<TimestampType>())});

  std::string test_data = R"([
    [18628, 43200000000, 1640995200000000],
    [18629, 86399000000, 1641081599000000]
  ])";

  WriteAvroFile(schema, test_data);
  VerifyWrittenData(test_data);
}

TEST_P(AvroWriterTest, WriteNestedStruct) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeRequired(
          2, "person",
          std::make_shared<iceberg::StructType>(std::vector<SchemaField>{
              SchemaField::MakeRequired(3, "name", std::make_shared<StringType>()),
              SchemaField::MakeRequired(4, "age", std::make_shared<IntType>())}))});

  std::string test_data = R"([
    [1, ["Alice", 30]],
    [2, ["Bob", 25]]
  ])";

  WriteAvroFile(schema, test_data);
  VerifyWrittenData(test_data);
}

TEST_P(AvroWriterTest, WriteListType) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeRequired(2, "tags",
                                std::make_shared<ListType>(SchemaField::MakeRequired(
                                    3, "element", std::make_shared<StringType>())))});

  std::string test_data = R"([
    [1, ["tag1", "tag2", "tag3"]],
    [2, ["foo", "bar"]],
    [3, []]
  ])";

  WriteAvroFile(schema, test_data);
  VerifyWrittenData(test_data);
}

TEST_P(AvroWriterTest, WriteMapTypeWithStringKey) {
  auto schema = std::make_shared<iceberg::Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(
          1, "properties",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(2, "key", std::make_shared<StringType>()),
              SchemaField::MakeRequired(3, "value", std::make_shared<IntType>())))});

  std::string test_data = R"([
    [[["key1", 100], ["key2", 200]]],
    [[["a", 1], ["b", 2], ["c", 3]]]
  ])";

  WriteAvroFile(schema, test_data);
  VerifyWrittenData(test_data);
}

TEST_P(AvroWriterTest, WriteMapTypeWithNonStringKey) {
  auto schema = std::make_shared<iceberg::Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(
          1, "int_map",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(2, "key", std::make_shared<IntType>()),
              SchemaField::MakeRequired(3, "value", std::make_shared<StringType>())))});

  std::string test_data = R"([
    [[[1, "one"], [2, "two"], [3, "three"]]],
    [[[10, "ten"], [20, "twenty"]]]
  ])";

  WriteAvroFile(schema, test_data);
  VerifyWrittenData(test_data);
}

TEST_P(AvroWriterTest, WriteEmptyMaps) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(
          1, "string_map",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(2, "key", std::make_shared<StringType>()),
              SchemaField::MakeRequired(3, "value", std::make_shared<IntType>()))),
      SchemaField::MakeRequired(
          4, "int_map",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(5, "key", std::make_shared<IntType>()),
              SchemaField::MakeRequired(6, "value", std::make_shared<StringType>())))});

  // Test empty maps for both string and non-string keys
  std::string test_data = R"([
    [[], []],
    [[["a", 1]], []]
  ])";

  // Just verify writing succeeds (empty maps are handled correctly by the encoder)
  WriteAvroFile(schema, test_data);
  VerifyWrittenData(test_data);
}

TEST_P(AvroWriterTest, WriteOptionalFields) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeOptional(2, "name", std::make_shared<StringType>()),
      SchemaField::MakeOptional(3, "age", std::make_shared<IntType>())});

  std::string test_data = R"([
    [1, "Alice", 30],
    [2, null, 25],
    [3, "Charlie", null],
    [4, null, null]
  ])";

  WriteAvroFile(schema, test_data);
  VerifyWrittenData(test_data);
}

TEST_P(AvroWriterTest, WriteLargeDataset) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", std::make_shared<LongType>()),
      SchemaField::MakeRequired(2, "value", std::make_shared<DoubleType>())});

  // Generate large dataset JSON
  std::ostringstream json;
  json << "[";
  for (int i = 0; i < 1000; ++i) {
    if (i > 0) json << ", ";
    json << "[" << i << ", " << (i * 1.5) << "]";
  }
  json << "]";

  WriteAvroFile(schema, json.str());
  VerifyWrittenData(json.str());
}

// Instantiate parameterized tests for both direct encoder and GenericDatum paths
INSTANTIATE_TEST_SUITE_P(DirectEncoderModes, AvroWriterTest,
                         ::testing::Values(true, false),
                         [](const ::testing::TestParamInfo<bool>& info) {
                           return info.param ? "DirectEncoder" : "GenericDatum";
                         });

}  // namespace iceberg::avro
