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

#include <arrow/array/array_base.h>
#include <arrow/c/bridge.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/file.h>
#include <arrow/json/from_string.h>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/avro/avro_writer.h"
#include "iceberg/file_reader.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/type.h"
#include "matchers.h"
#include "temp_file_test_base.h"

namespace iceberg::avro {

class AvroReaderTest : public TempFileTestBase {
 protected:
  static void SetUpTestSuite() { RegisterAll(); }

  void SetUp() override {
    TempFileTestBase::SetUp();
    local_fs_ = std::make_shared<::arrow::fs::LocalFileSystem>();
    file_io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(local_fs_);
    temp_avro_file_ = CreateNewTempFilePathWithSuffix(".avro");
  }

  void CreateSimpleAvroFile() {
    const std::string avro_schema_json = R"({
      "type": "record",
      "name": "TestRecord",
      "fields": [
        {"name": "id", "type": "int", "field-id": 1},
        {"name": "name", "type": ["null", "string"], "field-id": 2}
      ]
    })";
    auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

    const std::vector<std::tuple<int32_t, std::string>> test_data = {
        {1, "Alice"}, {2, "Bob"}, {3, "Charlie"}};

    ::avro::DataFileWriter<::avro::GenericDatum> writer(temp_avro_file_.c_str(),
                                                        avro_schema);
    for (const auto& [id, name] : test_data) {
      ::avro::GenericDatum datum(avro_schema.root());
      auto& record = datum.value<::avro::GenericRecord>();
      record.fieldAt(0).value<int32_t>() = id;
      record.fieldAt(1).selectBranch(1);  // non-null
      record.fieldAt(1).value<std::string>() = name;
      writer.write(datum);
    }
    writer.close();
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

    auto writer_result = WriterFactoryRegistry::Open(
        FileFormatType::kAvro,
        {.path = temp_avro_file_, .schema = schema, .io = file_io_});
    ASSERT_TRUE(writer_result.has_value());
    auto writer = std::move(writer_result.value());
    ASSERT_THAT(writer->Write(&arrow_array), IsOk());
    ASSERT_THAT(writer->Close(), IsOk());

    auto file_info_result = local_fs_->GetFileInfo(temp_avro_file_);
    ASSERT_TRUE(file_info_result.ok());
    ASSERT_EQ(file_info_result->size(), writer->length().value());

    auto reader_result = ReaderFactoryRegistry::Open(FileFormatType::kAvro,
                                                     {.path = temp_avro_file_,
                                                      .length = file_info_result->size(),
                                                      .io = file_io_,
                                                      .projection = schema});
    ASSERT_THAT(reader_result, IsOk());
    auto reader = std::move(reader_result.value());
    ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, expected_string));
    ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
  }

  std::shared_ptr<::arrow::fs::LocalFileSystem> local_fs_;
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

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kAvro,
      {.path = temp_avro_file_, .batch_size = 2, .io = file_io_, .projection = schema});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[1], [2]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[3]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(AvroReaderTest, AvroWriterBasicType) {
  auto schema = std::make_shared<iceberg::Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "name", std::make_shared<StringType>())});

  std::string expected_string = R"([["Hello"], ["世界"], ["nanoarrow"]])";

  WriteAndVerify(schema, expected_string);
}

TEST_F(AvroReaderTest, AvroWriterNestedType) {
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

}  // namespace iceberg::avro
