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

#include <optional>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/metadata.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/file_reader.h"
#include "iceberg/file_writer.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::parquet {

namespace {

Status WriteArray(std::shared_ptr<::arrow::Array> data, Writer& writer) {
  ArrowArray arr;
  ICEBERG_ARROW_RETURN_NOT_OK(::arrow::ExportArray(*data, &arr));
  ICEBERG_RETURN_UNEXPECTED(writer.Write(&arr));
  return writer.Close();
}

Status WriteArray(std::shared_ptr<::arrow::Array> data,
                  const WriterOptions& writer_options) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer, WriterFactoryRegistry::Open(FileFormatType::kParquet, writer_options));
  return WriteArray(data, *writer);
}

Status ReadArray(std::shared_ptr<::arrow::Array>& out,
                 const ReaderOptions& reader_options) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto reader, ReaderFactoryRegistry::Open(FileFormatType::kParquet, reader_options));
  ICEBERG_ASSIGN_OR_RAISE(auto read_data, reader->Next());

  if (!read_data.has_value()) {
    out = nullptr;
    return {};
  }
  auto arrow_c_array = read_data.value();

  ICEBERG_ASSIGN_OR_RAISE(ArrowSchema arrow_schema, reader->Schema());
  ICEBERG_ARROW_ASSIGN_OR_RETURN(out,
                                 ::arrow::ImportArray(&arrow_c_array, &arrow_schema));
  return {};
}

void DoRoundtrip(std::shared_ptr<::arrow::Array> data, std::shared_ptr<Schema> schema,
                 std::shared_ptr<::arrow::Array>& out) {
  std::shared_ptr<FileIO> file_io = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
  const std::string basePath = "base.parquet";

  auto writer_data = WriterFactoryRegistry::Open(
      FileFormatType::kParquet, {.path = basePath, .schema = schema, .io = file_io});
  ASSERT_THAT(writer_data, IsOk())
      << "Failed to create writer: " << writer_data.error().message;
  auto writer = std::move(writer_data.value());
  ASSERT_THAT(WriteArray(data, *writer), IsOk());

  ASSERT_THAT(ReadArray(out, {.path = basePath,
                              .length = writer->length(),
                              .io = file_io,
                              .projection = schema}),
              IsOk());

  ASSERT_TRUE(out != nullptr) << "Reader.Next() returned no data";
}

}  // namespace

class ParquetReaderTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { parquet::RegisterAll(); }

  void SetUp() override {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
    temp_parquet_file_ = "parquet_reader_test.parquet";
  }

  void CreateSimpleParquetFile() {
    auto schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "name", string())});

    ArrowSchema arrow_c_schema;
    ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
    auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

    auto array =
        ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()),
                                           R"([[1, "Foo"],[2, "Bar"],[3, "Baz"]])")
            .ValueOrDie();

    ASSERT_TRUE(WriteArray(
        array, {.path = temp_parquet_file_, .schema = schema, .io = file_io_}));
  }

  void CreateSplitParquetFile() {
    const std::string kParquetFieldIdKey = "PARQUET:field_id";
    auto arrow_schema = ::arrow::schema(
        {::arrow::field("id", ::arrow::int32(), /*nullable=*/false,
                        ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"1"})),
         ::arrow::field("name", ::arrow::utf8(), /*nullable=*/true,
                        ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"2"}))});
    auto table = ::arrow::Table::FromRecordBatches(
                     arrow_schema, {::arrow::RecordBatch::FromStructArray(
                                        ::arrow::json::ArrayFromJSONString(
                                            ::arrow::struct_(arrow_schema->fields()),
                                            R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])")
                                            .ValueOrDie())
                                        .ValueOrDie()})
                     .ValueOrDie();

    auto io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
    auto outfile = io.fs()->OpenOutputStream(temp_parquet_file_).ValueOrDie();

    ASSERT_TRUE(::parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(),
                                             outfile, /*chunk_size=*/2)
                    .ok());
    ASSERT_TRUE(outfile->Close().ok());
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
    ASSERT_THAT(data, IsOk()) << "Reader.Next() failed: " << data.error().message;
    ASSERT_TRUE(data.value().has_value()) << "Reader.Next() returned no data";
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

  std::shared_ptr<FileIO> file_io_;
  std::string temp_parquet_file_;
};

TEST_F(ParquetReaderTest, ReadTwoFields) {
  CreateSimpleParquetFile();

  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "name", string())});

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet,
      {.path = temp_parquet_file_, .io = file_io_, .projection = schema});
  ASSERT_THAT(reader_result, IsOk())
      << "Failed to create reader: " << reader_result.error().message;
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(
      VerifyNextBatch(*reader, R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, ReadReorderedFieldsWithNulls) {
  CreateSimpleParquetFile();

  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(2, "name", string()),
                               SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(3, "score", float64())});

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet,
      {.path = temp_parquet_file_, .io = file_io_, .projection = schema});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(
      *reader, R"([["Foo", 1, null], ["Bar", 2, null], ["Baz", 3, null]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, ReadWithBatchSize) {
  CreateSimpleParquetFile();

  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});

  auto reader_result =
      ReaderFactoryRegistry::Open(FileFormatType::kParquet, {.path = temp_parquet_file_,
                                                             .batch_size = 2,
                                                             .io = file_io_,
                                                             .projection = schema});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[1], [2]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[3]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, ReadSplit) {
  CreateSplitParquetFile();

  // Read split offsets
  auto io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
  auto input_stream = io.fs()->OpenInputFile(temp_parquet_file_).ValueOrDie();
  auto metadata = ::parquet::ReadMetaData(input_stream);
  std::vector<size_t> split_offsets;
  for (int i = 0; i < metadata->num_row_groups(); ++i) {
    split_offsets.push_back(static_cast<size_t>(metadata->RowGroup(i)->file_offset()));
  }
  ASSERT_EQ(split_offsets.size(), 2);

  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});

  std::vector<Split> splits = {
      {.offset = 0, .length = std::numeric_limits<size_t>::max()},
      {.offset = split_offsets[0], .length = split_offsets[1] - split_offsets[0]},
      {.offset = split_offsets[1], .length = 1},
      {.offset = split_offsets[1] + 1, .length = std::numeric_limits<size_t>::max()},
      {.offset = 0, .length = split_offsets[0]},
  };
  std::vector<std::string> expected_json = {
      R"([[1], [2], [3]])", R"([[1], [2]])", R"([[3]])", "", "",
  };

  for (size_t i = 0; i < splits.size(); ++i) {
    auto reader_result =
        ReaderFactoryRegistry::Open(FileFormatType::kParquet, {.path = temp_parquet_file_,
                                                               .split = splits[i],
                                                               .batch_size = 100,
                                                               .io = file_io_,
                                                               .projection = schema});
    ASSERT_THAT(reader_result, IsOk());
    auto reader = std::move(reader_result.value());
    if (!expected_json[i].empty()) {
      ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, expected_json[i]));
    }
    ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
  }
}

class ParquetReadWrite : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { parquet::RegisterAll(); }
};

TEST_F(ParquetReadWrite, EmptyStruct) {
  auto schema =
      std::make_shared<Schema>(std::vector<SchemaField>{SchemaField::MakeRequired(
          1, "empty_struct", std::make_shared<StructType>(std::vector<SchemaField>{}))});

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

  auto array = ::arrow::json::ArrayFromJSONString(
                   ::arrow::struct_(arrow_schema->fields()), R"([null, {}])")
                   .ValueOrDie();

  std::shared_ptr<FileIO> file_io = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
  const std::string basePath = "base.parquet";

  ASSERT_THAT(WriteArray(array, {.path = basePath, .schema = schema, .io = file_io}),
              IsError(ErrorKind::kNotImplemented));
}

TEST_F(ParquetReadWrite, SimpleStructRoundTrip) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "a",
                                struct_({
                                    SchemaField::MakeOptional(2, "a1", int64()),
                                    SchemaField::MakeOptional(3, "a2", string()),
                                })),
  });

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

  auto array = ::arrow::json::ArrayFromJSONString(
                   ::arrow::struct_(arrow_schema->fields()),
                   R"([[{"a1": 1, "a2": "abc"}], [{"a1": 0}], [{"a2": "edf"}], [{}]])")
                   .ValueOrDie();

  std::shared_ptr<::arrow::Array> out;
  DoRoundtrip(array, schema, out);

  ASSERT_TRUE(out->Equals(*array));
}

TEST_F(ParquetReadWrite, SimpleTypeRoundTrip) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "a", boolean()),
      SchemaField::MakeOptional(2, "b", int32()),
      SchemaField::MakeOptional(3, "c", int64()),
      SchemaField::MakeOptional(4, "d", float32()),
      SchemaField::MakeOptional(5, "e", float64()),
      SchemaField::MakeOptional(6, "f", string()),
      SchemaField::MakeOptional(7, "g", time()),
      SchemaField::MakeOptional(8, "h", timestamp()),
  });

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

  auto array = ::arrow::json::ArrayFromJSONString(
                   ::arrow::struct_(arrow_schema->fields()),
                   R"([[true, 1, 2, 1.1, 1.2, "abc", 44614000, 1756696503000000],
                       [false, 0, 0, 0, 0, "", 0, 0],
                       [null, null, null, null, null, null, null, null]])")
                   .ValueOrDie();

  std::shared_ptr<::arrow::Array> out;
  DoRoundtrip(array, schema, out);

  ASSERT_TRUE(out->Equals(*array));
}

}  // namespace iceberg::parquet
