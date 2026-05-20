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
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/file_reader.h"
#include "iceberg/file_writer.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/std_io.h"
#include "iceberg/test/temp_file_test_base.h"
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
                 const ReaderOptions& reader_options,
                 std::unordered_map<std::string, std::string>* metadata) {
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

  if (metadata) {
    ICEBERG_ASSIGN_OR_RAISE(*metadata, reader->Metadata());
  }

  return {};
}

void DoRoundtrip(std::shared_ptr<::arrow::Array> data, std::shared_ptr<Schema> schema,
                 std::shared_ptr<::arrow::Array>& out) {
  std::shared_ptr<FileIO> file_io = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
  const std::string basePath = "base.parquet";

  std::unordered_map<std::string, std::string> metadata = {{"k1", "v1"}, {"k2", "v2"}};

  WriterProperties writer_properties;
  writer_properties.Set(WriterProperties::kParquetCompression,
                        std::string("uncompressed"));

  auto writer_data = WriterFactoryRegistry::Open(
      FileFormatType::kParquet, {.path = basePath,
                                 .schema = schema,
                                 .io = file_io,
                                 .metadata = metadata,
                                 .properties = std::move(writer_properties)});
  ASSERT_THAT(writer_data, IsOk())
      << "Failed to create writer: " << writer_data.error().message;
  auto writer = std::move(writer_data.value());
  ASSERT_THAT(WriteArray(data, *writer), IsOk());

  std::unordered_map<std::string, std::string> read_metadata;
  ICEBERG_UNWRAP_OR_FAIL(auto length, writer->length());
  ASSERT_THAT(
      ReadArray(out,
                {.path = basePath, .length = length, .io = file_io, .projection = schema},
                &read_metadata),
      IsOk());
  ASSERT_EQ(read_metadata, metadata);

  ASSERT_TRUE(out != nullptr) << "Reader.Next() returned no data";
}

struct ParquetCodec {
  std::string name;
  ::arrow::Compression::type compression;
};

std::optional<ParquetCodec> FirstUnavailableParquetCodec() {
  const std::vector<ParquetCodec> codecs = {
      {.name = "snappy", .compression = ::arrow::Compression::SNAPPY},
      {.name = "gzip", .compression = ::arrow::Compression::GZIP},
      {.name = "brotli", .compression = ::arrow::Compression::BROTLI},
      {.name = "lz4", .compression = ::arrow::Compression::LZ4},
      {.name = "zstd", .compression = ::arrow::Compression::ZSTD},
  };
  for (const auto& codec : codecs) {
    if (!::arrow::util::Codec::IsAvailable(codec.compression)) {
      return codec;
    }
  }
  return std::nullopt;
}

}  // namespace

class ParquetReaderTest : public TempFileTestBase {
 protected:
  static void SetUpTestSuite() { parquet::RegisterAll(); }

  void SetUp() override {
    TempFileTestBase::SetUp();
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

    WriterProperties writer_properties;
    writer_properties.Set(WriterProperties::kParquetCompression,
                          std::string("uncompressed"));

    ASSERT_TRUE(WriteArray(array, {.path = temp_parquet_file_,
                                   .schema = schema,
                                   .io = file_io_,
                                   .properties = std::move(writer_properties)}));
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

TEST_F(ParquetReaderTest, RoundTripWithGenericFileIO) {
  auto file_io = std::make_shared<iceberg::test::StdFileIO>();
  auto path = CreateNewTempFilePathWithSuffix(".parquet");

  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "name", string())});
  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();
  auto array =
      ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()),
                                         R"([[1, "Foo"], [2, "Bar"]])")
          .ValueOrDie();

  WriterProperties writer_properties;
  writer_properties.Set(WriterProperties::kParquetCompression,
                        std::string("uncompressed"));
  auto writer_result = WriterFactoryRegistry::Open(
      FileFormatType::kParquet, {.path = path,
                                 .schema = schema,
                                 .io = file_io,
                                 .properties = std::move(writer_properties)});
  ASSERT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());
  ASSERT_THAT(WriteArray(array, *writer), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto length, writer->length());

  std::shared_ptr<::arrow::Array> out;
  auto read_status = ReadArray(
      out, {.path = path, .length = length, .io = file_io, .projection = schema},
      nullptr);
  ASSERT_THAT(read_status, IsOk());
  ASSERT_TRUE(out->Equals(*array));
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

  ReaderProperties reader_properties;
  reader_properties.Set(ReaderProperties::kBatchSize, int64_t{2});

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet, {.path = temp_parquet_file_,
                                 .io = file_io_,
                                 .projection = schema,
                                 .properties = std::move(reader_properties)});
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

  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      MetadataColumns::kRowPosition,
  });

  std::vector<Split> splits = {
      {.offset = 0, .length = std::numeric_limits<size_t>::max()},
      {.offset = split_offsets[0], .length = split_offsets[1] - split_offsets[0]},
      {.offset = split_offsets[1], .length = 1},
      {.offset = split_offsets[1] + 1, .length = std::numeric_limits<size_t>::max()},
      {.offset = 0, .length = split_offsets[0]},
  };
  std::vector<std::string> expected_json = {
      R"([[1, 0], [2, 1], [3, 2]])", R"([[1, 0], [2, 1]])", R"([[3, 2]])", "", "",
  };

  ReaderProperties reader_properties;
  reader_properties.Set(ReaderProperties::kBatchSize, int64_t{100});

  for (size_t i = 0; i < splits.size(); ++i) {
    auto reader_result = ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                                     {
                                                         .path = temp_parquet_file_,
                                                         .split = splits[i],
                                                         .io = file_io_,
                                                         .projection = schema,
                                                         .properties = reader_properties,
                                                     });
    ASSERT_THAT(reader_result, IsOk());
    auto reader = std::move(reader_result.value());
    if (!expected_json[i].empty()) {
      ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, expected_json[i]));
    }
    ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
  }
}

TEST_F(ParquetReaderTest, ReadMetadataColumns) {
  temp_parquet_file_ = "metadata_columns.parquet";
  CreateSimpleParquetFile();

  // Create schema with both _file and _pos metadata columns
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      MetadataColumns::kFilePath,
      MetadataColumns::kRowPosition,
  });

  const std::string kExpectedJson =
      R"([[1, "metadata_columns.parquet", 0],
          [2, "metadata_columns.parquet", 1],
          [3, "metadata_columns.parquet", 2]])";

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      ReaderFactoryRegistry::Open(
          FileFormatType::kParquet,
          {.path = temp_parquet_file_, .io = file_io_, .projection = schema}));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, kExpectedJson));
}

TEST_F(ParquetReaderTest, ReadRowPositionWithBatchSize) {
  CreateSimpleParquetFile();

  // Create schema with _pos metadata column
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      MetadataColumns::kRowPosition,
  });

  ReaderProperties reader_properties;
  reader_properties.Set(ReaderProperties::kBatchSize, int64_t{2});

  ICEBERG_UNWRAP_OR_FAIL(auto reader, ReaderFactoryRegistry::Open(
                                          FileFormatType::kParquet,
                                          {.path = temp_parquet_file_,
                                           .io = file_io_,
                                           .projection = schema,
                                           .properties = std::move(reader_properties)}));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[1, 0], [2, 1]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[3, 2]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, ReadMetadataOnlyProjection) {
  temp_parquet_file_ = "metadata_only.parquet";
  CreateSimpleParquetFile();

  // Create schema with only metadata columns (no data columns)
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      MetadataColumns::kFilePath,
      MetadataColumns::kRowPosition,
  });

  const std::string kExpectedJson =
      R"([["metadata_only.parquet", 0],
          ["metadata_only.parquet", 1],
          ["metadata_only.parquet", 2]])";

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      ReaderFactoryRegistry::Open(
          FileFormatType::kParquet,
          {.path = temp_parquet_file_, .io = file_io_, .projection = schema}));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, kExpectedJson));
}

TEST_F(ParquetReaderTest, ReadNestedUnknownProjection) {
  temp_parquet_file_ = "nested_unknown.parquet";
  auto write_schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "profile",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeOptional(2, "name", string()),
                                    SchemaField::MakeOptional(3, "mystery", int32()),
                                })),
      SchemaField::MakeOptional(
          4, "mysteries",
          std::make_shared<ListType>(SchemaField::MakeOptional(5, "element", int32()))),
      SchemaField::MakeOptional(
          6, "properties",
          std::make_shared<MapType>(SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeOptional(8, "value", int32()))),
      SchemaField::MakeOptional(9, "wrapper",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeOptional(10, "mystery", int32()),
                                })),
  });
  auto read_schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "profile",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeOptional(2, "name", string()),
                                    SchemaField::MakeOptional(3, "mystery", unknown()),
                                })),
      SchemaField::MakeOptional(
          4, "mysteries",
          std::make_shared<ListType>(SchemaField::MakeOptional(5, "element", unknown()))),
      SchemaField::MakeOptional(
          6, "properties",
          std::make_shared<MapType>(SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeOptional(8, "value", unknown()))),
      SchemaField::MakeOptional(9, "wrapper",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeOptional(10, "mystery", unknown()),
                                })),
  });

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(*write_schema, &arrow_c_schema), IsOk());
  auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();
  auto array = ::arrow::json::ArrayFromJSONString(arrow_type,
                                                  R"([
                     {"profile": {"name": "Person0", "mystery": 10}, "mysteries": [1, 2], "properties": [["a", 100], ["b", 200]], "wrapper": {"mystery": 300}},
                     {"profile": {"name": "Person1", "mystery": null}, "mysteries": [], "properties": [], "wrapper": {"mystery": null}}
                   ])")
                   .ValueOrDie();

  WriterProperties writer_properties;
  writer_properties.Set(WriterProperties::kParquetCompression,
                        std::string("uncompressed"));
  ASSERT_THAT(WriteArray(array, {.path = temp_parquet_file_,
                                 .schema = write_schema,
                                 .io = file_io_,
                                 .properties = std::move(writer_properties)}),
              IsOk());

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      ReaderFactoryRegistry::Open(
          FileFormatType::kParquet,
          {.path = temp_parquet_file_, .io = file_io_, .projection = read_schema}));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader,
                                          R"([
        {"profile": {"name": "Person0", "mystery": null}, "mysteries": [null, null], "properties": [["a", null], ["b", null]], "wrapper": {"mystery": null}},
        {"profile": {"name": "Person1", "mystery": null}, "mysteries": [], "properties": [], "wrapper": {"mystery": null}}
      ])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
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

TEST_F(ParquetReadWrite, RejectsUnavailableCompressionCodec) {
  auto unavailable_codec = FirstUnavailableParquetCodec();
  if (!unavailable_codec.has_value()) {
    GTEST_SKIP() << "All optional Parquet compression codecs are available";
  }

  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});
  WriterProperties writer_properties;
  writer_properties.Set(WriterProperties::kParquetCompression, unavailable_codec->name);

  auto writer = WriterFactoryRegistry::Open(
      FileFormatType::kParquet, {.path = "unavailable_codec.parquet",
                                 .schema = schema,
                                 .io = arrow::ArrowFileSystemFileIO::MakeMockFileIO(),
                                 .properties = std::move(writer_properties)});

  EXPECT_THAT(writer, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(writer,
              HasErrorMessage("Parquet compression codec " + unavailable_codec->name +
                              " is not available in the current build"));
}

TEST_F(ParquetReadWrite, RejectsUnknownInsideListOrMapOnWrite) {
  std::vector<std::shared_ptr<Schema>> schemas = {
      std::make_shared<Schema>(std::vector<SchemaField>{
          SchemaField::MakeOptional(1, "mysteries",
                                    std::make_shared<ListType>(SchemaField::MakeOptional(
                                        2, ListType::kElementName, iceberg::unknown()))),
      }),
      std::make_shared<Schema>(std::vector<SchemaField>{
          SchemaField::MakeOptional(
              1, "properties",
              std::make_shared<MapType>(
                  SchemaField::MakeRequired(2, MapType::kKeyName, iceberg::string()),
                  SchemaField::MakeOptional(3, MapType::kValueName, iceberg::unknown()))),
      }),
  };

  for (const auto& schema : schemas) {
    auto writer = WriterFactoryRegistry::Open(
        FileFormatType::kParquet, {.path = "unknown_nested.parquet",
                                   .schema = schema,
                                   .io = arrow::ArrowFileSystemFileIO::MakeMockFileIO()});

    EXPECT_THAT(writer, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(writer, HasErrorMessage("physical Parquet representation"));
  }
}

TEST_F(ParquetReadWrite, RejectsRequiredUnknownOnWrite) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "mystery", unknown(),
                                           /*optional=*/false)});

  auto writer = WriterFactoryRegistry::Open(
      FileFormatType::kParquet, {.path = "required_unknown.parquet",
                                 .schema = schema,
                                 .io = arrow::ArrowFileSystemFileIO::MakeMockFileIO()});

  EXPECT_THAT(writer, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(writer, HasErrorMessage("Unknown type field 'mystery' must be optional"));
}

TEST_F(ParquetReadWrite, RejectsUnknownOnlyStructOnWrite) {
  std::vector<std::shared_ptr<Schema>> schemas = {
      std::make_shared<Schema>(std::vector<SchemaField>{
          SchemaField::MakeOptional(1, "wrapper",
                                    std::make_shared<StructType>(std::vector<SchemaField>{
                                        SchemaField::MakeOptional(2, "secret", unknown()),
                                    })),
      }),
      std::make_shared<Schema>(std::vector<SchemaField>{
          SchemaField::MakeOptional(
              1, "wrappers",
              std::make_shared<ListType>(SchemaField::MakeOptional(
                  2, ListType::kElementName,
                  std::make_shared<StructType>(std::vector<SchemaField>{
                      SchemaField::MakeOptional(3, "secret", unknown()),
                  })))),
      }),
      std::make_shared<Schema>(std::vector<SchemaField>{
          SchemaField::MakeOptional(
              1, "properties",
              std::make_shared<MapType>(
                  SchemaField::MakeRequired(2, MapType::kKeyName, iceberg::string()),
                  SchemaField::MakeOptional(
                      3, MapType::kValueName,
                      std::make_shared<StructType>(std::vector<SchemaField>{
                          SchemaField::MakeOptional(4, "secret", unknown()),
                      })))),
      }),
  };

  for (const auto& schema : schemas) {
    auto writer = WriterFactoryRegistry::Open(
        FileFormatType::kParquet, {.path = "unknown_only_struct.parquet",
                                   .schema = schema,
                                   .io = arrow::ArrowFileSystemFileIO::MakeMockFileIO()});

    EXPECT_THAT(writer, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(writer, HasErrorMessage("all child fields are unknown"));
  }
}

TEST_F(ParquetReadWrite, WritesUnknownFieldsNestedInsideListOrMapStructs) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "id", int32()),
      SchemaField::MakeOptional(2, "events",
                                std::make_shared<ListType>(SchemaField::MakeOptional(
                                    3, ListType::kElementName,
                                    std::make_shared<StructType>(std::vector<SchemaField>{
                                        SchemaField::MakeOptional(4, "name", string()),
                                        SchemaField::MakeOptional(5, "secret", unknown()),
                                    })))),
      SchemaField::MakeOptional(
          6, "properties",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(7, MapType::kKeyName, iceberg::string()),
              SchemaField::MakeOptional(
                  8, MapType::kValueName,
                  std::make_shared<StructType>(std::vector<SchemaField>{
                      SchemaField::MakeOptional(9, "label", string()),
                      SchemaField::MakeOptional(10, "secret", unknown()),
                  })))),
  });

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

  auto array =
      ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()),
                                         R"([
                      {"id": 1, "events": [{"name": "open", "secret": null}, {"name": "close", "secret": null}], "properties": [["a", {"label": "A", "secret": null}]]},
                      {"id": 2, "events": [], "properties": []}
                    ])")
          .ValueOrDie();

  std::shared_ptr<FileIO> file_io = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
  const std::string basePath = "nested_unknown_fields.parquet";
  WriterProperties writer_properties;
  writer_properties.Set(WriterProperties::kParquetCompression,
                        std::string("uncompressed"));
  ASSERT_THAT(WriteArray(array, {.path = basePath,
                                 .schema = schema,
                                 .io = file_io,
                                 .properties = std::move(writer_properties)}),
              IsOk());

  auto& arrow_file_io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io);
  auto input_file = arrow_file_io.fs()->OpenInputFile(basePath).ValueOrDie();
  auto parquet_reader = ::parquet::ParquetFileReader::Open(input_file);
  auto parquet_schema = parquet_reader->metadata()->schema();

  std::vector<int32_t> field_ids;
  for (int i = 0; i < parquet_schema->num_columns(); ++i) {
    field_ids.push_back(parquet_schema->Column(i)->schema_node()->field_id());
  }
  EXPECT_THAT(field_ids, ::testing::UnorderedElementsAre(1, 4, 7, 9));

  std::shared_ptr<::arrow::Array> out;
  ASSERT_THAT(ReadArray(out, {.path = basePath, .io = file_io, .projection = schema},
                        /*metadata=*/nullptr),
              IsOk());
  auto expected =
      ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()),
                                         R"([
                      {"id": 1, "events": [{"name": "open", "secret": null}, {"name": "close", "secret": null}], "properties": [["a", {"label": "A", "secret": null}]]},
                      {"id": 2, "events": [], "properties": []}
                    ])")
          .ValueOrDie();
  ASSERT_TRUE(out->Equals(*expected)) << "actual:\n"
                                      << out->ToString() << "expected:\n"
                                      << expected->ToString();
}

TEST_F(ParquetReadWrite, DoesNotMaterializeUnknownFieldsOnWrite) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "id", int32()),
      SchemaField::MakeOptional(2, "mystery", unknown()),
      SchemaField::MakeOptional(3, "profile",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeOptional(4, "name", string()),
                                    SchemaField::MakeOptional(5, "secret", unknown()),
                                })),
  });

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

  auto array =
      ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()),
                                         R"([
                      [1, null, {"name": "Person0", "secret": null}],
                      [2, null, {"name": "Person1", "secret": null}]
                    ])")
          .ValueOrDie();

  std::shared_ptr<FileIO> file_io = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
  const std::string basePath = "unknown_fields.parquet";

  WriterProperties writer_properties;
  writer_properties.Set(WriterProperties::kParquetCompression,
                        std::string("uncompressed"));
  ASSERT_THAT(WriteArray(array, {.path = basePath,
                                 .schema = schema,
                                 .io = file_io,
                                 .properties = std::move(writer_properties)}),
              IsOk());

  auto& arrow_file_io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io);
  auto input_file = arrow_file_io.fs()->OpenInputFile(basePath).ValueOrDie();
  auto parquet_reader = ::parquet::ParquetFileReader::Open(input_file);
  auto parquet_schema = parquet_reader->metadata()->schema();

  ASSERT_EQ(parquet_schema->num_columns(), 2);
  EXPECT_EQ(parquet_schema->Column(0)->schema_node()->field_id(), 1);
  EXPECT_EQ(parquet_schema->Column(1)->schema_node()->field_id(), 4);

  std::shared_ptr<::arrow::Array> out;
  ASSERT_THAT(ReadArray(out, {.path = basePath, .io = file_io, .projection = schema},
                        /*metadata=*/nullptr),
              IsOk());
  auto expected =
      ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()),
                                         R"([
                      [1, null, {"name": "Person0", "secret": null}],
                      [2, null, {"name": "Person1", "secret": null}]
                    ])")
          .ValueOrDie();
  ASSERT_TRUE(out->Equals(*expected)) << "actual:\n"
                                      << out->ToString() << "expected:\n"
                                      << expected->ToString();
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
