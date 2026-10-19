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

#include <array>
#include <format>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/c/bridge.h>
#include <arrow/extension/uuid.h>
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
#include <parquet/file_writer.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/file_reader.h"
#include "iceberg/file_writer.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/name_mapping.h"
#include "iceberg/parquet/parquet_data_util_internal.h"
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
#include "iceberg/util/uuid.h"

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

std::unique_ptr<::parquet::FileMetaData> MakeMetadataWithPageOffsets(
    const std::vector<std::pair<int64_t, std::optional<int64_t>>>& page_offsets) {
  ::parquet::schema::NodeVector fields;
  for (size_t i = 0; i < page_offsets.size(); ++i) {
    fields.push_back(::parquet::schema::PrimitiveNode::Make(
        "col" + std::to_string(i), ::parquet::Repetition::REQUIRED, nullptr,
        ::parquet::Type::INT32, /*primitive_length=*/-1, /*field_id=*/i + 1));
  }
  ::parquet::SchemaDescriptor schema;
  schema.Init(::parquet::schema::GroupNode::Make(
      "schema", ::parquet::Repetition::REQUIRED, fields));
  auto builder = ::parquet::FileMetaDataBuilder::Make(
      &schema, ::parquet::default_writer_properties());
  auto row_group = builder->AppendRowGroup();
  row_group->set_num_rows(1);
  for (const auto& [data_offset, dictionary_offset] : page_offsets) {
    row_group->NextColumnChunk()->Finish(
        /*num_values=*/1, dictionary_offset.value_or(0), /*index_page_offset=*/0,
        data_offset, /*compressed_size=*/10, /*uncompressed_size=*/10,
        dictionary_offset.has_value(), /*dictionary_fallback=*/false, {}, {});
  }
  row_group->Finish(10 * page_offsets.size());
  return builder->Finish();
}

constexpr std::array<uint8_t, Uuid::kLength> kUuidBytes1 = {
    0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3,
    0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00};
constexpr std::array<uint8_t, Uuid::kLength> kUuidBytes2 = {
    0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd,
    0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85, 0xe7};

}  // namespace

TEST(ParquetSplitTest, CanonicalStartUsesFirstColumnPages) {
  for (auto dictionary_offset :
       {std::optional<int64_t>{}, std::optional<int64_t>{0}, std::optional<int64_t>{80},
        std::optional<int64_t>{100}, std::optional<int64_t>{120}}) {
    // The second column starts earlier and must not be used as the assignment key.
    auto metadata = MakeMetadataWithPageOffsets({{100, dictionary_offset}, {50, 40}});
    const int64_t expected = dictionary_offset == 80 ? 80 : 100;
    EXPECT_THAT(RowGroupStart(*metadata->RowGroup(0), 200),
                HasValue(::testing::Eq(expected)));
  }
}

TEST(ParquetSplitTest, CanonicalStartRejectsMissingOrOutOfFilePages) {
  for (int64_t offset : {-1, 0, 200, 201}) {
    auto metadata = MakeMetadataWithPageOffsets({{offset, std::nullopt}});
    EXPECT_THAT(RowGroupStart(*metadata->RowGroup(0), 200), IsError(ErrorKind::kInvalid));
  }
  auto no_columns = MakeMetadataWithPageOffsets({});
  EXPECT_THAT(RowGroupStart(*no_columns->RowGroup(0), 200),
              HasErrorMessage("without columns"));
}

TEST(ParquetSplitTest, MidpointAddsHalfTheRowGroupSize) {
  // Each column chunk is 10 bytes, so the row group is 20 bytes from its start at 80.
  auto metadata = MakeMetadataWithPageOffsets({{100, 80}, {50, 40}});
  ASSERT_EQ(metadata->RowGroup(0)->total_compressed_size(), 20);
  EXPECT_THAT(RowGroupMidpoint(*metadata->RowGroup(0), 200), HasValue(::testing::Eq(90)));
  // The size may reach, but not pass, the end of the file.
  EXPECT_THAT(RowGroupMidpoint(*metadata->RowGroup(0), 100), HasValue(::testing::Eq(90)));
  EXPECT_THAT(RowGroupMidpoint(*metadata->RowGroup(0), 99),
              HasErrorMessage("invalid compressed size 20"));
}

TEST(ParquetSplitTest, MidpointRejectsInvalidStart) {
  auto metadata = MakeMetadataWithPageOffsets({{0, std::nullopt}});
  EXPECT_THAT(RowGroupMidpoint(*metadata->RowGroup(0), 200),
              HasErrorMessage("starting page 0"));
}

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

  void CreateParquetFileWithoutFieldIds(const std::shared_ptr<::arrow::DataType>& type,
                                        std::string_view json) {
    auto array = ::arrow::json::ArrayFromJSONString(type, json).ValueOrDie();
    auto batch = ::arrow::RecordBatch::FromStructArray(array).ValueOrDie();
    auto table = ::arrow::Table::FromRecordBatches({batch}).ValueOrDie();
    auto io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
    auto outfile = io.fs()->OpenOutputStream(temp_parquet_file_).ValueOrDie();

    ASSERT_TRUE(::parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(),
                                             outfile, table->num_rows())
                    .ok());
    ASSERT_TRUE(outfile->Close().ok());
  }

  void CreateListParquetFile() {
    auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(1, "id", int32()),
        SchemaField::MakeOptional(2, "numbers",
                                  std::make_shared<ListType>(SchemaField::MakeOptional(
                                      /*field_id=*/101, "element", int32())))});

    ArrowSchema arrow_c_schema;
    ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
    auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

    auto array =
        ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()),
                                           R"([[1, [1, 2]], [2, [3]], [3, null]])")
            .ValueOrDie();

    WriterProperties writer_properties;
    writer_properties.Set(WriterProperties::kParquetCompression,
                          std::string("uncompressed"));

    ASSERT_TRUE(WriteArray(array, {.path = temp_parquet_file_,
                                   .schema = schema,
                                   .io = file_io_,
                                   .properties = std::move(writer_properties)}));
  }

  void CreateRowLineageParquetFile() {
    auto schema = RowLineageSchema();

    ArrowSchema arrow_c_schema;
    ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
    auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

    auto array =
        ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()),
                                           R"([[1, null, null],
                                              [2, 777, 8],
                                              [3, null, null]])")
            .ValueOrDie();

    WriterProperties writer_properties;
    writer_properties.Set(WriterProperties::kParquetCompression,
                          std::string("uncompressed"));

    ASSERT_TRUE(WriteArray(array, {.path = temp_parquet_file_,
                                   .schema = schema,
                                   .io = file_io_,
                                   .properties = std::move(writer_properties)}));
  }

  void CreateSplitParquetFile(int64_t row_group_size = 2, bool dictionary = true) {
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

    ::parquet::WriterProperties::Builder properties;
    if (!dictionary) {
      properties.disable_dictionary();
    }
    ASSERT_TRUE(::parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(),
                                             outfile, row_group_size, properties.build())
                    .ok());
    ASSERT_TRUE(outfile->Close().ok());
  }

  void ReorderRowGroups(const std::vector<int>& row_groups) {
    ICEBERG_UNWRAP_OR_FAIL(auto input,
                           arrow::OpenArrowInputStream(file_io_, temp_parquet_file_));
    auto metadata = ::parquet::ReadMetaData(input);
    auto file_size = input->GetSize().ValueOrDie();
    auto pages = input->ReadAt(0, file_size - metadata->size() - 8).ValueOrDie();
    ASSERT_TRUE(input->Close().ok());
    ICEBERG_UNWRAP_OR_FAIL(auto output,
                           arrow::OpenArrowOutputStream(file_io_, temp_parquet_file_));
    ASSERT_TRUE(output->Write(pages).ok());
    ::parquet::WriteFileMetaData(*metadata->Subset(row_groups), output.get());
    ASSERT_TRUE(output->Close().ok());
  }

  std::shared_ptr<Schema> RowLineageSchema() {
    return std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(1, "id", int32()),
        MetadataColumns::kRowId,
        MetadataColumns::kLastUpdatedSequenceNumber,
    });
  }

  Result<std::unique_ptr<Reader>> OpenRowLineageReader(
      const std::shared_ptr<Schema>& schema,
      std::optional<int64_t> first_row_id = std::nullopt,
      std::optional<int64_t> data_sequence_number = std::nullopt) {
    return ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                       {.path = temp_parquet_file_,
                                        .io = file_io_,
                                        .projection = schema,
                                        .first_row_id = first_row_id,
                                        .data_sequence_number = data_sequence_number});
  }

  // Writes a list parquet file through parquet::arrow::WriteTable, which serializes the
  // Arrow schema of the table into the ARROW:schema key value metadata of the file.
  void CreateListParquetFileWithArrowSchema() {
    const std::string kParquetFieldIdKey = "PARQUET:field_id";
    auto arrow_schema = ::arrow::schema(
        {::arrow::field("id", ::arrow::int32(), /*nullable=*/false,
                        ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"1"})),
         ::arrow::field(
             "numbers",
             ::arrow::list(::arrow::field(
                 "element", ::arrow::int32(), /*nullable=*/true,
                 ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"101"}))),
             /*nullable=*/true,
             ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"2"}))});
    auto batch =
        ::arrow::RecordBatch::FromStructArray(
            ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()),
                                               R"([[1, [1, 2]], [2, [3]]])")
                .ValueOrDie())
            .ValueOrDie();
    auto table = ::arrow::Table::FromRecordBatches(arrow_schema, {batch}).ValueOrDie();

    auto io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
    auto outfile = io.fs()->OpenOutputStream(temp_parquet_file_).ValueOrDie();

    // Build ArrowWriterProperties to store the Arrow schema in ARROW:schema metadata
    auto arrow_writer_props =
        ::parquet::ArrowWriterProperties::Builder().store_schema()->build();

    // write a single row group so that one batch holds every row
    ASSERT_TRUE(::parquet::arrow::WriteTable(
                    *table, ::arrow::default_memory_pool(), outfile, table->num_rows(),
                    ::parquet::default_writer_properties(), arrow_writer_props)
                    .ok());
    ASSERT_TRUE(outfile->Close().ok());

    // Verify ARROW:schema is stored
    auto input_file = io.fs()->OpenInputFile(temp_parquet_file_).ValueOrDie();
    auto metadata = ::parquet::ReadMetaData(input_file);
    const auto& kv_metadata = metadata->key_value_metadata();
    ASSERT_TRUE(kv_metadata != nullptr);
    ASSERT_TRUE(kv_metadata->FindKey("ARROW:schema") >= 0)
        << "ARROW:schema not found in file metadata";
  }

  // Writes a mixed list/large_list parquet file with stored ARROW:schema.
  void CreateMixedListParquetFileWithArrowSchema() {
    const std::string kParquetFieldIdKey = "PARQUET:field_id";
    auto arrow_schema = ::arrow::schema(
        {::arrow::field("id", ::arrow::int32(), /*nullable=*/false,
                        ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"1"})),
         ::arrow::field(
             "small_lists",
             ::arrow::list(::arrow::field(
                 "element", ::arrow::int32(), /*nullable=*/true,
                 ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"101"}))),
             /*nullable=*/true,
             ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"2"})),
         ::arrow::field(
             "large_lists",
             ::arrow::large_list(::arrow::field(
                 "element", ::arrow::int32(), /*nullable=*/true,
                 ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"102"}))),
             /*nullable=*/true,
             ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"3"}))});
    auto batch = ::arrow::RecordBatch::FromStructArray(
                     ::arrow::json::ArrayFromJSONString(
                         ::arrow::struct_(arrow_schema->fields()),
                         R"([[1, [10, 20], [100, 200]], [2, [30], [300]]])")
                         .ValueOrDie())
                     .ValueOrDie();
    auto table = ::arrow::Table::FromRecordBatches(arrow_schema, {batch}).ValueOrDie();

    auto io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
    auto outfile = io.fs()->OpenOutputStream(temp_parquet_file_).ValueOrDie();

    // Build ArrowWriterProperties to store the Arrow schema in ARROW:schema metadata
    auto arrow_writer_props =
        ::parquet::ArrowWriterProperties::Builder().store_schema()->build();

    // write a single row group so that one batch holds every row
    ASSERT_TRUE(::parquet::arrow::WriteTable(
                    *table, ::arrow::default_memory_pool(), outfile, table->num_rows(),
                    ::parquet::default_writer_properties(), arrow_writer_props)
                    .ok());
    ASSERT_TRUE(outfile->Close().ok());

    // Verify ARROW:schema is stored
    auto input_file = io.fs()->OpenInputFile(temp_parquet_file_).ValueOrDie();
    auto metadata = ::parquet::ReadMetaData(input_file);
    const auto& kv_metadata = metadata->key_value_metadata();
    ASSERT_TRUE(kv_metadata != nullptr);
    ASSERT_TRUE(kv_metadata->FindKey("ARROW:schema") >= 0)
        << "ARROW:schema not found in file metadata";
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

  void VerifyEmptySplitSchema(const std::shared_ptr<Schema>& projection,
                              const ReaderProperties& properties,
                              const std::shared_ptr<::arrow::DataType>& expected_type) {
    ReaderOptions options{.path = temp_parquet_file_,
                          .io = file_io_,
                          .projection = projection,
                          .properties = properties};
    ICEBERG_UNWRAP_OR_FAIL(
        auto full_reader, ReaderFactoryRegistry::Open(FileFormatType::kParquet, options));
    ICEBERG_UNWRAP_OR_FAIL(auto full_c_schema, full_reader->Schema());
    auto full_schema = ::arrow::ImportSchema(&full_c_schema).ValueOrDie();
    ASSERT_THAT(full_reader->Close(), IsOk());

    auto& io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
    auto file_info = io.fs()->GetFileInfo(temp_parquet_file_).ValueOrDie();
    options.split = Split{.offset = static_cast<size_t>(file_info.size()), .length = 1};
    ICEBERG_UNWRAP_OR_FAIL(
        auto reader, ReaderFactoryRegistry::Open(FileFormatType::kParquet, options));
    ICEBERG_UNWRAP_OR_FAIL(auto arrow_c_schema, reader->Schema());
    auto schema = ::arrow::ImportSchema(&arrow_c_schema).ValueOrDie();
    EXPECT_TRUE(schema->Equals(*full_schema, /*check_metadata=*/true))
        << schema->ToString() << " vs " << full_schema->ToString();
    auto actual_type = ::arrow::struct_(schema->fields());
    EXPECT_TRUE(actual_type->Equals(expected_type))
        << actual_type->ToString() << " vs " << expected_type->ToString();
    ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
    ASSERT_THAT(reader->Close(), IsOk());
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

TEST_F(ParquetReaderTest, ReadWithoutFieldIdsOrNameMapping) {
  ASSERT_NO_FATAL_FAILURE(CreateParquetFileWithoutFieldIds(
      ::arrow::struct_({::arrow::field("id", ::arrow::int32())}), R"([[1]])"));
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "id", int32())});

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet,
      {.path = temp_parquet_file_, .io = file_io_, .projection = schema});
  ASSERT_THAT(reader_result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(reader_result, HasErrorMessage("no field IDs and no name mapping"));
}

TEST_F(ParquetReaderTest, ReadWithNameMapping) {
  ASSERT_NO_FATAL_FAILURE(CreateParquetFileWithoutFieldIds(
      ::arrow::struct_({::arrow::field("id", ::arrow::int32()),
                        ::arrow::field("name", ::arrow::utf8())}),
      R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])"));
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(2, "full_name", string()),
      SchemaField::MakeOptional(1, "id", int64()),
      SchemaField::MakeOptional(3, "extra", string()),
  });
  auto mapping = NameMapping::Make({
      MappedField{.names = {"id"}, .field_id = 1},
      MappedField{.names = {"name", "full_name"}, .field_id = 2},
  });

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                               {.path = temp_parquet_file_,
                                                .io = file_io_,
                                                .projection = schema,
                                                .name_mapping = std::move(mapping)}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(
      *reader, R"([["Foo", 1, null], ["Bar", 2, null], ["Baz", 3, null]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, ReadWithFieldIdsAndNameMapping) {
  CreateSimpleParquetFile();
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "name", string()),
  });
  auto mapping = NameMapping::Make({
      MappedField{.names = {"id"}, .field_id = 2},
      MappedField{.names = {"name"}, .field_id = 1},
  });

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                               {.path = temp_parquet_file_,
                                                .io = file_io_,
                                                .projection = schema,
                                                .name_mapping = std::move(mapping)}));
  ASSERT_NO_FATAL_FAILURE(
      VerifyNextBatch(*reader, R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, ReadNestedFieldsWithNameMapping) {
  auto value_type = ::arrow::struct_({::arrow::field("value", ::arrow::int32())});
  auto type = ::arrow::struct_({
      ::arrow::field("profile", value_type),
      ::arrow::field("numbers", ::arrow::list(::arrow::field("item", value_type))),
      ::arrow::field("counts", ::arrow::map(::arrow::utf8(), value_type)),
  });
  const std::string json = R"([
    [[1], [[2], null], [["a", [3]], ["b", null]]],
    [null, [], []],
    [[null], null, null]
  ])";
  ASSERT_NO_FATAL_FAILURE(CreateParquetFileWithoutFieldIds(type, json));

  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "profile",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeOptional(101, "value", int64()),
                                })),
      SchemaField::MakeOptional(2, "numbers",
                                std::make_shared<ListType>(SchemaField::MakeOptional(
                                    201, "element",
                                    std::make_shared<StructType>(std::vector<SchemaField>{
                                        SchemaField::MakeOptional(202, "value", int64()),
                                    })))),
      SchemaField::MakeOptional(
          3, "counts",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(301, "key", string()),
              SchemaField::MakeOptional(
                  302, "value",
                  std::make_shared<StructType>(std::vector<SchemaField>{
                      SchemaField::MakeOptional(303, "value", int64()),
                  })))),
  });
  ICEBERG_UNWRAP_OR_FAIL(auto unique_mapping, CreateMapping(*schema));
  std::shared_ptr<NameMapping> mapping = std::move(unique_mapping);

  for (bool use_large_list : {false, true}) {
    SCOPED_TRACE(use_large_list);
    ReaderProperties properties;
    properties.Set(ReaderProperties::kArrowUseLargeList, use_large_list);
    ICEBERG_UNWRAP_OR_FAIL(
        auto reader, ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                                 {.path = temp_parquet_file_,
                                                  .io = file_io_,
                                                  .projection = schema,
                                                  .name_mapping = mapping,
                                                  .properties = std::move(properties)}));
    ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, json));
    ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
  }
}

TEST_F(ParquetReaderTest, ReadMissingFieldsWithDefaults) {
  // The file contains only fields 1 and 2; the projected schema adds fields 3 and 4
  // with initial-defaults, which are filled for all rows written before the columns
  // existed.
  CreateSimpleParquetFile();

  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "name", string()),
      SchemaField(3, "score", int64(), /*optional=*/false, /*doc=*/{},
                  std::make_shared<const Literal>(Literal::Long(100))),
      SchemaField(4, "status", string(), /*optional=*/true, /*doc=*/{},
                  std::make_shared<const Literal>(Literal::String("active"))),
  });

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet,
      {.path = temp_parquet_file_, .io = file_io_, .projection = schema});
  ASSERT_THAT(reader_result, IsOk())
      << "Failed to create reader: " << reader_result.error().message;
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader,
                                          R"([[1, "Foo", 100, "active"],
                                              [2, "Bar", 100, "active"],
                                              [3, "Baz", 100, "active"]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, ReadOnlyDefaultColumn) {
  // The projected schema selects a single column that is absent from the file, so no
  // physical column is read. The default must still be filled once per row, using the
  // file's row count as the anchor.
  CreateSimpleParquetFile();

  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField(3, "score", int64(), /*optional=*/false, /*doc=*/{},
                  std::make_shared<const Literal>(Literal::Long(100))),
  });

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet,
      {.path = temp_parquet_file_, .io = file_io_, .projection = schema});
  ASSERT_THAT(reader_result, IsOk())
      << "Failed to create reader: " << reader_result.error().message;
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[100], [100], [100]])"));
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

TEST_F(ParquetReaderTest, ReadSmallDecimalsWithStoredArrowSchema) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "value", decimal(20, 2)),
  });
  for (const auto& type : {::arrow::decimal32(9, 2), ::arrow::decimal64(18, 2)}) {
    SCOPED_TRACE(type->ToString());
    auto arrow_schema = ::arrow::schema(
        {::arrow::field("value", type, /*nullable=*/false,
                        ::arrow::key_value_metadata({"PARQUET:field_id"}, {"1"}))});
    auto values = ::arrow::json::ArrayFromJSONString(type, R"(["-1.23", "0.00", "4.56"])")
                      .ValueOrDie();
    auto table = ::arrow::Table::Make(arrow_schema, {values});
    auto& io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
    auto output = io.fs()->OpenOutputStream(temp_parquet_file_).ValueOrDie();
    ASSERT_TRUE(::parquet::arrow::WriteTable(
                    *table, ::arrow::default_memory_pool(), output, table->num_rows(),
                    ::parquet::WriterProperties::Builder()
                        .enable_store_decimal_as_integer()
                        ->build(),
                    ::parquet::ArrowWriterProperties::Builder().store_schema()->build())
                    .ok());
    ASSERT_TRUE(output->Close().ok());

    auto reader = ReaderFactoryRegistry::Open(
        FileFormatType::kParquet,
        {.path = temp_parquet_file_, .io = file_io_, .projection = schema});
    ASSERT_THAT(reader, IsOk());
    ASSERT_NO_FATAL_FAILURE(
        VerifyNextBatch(**reader, R"([["-1.23"], ["0.00"], ["4.56"]])"));
    ASSERT_NO_FATAL_FAILURE(VerifyExhausted(**reader));
  }
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

TEST_F(ParquetReaderTest, ReadListType) {
  CreateListParquetFile();

  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "numbers",
                                std::make_shared<ListType>(SchemaField::MakeOptional(
                                    /*field_id=*/101, "element", int32())))});

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet,
      {.path = temp_parquet_file_, .io = file_io_, .projection = schema});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  // By default list columns are read as 32-bit offset list arrays.
  auto schema_result = reader->Schema();
  ASSERT_THAT(schema_result, IsOk());
  auto arrow_c_schema = std::move(schema_result.value());
  auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();
  ASSERT_EQ(arrow_type->field(1)->type()->id(), ::arrow::Type::LIST);

  ASSERT_NO_FATAL_FAILURE(
      VerifyNextBatch(*reader, R"([[1, [1, 2]], [2, [3]], [3, null]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, ReadListAsLargeList) {
  CreateListParquetFile();

  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "numbers",
                                std::make_shared<ListType>(SchemaField::MakeOptional(
                                    /*field_id=*/101, "element", int32())))});

  ReaderProperties reader_properties;
  reader_properties.Set(ReaderProperties::kArrowUseLargeList, true);

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet, {.path = temp_parquet_file_,
                                 .io = file_io_,
                                 .projection = schema,
                                 .properties = std::move(reader_properties)});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  // The output schema should expose list columns as large_list.
  auto schema_result = reader->Schema();
  ASSERT_THAT(schema_result, IsOk());
  auto arrow_c_schema = std::move(schema_result.value());
  auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();
  ASSERT_EQ(arrow_type->field(1)->type()->id(), ::arrow::Type::LARGE_LIST);

  // JSON parsing creates regular ListArray, so verify large_list data manually.
  auto data = reader->Next();
  ASSERT_THAT(data, IsOk());
  ASSERT_TRUE(data.value().has_value());
  auto arrow_c_array = data.value().value();
  auto arrow_array = ::arrow::ImportArray(&arrow_c_array, arrow_type).ValueOrDie();

  const auto& struct_array =
      internal::checked_cast<const ::arrow::StructArray&>(*arrow_array);
  ASSERT_EQ(struct_array.length(), 3);

  const auto& id_array =
      internal::checked_cast<const ::arrow::Int32Array&>(*struct_array.field(0));
  ASSERT_EQ(id_array.Value(0), 1);
  ASSERT_EQ(id_array.Value(1), 2);
  ASSERT_EQ(id_array.Value(2), 3);

  const auto& numbers_array =
      internal::checked_cast<const ::arrow::LargeListArray&>(*struct_array.field(1));
  ASSERT_EQ(numbers_array.value_slice(0)->length(), 2);
  ASSERT_EQ(numbers_array.value_slice(1)->length(), 1);
  ASSERT_TRUE(numbers_array.IsNull(2));

  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, ReadListAsLargeListWithArrowSchema) {
  // Reading a file that carries serialized ARROW:schema metadata must report an output
  // schema that describes the arrays the reader actually produces. Arrow decides the list
  // type of the arrays, so the output schema follows the schema of the reader instead of
  // assuming that the requested large_list type was applied.
  CreateListParquetFileWithArrowSchema();

  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "numbers",
                                std::make_shared<ListType>(SchemaField::MakeOptional(
                                    /*field_id=*/101, "element", int32())))});

  ReaderProperties reader_properties;
  reader_properties.Set(ReaderProperties::kArrowUseLargeList, true);

  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet, {.path = temp_parquet_file_,
                                 .io = file_io_,
                                 .projection = schema,
                                 .properties = std::move(reader_properties)});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  auto schema_result = reader->Schema();
  ASSERT_THAT(schema_result, IsOk());
  auto arrow_c_schema = std::move(schema_result.value());
  auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();
  auto list_type_id = arrow_type->field(1)->type()->id();
  ASSERT_TRUE(list_type_id == ::arrow::Type::LIST ||
              list_type_id == ::arrow::Type::LARGE_LIST)
      << "unexpected list type: " << arrow_type->field(1)->type()->ToString();

  auto data = reader->Next();
  ASSERT_THAT(data, IsOk());
  ASSERT_TRUE(data.value().has_value());
  auto arrow_c_array = data.value().value();

  // Importing the array against the reported schema fails if the two disagree, which is
  // what this test guards: the output schema must not claim a list type that the reader
  // did not produce.
  auto import_result = ::arrow::ImportArray(&arrow_c_array, arrow_type);
  ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
  auto arrow_array = import_result.ValueOrDie();
  ASSERT_TRUE(arrow_array->ValidateFull().ok());

  const auto& struct_array =
      internal::checked_cast<const ::arrow::StructArray&>(*arrow_array);
  ASSERT_EQ(struct_array.length(), 2);

  const auto& id_array =
      internal::checked_cast<const ::arrow::Int32Array&>(*struct_array.field(0));
  ASSERT_EQ(id_array.Value(0), 1);
  ASSERT_EQ(id_array.Value(1), 2);

  // The list offsets are 32 or 64 bit wide depending on the type the reader produced.
  ASSERT_EQ(struct_array.field(1)->type()->id(), list_type_id);
  if (list_type_id == ::arrow::Type::LARGE_LIST) {
    const auto& numbers_array =
        internal::checked_cast<const ::arrow::LargeListArray&>(*struct_array.field(1));
    ASSERT_EQ(numbers_array.value_slice(0)->length(), 2);
    ASSERT_EQ(numbers_array.value_slice(1)->length(), 1);
  } else {
    const auto& numbers_array =
        internal::checked_cast<const ::arrow::ListArray&>(*struct_array.field(1));
    ASSERT_EQ(numbers_array.value_slice(0)->length(), 2);
    ASSERT_EQ(numbers_array.value_slice(1)->length(), 1);
  }
}

TEST_F(ParquetReaderTest, ReadMixedListAndLargeListWithArrowSchema) {
  // Reading a file with both list and large_list fields (stored via ARROW:schema
  // metadata) must report an output schema that describes the actual types of each field,
  // preserving the per-field list type. This tests the per-field mapping logic.
  CreateMixedListParquetFileWithArrowSchema();

  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "small_lists",
                                std::make_shared<ListType>(SchemaField::MakeOptional(
                                    /*field_id=*/101, "element", int32()))),
      SchemaField::MakeOptional(3, "large_lists",
                                std::make_shared<ListType>(SchemaField::MakeOptional(
                                    /*field_id=*/102, "element", int32())))});

  // Read with default setting (use_large_list=false)
  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet,
      {.path = temp_parquet_file_, .io = file_io_, .projection = schema});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  // Verify the output schema has the correct per-field list types
  auto schema_result = reader->Schema();
  ASSERT_THAT(schema_result, IsOk());
  auto arrow_c_schema = std::move(schema_result.value());
  auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

  // small_lists should be LIST (as stored in the file)
  ASSERT_EQ(arrow_type->field(1)->type()->id(), ::arrow::Type::LIST)
      << "small_lists field should be LIST";

  // large_lists should be LARGE_LIST (as stored in the file)
  ASSERT_EQ(arrow_type->field(2)->type()->id(), ::arrow::Type::LARGE_LIST)
      << "large_lists field should be LARGE_LIST";

  // Verify the arrays are readable with the reported schema
  auto data = reader->Next();
  ASSERT_THAT(data, IsOk());
  ASSERT_TRUE(data.value().has_value());
  auto arrow_c_array = data.value().value();

  auto import_result = ::arrow::ImportArray(&arrow_c_array, arrow_type);
  ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
  auto arrow_array = import_result.ValueOrDie();
  ASSERT_TRUE(arrow_array->ValidateFull().ok());

  const auto& struct_array =
      internal::checked_cast<const ::arrow::StructArray&>(*arrow_array);
  ASSERT_EQ(struct_array.length(), 2);

  // Verify the field types in the actual arrays
  ASSERT_EQ(struct_array.field(1)->type()->id(), ::arrow::Type::LIST);
  ASSERT_EQ(struct_array.field(2)->type()->id(), ::arrow::Type::LARGE_LIST);

  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, ReadRenamedLargeListColumnWithArrowSchema) {
  // The projected column has a different name than the file but the same field id. The
  // output schema must be aligned to the reader by field id, not by name: the file stores
  // this column as large_list (via ARROW:schema), so a name-based match would miss the
  // rename, report the column as list while the array is large_list, and fail to import
  // it.
  CreateMixedListParquetFileWithArrowSchema();

  // field id 3 is stored in the file as a large_list named "large_lists"; project it
  // under a different name to force matching by field id
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(3, "renamed",
                                std::make_shared<ListType>(SchemaField::MakeOptional(
                                    /*field_id=*/102, "element", int32())))});

  // read with the default use_large_list=false, so only field-id matching can preserve
  // large_list
  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet,
      {.path = temp_parquet_file_, .io = file_io_, .projection = schema});
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  auto schema_result = reader->Schema();
  ASSERT_THAT(schema_result, IsOk());
  auto arrow_c_schema = std::move(schema_result.value());
  auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

  // the renamed column keeps the file's large_list type because it is matched by field id
  ASSERT_EQ(arrow_type->field(0)->type()->id(), ::arrow::Type::LARGE_LIST)
      << "renamed column must keep the file's large_list type, matched by field id";

  // importing the produced array against the reported schema must succeed; a name-based
  // mismatch would report list here while the array is large_list and this import would
  // fail
  auto data = reader->Next();
  ASSERT_THAT(data, IsOk());
  ASSERT_TRUE(data.value().has_value());
  auto arrow_c_array = data.value().value();
  auto import_result = ::arrow::ImportArray(&arrow_c_array, arrow_type);
  ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
  ASSERT_TRUE(import_result.ValueOrDie()->ValidateFull().ok());

  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, EmptySplitReadsListsAsLargeListWhenRequested) {
  ASSERT_NO_FATAL_FAILURE(CreateListParquetFile());
  auto schema =
      std::make_shared<Schema>(std::vector<SchemaField>{SchemaField::MakeOptional(
          2, "numbers", list(SchemaField::MakeOptional(101, "element", int32())))});

  for (bool use_large_list : {false, true}) {
    SCOPED_TRACE(use_large_list);
    ReaderProperties properties;
    properties.Set(ReaderProperties::kArrowUseLargeList, use_large_list);
    auto element = ::arrow::field("element", ::arrow::int32());
    auto list_type =
        use_large_list ? ::arrow::large_list(element) : ::arrow::list(element);
    auto expected_type = ::arrow::struct_({::arrow::field("numbers", list_type)});
    ASSERT_NO_FATAL_FAILURE(VerifyEmptySplitSchema(schema, properties, expected_type));
  }
}

TEST_F(ParquetReaderTest, EmptySplitKeepsStoredListTypes) {
  ASSERT_NO_FATAL_FAILURE(CreateMixedListParquetFileWithArrowSchema());
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(3, "renamed",
                                list(SchemaField::MakeOptional(102, "element", int32()))),
      SchemaField::MakeOptional(
          2, "small_lists", list(SchemaField::MakeOptional(101, "element", int32())))});
  auto element = ::arrow::field("element", ::arrow::int32());
  auto expected_type =
      ::arrow::struct_({::arrow::field("renamed", ::arrow::large_list(element)),
                        ::arrow::field("small_lists", ::arrow::list(element))});

  for (bool use_large_list : {false, true}) {
    SCOPED_TRACE(use_large_list);
    ReaderProperties properties;
    properties.Set(ReaderProperties::kArrowUseLargeList, use_large_list);
    ASSERT_NO_FATAL_FAILURE(VerifyEmptySplitSchema(schema, properties, expected_type));
  }
}

TEST_F(ParquetReaderTest, EmptySplitAppliesLargeListToMissingFields) {
  ASSERT_NO_FATAL_FAILURE(CreateSimpleParquetFile());
  for (bool include_physical_field : {false, true}) {
    SCOPED_TRACE(include_physical_field);
    for (bool use_large_list : {false, true}) {
      SCOPED_TRACE(use_large_list);
      auto element = ::arrow::field("element", ::arrow::int32());
      auto list_type =
          use_large_list ? ::arrow::large_list(element) : ::arrow::list(element);
      std::vector<SchemaField> fields{SchemaField::MakeOptional(
          9, "missing", list(SchemaField::MakeOptional(109, "element", int32())))};
      ::arrow::FieldVector expected_fields{::arrow::field("missing", list_type)};
      if (include_physical_field) {
        fields.push_back(SchemaField::MakeRequired(1, "id", int32()));
        expected_fields.push_back(::arrow::field("id", ::arrow::int32(), false));
      }
      auto schema = std::make_shared<Schema>(std::move(fields));
      ReaderProperties properties;
      properties.Set(ReaderProperties::kArrowUseLargeList, use_large_list);
      ASSERT_NO_FATAL_FAILURE(VerifyEmptySplitSchema(
          schema, properties, ::arrow::struct_(std::move(expected_fields))));
    }
  }
}

TEST_F(ParquetReaderTest, EmptySplitWithoutPhysicalColumns) {
  ASSERT_NO_FATAL_FAILURE(CreateSimpleParquetFile());
  for (const auto& fields : {std::vector<SchemaField>{},
                             std::vector<SchemaField>{MetadataColumns::kFilePath,
                                                      MetadataColumns::kRowPosition}}) {
    SCOPED_TRACE(fields.size());
    auto schema = std::make_shared<Schema>(fields);
    ArrowSchema arrow_c_schema;
    ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
    auto expected_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();
    ASSERT_NO_FATAL_FAILURE(VerifyEmptySplitSchema(schema, {}, expected_type));
  }
}

TEST_F(ParquetReaderTest, EmptySplitKeepsPrunedNestedListTypes) {
  auto write_schema =
      std::make_shared<Schema>(std::vector<SchemaField>{SchemaField::MakeOptional(
          1, "profile",
          struct_({SchemaField::MakeOptional(2, "ignored", int32()),
                   SchemaField::MakeOptional(
                       3, "numbers",
                       list(SchemaField::MakeOptional(103, "element", int32())))}))});
  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(*write_schema, &arrow_c_schema), IsOk());
  auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();
  auto array = ::arrow::json::ArrayFromJSONString(
                   arrow_type, R"([[{"ignored": 1, "numbers": [1, 2]}]])")
                   .ValueOrDie();
  WriterProperties writer_properties;
  writer_properties.Set(WriterProperties::kParquetCompression,
                        std::string("uncompressed"));
  ASSERT_THAT(WriteArray(array, {.path = temp_parquet_file_,
                                 .schema = write_schema,
                                 .io = file_io_,
                                 .properties = std::move(writer_properties)}),
              IsOk());

  auto schema =
      std::make_shared<Schema>(std::vector<SchemaField>{SchemaField::MakeOptional(
          1, "profile",
          struct_({SchemaField::MakeOptional(
              3, "renamed",
              list(SchemaField::MakeOptional(103, "element", int32())))}))});
  for (bool use_large_list : {false, true}) {
    SCOPED_TRACE(use_large_list);
    ReaderProperties properties;
    properties.Set(ReaderProperties::kArrowUseLargeList, use_large_list);
    auto element = ::arrow::field("element", ::arrow::int32());
    auto list_type =
        use_large_list ? ::arrow::large_list(element) : ::arrow::list(element);
    auto expected_type = ::arrow::struct_({::arrow::field(
        "profile", ::arrow::struct_({::arrow::field("renamed", list_type)}))});
    ASSERT_NO_FATAL_FAILURE(VerifyEmptySplitSchema(schema, properties, expected_type));
  }
}

TEST_F(ParquetReaderTest, ReadSplit) {
  CreateSplitParquetFile();

  // Read row-group starts and midpoints
  auto io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
  auto input_stream = io.fs()->OpenInputFile(temp_parquet_file_).ValueOrDie();
  auto metadata = ::parquet::ReadMetaData(input_stream);
  const int64_t file_size = input_stream->GetSize().ValueOrDie();
  std::vector<size_t> starts;
  std::vector<size_t> midpoints;
  for (int i = 0; i < metadata->num_row_groups(); ++i) {
    auto column = metadata->RowGroup(i)->ColumnChunk(0);
    ASSERT_TRUE(column->has_dictionary_page());
    ASSERT_LT(column->dictionary_page_offset(), column->data_page_offset());
    starts.push_back(static_cast<size_t>(column->dictionary_page_offset()));
    ICEBERG_UNWRAP_OR_FAIL(auto midpoint,
                           RowGroupMidpoint(*metadata->RowGroup(i), file_size));
    ASSERT_GT(static_cast<size_t>(midpoint), starts.back());
    midpoints.push_back(static_cast<size_t>(midpoint));
  }
  ASSERT_EQ(starts.size(), 2);

  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      MetadataColumns::kRowPosition,
  });

  std::vector<Split> splits = {
      {.offset = 0, .length = std::numeric_limits<size_t>::max()},
      {.offset = starts[0], .length = starts[1] - starts[0]},
      {.offset = midpoints[1], .length = 1},
      {.offset = midpoints[1] + 1, .length = std::numeric_limits<size_t>::max()},
      {.offset = 0, .length = midpoints[0]},
      {.offset = midpoints[0] + 1, .length = midpoints[1] - midpoints[0]},
      {.offset = starts[0], .length = 0},
      // A row group belongs to the split containing its midpoint, not its start.
      {.offset = starts[1], .length = 1},
      {.offset = starts[0] + 1, .length = midpoints[0] - starts[0]},
  };
  std::vector<std::string> expected_json = {
      R"([[1, 0], [2, 1], [3, 2]])",
      R"([[1, 0], [2, 1]])",
      R"([[3, 2]])",
      "",
      "",
      R"([[3, 2]])",
      "",
      "",
      R"([[1, 0], [2, 1]])",
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

TEST_F(ParquetReaderTest, ReadSplitWithoutDictionaryPages) {
  ASSERT_NO_FATAL_FAILURE(
      CreateSplitParquetFile(/*row_group_size=*/2, /*dictionary=*/false));
  ICEBERG_UNWRAP_OR_FAIL(auto input,
                         arrow::OpenArrowInputStream(file_io_, temp_parquet_file_));
  auto metadata = ::parquet::ReadMetaData(input);
  auto column = metadata->RowGroup(1)->ColumnChunk(0);
  ASSERT_FALSE(column->has_dictionary_page());
  const int64_t file_size = input->GetSize().ValueOrDie();
  EXPECT_THAT(RowGroupStart(*metadata->RowGroup(1), file_size),
              HasValue(::testing::Eq(column->data_page_offset())));
  ICEBERG_UNWRAP_OR_FAIL(auto midpoint,
                         RowGroupMidpoint(*metadata->RowGroup(1), file_size));
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()), MetadataColumns::kRowPosition});
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      ReaderFactoryRegistry::Open(
          FileFormatType::kParquet,
          {.path = temp_parquet_file_,
           .split = Split{.offset = static_cast<size_t>(midpoint), .length = 1},
           .io = file_io_,
           .projection = schema}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[3, 2]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(ParquetReaderTest, ReadSplitsWithOmittedRowGroupOffsets) {
  auto arrow_schema = ::arrow::schema(
      {::arrow::field("id", ::arrow::int32(), /*nullable=*/false,
                      ::arrow::KeyValueMetadata::Make({"PARQUET:field_id"}, {"1"}))});
  auto table = ::arrow::Table::Make(
      arrow_schema,
      {::arrow::json::ArrayFromJSONString(::arrow::int32(), "[1, 2, 3, 4, 5, 6]")
           .ValueOrDie()});
  ::parquet::WriterProperties::Builder properties_builder;
  properties_builder.version(::parquet::ParquetVersion::PARQUET_2_6)
      ->compression(::parquet::Compression::UNCOMPRESSED);
  auto properties = properties_builder.build();
  ICEBERG_UNWRAP_OR_FAIL(auto original_output,
                         arrow::OpenArrowOutputStream(file_io_, temp_parquet_file_));
  ASSERT_TRUE(::parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(),
                                           original_output, /*chunk_size=*/2, properties)
                  .ok());
  ASSERT_TRUE(original_output->Close().ok());
  ICEBERG_UNWRAP_OR_FAIL(auto original_input,
                         arrow::OpenArrowInputStream(file_io_, temp_parquet_file_));
  auto original_metadata = ::parquet::ReadMetaData(original_input);
  auto original_size = original_input->GetSize().ValueOrDie();
  auto pages = original_input->ReadAt(0, original_size - original_metadata->size() - 8)
                   .ValueOrDie();
  ASSERT_TRUE(original_input->Close().ok());

  auto builder =
      ::parquet::FileMetaDataBuilder::Make(original_metadata->schema(), properties);
  for (int i = 0; i < original_metadata->num_row_groups(); ++i) {
    auto original_row_group = original_metadata->RowGroup(i);
    auto row_group = builder->AppendRowGroup();
    row_group->set_num_rows(original_row_group->num_rows());
    for (int j = 0; j < original_row_group->num_columns(); ++j) {
      auto column = original_row_group->ColumnChunk(j);
      row_group->NextColumnChunk()->Finish(
          column->num_values(), column->dictionary_page_offset(),
          /*index_page_offset=*/0, column->data_page_offset(),
          column->total_compressed_size(), column->total_uncompressed_size(),
          column->has_dictionary_page(), /*dictionary_fallback=*/false, {}, {});
    }
    // RowGroupMetaDataBuilder::Finish fills in the optional file_offset and
    // total_compressed_size. Leave them unset to exercise legacy footer handling.
  }
  auto rebuilt_metadata = builder->Finish(original_metadata->key_value_metadata());
  ICEBERG_UNWRAP_OR_FAIL(auto output,
                         arrow::OpenArrowOutputStream(file_io_, temp_parquet_file_));
  ASSERT_TRUE(output->Write(pages).ok());
  ::parquet::WriteFileMetaData(*rebuilt_metadata, output.get());
  ASSERT_TRUE(output->Close().ok());

  ICEBERG_UNWRAP_OR_FAIL(auto input,
                         arrow::OpenArrowInputStream(file_io_, temp_parquet_file_));
  auto metadata = ::parquet::ReadMetaData(input);
  ASSERT_EQ(metadata->num_columns(), 1);
  ASSERT_EQ(metadata->num_rows(), 6);
  ASSERT_EQ(metadata->num_row_groups(), 3);
  for (int i = 0; i < metadata->num_row_groups(); ++i) {
    auto row_group = metadata->RowGroup(i);
    ASSERT_EQ(row_group->num_rows(), 2);
    ASSERT_EQ(row_group->file_offset(), 0);
    ASSERT_EQ(row_group->total_compressed_size(), 0);
    auto column = row_group->ColumnChunk(0);
    ASSERT_TRUE(column->has_dictionary_page());
    ASSERT_LT(column->dictionary_page_offset(), column->data_page_offset());
  }
  auto boundary = static_cast<size_t>(
      metadata->RowGroup(1)->ColumnChunk(0)->dictionary_page_offset());
  auto file_size = static_cast<size_t>(input->GetSize().ValueOrDie());
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()), MetadataColumns::kRowPosition,
      MetadataColumns::kRowId, MetadataColumns::kLastUpdatedSequenceNumber});
  const std::vector<std::optional<Split>> splits{
      std::nullopt, Split{.offset = 0, .length = boundary},
      Split{.offset = boundary, .length = file_size - boundary}};
  const std::vector<std::string> expected{
      R"([[1, 0, 100, 7], [2, 1, 101, 7], [3, 2, 102, 7], [4, 3, 103, 7], [5, 4, 104, 7], [6, 5, 105, 7]])",
      R"([[1, 0, 100, 7], [2, 1, 101, 7]])",
      R"([[3, 2, 102, 7], [4, 3, 103, 7], [5, 4, 104, 7], [6, 5, 105, 7]])"};
  for (size_t i = 0; i < splits.size(); ++i) {
    SCOPED_TRACE(i);
    ICEBERG_UNWRAP_OR_FAIL(auto reader,
                           ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                                       {.path = temp_parquet_file_,
                                                        .length = file_size,
                                                        .split = splits[i],
                                                        .io = file_io_,
                                                        .projection = schema,
                                                        .first_row_id = 100,
                                                        .data_sequence_number = 7}));
    ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, expected[i]));
    ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
  }
}

TEST_F(ParquetReaderTest, RejectsSplitWithInvalidPageOffsetsBeforeReading) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "col0", int32())});
  for (int64_t offset : {int64_t{-1}, int64_t{0}, std::numeric_limits<int64_t>::max()}) {
    auto metadata = MakeMetadataWithPageOffsets({{offset, std::nullopt}});
    ICEBERG_UNWRAP_OR_FAIL(auto output,
                           arrow::OpenArrowOutputStream(file_io_, temp_parquet_file_));
    ::parquet::WriteMetaDataFile(*metadata, output.get());
    ASSERT_TRUE(output->Close().ok());
    auto reader = ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                              {.path = temp_parquet_file_,
                                               .split = Split{.offset = 0, .length = 1},
                                               .io = file_io_,
                                               .projection = schema});
    EXPECT_THAT(reader, IsError(ErrorKind::kInvalid));
    EXPECT_THAT(reader, HasErrorMessage("row group 0"));
    EXPECT_THAT(reader, HasErrorMessage(temp_parquet_file_));
  }
}

TEST_F(ParquetReaderTest, ReadSplitWithNonmonotonicRowGroupStarts) {
  ASSERT_NO_FATAL_FAILURE(CreateSplitParquetFile(/*row_group_size=*/1));
  // Logical row order becomes [1, 3, 2], while the pages stay in physical order [1, 2,
  // 3].
  ASSERT_NO_FATAL_FAILURE(ReorderRowGroups({0, 2, 1}));
  ICEBERG_UNWRAP_OR_FAIL(auto input,
                         arrow::OpenArrowInputStream(file_io_, temp_parquet_file_));
  auto metadata = ::parquet::ReadMetaData(input);
  auto last_start = static_cast<size_t>(
      metadata->RowGroup(1)->ColumnChunk(0)->dictionary_page_offset());
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()), MetadataColumns::kRowPosition,
      MetadataColumns::kRowId, MetadataColumns::kLastUpdatedSequenceNumber});

  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                  {.path = temp_parquet_file_,
                                   .split = Split{.offset = 0, .length = last_start},
                                   .io = file_io_,
                                   .projection = schema,
                                   .first_row_id = 100,
                                   .data_sequence_number = 7}));
  // A batch can span skipped row groups while keeping absolute positions and lineage.
  ASSERT_NO_FATAL_FAILURE(
      VerifyNextBatch(*reader, R"([[1, 0, 100, 7], [2, 2, 102, 7]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));

  ICEBERG_UNWRAP_OR_FAIL(
      auto tail, ReaderFactoryRegistry::Open(
                     FileFormatType::kParquet,
                     {.path = temp_parquet_file_,
                      .split = Split{.offset = last_start,
                                     .length = std::numeric_limits<size_t>::max()},
                      .io = file_io_,
                      .projection = schema,
                      .first_row_id = 100,
                      .data_sequence_number = 7}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*tail, R"([[3, 1, 101, 7]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*tail));
}

TEST_F(ParquetReaderTest, ReadMetadataOnlySplitWithNonmonotonicRowGroups) {
  ASSERT_NO_FATAL_FAILURE(CreateSplitParquetFile(/*row_group_size=*/1));
  ASSERT_NO_FATAL_FAILURE(ReorderRowGroups({0, 2, 1}));
  ICEBERG_UNWRAP_OR_FAIL(auto input,
                         arrow::OpenArrowInputStream(file_io_, temp_parquet_file_));
  auto metadata = ::parquet::ReadMetaData(input);
  auto last_start = static_cast<size_t>(
      metadata->RowGroup(1)->ColumnChunk(0)->dictionary_page_offset());
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{MetadataColumns::kRowPosition, MetadataColumns::kRowId,
                               MetadataColumns::kLastUpdatedSequenceNumber});

  for (int64_t batch_size : {1, 100}) {
    SCOPED_TRACE(batch_size);
    ReaderProperties properties;
    properties.Set(ReaderProperties::kBatchSize, batch_size);
    ICEBERG_UNWRAP_OR_FAIL(
        auto reader,
        ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                    {.path = temp_parquet_file_,
                                     .split = Split{.offset = 0, .length = last_start},
                                     .io = file_io_,
                                     .projection = schema,
                                     .first_row_id = 100,
                                     .data_sequence_number = 7,
                                     .properties = properties}));
    // Arrow emits one batch per row group when no physical columns are read.
    ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[0, 100, 7]])"));
    ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[2, 102, 7]])"));
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

TEST_F(ParquetReaderTest, ReadRowLineage) {
  temp_parquet_file_ = "row_lineage.parquet";
  CreateSimpleParquetFile();

  ICEBERG_UNWRAP_OR_FAIL(auto reader, OpenRowLineageReader(RowLineageSchema(), 100L, 7L));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[1, 100, 7],
                                                       [2, 101, 7],
                                                       [3, 102, 7]])"));
}

TEST_F(ParquetReaderTest, ReadPartialLineage) {
  temp_parquet_file_ = "partial_lineage.parquet";
  CreateSimpleParquetFile();

  auto schema = RowLineageSchema();
  ICEBERG_UNWRAP_OR_FAIL(auto reader, OpenRowLineageReader(schema));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[1, null, null],
                                                       [2, null, null],
                                                       [3, null, null]])"));

  ICEBERG_UNWRAP_OR_FAIL(auto reader_with_row_id, OpenRowLineageReader(schema, 100L));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader_with_row_id,
                                          R"([[1, 100, null],
                                             [2, 101, null],
                                             [3, 102, null]])"));

  ICEBERG_UNWRAP_OR_FAIL(auto reader_with_sequence,
                         OpenRowLineageReader(schema, std::nullopt, 7L));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader_with_sequence,
                                          R"([[1, null, 7],
                                             [2, null, 7],
                                             [3, null, 7]])"));
}

TEST_F(ParquetReaderTest, ReadPhysicalLineage) {
  temp_parquet_file_ = "physical_lineage.parquet";
  CreateRowLineageParquetFile();

  auto schema = RowLineageSchema();
  ICEBERG_UNWRAP_OR_FAIL(auto reader, OpenRowLineageReader(schema, 100L));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, R"([[1, 100, null],
                                                       [2, 777, 8],
                                                       [3, 102, null]])"));
}

TEST_F(ParquetReaderTest, ReadPositionsAndLineageAcrossRowGroups) {
  temp_parquet_file_ = "positions_across_row_groups.parquet";
  constexpr int64_t kRowGroupRows = 100;
  constexpr int64_t kNumRows = 3 * kRowGroupRows;
  constexpr int64_t kFirstRowId = 1000;

  // Odd rows keep a physical _row_id; even rows inherit `first_row_id + _pos`.
  std::string input_json = "[";
  for (int64_t i = 0; i < kNumRows; ++i) {
    input_json += std::format("{}[{}, {}, null]", i == 0 ? "" : ",", i,
                              i % 2 == 0 ? "null" : std::to_string(5000 + i));
  }
  input_json += "]";

  auto write_schema = RowLineageSchema();
  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(*write_schema, &arrow_c_schema), IsOk());
  auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();
  auto array = ::arrow::json::ArrayFromJSONString(arrow_type, input_json).ValueOrDie();

  WriterProperties writer_properties;
  writer_properties.Set(WriterProperties::kParquetCompression,
                        std::string("uncompressed"));
  writer_properties.Set(WriterProperties::kParquetMaxRowGroupRows, kRowGroupRows);
  ASSERT_THAT(WriteArray(array, {.path = temp_parquet_file_,
                                 .schema = write_schema,
                                 .io = file_io_,
                                 .properties = std::move(writer_properties)}),
              IsOk());

  auto io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
  auto metadata =
      ::parquet::ReadMetaData(io.fs()->OpenInputFile(temp_parquet_file_).ValueOrDie());
  ASSERT_EQ(metadata->num_row_groups(), 3);

  auto read_schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      MetadataColumns::kRowPosition,
      MetadataColumns::kRowId,
  });

  auto expected_json = [&](int64_t start, int64_t end) {
    std::string json = "[";
    for (int64_t pos = start; pos < end; ++pos) {
      json += std::format("{}[{}, {}, {}]", pos == start ? "" : ",", pos, pos,
                          pos % 2 == 0 ? kFirstRowId + pos : 5000 + pos);
    }
    return json + "]";
  };

  ReaderProperties reader_properties;
  reader_properties.Set(ReaderProperties::kBatchSize, int64_t{150});

  // Skip the first row group; the first batch spans the second and third ones.
  const auto second_offset = static_cast<size_t>(metadata->RowGroup(1)->file_offset());
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      ReaderFactoryRegistry::Open(
          FileFormatType::kParquet,
          {.path = temp_parquet_file_,
           .split = Split{.offset = second_offset,
                          .length = std::numeric_limits<size_t>::max() - second_offset},
           .io = file_io_,
           .projection = read_schema,
           .first_row_id = kFirstRowId,
           .properties = reader_properties}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, expected_json(100, 250)));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, expected_json(250, 300)));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));

  // Read only the middle row group.
  const auto third_offset = static_cast<size_t>(metadata->RowGroup(2)->file_offset());
  ICEBERG_UNWRAP_OR_FAIL(
      auto middle_reader,
      ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                  {.path = temp_parquet_file_,
                                   .split = Split{.offset = second_offset,
                                                  .length = third_offset - second_offset},
                                   .io = file_io_,
                                   .projection = read_schema,
                                   .first_row_id = kFirstRowId,
                                   .properties = reader_properties}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*middle_reader, expected_json(100, 200)));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*middle_reader));
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
  // Unknown fields (secret, IDs 5 and 10) are also written as null-type columns.
  EXPECT_THAT(field_ids, ::testing::UnorderedElementsAre(1, 4, 5, 7, 9, 10));

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

  // Unknown fields (mystery, secret) are also written as null-type columns.
  ASSERT_EQ(parquet_schema->num_columns(), 4);
  EXPECT_EQ(parquet_schema->Column(0)->schema_node()->field_id(), 1);
  EXPECT_EQ(parquet_schema->Column(1)->schema_node()->field_id(), 2);
  EXPECT_EQ(parquet_schema->Column(2)->schema_node()->field_id(), 4);
  EXPECT_EQ(parquet_schema->Column(3)->schema_node()->field_id(), 5);

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

TEST_F(ParquetReadWrite, UuidRoundTrip) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "uuid_col", uuid())});

  ::arrow::FixedSizeBinaryBuilder uuid_storage_builder(
      ::arrow::fixed_size_binary(Uuid::kLength));
  ASSERT_TRUE(uuid_storage_builder.Append(kUuidBytes1.data()).ok());
  ASSERT_TRUE(uuid_storage_builder.Append(kUuidBytes2.data()).ok());
  auto uuid_storage = uuid_storage_builder.Finish().ValueOrDie();
  auto uuid_array =
      ::arrow::ExtensionType::WrapArray(::arrow::extension::uuid(), uuid_storage);
  auto array =
      ::arrow::StructArray::Make(
          {uuid_array},
          {::arrow::field("uuid_col", ::arrow::extension::uuid(), /*nullable=*/false)})
          .ValueOrDie();

  std::shared_ptr<::arrow::Array> out;
  DoRoundtrip(array, schema, out);

  ASSERT_TRUE(out->Equals(*array)) << "actual:\n"
                                   << out->ToString() << "\nexpected:\n"
                                   << array->ToString();
}

TEST_F(ParquetReadWrite, GeospatialWkbRoundTrip) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "geom", geometry()),
      SchemaField::MakeOptional(2, "geog",
                                geography("EPSG:4326", EdgeAlgorithm::kAndoyer)),
      SchemaField::MakeOptional(
          3, "nested",
          struct_({SchemaField::MakeOptional(4, "geom", geometry("EPSG:3857"))})),
  });

  ::arrow::BinaryBuilder geometry_builder;
  const std::array<uint8_t, 4> arbitrary_geometry = {0xff, 0x00, 0x7f, 0x42};
  ASSERT_TRUE(
      geometry_builder.Append(arbitrary_geometry.data(), arbitrary_geometry.size()).ok());
  ASSERT_TRUE(geometry_builder.AppendNull().ok());

  ::arrow::BinaryBuilder geography_builder;
  const std::array<uint8_t, 5> arbitrary_geography = {0x01, 0x02, 0x03, 0x04, 0x05};
  ASSERT_TRUE(geography_builder.AppendNull().ok());
  ASSERT_TRUE(
      geography_builder.Append(arbitrary_geography.data(), arbitrary_geography.size())
          .ok());

  ::arrow::BinaryBuilder nested_geometry_builder;
  const std::array<uint8_t, 3> arbitrary_nested_geometry = {0xde, 0xad, 0xbe};
  ASSERT_TRUE(
      nested_geometry_builder
          .Append(arbitrary_nested_geometry.data(), arbitrary_nested_geometry.size())
          .ok());
  ASSERT_TRUE(nested_geometry_builder.AppendNull().ok());
  auto nested =
      ::arrow::StructArray::Make({nested_geometry_builder.Finish().ValueOrDie()},
                                 {::arrow::field("geom", ::arrow::binary())})
          .ValueOrDie();

  auto array =
      ::arrow::StructArray::Make({geometry_builder.Finish().ValueOrDie(),
                                  geography_builder.Finish().ValueOrDie(), nested},
                                 {::arrow::field("geom", ::arrow::binary()),
                                  ::arrow::field("geog", ::arrow::binary()),
                                  ::arrow::field("nested", nested->type())})
          .ValueOrDie();

  std::shared_ptr<::arrow::Array> out;
  DoRoundtrip(array, schema, out);
  ASSERT_NE(out, nullptr);
  ASSERT_TRUE(out->Equals(*array)) << "actual:\n"
                                   << out->ToString() << "\nexpected:\n"
                                   << array->ToString();
}

}  // namespace iceberg::parquet
