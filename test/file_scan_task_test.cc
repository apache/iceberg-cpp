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

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/metadata.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow_array_reader.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/schema.h"
#include "iceberg/table_scan.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "matchers.h"
#include "temp_file_test_base.h"

namespace iceberg {

class FileScanTaskTest : public TempFileTestBase {
 protected:
  static void SetUpTestSuite() { parquet::RegisterAll(); }

  void SetUp() override {
    TempFileTestBase::SetUp();
    file_io_ = arrow::ArrowFileSystemFileIO::MakeLocalFileIO();
    temp_parquet_file_ = CreateNewTempFilePathWithSuffix(".parquet");
    CreateSimpleParquetFile();
  }

  // Helper method to create a Parquet file with sample data.
  // This is identical to the one in ParquetReaderTest.
  void CreateSimpleParquetFile() {
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
                                             outfile, /*chunk_size=*/1024)
                    .ok());
  }

  // Helper method to verify the content of the next batch from the reader.
  // This is identical to the one in ParquetReaderTest and AvroReaderTest.
  void VerifyNextBatch(ArrowArrayReader& reader, std::string_view expected_json) {
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
    ASSERT_TRUE(arrow_array->Equals(*expected_array))
        << "Expected: " << expected_array->ToString()
        << "\nGot: " << arrow_array->ToString();
  }

  // Helper method to verify that the reader is exhausted.
  void VerifyExhausted(ArrowArrayReader& reader) {
    auto data = reader.Next();
    ASSERT_THAT(data, IsOk());
    ASSERT_FALSE(data.value().has_value());
  }

  std::shared_ptr<FileIO> file_io_;
  std::string temp_parquet_file_;
};

TEST_F(FileScanTaskTest, ReadFullSchema) {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = temp_parquet_file_;
  data_file->file_format = FileFormatType::kParquet;

  auto io_internal = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
  data_file->file_size_in_bytes =
      io_internal.fs()->GetFileInfo(temp_parquet_file_).ValueOrDie().size();

  auto projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "name", string())});

  FileScanTask task(data_file);

  auto reader_result = task.ToArrowArrayReader(projected_schema, nullptr, file_io_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(
      VerifyNextBatch(*reader, R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

TEST_F(FileScanTaskTest, ReadProjectedAndReorderedSchema) {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = temp_parquet_file_;
  data_file->file_format = FileFormatType::kParquet;

  auto io_internal = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
  data_file->file_size_in_bytes =
      io_internal.fs()->GetFileInfo(temp_parquet_file_).ValueOrDie().size();

  auto projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(2, "name", string()),
                               SchemaField::MakeOptional(3, "score", float64())});

  FileScanTask task(data_file);

  auto reader_result = task.ToArrowArrayReader(projected_schema, nullptr, file_io_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(
      VerifyNextBatch(*reader, R"([["Foo", null], ["Bar", null], ["Baz", null]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

}  // namespace iceberg
