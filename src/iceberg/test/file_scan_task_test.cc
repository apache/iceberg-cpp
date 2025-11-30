/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/schema.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"

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
  void CreateSimpleParquetFile(int64_t chunk_size = 1024) {
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
                                             outfile, chunk_size)
                    .ok());
  }

  // Helper to create a valid but empty Parquet file.
  void CreateEmptyParquetFile() {
    const std::string kParquetFieldIdKey = "PARQUET:field_id";
    auto arrow_schema = ::arrow::schema(
        {::arrow::field("id", ::arrow::int32(), /*nullable=*/false,
                        ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"1"}))});
    auto empty_table = ::arrow::Table::FromRecordBatches(arrow_schema, {}).ValueOrDie();

    auto io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
    auto outfile = io.fs()->OpenOutputStream(temp_parquet_file_).ValueOrDie();
    ASSERT_TRUE(::parquet::arrow::WriteTable(*empty_table, ::arrow::default_memory_pool(),
                                             outfile, 1024)
                    .ok());
  }

  // Helper method to verify the content of the next batch from an ArrowArrayStream.
  void VerifyStreamNextBatch(struct ArrowArrayStream* stream,
                             std::string_view expected_json) {
    auto record_batch_reader = ::arrow::ImportRecordBatchReader(stream).ValueOrDie();

    auto result = record_batch_reader->Next();
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto actual_batch = result.ValueOrDie();
    ASSERT_NE(actual_batch, nullptr) << "Stream is exhausted but expected more data.";

    auto arrow_schema = actual_batch->schema();
    auto struct_type = ::arrow::struct_(arrow_schema->fields());
    auto expected_array =
        ::arrow::json::ArrayFromJSONString(struct_type, expected_json).ValueOrDie();
    auto expected_batch =
        ::arrow::RecordBatch::FromStructArray(expected_array).ValueOrDie();

    ASSERT_TRUE(actual_batch->Equals(*expected_batch))
        << "Actual batch:\n"
        << actual_batch->ToString() << "\nExpected batch:\n"
        << expected_batch->ToString();
  }

  // Helper method to verify that an ArrowArrayStream is exhausted.
  void VerifyStreamExhausted(struct ArrowArrayStream* stream) {
    auto record_batch_reader = ::arrow::ImportRecordBatchReader(stream).ValueOrDie();
    auto result = record_batch_reader->Next();
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(result.ValueOrDie(), nullptr) << "Reader was not exhausted as expected.";
  }

  std::shared_ptr<FileIO> file_io_;
  std::string temp_parquet_file_;
};

TEST_F(FileScanTaskTest, ReadFullSchema) {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = temp_parquet_file_;
  data_file->file_format = FileFormatType::kParquet;

  auto projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "name", string())});

  FileScanTask task(data_file);

  auto stream_result = task.ToArrow(file_io_, projected_schema, nullptr);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  ASSERT_NO_FATAL_FAILURE(
      VerifyStreamNextBatch(&stream, R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])"));
}

TEST_F(FileScanTaskTest, ReadProjectedAndReorderedSchema) {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = temp_parquet_file_;
  data_file->file_format = FileFormatType::kParquet;

  auto projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(2, "name", string()),
                               SchemaField::MakeOptional(3, "score", float64())});

  FileScanTask task(data_file);

  auto stream_result = task.ToArrow(file_io_, projected_schema, nullptr);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  ASSERT_NO_FATAL_FAILURE(
      VerifyStreamNextBatch(&stream, R"([["Foo", null], ["Bar", null], ["Baz", null]])"));
}

TEST_F(FileScanTaskTest, ReadEmptyFile) {
  CreateEmptyParquetFile();
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = temp_parquet_file_;
  data_file->file_format = FileFormatType::kParquet;

  auto projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});

  FileScanTask task(data_file);

  auto stream_result = task.ToArrow(file_io_, projected_schema, nullptr);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  // The stream should be immediately exhausted
  ASSERT_NO_FATAL_FAILURE(VerifyStreamExhausted(&stream));
}

}  // namespace iceberg
