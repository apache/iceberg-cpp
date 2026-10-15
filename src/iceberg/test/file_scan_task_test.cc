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

#include <cstdint>
#include <limits>
#include <memory>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/metadata.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/schema.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"
#include "iceberg/type.h"

namespace iceberg {

TEST(FileScanTaskRangeTest, WholeFileAndEmptyFile) {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = "test.parquet";
  data_file->file_size_in_bytes = 100;
  data_file->record_count = 7;

  FileScanTask whole(data_file);
  EXPECT_FALSE(whole.is_split());
  EXPECT_EQ(whole.start(), 0);
  EXPECT_EQ(whole.length(), 100);
  EXPECT_EQ(whole.size_bytes(), 100);
  EXPECT_EQ(whole.estimated_row_count(), 7);

  ICEBERG_UNWRAP_OR_FAIL(auto explicit_whole, FileScanTask::MakeSplit(data_file, 0, 100));
  EXPECT_FALSE(explicit_whole->is_split());
  EXPECT_EQ(explicit_whole->start(), 0);
  EXPECT_EQ(explicit_whole->length(), 100);
  EXPECT_EQ(explicit_whole->estimated_row_count(), 7);

  data_file->file_size_in_bytes = 0;
  data_file->record_count = 0;
  ICEBERG_UNWRAP_OR_FAIL(auto empty, FileScanTask::MakeSplit(data_file, 0, 0));
  EXPECT_FALSE(empty->is_split());
  EXPECT_EQ(empty->start(), 0);
  EXPECT_EQ(empty->length(), 0);
  EXPECT_EQ(empty->size_bytes(), 0);
  EXPECT_EQ(empty->estimated_row_count(), 0);
}

TEST(FileScanTaskRangeTest, DistinctSplitsShareMetadataAndEstimateRowsOnce) {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = "test.parquet";
  data_file->file_size_in_bytes = 100;
  data_file->record_count = 7;
  data_file->first_row_id = 1000;
  data_file->data_sequence_number = 22;
  auto delete_file = std::make_shared<DataFile>();
  auto residual = Expressions::AlwaysTrue();

  ICEBERG_UNWRAP_OR_FAIL(
      auto first, FileScanTask::MakeSplit(data_file, 0, 40, {delete_file}, residual));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second, FileScanTask::MakeSplit(data_file, 40, 60, {delete_file}, residual));

  EXPECT_TRUE(first->is_split());
  EXPECT_TRUE(second->is_split());
  EXPECT_EQ(first->start(), 0);
  EXPECT_EQ(first->length(), 40);
  EXPECT_EQ(second->start(), 40);
  EXPECT_EQ(second->length(), 60);
  EXPECT_EQ(first->size_bytes() + second->size_bytes(), 100);
  EXPECT_EQ(first->estimated_row_count(), 2);
  EXPECT_EQ(second->estimated_row_count(), 5);
  EXPECT_EQ(first->estimated_row_count() + second->estimated_row_count(), 7);
  EXPECT_EQ(first->data_file(), data_file);
  EXPECT_EQ(second->data_file(), data_file);
  ASSERT_EQ(first->delete_files().size(), 1);
  ASSERT_EQ(second->delete_files().size(), 1);
  EXPECT_EQ(first->delete_files()[0], delete_file);
  EXPECT_EQ(second->delete_files()[0], delete_file);
  EXPECT_EQ(first->residual_filter(), residual);
  EXPECT_EQ(second->residual_filter(), residual);
  EXPECT_EQ(first->data_file()->first_row_id, 1000);
  EXPECT_EQ(first->data_file()->data_sequence_number, 22);
  EXPECT_EQ(second->data_file()->first_row_id, 1000);
  EXPECT_EQ(second->data_file()->data_sequence_number, 22);
}

TEST(FileScanTaskRangeTest, RejectsInvalidRanges) {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = "test.parquet";
  data_file->file_size_in_bytes = 100;

  const auto expect_invalid = [&](int64_t start, int64_t length) {
    auto result = FileScanTask::MakeSplit(data_file, start, length);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().kind, ErrorKind::kInvalidArgument);
  };
  expect_invalid(-1, 1);
  expect_invalid(0, -1);
  expect_invalid(0, 0);
  expect_invalid(100, 0);
  expect_invalid(101, 1);
  expect_invalid(99, 2);

  auto null_file = FileScanTask::MakeSplit(nullptr, 0, 1);
  ASSERT_FALSE(null_file.has_value());
  EXPECT_EQ(null_file.error().kind, ErrorKind::kInvalidArgument);

  data_file->file_size_in_bytes = -1;
  expect_invalid(0, 1);

  data_file->file_size_in_bytes = std::numeric_limits<int64_t>::max();
  expect_invalid(std::numeric_limits<int64_t>::max() - 1, 2);
}

TEST(FileScanTaskRangeTest, RowEstimateDoesNotOverflow) {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_size_in_bytes = std::numeric_limits<int64_t>::max();
  data_file->record_count = std::numeric_limits<int64_t>::max();

  ICEBERG_UNWRAP_OR_FAIL(
      auto first,
      FileScanTask::MakeSplit(data_file, 0, data_file->file_size_in_bytes - 1));
  ICEBERG_UNWRAP_OR_FAIL(auto last, FileScanTask::MakeSplit(
                                        data_file, data_file->file_size_in_bytes - 1, 1));
  EXPECT_EQ(first->estimated_row_count(), data_file->record_count - 1);
  EXPECT_EQ(last->estimated_row_count(), 1);
}

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

    ICEBERG_UNWRAP_OR_FAIL(auto outfile,
                           arrow::OpenArrowOutputStream(file_io_, temp_parquet_file_));

    ASSERT_TRUE(::parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(),
                                             outfile, chunk_size)
                    .ok());
    ASSERT_TRUE(outfile->Close().ok());
    RefreshParquetFileSize();
  }

  // Helper to create a valid but empty Parquet file.
  void CreateEmptyParquetFile() {
    const std::string kParquetFieldIdKey = "PARQUET:field_id";
    auto arrow_schema = ::arrow::schema(
        {::arrow::field("id", ::arrow::int32(), /*nullable=*/false,
                        ::arrow::KeyValueMetadata::Make({kParquetFieldIdKey}, {"1"}))});
    auto empty_table = ::arrow::Table::FromRecordBatches(arrow_schema, {}).ValueOrDie();

    ICEBERG_UNWRAP_OR_FAIL(auto outfile,
                           arrow::OpenArrowOutputStream(file_io_, temp_parquet_file_));
    ASSERT_TRUE(::parquet::arrow::WriteTable(*empty_table, ::arrow::default_memory_pool(),
                                             outfile, 1024)
                    .ok());
    ASSERT_TRUE(outfile->Close().ok());
    RefreshParquetFileSize();
  }

  void RefreshParquetFileSize() {
    ICEBERG_UNWRAP_OR_FAIL(auto input_file, file_io_->NewInputFile(temp_parquet_file_));
    ICEBERG_UNWRAP_OR_FAIL(auto size, input_file->Size());
    ASSERT_GT(size, 0);
    parquet_file_size_ = size;
  }

  std::shared_ptr<DataFile> MakeDataFile() const {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = temp_parquet_file_;
    data_file->file_format = FileFormatType::kParquet;
    data_file->file_size_in_bytes = parquet_file_size_;
    return data_file;
  }

  Result<ArrowArrayStream> OpenTask(const FileScanTask& task,
                                    std::shared_ptr<Schema> projected_schema) {
    auto current_schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "name", string())});
    FileScanTaskReader::Options options{
        .io = file_io_,
        .table_schema = current_schema,
        .projected_schema = std::move(projected_schema),
    };
    ICEBERG_ASSIGN_OR_RAISE(auto reader, FileScanTaskReader::Make(std::move(options)));
    return reader->Open(task);
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
  int64_t parquet_file_size_ = 0;
};

TEST_F(FileScanTaskTest, ReadFullSchema) {
  auto data_file = MakeDataFile();

  auto projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "name", string())});

  FileScanTask task(data_file);

  ICEBERG_UNWRAP_OR_FAIL(auto stream, OpenTask(task, projected_schema));

  ASSERT_NO_FATAL_FAILURE(
      VerifyStreamNextBatch(&stream, R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])"));
}

TEST_F(FileScanTaskTest, ReadProjectedAndReorderedSchema) {
  auto data_file = MakeDataFile();

  auto projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(2, "name", string()),
                               SchemaField::MakeOptional(3, "score", float64())});

  FileScanTask task(data_file);

  ICEBERG_UNWRAP_OR_FAIL(auto stream, OpenTask(task, projected_schema));

  ASSERT_NO_FATAL_FAILURE(
      VerifyStreamNextBatch(&stream, R"([["Foo", null], ["Bar", null], ["Baz", null]])"));
}

TEST_F(FileScanTaskTest, ReadEmptyFile) {
  CreateEmptyParquetFile();
  auto data_file = MakeDataFile();

  auto projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});

  FileScanTask task(data_file);

  ICEBERG_UNWRAP_OR_FAIL(auto stream, OpenTask(task, projected_schema));

  // The stream should be immediately exhausted
  ASSERT_NO_FATAL_FAILURE(VerifyStreamExhausted(&stream));
}

}  // namespace iceberg
