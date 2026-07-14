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

#include "iceberg/data/file_scan_task_reader.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_register.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/data/deletion_vector_writer.h"
#include "iceberg/data/equality_delete_writer.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/file_format.h"
#include "iceberg/file_io.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

struct ExportedBatch {
  ArrowSchema schema{};
  ArrowArray array{};

  ~ExportedBatch() {
    if (array.release != nullptr) {
      array.release(&array);
    }
    if (schema.release != nullptr) {
      schema.release(&schema);
    }
  }

  ExportedBatch() = default;
  ExportedBatch(const ExportedBatch&) = delete;
  ExportedBatch& operator=(const ExportedBatch&) = delete;

  ExportedBatch(ExportedBatch&& other) noexcept
      : schema(other.schema), array(other.array) {
    other.schema.release = nullptr;
    other.array.release = nullptr;
  }
  ExportedBatch& operator=(ExportedBatch&& other) noexcept = delete;
};

}  // namespace

class FileScanTaskReaderTest : public TempFileTestBase {
 protected:
  static void SetUpTestSuite() {
    arrow::RegisterAll();
    parquet::RegisterAll();
  }

  void SetUp() override {
    TempFileTestBase::SetUp();
    file_io_ = arrow::ArrowFileSystemFileIO::MakeLocalFileIO();
    partition_spec_ = PartitionSpec::Unpartitioned();
    table_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "name", string()),
                                 SchemaField::MakeOptional(3, "category", string())},
        /*schema_id=*/2);
    projected_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "name", string())},
        table_schema_->schema_id());
  }

  Result<ExportedBatch> MakeBatch(const Schema& schema,
                                  const std::string& json_data) const {
    ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, MakeArrowSchema(schema));
    auto struct_type = ::arrow::struct_(arrow_schema->fields());
    auto array_result = ::arrow::json::ArrayFromJSONString(struct_type, json_data);
    if (!array_result.ok()) {
      return UnknownError(array_result.status().ToString());
    }

    ExportedBatch batch;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(schema, &batch.schema));
    auto export_status =
        ::arrow::ExportArray(*array_result.MoveValueUnsafe(), &batch.array);
    if (!export_status.ok()) {
      return UnknownError(export_status.ToString());
    }
    return std::move(batch);
  }

  Result<std::shared_ptr<::arrow::Schema>> MakeArrowSchema(const Schema& schema) const {
    ArrowSchema c_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(schema, &c_schema));
    auto arrow_schema_result = ::arrow::ImportSchema(&c_schema);
    if (!arrow_schema_result.ok()) {
      return UnknownError(arrow_schema_result.status().ToString());
    }
    return arrow_schema_result.MoveValueUnsafe();
  }

  Result<std::shared_ptr<::arrow::RecordBatch>> MakeRecordBatch(
      const std::shared_ptr<::arrow::Schema>& arrow_schema,
      const std::string& json_data) const {
    auto struct_type = ::arrow::struct_(arrow_schema->fields());
    auto array_result = ::arrow::json::ArrayFromJSONString(struct_type, json_data);
    if (!array_result.ok()) {
      return UnknownError(array_result.status().ToString());
    }

    auto batch_result =
        ::arrow::RecordBatch::FromStructArray(array_result.MoveValueUnsafe());
    if (!batch_result.ok()) {
      return UnknownError(batch_result.status().ToString());
    }
    return batch_result.MoveValueUnsafe();
  }

  Result<std::string> CreateParquetDataFile(std::shared_ptr<Schema> schema,
                                            const std::string& json_data,
                                            int64_t row_group_size = 1024) {
    auto path = CreateNewTempFilePathWithSuffix(".parquet");

    ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, MakeArrowSchema(*schema));
    ICEBERG_ASSIGN_OR_RAISE(auto batch, MakeRecordBatch(arrow_schema, json_data));
    auto table_result =
        ::arrow::Table::FromRecordBatches(arrow_schema, {std::move(batch)});
    if (!table_result.ok()) {
      return UnknownError(table_result.status().ToString());
    }

    ICEBERG_ASSIGN_OR_RAISE(auto outfile, arrow::OpenArrowOutputStream(file_io_, path));
    auto write_status = ::parquet::arrow::WriteTable(*table_result.MoveValueUnsafe(),
                                                     ::arrow::default_memory_pool(),
                                                     outfile, row_group_size);
    if (!write_status.ok()) {
      return UnknownError(write_status.ToString());
    }
    if (auto close_status = outfile->Close(); !close_status.ok()) {
      return UnknownError(close_status.ToString());
    }
    return path;
  }

  Result<std::string> CreateParquetDataFile(std::shared_ptr<Schema> schema,
                                            const std::vector<std::string>& json_batches,
                                            int64_t max_row_group_length) {
    ICEBERG_PRECHECK(!json_batches.empty(), "Parquet data file must have a batch");

    auto path = CreateNewTempFilePathWithSuffix(".parquet");
    ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, MakeArrowSchema(*schema));
    ICEBERG_ASSIGN_OR_RAISE(auto outfile, arrow::OpenArrowOutputStream(file_io_, path));

    auto properties_builder = ::parquet::WriterProperties::Builder();
    auto writer_properties =
        properties_builder.compression(::parquet::Compression::UNCOMPRESSED)
            ->max_row_group_length(max_row_group_length)
            ->build();
    auto writer_result = ::parquet::arrow::FileWriter::Open(
        *arrow_schema, ::arrow::default_memory_pool(), outfile, writer_properties);
    if (!writer_result.ok()) {
      return UnknownError(writer_result.status().ToString());
    }
    auto writer = writer_result.MoveValueUnsafe();

    for (const auto& json_batch : json_batches) {
      ICEBERG_ASSIGN_OR_RAISE(auto batch, MakeRecordBatch(arrow_schema, json_batch));
      if (auto write_status = writer->WriteRecordBatch(*batch); !write_status.ok()) {
        return UnknownError(write_status.ToString());
      }
    }
    if (auto close_status = writer->Close(); !close_status.ok()) {
      return UnknownError(close_status.ToString());
    }
    ICEBERG_PRECHECK(
        writer->metadata()->num_row_groups() == static_cast<int>(json_batches.size()),
        "Expected {} Parquet row groups, got {}", json_batches.size(),
        writer->metadata()->num_row_groups());
    if (auto close_status = outfile->Close(); !close_status.ok()) {
      return UnknownError(close_status.ToString());
    }
    return path;
  }

  Result<std::shared_ptr<DataFile>> MakeDataFile(std::shared_ptr<Schema> schema,
                                                 const std::string& json_data,
                                                 int64_t record_count = 3) {
    ICEBERG_ASSIGN_OR_RAISE(auto path,
                            CreateParquetDataFile(std::move(schema), json_data));
    ICEBERG_ASSIGN_OR_RAISE(auto input_file, file_io_->NewInputFile(path));
    ICEBERG_ASSIGN_OR_RAISE(auto size, input_file->Size());
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kData,
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .record_count = record_count,
        .file_size_in_bytes = size,
    });
  }

  Result<std::shared_ptr<DataFile>> MakeDataFile(
      std::shared_ptr<Schema> schema, const std::vector<std::string>& json_batches,
      int64_t record_count, int64_t max_row_group_length) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto path,
        CreateParquetDataFile(std::move(schema), json_batches, max_row_group_length));
    ICEBERG_ASSIGN_OR_RAISE(auto input_file, file_io_->NewInputFile(path));
    ICEBERG_ASSIGN_OR_RAISE(auto size, input_file->Size());
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kData,
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .record_count = record_count,
        .file_size_in_bytes = size,
    });
  }

  Result<std::shared_ptr<DataFile>> MakePositionDeleteFile(
      const std::string& path, const std::vector<int64_t>& positions,
      const std::string& data_path) {
    PositionDeleteWriterOptions options{
        .path = path,
        .schema = table_schema_,
        .spec = partition_spec_,
        .partition = PartitionValues{},
        .format = FileFormatType::kParquet,
        .io = file_io_,
        .flush_threshold = 10000,
        .properties = {{"write.parquet.compression-codec", "uncompressed"}},
    };

    ICEBERG_ASSIGN_OR_RAISE(auto writer, PositionDeleteWriter::Make(options));
    for (int64_t pos : positions) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteDelete(data_path, pos));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
    return metadata.data_files[0];
  }

  Result<std::shared_ptr<DataFile>> MakeEqualityDeleteFile(
      const std::string& path, std::shared_ptr<Schema> schema,
      const std::string& json_data, std::vector<int32_t> equality_field_ids) {
    EqualityDeleteWriterOptions options{
        .path = path,
        .schema = schema,
        .spec = partition_spec_,
        .partition = PartitionValues{},
        .format = FileFormatType::kParquet,
        .io = file_io_,
        .equality_field_ids = std::move(equality_field_ids),
        .properties = {{"write.parquet.compression-codec", "uncompressed"}},
    };

    ICEBERG_ASSIGN_OR_RAISE(auto writer, EqualityDeleteWriter::Make(options));
    ICEBERG_ASSIGN_OR_RAISE(auto batch, MakeBatch(*schema, json_data));
    ICEBERG_RETURN_UNEXPECTED(writer->Write(&batch.array));
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
    return metadata.data_files[0];
  }

  Result<std::shared_ptr<DataFile>> MakeDeletionVectorFile(
      const std::string& path, const std::vector<int64_t>& positions,
      const std::string& data_path) {
    DeletionVectorWriterOptions options{
        .path = path,
        .io = file_io_,
        .load_previous_deletes = [](std::string_view)
            -> Result<std::optional<PositionDeleteIndex>> { return std::nullopt; },
    };
    ICEBERG_ASSIGN_OR_RAISE(auto writer, DeletionVectorWriter::Make(std::move(options)));
    for (int64_t pos : positions) {
      ICEBERG_RETURN_UNEXPECTED(
          writer->Delete(data_path, pos, partition_spec_, PartitionValues{}));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
    return metadata.data_files[0];
  }

  void VerifyStream(struct ArrowArrayStream* stream, std::string_view expected_json) {
    auto record_batch_reader = ::arrow::ImportRecordBatchReader(stream).ValueOrDie();

    auto result = record_batch_reader->Next();
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto actual_batch = result.ValueOrDie();
    ASSERT_NE(actual_batch, nullptr) << "Stream is exhausted but expected more data.";

    auto struct_type = ::arrow::struct_(actual_batch->schema()->fields());
    auto expected_array =
        ::arrow::json::ArrayFromJSONString(struct_type, expected_json).ValueOrDie();
    auto expected_batch =
        ::arrow::RecordBatch::FromStructArray(expected_array).ValueOrDie();

    ASSERT_TRUE(actual_batch->Equals(*expected_batch))
        << "Actual batch:\n"
        << actual_batch->ToString() << "\nExpected batch:\n"
        << expected_batch->ToString();

    result = record_batch_reader->Next();
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(result.ValueOrDie(), nullptr) << "Reader returned an extra batch.";
  }

  void VerifyDataReaderBatchLengths(const DataFile& data_file,
                                    const std::vector<int64_t>& expected_lengths,
                                    ReaderProperties properties = {}) {
    ReaderOptions options{
        .path = data_file.file_path,
        .length = static_cast<size_t>(data_file.file_size_in_bytes),
        .io = file_io_,
        .projection = table_schema_,
        .properties = std::move(properties),
    };
    ICEBERG_UNWRAP_OR_FAIL(auto reader,
                           ReaderFactoryRegistry::Open(data_file.file_format, options));

    for (int64_t expected_length : expected_lengths) {
      ICEBERG_UNWRAP_OR_FAIL(auto maybe_batch, reader->Next());
      ASSERT_TRUE(maybe_batch.has_value()) << "Reader is exhausted too early.";

      ArrowArray batch = std::move(maybe_batch.value());
      internal::ArrowArrayGuard batch_guard(&batch);
      ASSERT_EQ(batch.length, expected_length);
    }

    ICEBERG_UNWRAP_OR_FAIL(auto maybe_batch, reader->Next());
    ASSERT_FALSE(maybe_batch.has_value()) << "Reader returned an extra batch.";
    ASSERT_THAT(reader->Close(), IsOk());
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<PartitionSpec> partition_spec_;
  std::shared_ptr<Schema> table_schema_;
  std::shared_ptr<Schema> projected_schema_;
};

TEST_F(FileScanTaskReaderTest, OpenWithoutDeletesReadsProjectedSchema) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  FileScanTask task(data_file);

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = projected_schema_,
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  auto stream_result = reader->Open(task);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  ASSERT_NO_FATAL_FAILURE(
      VerifyStream(&stream, R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])"));
}

TEST_F(FileScanTaskReaderTest, OpenWithPositionDeletesFiltersRowsAndPrunesPos) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto pos_delete, MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                              {1}, data_file->file_path));
  FileScanTask task(data_file, {pos_delete});

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = projected_schema_,
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  auto stream_result = reader->Open(task);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, R"([[1, "Foo"], [3, "Baz"]])"));
}

TEST_F(FileScanTaskReaderTest, OpenWithDeletionVectorFiltersRows) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto dv, MakeDeletionVectorFile(CreateNewTempFilePathWithSuffix(".puffin"), {1},
                                      data_file->file_path));
  FileScanTask task(data_file, {dv});

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = projected_schema_,
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  auto stream_result = reader->Open(task);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, R"([[1, "Foo"], [3, "Baz"]])"));
}

TEST_F(FileScanTaskReaderTest, OpenWithEqualityDeletesAddsAndPrunesDeleteOnlyColumns) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_delete,
      MakeEqualityDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"), table_schema_,
                             R"([[0, "unused", "red"]])", {3}));
  FileScanTask task(data_file, {eq_delete});

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = projected_schema_,
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  auto stream_result = reader->Open(task);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, R"([[1, "Foo"], [3, "Baz"]])"));
}

TEST_F(FileScanTaskReaderTest, OpenWithEqualityDeletesKeepsInputBatchWhenAllRowsAlive) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_delete,
      MakeEqualityDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"), table_schema_,
                             R"([[99, "unused", "unused"]])", {1}));
  FileScanTask task(data_file, {eq_delete});

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = projected_schema_,
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  auto stream_result = reader->Open(task);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  ASSERT_NO_FATAL_FAILURE(
      VerifyStream(&stream, R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])"));
}

TEST_F(FileScanTaskReaderTest, OpenWithSchemasResolvesDroppedEqualityField) {
  auto current_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "name", string())},
      /*schema_id=*/2);
  auto old_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "name", string()),
                               SchemaField::MakeOptional(4, "dropped_value", string())},
      /*schema_id=*/1);
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(old_schema,
                   R"([[1, "Foo", "keep"], [2, "Bar", "gone"], [3, "Baz", "keep"]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_delete,
      MakeEqualityDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"), old_schema,
                             R"([[0, "unused", "gone"]])", {4}));
  FileScanTask task(data_file, {eq_delete});

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = current_schema,
      .schemas = {current_schema, old_schema},
      .projected_schema = current_schema,
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  auto stream_result = reader->Open(task);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, R"([[1, "Foo"], [3, "Baz"]])"));
}

TEST_F(FileScanTaskReaderTest, OpenWithMixedDeletesSkipsFullyDeletedBatches) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   std::vector<std::string>{R"([[1, "Foo", "blue"], [2, "Bar", "red"]])",
                                            R"([[3, "Baz", "green"]])"},
                   /*record_count=*/3, /*max_row_group_length=*/2));
  // Parquet can coalesce row groups when the reader batch size is larger.
  ReaderProperties properties;
  properties.Set(ReaderProperties::kBatchSize, int64_t{2});
  ASSERT_NO_FATAL_FAILURE(VerifyDataReaderBatchLengths(*data_file, {2, 1}, properties));
  ICEBERG_UNWRAP_OR_FAIL(
      auto pos_delete, MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                              {0, 1}, data_file->file_path));
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_delete,
      MakeEqualityDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"), table_schema_,
                             R"([[0, "unused", "yellow"]])", {3}));
  FileScanTask task(data_file, {pos_delete, eq_delete});

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = projected_schema_,
      .properties = properties.configs(),
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  auto stream_result = reader->Open(task);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, R"([[3, "Baz"]])"));
}

}  // namespace iceberg
