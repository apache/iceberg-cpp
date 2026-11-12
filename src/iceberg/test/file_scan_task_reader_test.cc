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

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <nanoarrow/nanoarrow.hpp>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_register.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow_c_data.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/data/data_writer.h"
#include "iceberg/data/equality_delete_writer.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/data_table_scan.h"
#include "iceberg/deletes/dv_writer.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/expression/binder.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/literal.h"
#include "iceberg/file_format.h"
#include "iceberg/file_io.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/std_io.h"
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

// Default rows read by the residual-filter tests.
constexpr std::string_view kResidualDataJson =
    R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])";

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

  std::shared_ptr<Schema> RowLineageProjection() const {
    return std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 MetadataColumns::kRowId,
                                 MetadataColumns::kLastUpdatedSequenceNumber},
        table_schema_->schema_id());
  }

  using RowWithPositionAndLineage = std::array<int64_t, 4>;

  Result<std::vector<RowWithPositionAndLineage>> ReadRows(FileScanTaskReader& reader,
                                                          const FileScanTask& task) {
    ICEBERG_ASSIGN_OR_RAISE(auto stream, reader.Open(task));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto batches,
                                   ::arrow::ImportRecordBatchReader(&stream));
    std::vector<RowWithPositionAndLineage> rows;
    while (true) {
      ICEBERG_ARROW_ASSIGN_OR_RETURN(auto batch, batches->Next());
      if (batch == nullptr) {
        return rows;
      }
      ICEBERG_ARROW_RETURN_NOT_OK(batch->ValidateFull());
      const auto& ids = static_cast<const ::arrow::Int32Array&>(*batch->column(0));
      const auto& positions = static_cast<const ::arrow::Int64Array&>(*batch->column(1));
      const auto& row_ids = static_cast<const ::arrow::Int64Array&>(*batch->column(2));
      const auto& sequences = static_cast<const ::arrow::Int64Array&>(*batch->column(3));
      for (int64_t i = 0; i < batch->num_rows(); ++i) {
        rows.push_back(
            {ids.Value(i), positions.Value(i), row_ids.Value(i), sequences.Value(i)});
      }
    }
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

  Result<std::shared_ptr<DataFile>> MakeDataFileForPath(const std::string& path,
                                                        int64_t record_count) {
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

  Result<std::shared_ptr<DataFile>> MakeDataFile(std::shared_ptr<Schema> schema,
                                                 const std::string& json_data,
                                                 int64_t record_count = 3) {
    ICEBERG_ASSIGN_OR_RAISE(auto path,
                            CreateParquetDataFile(std::move(schema), json_data));
    return MakeDataFileForPath(path, record_count);
  }

  Result<std::shared_ptr<DataFile>> MakeDataFile(
      std::shared_ptr<Schema> schema, const std::vector<std::string>& json_batches,
      int64_t record_count, int64_t max_row_group_length) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto path,
        CreateParquetDataFile(std::move(schema), json_batches, max_row_group_length));
    return MakeDataFileForPath(path, record_count);
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
    DVWriterOptions options{
        .path = path,
        .io = file_io_,
        .load_previous_deletes = [](std::string_view)
            -> Result<std::optional<PositionDeleteIndex>> { return std::nullopt; },
    };
    ICEBERG_ASSIGN_OR_RAISE(auto writer, DVWriter::Make(std::move(options)));
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

  // Opens a task with a residual filter and the fixture's schemas.
  Result<ArrowArrayStream> OpenResidualTask(
      std::shared_ptr<Expression> residual,
      std::vector<std::shared_ptr<DataFile>> delete_files = {},
      bool case_sensitive = true, bool apply_residuals = true,
      std::string_view data_json = kResidualDataJson) {
    ICEBERG_ASSIGN_OR_RAISE(auto data_file,
                            MakeDataFile(table_schema_, std::string(data_json)));
    FileScanTask task(data_file, std::move(delete_files), std::move(residual));
    FileScanTaskReader::Options options{
        .io = file_io_,
        .table_schema = table_schema_,
        .schemas = {table_schema_},
        .projected_schema = projected_schema_,
        .case_sensitive = case_sensitive,
        .apply_residuals = apply_residuals,
    };
    ICEBERG_ASSIGN_OR_RAISE(auto reader, FileScanTaskReader::Make(std::move(options)));
    return reader->Open(task);
  }

  // Opens a task with `residual` enforced and verifies the streamed rows.
  void VerifyResidualStream(std::shared_ptr<Expression> residual,
                            std::string_view expected_json,
                            std::vector<std::shared_ptr<DataFile>> delete_files = {},
                            bool case_sensitive = true,
                            std::string_view data_json = kResidualDataJson) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto stream, OpenResidualTask(std::move(residual), delete_files, case_sensitive,
                                      /*apply_residuals=*/true, data_json));
    ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, expected_json));
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

TEST_F(FileScanTaskReaderTest, RejectsPartialTasksForUnsupportedFormats) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  ICEBERG_UNWRAP_OR_FAIL(auto task, FileScanTask::MakeSplit(data_file, 0, 1));

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = projected_schema_,
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  for (auto format : {FileFormatType::kAvro, FileFormatType::kOrc}) {
    data_file->file_format = format;
    auto result = reader->Open(*task);
    EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
    EXPECT_THAT(result,
                HasErrorMessage("Reading a partial FileScanTask requires Parquet"));
  }
}

TEST_F(FileScanTaskReaderTest, RechecksSplitBoundsAfterFileSizeChanges) {
  ICEBERG_UNWRAP_OR_FAIL(auto data_file,
                         MakeDataFile(table_schema_, R"([[1, "a", "b"]])", 1));
  const int64_t file_size = data_file->file_size_in_bytes;
  ICEBERG_UNWRAP_OR_FAIL(auto task, FileScanTask::MakeSplit(data_file, 4, file_size - 4));
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make({
                                          .io = file_io_,
                                          .table_schema = table_schema_,
                                          .projected_schema = projected_schema_,
                                      }));
  for (int64_t size : {file_size - 1, int64_t{3}, int64_t{0}}) {
    data_file->file_size_in_bytes = size;
    auto result = reader->Open(*task);
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(result, HasErrorMessage("Task range is outside file"));
  }
  data_file->file_size_in_bytes = -1;
  EXPECT_THAT(reader->Open(*task),
              HasErrorMessage("Data file size must not be negative"));
}

TEST_F(FileScanTaskReaderTest, EmptyParquetFileAndSplitReturnNoRows) {
  ICEBERG_UNWRAP_OR_FAIL(auto data_file, MakeDataFile(table_schema_, "[]", 0));
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make({
                                          .io = file_io_,
                                          .table_schema = table_schema_,
                                          .projected_schema = projected_schema_,
                                      }));
  ICEBERG_UNWRAP_OR_FAIL(auto split, FileScanTask::MakeSplit(data_file, 0, 1));
  for (const auto& task : {std::make_shared<FileScanTask>(data_file), split}) {
    ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(*task));
    auto batches = ::arrow::ImportRecordBatchReader(&stream).ValueOrDie();
    auto batch = batches->Next();
    ASSERT_TRUE(batch.ok()) << batch.status();
    ASSERT_EQ(batch.ValueOrDie(), nullptr);
  }
}

class FileScanTaskReaderSplitTest
    : public FileScanTaskReaderTest,
      public ::testing::WithParamInterface<std::tuple<std::string, int64_t>> {};

TEST_P(FileScanTaskReaderSplitTest, SplitsMatchWholeFileWithAbsolutePositionsAndLineage) {
  const auto& [delete_kind, batch_size] = GetParam();
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   std::vector<std::string>{R"([[1, "a", "keep"], [2, "b", "delete"]])",
                                            R"([[3, "c", "delete"], [4, "d", "delete"]])",
                                            R"([[5, "e", "keep"], [6, "f", "delete"]])"},
                   /*record_count=*/6, /*max_row_group_length=*/2));
  data_file->first_row_id = 100;
  data_file->data_sequence_number = 7;
  std::vector<std::shared_ptr<DataFile>> deletes;
  if (delete_kind == "position" || delete_kind == "mixed") {
    ICEBERG_UNWRAP_OR_FAIL(
        auto deletion, MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                              {1, 2, 3, 5}, data_file->file_path));
    deletes.push_back(std::move(deletion));
  }
  if (delete_kind == "equality" || delete_kind == "mixed") {
    // The equality field is absent from the output projection.
    ICEBERG_UNWRAP_OR_FAIL(
        auto deletion,
        MakeEqualityDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"), table_schema_,
                               R"([[0, "", "delete"]])", {3}));
    deletes.push_back(std::move(deletion));
  }
  if (delete_kind == "dv") {
    ICEBERG_UNWRAP_OR_FAIL(
        auto deletion, MakeDeletionVectorFile(CreateNewTempFilePathWithSuffix(".puffin"),
                                              {1, 2, 3, 5}, data_file->file_path));
    deletes.push_back(std::move(deletion));
  }
  auto projection = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()), MetadataColumns::kRowPosition,
      MetadataColumns::kRowId, MetadataColumns::kLastUpdatedSequenceNumber});
  // This FileIO honors the supplied length hint, so a split length passed as the
  // file length would make the footer unreadable.
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, FileScanTaskReader::Make({
                       .io = std::make_shared<test::StdFileIO>(),
                       .table_schema = table_schema_,
                       .projected_schema = projection,
                       .properties = {{"read.batch-size", std::to_string(batch_size)}},
                   }));

  std::vector<RowWithPositionAndLineage> expected;
  for (int64_t id = 1; id <= 6; ++id) {
    if (delete_kind == "none" || id == 1 || id == 5) {
      expected.push_back({id, id - 1, 100 + id - 1, 7});
    }
  }
  ICEBERG_UNWRAP_OR_FAIL(auto whole_rows,
                         ReadRows(*reader, FileScanTask(data_file, deletes)));
  ASSERT_EQ(whole_rows, expected);

  ICEBERG_UNWRAP_OR_FAIL(auto input,
                         arrow::OpenArrowInputStream(file_io_, data_file->file_path));
  auto metadata = ::parquet::ReadMetaData(input);
  ASSERT_EQ(metadata->num_row_groups(), 3);
  std::vector<int64_t> boundaries{0, 1, data_file->file_size_in_bytes};
  for (int i = 0; i < metadata->num_row_groups(); ++i) {
    auto column = metadata->RowGroup(i)->ColumnChunk(0);
    ASSERT_TRUE(column->has_dictionary_page());
    ASSERT_LT(column->dictionary_page_offset(), column->data_page_offset());
    // Include exact starts, ranges containing no starts, and boundaries inside groups.
    boundaries.push_back(column->dictionary_page_offset());
    boundaries.push_back(column->dictionary_page_offset() + 1);
    boundaries.push_back(column->data_page_offset());
  }
  std::ranges::sort(boundaries);
  auto duplicates = std::ranges::unique(boundaries);
  boundaries.erase(duplicates.begin(), duplicates.end());
  std::vector<RowWithPositionAndLineage> split_rows;
  for (size_t i = 1; i < boundaries.size(); ++i) {
    SCOPED_TRACE(i);
    ICEBERG_UNWRAP_OR_FAIL(
        auto split, FileScanTask::MakeSplit(data_file, boundaries[i - 1],
                                            boundaries[i] - boundaries[i - 1], deletes));
    ICEBERG_UNWRAP_OR_FAIL(auto rows, ReadRows(*reader, *split));
    split_rows.insert(split_rows.end(), rows.begin(), rows.end());
  }
  EXPECT_EQ(split_rows, whole_rows);
  EXPECT_EQ(split_rows, expected);
}

INSTANTIATE_TEST_SUITE_P(DeleteKindsAndBatchSizes, FileScanTaskReaderSplitTest,
                         ::testing::Combine(::testing::Values("none", "position",
                                                              "equality", "dv", "mixed"),
                                            ::testing::Values(int64_t{1}, int64_t{100})));

TEST_F(FileScanTaskReaderTest, ReadLastUpdatedFromDataSeq) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  data_file->first_row_id = 100L;
  data_file->data_sequence_number = 5L;
  FileScanTask task(data_file);

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = RowLineageProjection(),
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  auto stream_result = reader->Open(task);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, R"([[1, 100, 5],
                                                   [2, 101, 5],
                                                   [3, 102, 5]])"));
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

TEST_F(FileScanTaskReaderTest, OpenAppliesResidualFilterWithoutDeletes) {
  ASSERT_NO_FATAL_FAILURE(VerifyResidualStream(
      Expressions::GreaterThan("id", Literal::Int(1)), R"([[2, "Bar"], [3, "Baz"]])"));
}

TEST_F(FileScanTaskReaderTest, OpenAppliesFoldedResidualsOnProjectedRequiredColumn) {
  table_schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "flag", boolean()),
                               SchemaField::MakeRequired(2, "id", int32())});
  projected_schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{table_schema_->fields()[1], table_schema_->fields()[0]});
  constexpr std::string_view data = "[[true, 1], [false, 2], [true, 3]]";

  ASSERT_NO_FATAL_FAILURE(VerifyResidualStream(Expressions::NotNull("flag"),
                                               "[[1, true], [2, false], [3, true]]", {},
                                               true, data));
  // The projection reorders the fields; folding must preserve the read positions.
  ASSERT_NO_FATAL_FAILURE(VerifyResidualStream(
      Expressions::And(Expressions::NotNull("flag"),
                       Expressions::GreaterThan("id", Literal::Int(1))),
      "[[2, false], [3, true]]", {}, true, data));
  ASSERT_NO_FATAL_FAILURE(VerifyResidualStream(
      Expressions::Or(Expressions::IsNull("flag"),
                      Expressions::GreaterThan("id", Literal::Int(1))),
      "[[2, false], [3, true]]", {}, true, data));

  ICEBERG_UNWRAP_OR_FAIL(
      auto stream, OpenResidualTask(Expressions::IsNull("flag"), {}, true, true, data));
  auto batches = ::arrow::ImportRecordBatchReader(&stream).ValueOrDie();
  auto next = batches->Next();
  ASSERT_TRUE(next.ok()) << next.status();
  EXPECT_EQ(next.ValueOrDie(), nullptr);
}

TEST_F(FileScanTaskReaderTest, OpenAppliesNullableOutOfRangeResiduals) {
  table_schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "n", int32())});
  projected_schema_ = table_schema_;
  const int64_t above = int64_t{std::numeric_limits<int32_t>::max()} + 1;
  const int64_t below = int64_t{std::numeric_limits<int32_t>::min()} - 1;
  for (auto residual : {Expressions::LessThan("n", Literal::Long(above)),
                        Expressions::LessThanOrEqual("n", Literal::Long(above)),
                        Expressions::GreaterThan("n", Literal::Long(below)),
                        Expressions::GreaterThanOrEqual("n", Literal::Long(below))}) {
    SCOPED_TRACE(residual->ToString());
    VerifyResidualStream(residual, "[[1, 1]]", {}, true, "[[1, 1], [2, null]]");
  }
}

TEST_F(FileScanTaskReaderTest, OpenAppliesResidualFilterWithPositionDeletes) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto pos_delete, MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                              {1}, data_file->file_path));
  // The position delete removes "Bar" and the residual removes "Baz".
  FileScanTask task(data_file, {pos_delete},
                    Expressions::LessThan("id", Literal::Int(3)));

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = projected_schema_,
      .apply_residuals = true,
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  auto stream_result = reader->Open(task);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, R"([[1, "Foo"]])"));
}

TEST_F(FileScanTaskReaderTest, OpenAppliesResidualFilterWithEqualityDeletes) {
  projected_schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{table_schema_->fields()[0], table_schema_->fields()[2]});
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_delete,
      MakeEqualityDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"), table_schema_,
                             R"([[0, "Bar", "unused"]])", {2}));

  // The residual removes "Foo"; the equality delete removes "Bar".
  ASSERT_NO_FATAL_FAILURE(
      VerifyResidualStream(Expressions::NotEqual("category", Literal::String("blue")),
                           R"([[3, "green"]])", {eq_delete}));
}

TEST_F(FileScanTaskReaderTest, OpenRejectsResidualOnColumnAbsentFromProjection) {
  EXPECT_THAT(OpenResidualTask(Expressions::Equal("category", Literal::String("blue"))),
              HasErrorMessage("Cannot find field 'category'"));
  // Missing references are rejected even if the table schema would fold them away.
  projected_schema_ =
      std::make_shared<Schema>(std::vector<SchemaField>{table_schema_->fields()[1]});
  EXPECT_THAT(OpenResidualTask(Expressions::NotNull("id")),
              HasErrorMessage("Cannot find field 'id'"));
}

TEST_F(FileScanTaskReaderTest, ScanProjectionKeepsFilterColumnsInOutput) {
  internal::TableScanContext context;
  context.selected_columns = std::vector<std::string>{"id"};
  context.filter = Expressions::Equal("category", Literal::String("blue"));
  auto metadata = std::make_shared<TableMetadata>();
  metadata->schemas = {table_schema_};
  metadata->current_schema_id = table_schema_->schema_id();
  ICEBERG_UNWRAP_OR_FAIL(auto scan,
                         DataTableScan::Make(metadata, table_schema_, file_io_, context));
  ICEBERG_UNWRAP_OR_FAIL(auto projection, scan->schema());
  ASSERT_EQ(projection->fields().size(), 2);
  EXPECT_EQ(projection->fields()[0].field_id(), 1);
  EXPECT_EQ(projection->fields()[1].field_id(), 3);

  ICEBERG_UNWRAP_OR_FAIL(auto data_file,
                         MakeDataFile(table_schema_, std::string(kResidualDataJson)));
  ICEBERG_UNWRAP_OR_FAIL(
      auto pos_delete, MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                              {1}, data_file->file_path));
  for (bool with_deletes : {false, true}) {
    SCOPED_TRACE(with_deletes);
    FileScanTask task(
        data_file,
        with_deletes ? std::vector{pos_delete} : std::vector<std::shared_ptr<DataFile>>{},
        scan->filter());
    ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make({
                                            .io = scan->io(),
                                            .table_schema = table_schema_,
                                            .projected_schema = projection,
                                            .case_sensitive = scan->is_case_sensitive(),
                                            .apply_residuals = true,
                                        }));
    ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));
    auto batches = ::arrow::ImportRecordBatchReader(&stream).ValueOrDie();
    ICEBERG_UNWRAP_OR_FAIL(auto expected_schema, MakeArrowSchema(*projection));
    ASSERT_TRUE(batches->schema()->Equals(*expected_schema));
    auto batch = batches->Next().ValueOrDie();
    ASSERT_NE(batch, nullptr);
    ICEBERG_UNWRAP_OR_FAIL(auto expected,
                           MakeRecordBatch(expected_schema, R"([[1, "blue"]])"));
    EXPECT_TRUE(batch->Equals(*expected)) << batch->ToString();
    EXPECT_EQ(batches->Next().ValueOrDie(), nullptr);
  }
}

TEST_F(FileScanTaskReaderTest, OpenAppliesCaseInsensitiveResidualFilter) {
  ASSERT_NO_FATAL_FAILURE(
      VerifyResidualStream(Expressions::GreaterThan("ID", Literal::Int(1)),
                           R"([[2, "Bar"], [3, "Baz"]])", {}, /*case_sensitive=*/false));
}

TEST_F(FileScanTaskReaderTest, OpenRejectsCaseSensitiveResidualWithDifferentCase) {
  EXPECT_THAT(OpenResidualTask(Expressions::GreaterThan("ID", Literal::Int(1))),
              HasErrorMessage("Cannot find field 'ID' in struct"));
}

TEST_F(FileScanTaskReaderTest, OpenAppliesResidualFilterOnNullValues) {
  // Null values never satisfy a comparison residual.
  ASSERT_NO_FATAL_FAILURE(VerifyResidualStream(
      Expressions::NotNull("name"), R"([[1, "Foo"], [3, "Baz"]])", {}, true,
      R"([[1, "Foo", "blue"], [2, null, "red"], [3, "Baz", "green"]])"));
}

TEST_F(FileScanTaskReaderTest, OpenAppliesResidualMatchingAllRows) {
  ASSERT_NO_FATAL_FAILURE(
      VerifyResidualStream(Expressions::GreaterThanOrEqual("id", Literal::Int(1)),
                           R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])"));
}

TEST_F(FileScanTaskReaderTest, OpenAppliesAlwaysFalseResidualReturningNoRows) {
  ICEBERG_UNWRAP_OR_FAIL(auto stream, OpenResidualTask(Expressions::AlwaysFalse()));

  auto record_batch_reader = ::arrow::ImportRecordBatchReader(&stream).ValueOrDie();
  auto result = record_batch_reader->Next();
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.ValueOrDie(), nullptr);
}

TEST_F(FileScanTaskReaderTest, OpenAppliesResidualFilterKeepingRowIdentities) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  data_file->first_row_id = 100L;
  data_file->data_sequence_number = 5L;
  FileScanTask task(data_file, {}, Expressions::GreaterThan("id", Literal::Int(1)));

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = RowLineageProjection(),
      .apply_residuals = true,
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  auto stream_result = reader->Open(task);
  ASSERT_THAT(stream_result, IsOk());
  auto stream = std::move(stream_result.value());

  // Row identities of the surviving rows only: dropped rows must not shift ids.
  ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, R"([[2, 101, 5],
                                                   [3, 102, 5]])"));
}

TEST_F(FileScanTaskReaderTest, OpenWithoutResidualEnforcementKeepsAllRows) {
  // Residual enforcement is off; rows are returned unfiltered.
  ICEBERG_UNWRAP_OR_FAIL(
      auto stream, OpenResidualTask(Expressions::GreaterThan("id", Literal::Int(1)), {},
                                    /*case_sensitive=*/true, /*apply_residuals=*/false));

  ASSERT_NO_FATAL_FAILURE(
      VerifyStream(&stream, R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])"));
}

TEST_F(FileScanTaskReaderTest, OpenRejectsResidualWithUnknownColumn) {
  EXPECT_THAT(OpenResidualTask(Expressions::Equal("missing", Literal::Int(1))),
              HasErrorMessage("Cannot find field 'missing'"));
}

TEST_F(FileScanTaskReaderTest, OpenAppliesResidualOnNestedColumn) {
  table_schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "s",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeOptional(3, "tags", string())}))});
  projected_schema_ = table_schema_;
  constexpr std::string_view data =
      R"([[1, {"tags": "a"}], [2, null], [3, {"tags": null}]])";
  VerifyResidualStream(Expressions::NotNull("s.tags"), R"([[1, {"tags": "a"}]])", {},
                       true, data);
  VerifyResidualStream(Expressions::IsNull("s.tags"),
                       R"([[2, null], [3, {"tags": null}]])", {}, true, data);
}

TEST_F(FileScanTaskReaderTest, NestedResidualsPreserveReadPositionsWithDeletes) {
  auto value = SchemaField::MakeOptional(3, "value", string());
  auto key = SchemaField::MakeRequired(4, "key", int32());
  table_schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "record", struct_({value, key}))});
  // Reorder both the top-level fields and the nested children in the read schema.
  projected_schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{table_schema_->fields()[1].WithType(struct_({key, value})),
                               table_schema_->fields()[0]});
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, {"value": "a", "key": 1}], [2, {"value": "b", "key": 2}],
                       [3, null], [4, {"value": "d", "key": 3}],
                       [5, {"value": "e", "key": 4}]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto pos_delete, MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                              {1}, data_file->file_path));
  auto delete_schema =
      std::make_shared<Schema>(std::vector<SchemaField>{table_schema_->fields()[0]});
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_delete, MakeEqualityDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                             delete_schema, "[[4]]", {1}));
  for (bool case_sensitive : {true, false}) {
    for (const std::string deletes : {"none", "position", "equality", "mixed"}) {
      SCOPED_TRACE(case_sensitive);
      SCOPED_TRACE(deletes);
      std::vector<std::shared_ptr<DataFile>> delete_files;
      if (deletes == "position" || deletes == "mixed") {
        delete_files.push_back(pos_delete);
      }
      if (deletes == "equality" || deletes == "mixed") {
        delete_files.push_back(eq_delete);
      }
      FileScanTask task(
          data_file, std::move(delete_files),
          Expressions::GreaterThan(case_sensitive ? "record.key" : "RECORD.KEY",
                                   Literal::Int(1)));
      ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make({
                                              .io = file_io_,
                                              .table_schema = table_schema_,
                                              .projected_schema = projected_schema_,
                                              .case_sensitive = case_sensitive,
                                              .apply_residuals = true,
                                          }));
      ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));
      const std::string_view expected =
          deletes == "none"
              ? R"([[{"key": 2, "value": "b"}, 2], [{"key": 3, "value": "d"}, 4],
                     [{"key": 4, "value": "e"}, 5]])"
          : deletes == "position"
              ? R"([[{"key": 3, "value": "d"}, 4], [{"key": 4, "value": "e"}, 5]])"
          : deletes == "equality"
              ? R"([[{"key": 2, "value": "b"}, 2], [{"key": 4, "value": "e"}, 5]])"
              : R"([[{"key": 4, "value": "e"}, 5]])";
      VerifyStream(&stream, expected);
    }
  }
}

TEST_F(FileScanTaskReaderTest, OpenRejectsResidualOnListElement) {
  table_schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(
          2, "items",
          list(SchemaField::MakeOptional(
              3, "element",
              struct_({SchemaField::MakeOptional(4, "value", int32())}))))});
  projected_schema_ = table_schema_;
  EXPECT_THAT(
      OpenResidualTask(Expressions::GreaterThan("items.element.value", Literal::Int(0)),
                       {}, true, true, R"([[1, [{"value": 1}]]])"),
      IsError(ErrorKind::kNotFound));
}

TEST_F(FileScanTaskReaderTest, OpenRejectsResidualComparisonOnCompositeColumn) {
  table_schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(
          2, "s", struct_({SchemaField::MakeOptional(3, "value", int32())}))});
  projected_schema_ = table_schema_;
  EXPECT_THAT(OpenResidualTask(Expressions::Equal("s", Literal::Int(1)), {}, true, true,
                               R"([[1, {"value": 1}]])"),
              IsError(ErrorKind::kInvalidExpression));
}

TEST_F(FileScanTaskReaderTest, OpenAppliesNullPredicatesOnCompositeColumns) {
  struct Case {
    std::shared_ptr<Type> type;
    std::string data;
    std::string non_null_rows;
    std::string matching_row;
  };
  const std::vector<Case> cases = {
      {.type = struct_({SchemaField::MakeOptional(3, "value", int32())}),
       .data = R"([[1, null], [2, {"value": null}], [3, {"value": 10}], [4, null]])",
       .non_null_rows = R"([[2, {"value": null}], [3, {"value": 10}]])",
       .matching_row = R"([[3, {"value": 10}]])"},
      {.type = list(SchemaField::MakeOptional(3, "element", int32())),
       .data = "[[1, null], [2, []], [3, [10]], [4, null]]",
       .non_null_rows = "[[2, []], [3, [10]]]",
       .matching_row = "[[3, [10]]]"},
      {.type = map(SchemaField::MakeRequired(3, "key", string()),
                   SchemaField::MakeOptional(4, "value", int32())),
       .data = R"([[1, null], [2, []], [3, [["a", 10]]], [4, null]])",
       .non_null_rows = R"([[2, []], [3, [["a", 10]]]])",
       .matching_row = R"([[3, [["a", 10]]]])"},
  };
  for (const auto& [type, data, non_null_rows, matching_row] : cases) {
    SCOPED_TRACE(type->ToString());
    table_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "value", type)});
    projected_schema_ = table_schema_;
    VerifyResidualStream(Expressions::IsNull("value"), "[[1, null], [4, null]]", {}, true,
                         data);
    VerifyResidualStream(Expressions::NotNull("value"), non_null_rows, {}, true, data);
    VerifyResidualStream(
        Expressions::And(Expressions::NotNull("value"),
                         Expressions::GreaterThan("id", Literal::Int(2))),
        matching_row, {}, true, data);
  }
}

TEST_F(FileScanTaskReaderTest, OpenAppliesGeospatialNullPredicates) {
  // Valid little-endian WKB POINT(1 2).
  const std::array<uint8_t, 21> point = {0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
                                         0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00,
                                         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40};
  for (auto type : std::vector<std::shared_ptr<PrimitiveType>>{geometry(), geography()}) {
    SCOPED_TRACE(type->ToString());
    table_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "geom", type)});
    projected_schema_ = table_schema_;
    ::arrow::Int32Builder ids;
    ASSERT_TRUE(ids.AppendValues({1, 2}).ok());
    ::arrow::BinaryBuilder values;
    ASSERT_TRUE(values.Append(point.data(), point.size()).ok());
    ASSERT_TRUE(values.AppendNull().ok());
    auto array = ::arrow::StructArray::Make(
                     {ids.Finish().ValueOrDie(), values.Finish().ValueOrDie()},
                     {::arrow::field("id", ::arrow::int32(), false),
                      ::arrow::field("geom", ::arrow::binary())})
                     .ValueOrDie();
    ExportedBatch batch;
    ASSERT_TRUE(::arrow::ExportArray(*array, &batch.array).ok());
    ICEBERG_UNWRAP_OR_FAIL(auto writer,
                           DataWriter::Make({
                               .path = CreateNewTempFilePathWithSuffix(".parquet"),
                               .schema = table_schema_,
                               .spec = partition_spec_,
                               .partition = PartitionValues{},
                               .format = FileFormatType::kParquet,
                               .io = file_io_,
                           }));
    ASSERT_THAT(writer->Write(&batch.array), IsOk());
    ASSERT_THAT(writer->Close(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto metadata, writer->Metadata());
    for (bool is_null : {false, true}) {
      FileScanTask task(
          metadata.data_files[0], {},
          is_null ? Expressions::IsNull("geom") : Expressions::NotNull("geom"));
      ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make({
                                              .io = file_io_,
                                              .table_schema = table_schema_,
                                              .projected_schema = projected_schema_,
                                              .apply_residuals = true,
                                          }));
      ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));
      auto batches = ::arrow::ImportRecordBatchReader(&stream).ValueOrDie();
      ICEBERG_UNWRAP_OR_FAIL(auto expected_schema, MakeArrowSchema(*projected_schema_));
      ASSERT_TRUE(batches->schema()->Equals(*expected_schema));
      auto result = batches->Next().ValueOrDie();
      ASSERT_NE(result, nullptr);
      ASSERT_EQ(result->num_rows(), 1);
      ASSERT_EQ(result->num_columns(), 2);
      auto result_ids = std::static_pointer_cast<::arrow::Int32Array>(result->column(0));
      EXPECT_EQ(result_ids->Value(0), is_null ? 2 : 1);
      auto result_values =
          std::static_pointer_cast<::arrow::BinaryArray>(result->column(1));
      if (is_null) {
        EXPECT_TRUE(result_values->IsNull(0));
      } else {
        EXPECT_FALSE(result_values->IsNull(0));
        EXPECT_EQ(
            result_values->GetView(0),
            std::string_view(reinterpret_cast<const char*>(point.data()), point.size()));
      }
      EXPECT_EQ(batches->Next().ValueOrDie(), nullptr);
    }
  }
}

TEST_F(FileScanTaskReaderTest, EmptyProjectionPreservesLiveRowCount) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto deletion, MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                            {1}, data_file->file_path));
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      FileScanTaskReader::Make({
          .io = file_io_,
          .table_schema = table_schema_,
          .projected_schema = std::make_shared<Schema>(std::vector<SchemaField>{}),
      }));
  ICEBERG_UNWRAP_OR_FAIL(auto opened_stream,
                         reader->Open(FileScanTask(data_file, {deletion})));
  nanoarrow::UniqueArrayStream stream(&opened_stream);
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(stream->get_schema(stream.get(), schema.get()), 0)
      << stream->get_last_error(stream.get());
  EXPECT_EQ(schema->n_children, 0);

  nanoarrow::UniqueArray batch;
  ASSERT_EQ(stream->get_next(stream.get(), batch.get()), 0)
      << stream->get_last_error(stream.get());
  ASSERT_NE(batch->release, nullptr);
  EXPECT_EQ(batch->n_children, 0);
  EXPECT_EQ(batch->length, 2);

  nanoarrow::UniqueArray end;
  ASSERT_EQ(stream->get_next(stream.get(), end.get()), 0)
      << stream->get_last_error(stream.get());
  EXPECT_EQ(end->release, nullptr);
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

TEST_F(FileScanTaskReaderTest, ReadsArrowValidateBatchProperty) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto pos_delete, MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                              {1}, data_file->file_path));
  FileScanTask task(data_file, {pos_delete});

  struct RestoreProjection {
    ProjectionContext::ProjectBatchFunction function =
        ProjectionContext::ResolveProjectBatchFunction();
    ~RestoreProjection() { ProjectionContext::RegisterProjectBatchFunction(function); }
  } restore;
  ProjectionContext::RegisterProjectBatchFunction(
      [](ArrowArray*, std::span<const int32_t>,
         ProjectionContext& projection) -> Result<ArrowArray> {
        return InvalidArgument("projection mode: {}",
                               projection.validate_batch() ? "full" : "basic");
      });

  for (bool validate_batch : {true, false}) {
    FileScanTaskReader::Options options{
        .io = file_io_,
        .table_schema = table_schema_,
        .projected_schema = projected_schema_,
    };
    if (!validate_batch) {
      options.properties[ReaderProperties::kArrowValidateBatch.key()] = "false";
    }
    ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
    ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));

    ArrowArray array{};
    EXPECT_NE(stream.get_next(&stream, &array), 0);
    EXPECT_THAT(stream.get_last_error(&stream),
                ::testing::HasSubstr(validate_batch ? "projection mode: full"
                                                    : "projection mode: basic"));
    stream.release(&stream);
  }
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

TEST_F(FileScanTaskReaderTest, PositionDeletesPreserveRowLineage) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  data_file->first_row_id = 100;
  data_file->data_sequence_number = 5;
  ICEBERG_UNWRAP_OR_FAIL(
      auto pos_delete, MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                              {1}, data_file->file_path));
  FileScanTask task(data_file, {pos_delete});

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = RowLineageProjection(),
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));

  ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, R"([[1, 100, 5], [3, 102, 5]])"));
}

TEST_F(FileScanTaskReaderTest, DeletionVectorDeletesPreserveRowLineage) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  data_file->first_row_id = 100;
  data_file->data_sequence_number = 5;
  ICEBERG_UNWRAP_OR_FAIL(
      auto deletion_vector,
      MakeDeletionVectorFile(CreateNewTempFilePathWithSuffix(".puffin"), {1},
                             data_file->file_path));
  FileScanTask task(data_file, {deletion_vector});

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = RowLineageProjection(),
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));

  ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, R"([[1, 100, 5], [3, 102, 5]])"));
}

TEST_F(FileScanTaskReaderTest, EqualityDeletesPreserveRowLineage) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   R"([[1, "Foo", "blue"], [2, "Bar", "red"], [3, "Baz", "green"]])"));
  data_file->first_row_id = 100;
  data_file->data_sequence_number = 5;
  ICEBERG_UNWRAP_OR_FAIL(
      auto equality_delete,
      MakeEqualityDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"), table_schema_,
                             R"([[0, "unused", "red"]])", {3}));
  FileScanTask task(data_file, {equality_delete});

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = table_schema_,
      .schemas = {table_schema_},
      .projected_schema = RowLineageProjection(),
  };
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));

  ASSERT_NO_FATAL_FAILURE(VerifyStream(&stream, R"([[1, 100, 5], [3, 102, 5]])"));
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

enum class PhysicalListLayout { kList, kLargeList, kStoredMixed, kStoredMixedLargeList };
enum class DeleteBatchSelection {
  kFirstRow,
  kSecondBatchRow,
  kNoOp,
  kFirstBatch,
  kAll,
  kEmptySplit,
  kNone,
  kPosition
};

class FileScanTaskPhysicalSchemaTest
    : public FileScanTaskReaderTest,
      public ::testing::WithParamInterface<
          std::tuple<DeleteBatchSelection, bool, PhysicalListLayout>> {
 public:
  static std::string ParamName(const ::testing::TestParamInfo<ParamType>& info) {
    const auto& [deletes, prune, layout] = info.param;
    constexpr std::array names{"DeleteFirst",      "DeleteSecond",  "NoOp",
                               "DeleteFirstBatch", "DeleteAll",     "EmptySplit",
                               "NoDeletes",        "PositionDelete"};
    std::string name = names[static_cast<size_t>(deletes)];
    name += prune ? "Pruned" : "AllColumns";
    constexpr std::array layouts{"List", "LargeList", "StoredMixed",
                                 "StoredMixedLargeList"};
    name += layouts[static_cast<size_t>(layout)];
    return name;
  }

 protected:
  Result<std::shared_ptr<DataFile>> MakeDataFileWithStoredSchema(
      const std::shared_ptr<::arrow::Schema>& schema, const std::string& json) {
    ICEBERG_ASSIGN_OR_RAISE(auto batch, MakeRecordBatch(schema, json));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto table,
                                   ::arrow::Table::FromRecordBatches(schema, {batch}));
    auto path = CreateNewTempFilePathWithSuffix(".parquet");
    ICEBERG_ASSIGN_OR_RAISE(auto output, arrow::OpenArrowOutputStream(file_io_, path));
    // store_schema() embeds the ARROW:schema metadata so the file keeps the exact
    // list/large_list layout of `schema` instead of the reader's configured type.
    auto writer_properties =
        ::parquet::ArrowWriterProperties::Builder().store_schema()->build();
    ICEBERG_ARROW_RETURN_NOT_OK(::parquet::arrow::WriteTable(
        *table, ::arrow::default_memory_pool(), output, table->num_rows(),
        ::parquet::default_writer_properties(), writer_properties));
    ICEBERG_ARROW_RETURN_NOT_OK(output->Close());
    return MakeDataFileForPath(path, table->num_rows());
  }
};

TEST_P(FileScanTaskPhysicalSchemaTest, StablePhysicalSchemaAcrossDeleteBatches) {
  const auto [deletes, prune_columns, layout] = GetParam();
  const bool use_large_list = layout == PhysicalListLayout::kLargeList ||
                              layout == PhysicalListLayout::kStoredMixedLargeList;
  const bool stored_mixed = layout == PhysicalListLayout::kStoredMixed ||
                            layout == PhysicalListLayout::kStoredMixedLargeList;
  table_schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(
          2, "numbers",
          std::make_shared<ListType>(SchemaField::MakeOptional(3, "element", int32()))),
      SchemaField::MakeOptional(
          4, "nested",
          std::make_shared<StructType>(std::vector<SchemaField>{SchemaField::MakeOptional(
              5, "tags",
              std::make_shared<ListType>(
                  SchemaField::MakeOptional(6, "element", int32())))}))});
  projected_schema_ = prune_columns
                          ? std::make_shared<Schema>(std::vector<SchemaField>{
                                table_schema_->fields()[2], table_schema_->fields()[1]})
                          : table_schema_;
  ICEBERG_UNWRAP_OR_FAIL(auto expected_schema, MakeArrowSchema(*table_schema_));
  if (layout != PhysicalListLayout::kList) {
    auto fields = expected_schema->fields();
    fields[1] = fields[1]->WithType(::arrow::large_list(fields[1]->type()->field(0)));
    if (use_large_list && !stored_mixed) {
      auto tags = fields[2]->type()->field(0);
      fields[2] = fields[2]->WithType(::arrow::struct_(
          {tags->WithType(::arrow::large_list(tags->type()->field(0)))}));
    }
    expected_schema = ::arrow::schema(std::move(fields));
  }
  const std::string json =
      R"([[1, [10, 11], {"tags": [1, null]}], [2, null, {"tags": []}],
          [3, [], null], [4, [40], {"tags": null}]])";
  std::string delete_json;
  std::vector<std::string> expected_json{
      R"([[1, [10, 11], {"tags": [1, null]}], [2, null, {"tags": []}]])",
      R"([[3, [], null], [4, [40], {"tags": null}]])"};
  switch (deletes) {
    case DeleteBatchSelection::kFirstRow:
    case DeleteBatchSelection::kPosition:
      delete_json = "[[1]]";
      expected_json[0] = R"([[2, null, {"tags": []}]])";
      break;
    case DeleteBatchSelection::kSecondBatchRow:
      delete_json = "[[3]]";
      expected_json[1] = R"([[4, [40], {"tags": null}]])";
      break;
    case DeleteBatchSelection::kNone:
    case DeleteBatchSelection::kNoOp:
      delete_json = "[[99]]";
      break;
    case DeleteBatchSelection::kFirstBatch:
      delete_json = "[[1], [2]]";
      expected_json.erase(expected_json.begin());
      break;
    case DeleteBatchSelection::kAll:
      delete_json = "[[1], [2], [3], [4]]";
      expected_json.clear();
      break;
    case DeleteBatchSelection::kEmptySplit:
      delete_json = "[[99]]";
      expected_json.clear();
      break;
  }
  ICEBERG_UNWRAP_OR_FAIL(auto data_file,
                         stored_mixed
                             ? MakeDataFileWithStoredSchema(expected_schema, json)
                             : MakeDataFile(table_schema_, json, /*record_count=*/4));
  // A split covering only the first Parquet magic byte selects no row groups.
  const int64_t length =
      deletes == DeleteBatchSelection::kEmptySplit ? 1 : data_file->file_size_in_bytes;
  std::vector<std::shared_ptr<DataFile>> delete_files;
  if (deletes == DeleteBatchSelection::kPosition) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto pos_delete,
        MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"), {0},
                               data_file->file_path));
    delete_files.push_back(pos_delete);
  } else if (deletes != DeleteBatchSelection::kNone) {
    auto delete_schema =
        std::make_shared<Schema>(std::vector<SchemaField>{table_schema_->fields()[0]});
    ICEBERG_UNWRAP_OR_FAIL(
        auto eq_delete,
        MakeEqualityDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"), delete_schema,
                               delete_json, {1}));
    delete_files.push_back(eq_delete);
  }
  ICEBERG_UNWRAP_OR_FAIL(auto task,
                         FileScanTask::MakeSplit(data_file, 0, length, delete_files));

  ReaderProperties properties;
  properties.Set(ReaderProperties::kBatchSize, int64_t{2});
  properties.Set(ReaderProperties::kArrowUseLargeList, use_large_list);
  FileScanTaskReader::Options options{.io = file_io_,
                                      .table_schema = table_schema_,
                                      .projected_schema = projected_schema_,
                                      .properties = properties.configs()};
  ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
  auto result = reader->Open(*task);
  if (stored_mixed) {
    EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
    EXPECT_THAT(result, HasErrorMessage("read.arrow.use-large-list="));
    EXPECT_THAT(result, HasErrorMessage(use_large_list ? "nested.tags" : "numbers"));
    return;
  }
  ICEBERG_UNWRAP_OR_FAIL(auto opened_stream, std::move(result));
  nanoarrow::UniqueArrayStream stream(&opened_stream);
  // The stream owns everything it needs after the task reader is destroyed.
  reader.reset();
  nanoarrow::UniqueSchema retained_schema;
  ASSERT_EQ(stream->get_schema(stream.get(), retained_schema.get()), 0)
      << stream->get_last_error(stream.get());
  auto output_schema =
      prune_columns
          ? ::arrow::schema({expected_schema->field(2), expected_schema->field(1)})
          : expected_schema;
  auto verify_schema = [&]() {
    nanoarrow::UniqueSchema declared_schema;
    ASSERT_EQ(stream->get_schema(stream.get(), declared_schema.get()), 0)
        << stream->get_last_error(stream.get());
    auto import_result = ::arrow::ImportSchema(declared_schema.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    auto actual = import_result.ValueOrDie();
    ASSERT_TRUE(actual->Equals(*output_schema)) << actual->ToString() << "\nExpected:\n"
                                                << output_schema->ToString();
  };
  // Check the declaration before importing arrays: a list/large_list mismatch can
  // otherwise interpret 64-bit offsets as 32-bit offsets without an import error.
  // Each callback result is released independently of the stream's cached schema.
  ASSERT_NO_FATAL_FAILURE(verify_schema());
  ASSERT_NO_FATAL_FAILURE(verify_schema());

  std::vector<std::shared_ptr<::arrow::RecordBatch>> retained_batches;
  std::vector<std::shared_ptr<::arrow::RecordBatch>> expected_batches;
  for (const auto& json : expected_json) {
    nanoarrow::UniqueArray array;
    ASSERT_EQ(stream->get_next(stream.get(), array.get()), 0)
        << stream->get_last_error(stream.get());
    ASSERT_NE(array->release, nullptr);
    ASSERT_NO_FATAL_FAILURE(verify_schema());
    auto result = ::arrow::ImportRecordBatch(array.get(), output_schema);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    auto actual = result.ValueOrDie();
    ASSERT_NE(actual, nullptr);
    ASSERT_TRUE(actual->ValidateFull().ok()) << actual->ValidateFull().ToString();
    ASSERT_TRUE(actual->schema()->Equals(*output_schema));
    ICEBERG_UNWRAP_OR_FAIL(auto expected, MakeRecordBatch(expected_schema, json));
    if (prune_columns) {
      expected = expected->SelectColumns({2, 1}).ValueOrDie();
    }
    EXPECT_TRUE(actual->Equals(*expected)) << "Actual:\n"
                                           << actual->ToString() << "\nExpected:\n"
                                           << expected->ToString();
    retained_batches.push_back(std::move(actual));
    expected_batches.push_back(std::move(expected));
  }
  nanoarrow::UniqueArray end;
  ASSERT_EQ(stream->get_next(stream.get(), end.get()), 0)
      << stream->get_last_error(stream.get());
  EXPECT_EQ(end->release, nullptr);
  ASSERT_NO_FATAL_FAILURE(verify_schema());

  // Returned schemas and batches remain valid after the stream's context is released.
  stream.reset();
  auto retained_result = ::arrow::ImportSchema(retained_schema.get());
  ASSERT_TRUE(retained_result.ok()) << retained_result.status().ToString();
  EXPECT_TRUE(retained_result.ValueOrDie()->Equals(*output_schema));
  for (size_t i = 0; i < retained_batches.size(); ++i) {
    ASSERT_TRUE(retained_batches[i]->ValidateFull().ok());
    EXPECT_TRUE(retained_batches[i]->Equals(*expected_batches[i]));
  }
}

INSTANTIATE_TEST_SUITE_P(
    DeleteBatches, FileScanTaskPhysicalSchemaTest,
    ::testing::Combine(
        ::testing::Values(DeleteBatchSelection::kFirstRow,
                          DeleteBatchSelection::kSecondBatchRow,
                          DeleteBatchSelection::kNoOp, DeleteBatchSelection::kFirstBatch,
                          DeleteBatchSelection::kAll, DeleteBatchSelection::kEmptySplit,
                          DeleteBatchSelection::kNone, DeleteBatchSelection::kPosition),
        ::testing::Bool(),
        ::testing::Values(PhysicalListLayout::kList, PhysicalListLayout::kLargeList,
                          PhysicalListLayout::kStoredMixed,
                          PhysicalListLayout::kStoredMixedLargeList)),
    FileScanTaskPhysicalSchemaTest::ParamName);

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

TEST_F(FileScanTaskReaderTest, ResidualsWithReusedEqualityDeleteNames) {
  for (const std::string old_name : {"name", "NAME"}) {
    auto current_schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(4, "name", string())},
        2);
    auto old_schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, old_name, string())},
        1);
    // Reorder output columns so residual field positions must be mapped by id.
    auto projection = std::make_shared<Schema>(std::vector<SchemaField>{
        current_schema->fields()[1], current_schema->fields()[0]});
    ICEBERG_UNWRAP_OR_FAIL(
        auto data_file,
        MakeDataFile(old_schema, R"([[1, "keep"], [2, "gone"], [3, "keep"]])"));
    ICEBERG_UNWRAP_OR_FAIL(
        auto deletion, MakeEqualityDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                              old_schema, R"([[0, "gone"]])", {2}));
    for (bool case_sensitive : {false, true}) {
      auto is_null = Expressions::IsNull(case_sensitive ? "name" : "NAME");
      auto greater = Expressions::GreaterThan("id", Literal::Int(1));
      for (const auto& [residual, expected] :
           std::vector<std::pair<std::shared_ptr<Expression>, std::string>>{
               {is_null, "[[null, 1], [null, 3]]"},
               {greater, "[[null, 3]]"},
               {Expressions::And(is_null, greater), "[[null, 3]]"}}) {
        SCOPED_TRACE(old_name + ": " + residual->ToString());
        ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make({
                                                .io = file_io_,
                                                .table_schema = current_schema,
                                                .schemas = {current_schema, old_schema},
                                                .projected_schema = projection,
                                                .case_sensitive = case_sensitive,
                                                .apply_residuals = true,
                                            }));
        FileScanTask task(data_file, {deletion}, residual);
        ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));
        VerifyStream(&stream, expected);
      }
    }
  }
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

TEST_F(FileScanTaskReaderTest, RowGroupPruningPreservesDeletesAndLineage) {
  auto lineage_schema = RowLineageProjection();
  std::vector<SchemaField> read_fields(lineage_schema->fields().begin(),
                                       lineage_schema->fields().end());
  read_fields.push_back(table_schema_->fields()[2]);
  auto read_schema = std::make_shared<Schema>(std::move(read_fields));
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   std::vector<std::string>{R"([[0,"a","keep"],[1,"b","keep"]])",
                                            R"([[2,"c","skip"],[3,"d","skip"]])",
                                            R"([[4,"e","keep"],[5,"f","keep"]])"},
                   6, 2));
  data_file->first_row_id = 100;
  data_file->data_sequence_number = 5;
  ICEBERG_UNWRAP_OR_FAIL(
      auto pos_delete, MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                              {4}, data_file->file_path));
  ICEBERG_UNWRAP_OR_FAIL(
      auto dv, MakeDeletionVectorFile(CreateNewTempFilePathWithSuffix(".puffin"), {4},
                                      data_file->file_path));
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_delete, MakeEqualityDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                             table_schema_, R"([[4,"e","keep"]])", {1}));
  for (auto delete_file : {pos_delete, dv, eq_delete}) {
    FileScanTask task(data_file, {delete_file},
                      Expressions::Equal("category", Literal::String("keep")));
    FileScanTaskReader::Options options{
        .io = file_io_,
        .table_schema = table_schema_,
        .schemas = {table_schema_},
        .projected_schema = read_schema,
    };
    ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
    ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));
    auto batches = ::arrow::ImportRecordBatchReader(&stream).ValueOrDie();
    std::vector<int32_t> actual_ids;
    std::vector<int64_t> lineage;
    while (true) {
      auto batch = batches->Next().ValueOrDie();
      if (!batch) break;
      auto ids = std::static_pointer_cast<::arrow::Int32Array>(batch->column(0));
      auto row_ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
      for (int64_t i = 0; i < batch->num_rows(); ++i) {
        actual_ids.push_back(ids->Value(i));
        lineage.push_back(row_ids->Value(i));
      }
    }
    EXPECT_EQ(actual_ids, (std::vector<int32_t>{0, 1, 5}));
    EXPECT_EQ(lineage, (std::vector<int64_t>{100, 101, 105}));
  }
}

TEST_F(FileScanTaskReaderTest, RowGroupPruningUsesCallerReadSchema) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   std::vector<std::string>{R"([[0,"a","skip"],[1,"b","skip"]])",
                                            R"([[2,"c","keep"],[3,"d","keep"]])"},
                   4, 2));
  auto filter = Expressions::Equal("category", Literal::String("keep"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound, Binder::Bind(*table_schema_, filter, true));
  for (const auto& predicate : std::vector<std::shared_ptr<Expression>>{
           filter, bound, Expressions::Equal("CATEGORY", Literal::String("keep"))}) {
    FileScanTask task(data_file, {}, predicate);
    FileScanTaskReader::Options options{
        .io = file_io_,
        .table_schema = table_schema_,
        .projected_schema = table_schema_,
    };
    options.case_sensitive = false;
    ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(std::move(options)));
    ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));
    VerifyStream(&stream, R"([[2,"c","keep"],[3,"d","keep"]])");
  }
}

TEST_F(FileScanTaskReaderTest, ConstantResidualsAndNestedConstants) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file, MakeDataFile(table_schema_, R"([[0,"a","keep"],[1,"b","keep"]])"));
  auto predicate = Expressions::Equal("name", Literal::String("a"));
  ICEBERG_UNWRAP_OR_FAIL(std::shared_ptr<Expression> conjunction,
                         And::Make(Expressions::AlwaysTrue(), predicate));
  ICEBERG_UNWRAP_OR_FAIL(std::shared_ptr<Expression> disjunction,
                         Or::Make(Expressions::AlwaysFalse(), predicate));
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, FileScanTaskReader::Make({.io = file_io_,
                                             .table_schema = table_schema_,
                                             .projected_schema = projected_schema_}));
  for (const auto& filter : std::vector<std::shared_ptr<Expression>>{
           Expressions::AlwaysTrue(), conjunction, disjunction}) {
    SCOPED_TRACE(filter->ToString());
    FileScanTask task(data_file, {}, filter);
    ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));
    VerifyStream(&stream, R"([[0,"a"],[1,"b"]])");
  }
  FileScanTask empty_task(data_file, {}, Expressions::AlwaysFalse());
  ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(empty_task));
  auto batches = ::arrow::ImportRecordBatchReader(&stream).ValueOrDie();
  EXPECT_EQ(batches->schema()->num_fields(), 2);
  EXPECT_EQ(batches->Next().ValueOrDie(), nullptr);
}

TEST_F(FileScanTaskReaderTest, ScanCaseSensitivityAndProjection) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file, MakeDataFile(table_schema_, R"([[0,"a","keep"],[1,"b","keep"]])"));
  auto filter = Expressions::Equal("CATEGORY", Literal::String("keep"));
  for (bool select_columns : {false, true}) {
    internal::TableScanContext context;
    context.filter = filter;
    context.case_sensitive = false;
    if (select_columns) {
      context.selected_columns = std::vector<std::string>{"id", "name"};
    } else {
      // Explicit projections are prepared by the caller, including filter columns.
      context.projected_schema = table_schema_;
    }
    auto metadata = std::make_shared<TableMetadata>();
    metadata->schemas = {table_schema_};
    metadata->current_schema_id = table_schema_->schema_id();
    ICEBERG_UNWRAP_OR_FAIL(
        auto scan, DataTableScan::Make(metadata, table_schema_, file_io_, context));
    ICEBERG_UNWRAP_OR_FAIL(auto projection, scan->schema());
    FileScanTaskReader::Options options{
        .io = scan->io(),
        .table_schema = table_schema_,
        .projected_schema = projection,
        .case_sensitive = scan->is_case_sensitive(),
    };
    FileScanTask task(data_file, {}, scan->filter());
    ICEBERG_UNWRAP_OR_FAIL(auto reader, FileScanTaskReader::Make(options));
    ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));
    VerifyStream(&stream, R"([[0,"a","keep"],[1,"b","keep"]])");
    options.case_sensitive = true;
    ICEBERG_UNWRAP_OR_FAIL(auto case_sensitive_reader, FileScanTaskReader::Make(options));
    EXPECT_THAT(case_sensitive_reader->Open(task),
                IsError(ErrorKind::kInvalidExpression));
  }
}

TEST_F(FileScanTaskReaderTest, UnboundFilterRequiresProjectedField) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file, MakeDataFile(table_schema_, R"([[0,"a","keep"],[1,"b","keep"]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, FileScanTaskReader::Make({.io = file_io_,
                                             .table_schema = table_schema_,
                                             .projected_schema = projected_schema_}));
  FileScanTask task(data_file, {},
                    Expressions::Equal("category", Literal::String("keep")));
  EXPECT_THAT(reader->Open(task), IsError(ErrorKind::kInvalidExpression));
}

TEST_F(FileScanTaskReaderTest, NestedFilterFieldsComeFromScanSchema) {
  table_schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "record",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeOptional(3, "value", string()),
                                    SchemaField::MakeOptional(4, "key", int32())}))});
  internal::TableScanContext context;
  context.selected_columns = std::vector<std::string>{"id", "record.value"};
  context.filter = Expressions::GreaterThan("record.key", Literal::Int(5));
  auto metadata = std::make_shared<TableMetadata>();
  metadata->schemas = {table_schema_};
  metadata->current_schema_id = table_schema_->schema_id();
  ICEBERG_UNWRAP_OR_FAIL(auto scan,
                         DataTableScan::Make(metadata, table_schema_, file_io_, context));
  ICEBERG_UNWRAP_OR_FAIL(projected_schema_, scan->schema());
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      MakeDataFile(table_schema_,
                   std::vector<std::string>{R"([[0,["a",0]],[1,["b",1]]])",
                                            R"([[2,["c",10]],[3,["d",11]]])"},
                   4, 2));
  ICEBERG_UNWRAP_OR_FAIL(
      auto pos_delete, MakePositionDeleteFile(CreateNewTempFilePathWithSuffix(".parquet"),
                                              {2}, data_file->file_path));
  for (bool with_deletes : {false, true}) {
    FileScanTask task(
        data_file,
        with_deletes ? std::vector{pos_delete} : std::vector<std::shared_ptr<DataFile>>{},
        scan->filter());
    ICEBERG_UNWRAP_OR_FAIL(
        auto reader, FileScanTaskReader::Make({.io = file_io_,
                                               .table_schema = table_schema_,
                                               .projected_schema = projected_schema_}));
    ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(task));
    VerifyStream(&stream,
                 with_deletes ? R"([[3,["d",11]]])" : R"([[2,["c",10]],[3,["d",11]]])");
  }
}

}  // namespace iceberg
