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

#include <memory>
#include <set>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/data/data_writer.h"
#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/data/output_file_cleanup_internal.h"
#include "iceberg/data/position_deletes_internal.h"
#include "iceberg/data_table_scan.h"
#include "iceberg/file_reader.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/update/delete_files.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/row_delta.h"
#include "iceberg/update/update_partition_spec.h"
#include "iceberg/update/update_properties.h"

namespace iceberg {
namespace {

class ScanFileIO : public FileIO {
 public:
  MOCK_METHOD(Result<std::unique_ptr<InputFile>>, NewInputFile, (std::string),
              (override));
};

class PositionDeleteV3Test : public MinimalUpdateTestBase {
 protected:
  static void SetUpTestSuite() {
    avro::RegisterAll();
    parquet::RegisterAll();
  }

  int8_t format_version() const override { return 3; }
  virtual bool unpartitioned() const { return false; }

  void SetUp() override {
    MinimalUpdateTestBase::SetUp();
    ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
    properties->Set(TableProperties::kDeleteParquetCompression.key(), "uncompressed");
    ASSERT_THAT(properties->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
    if (unpartitioned()) {
      ICEBERG_UNWRAP_OR_FAIL(auto evolution, table_->NewUpdatePartitionSpec());
      evolution->RemoveField("x");
      ASSERT_THAT(evolution->Commit(), IsOk());
      ASSERT_THAT(table_->Refresh(), IsOk());
    }
    ICEBERG_UNWRAP_OR_FAIL(auto schema, table_->schema());
    ICEBERG_UNWRAP_OR_FAIL(auto spec, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(
        auto writer,
        DataWriter::Make({
            .path = table_location_ + "/data/file.parquet",
            .schema = schema,
            .spec = spec,
            .partition =
                unpartitioned() ? PartitionValues{} : PartitionValues(Literal::Long(10)),
            .format = FileFormatType::kParquet,
            .io = file_io_,
            .properties = {{"write.parquet.compression-codec", "uncompressed"}},
        }));
    ArrowSchema c_schema{};
    ASSERT_THAT(ToArrowSchema(*schema, &c_schema), IsOk());
    auto type = ::arrow::ImportType(&c_schema).ValueOrDie();
    auto array = ::arrow::json::ArrayFromJSONString(
                     type, "[[10,0,0],[10,1,10],[10,2,20],[10,3,30],[10,4,40]]")
                     .ValueOrDie();
    ArrowArray c_array{};
    ASSERT_TRUE(::arrow::ExportArray(*array, &c_array).ok());
    ASSERT_THAT(writer->Write(&c_array), IsOk());
    ASSERT_THAT(writer->Close(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
    data_file_ = result.data_files.front();
    ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
    append->AppendFile(data_file_);
    ASSERT_THAT(append->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
  }

  Result<DeleteWriteResult> WriteFromScan(int64_t snapshot_id,
                                          std::vector<int64_t> positions,
                                          std::set<std::string>& outputs,
                                          std::shared_ptr<FileIO> scan_io = nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(auto builder, table_->NewScan());
    builder->UseSnapshot(snapshot_id);
    ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Build());
    ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan->PlanFiles());
    return internal::WritePositionDeletes(*table_, scan_io ? scan_io : scan->io(),
                                          {{tasks.at(0), std::move(positions)}}, outputs);
  }

  Status CommitWritten(int64_t snapshot_id, const DeleteWriteResult& written) {
    ICEBERG_ASSIGN_OR_RAISE(auto delta, table_->NewRowDelta());
    delta->ValidateFromSnapshot(snapshot_id)
        .ValidateDataFilesExist(written.referenced_data_files)
        .ValidateDeletedFiles();
    for (const auto& file : written.data_files) delta->AddDeletes(file);
    for (const auto& file : written.rewritten_delete_files) delta->RemoveDeletes(file);
    return delta->Commit();
  }

  void DeletePositions(std::vector<int64_t> positions,
                       std::shared_ptr<FileIO> scan_io = nullptr) {
    auto snapshot_id = table_->metadata()->current_snapshot_id;
    std::set<std::string> outputs;
    ICEBERG_UNWRAP_OR_FAIL(auto written, WriteFromScan(snapshot_id, std::move(positions),
                                                       outputs, std::move(scan_io)));
    ASSERT_THAT(CommitWritten(snapshot_id, written), IsOk());
  }

  std::vector<std::shared_ptr<FileScanTask>> Tasks() {
    ICEBERG_THROW_NOT_OK(table_->Refresh());
    auto builder = table_->NewScan().value();
    return builder->Build().value()->PlanFiles().value();
  }

  std::vector<int64_t> ReadRows() {
    table_ = catalog_->LoadTable(table_ident_).value();
    auto tasks = Tasks();
    auto schema = table_->schema().value();
    auto reader = FileScanTaskReader::Make({
                                               .io = file_io_,
                                               .table_schema = schema,
                                               .schemas = table_->metadata()->schemas,
                                               .projected_schema = schema,
                                           })
                      .value();
    auto stream = reader->Open(*tasks.at(0)).value();
    auto batches = ::arrow::ImportRecordBatchReader(&stream).ValueOrDie();
    std::vector<int64_t> rows;
    while (auto batch = batches->Next().ValueOrDie()) {
      const auto& values = static_cast<const ::arrow::Int64Array&>(*batch->column(1));
      for (int64_t i = 0; i < values.length(); ++i) rows.push_back(values.Value(i));
    }
    return rows;
  }

  std::shared_ptr<DataFile> data_file_;
};

class PositionDeleteUpdateTest : public PositionDeleteV3Test {
 protected:
  int8_t format_version() const override { return 2; }
};

TEST_F(PositionDeleteUpdateTest, WritesSortedParquetDeletes) {
  DeletePositions({3, 1});
  EXPECT_THAT(ReadRows(), ::testing::ElementsAre(0, 2, 4));
  const auto old_delete = Tasks().at(0)->delete_files().at(0);
  ASSERT_EQ(old_delete->file_format, FileFormatType::kParquet);
  auto projection = std::make_shared<Schema>(std::vector<SchemaField>{
      MetadataColumns::kDeleteFilePath, MetadataColumns::kDeleteFilePos});
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      ReaderFactoryRegistry::Open(
          FileFormatType::kParquet,
          {.path = old_delete->file_path, .io = file_io_, .projection = projection}));
  ICEBERG_UNWRAP_OR_FAIL(auto batch, reader->Next());
  ASSERT_TRUE(batch.has_value());
  ArrowSchema schema{};
  ASSERT_THAT(ToArrowSchema(*projection, &schema), IsOk());
  auto rows = ::arrow::ImportArray(&*batch, ::arrow::ImportType(&schema).ValueOrDie())
                  .ValueOrDie();
  const auto& positions = static_cast<const ::arrow::Int64Array&>(
      *static_cast<const ::arrow::StructArray&>(*rows).field(1));
  ASSERT_EQ(positions.length(), 2);
  EXPECT_EQ(positions.Value(0), 1);
  EXPECT_EQ(positions.Value(1), 3);
}

TEST_F(PositionDeleteUpdateTest, UpgradeMergesPreviousParquetDeletes) {
  DeletePositions({3, 1});
  ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
  properties->Set(TableProperties::kFormatVersion.key(), "3");
  ASSERT_THAT(properties->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  DeletePositions({2});
  EXPECT_THAT(ReadRows(), ::testing::ElementsAre(0, 4));
  auto task = Tasks().at(0);
  ASSERT_EQ(task->delete_files().size(), 1);
  EXPECT_TRUE(task->delete_files().front()->IsDeletionVector());
}

TEST_F(PositionDeleteV3Test, MergesPreviousVectorAfterPartitionEvolution) {
  DeletePositions({3, 1});
  EXPECT_THAT(ReadRows(), ::testing::ElementsAre(0, 2, 4));
  const auto old_delete = Tasks().at(0)->delete_files().at(0);
  ICEBERG_UNWRAP_OR_FAIL(auto evolution, table_->NewUpdatePartitionSpec());
  evolution->RemoveField("x");
  ASSERT_THAT(evolution->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ASSERT_NE(table_->metadata()->default_spec_id, data_file_->partition_spec_id);
  auto scan_io = std::make_shared<ScanFileIO>();
  EXPECT_CALL(*scan_io, NewInputFile(old_delete->file_path))
      .WillOnce(
          [this](std::string path) { return file_io_->NewInputFile(std::move(path)); });
  DeletePositions({2}, scan_io);
  EXPECT_THAT(ReadRows(), ::testing::ElementsAre(0, 4));
  auto task = Tasks().at(0);
  ASSERT_EQ(task->delete_files().size(), 1);
  const auto& dv = task->delete_files().front();
  EXPECT_EQ(dv->file_format, FileFormatType::kPuffin);
  EXPECT_NE(dv->file_path, old_delete->file_path);
  EXPECT_EQ(dv->partition_spec_id, data_file_->partition_spec_id);
  EXPECT_EQ(dv->partition, data_file_->partition);
}

class UnpartitionedPositionDeleteV3Test : public PositionDeleteV3Test {
 protected:
  bool unpartitioned() const override { return true; }
};

TEST_F(UnpartitionedPositionDeleteV3Test, ReloadsCommittedDeletes) {
  DeletePositions({1, 2});
  EXPECT_THAT(ReadRows(), ::testing::ElementsAre(0, 3, 4));
}

TEST_F(PositionDeleteV3Test, RejectsPositionPastEndOfFile) {
  std::set<std::string> outputs;
  EXPECT_THAT(WriteFromScan(table_->metadata()->current_snapshot_id,
                            {data_file_->record_count}, outputs),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_TRUE(outputs.empty());
}

TEST_F(PositionDeleteV3Test, ConcurrentRemovalRejectsPreparedDeletes) {
  auto snapshot_id = table_->metadata()->current_snapshot_id;
  std::set<std::string> outputs;
  ICEBERG_UNWRAP_OR_FAIL(auto written, WriteFromScan(snapshot_id, {1}, outputs));

  ICEBERG_UNWRAP_OR_FAIL(auto removal, table_->NewDeleteFiles());
  removal->DeleteFile(data_file_);
  ASSERT_THAT(removal->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  EXPECT_THAT(CommitWritten(snapshot_id, written), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(internal::CleanupOutputFiles(*file_io_, outputs), IsOk());
  EXPECT_TRUE(Tasks().empty());
}

}  // namespace
}  // namespace iceberg
