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

#include "iceberg/data/position_delete_update.h"

#include <memory>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/data/data_writer.h"
#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/file_reader.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/update/delete_files.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/update_partition_spec.h"
#include "iceberg/update/update_properties.h"

namespace iceberg {
namespace {

class FailOncePuffinDeleteFileIO : public arrow::ArrowFileSystemFileIO {
 public:
  explicit FailOncePuffinDeleteFileIO(std::shared_ptr<::arrow::fs::FileSystem> fs)
      : ArrowFileSystemFileIO(std::move(fs)) {}

  Status DeleteFile(const std::string& path) override {
    if (path.ends_with(".puffin") && delete_attempts++ == 0) {
      return IOError("injected cleanup failure");
    }
    return ArrowFileSystemFileIO::DeleteFile(path);
  }

  int delete_attempts = 0;
};

class PositionDeleteV3Test : public MinimalUpdateTestBase {
 protected:
  static void SetUpTestSuite() {
    avro::RegisterAll();
    parquet::RegisterAll();
  }

  int8_t format_version() const override { return 3; }
  virtual bool unpartitioned() const { return false; }

  void SetRetries(int retries) {
    ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
    properties->Set(TableProperties::kCommitNumRetries.key(), std::to_string(retries))
        .Set(TableProperties::kCommitMinRetryWaitMs.key(), "1")
        .Set(TableProperties::kCommitMaxRetryWaitMs.key(), "1")
        .Set(TableProperties::kDeleteParquetCompression.key(), "uncompressed");
    ASSERT_THAT(properties->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
  }

  void SetUp() override {
    MinimalUpdateTestBase::SetUp();
    SetRetries(0);
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

  std::vector<std::string> PuffinFiles() {
    auto io = std::static_pointer_cast<arrow::ArrowFileSystemFileIO>(file_io_);
    ::arrow::fs::FileSelector selector;
    selector.base_dir = table_location_ + "/data";
    std::vector<std::string> paths;
    for (const auto& info : io->fs()->GetFileInfo(selector).ValueOrDie()) {
      if (info.path().ends_with(".puffin")) paths.push_back(info.path());
    }
    return paths;
  }

  std::shared_ptr<DataFile> data_file_;
};

struct RoutingCase {
  int8_t format_version;
  bool unpartitioned;
};

class PositionDeleteUpdateTest : public PositionDeleteV3Test,
                                 public ::testing::WithParamInterface<RoutingCase> {
 protected:
  int8_t format_version() const override { return GetParam().format_version; }
  bool unpartitioned() const override { return GetParam().unpartitioned; }
};

TEST_P(PositionDeleteUpdateTest, DeleteCommitReloadAndMerge) {
  ICEBERG_UNWRAP_OR_FAIL(auto first, PositionDeleteUpdate::Make(table_));
  first->Delete(data_file_->file_path, 3).Delete(data_file_->file_path, 1);
  ASSERT_THAT(first->Commit(), IsOk());
  EXPECT_THAT(ReadRows(), ::testing::ElementsAre(0, 2, 4));
  auto task = Tasks().at(0);
  ASSERT_EQ(task->delete_files().size(), 1);
  const auto old_delete = task->delete_files().front();
  EXPECT_EQ(old_delete->file_format,
            format_version() == 2 ? FileFormatType::kParquet : FileFormatType::kPuffin);

  if (format_version() == 2) {
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
    ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
    properties->Set(TableProperties::kFormatVersion.key(), "3");
    ASSERT_THAT(properties->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
  }
  if (!unpartitioned()) {
    ICEBERG_UNWRAP_OR_FAIL(auto evolution, table_->NewUpdatePartitionSpec());
    evolution->RemoveField("x");
    ASSERT_THAT(evolution->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
    ASSERT_NE(table_->metadata()->default_spec_id, data_file_->partition_spec_id);
  }

  ICEBERG_UNWRAP_OR_FAIL(auto second, PositionDeleteUpdate::Make(table_));
  second->Delete(data_file_->file_path, 2);
  ASSERT_THAT(second->Commit(), IsOk());
  EXPECT_THAT(ReadRows(), ::testing::ElementsAre(0, 4));
  task = Tasks().at(0);
  ASSERT_EQ(task->delete_files().size(), 1);
  const auto& dv = task->delete_files().front();
  EXPECT_EQ(dv->file_format, FileFormatType::kPuffin);
  EXPECT_NE(dv->file_path, old_delete->file_path);
  EXPECT_EQ(dv->partition_spec_id, data_file_->partition_spec_id);
  EXPECT_EQ(dv->partition, data_file_->partition);
}

INSTANTIATE_TEST_SUITE_P(FormatAndPartitioning, PositionDeleteUpdateTest,
                         ::testing::Values(RoutingCase{2, false}, RoutingCase{3, false},
                                           RoutingCase{3, true}));

TEST_F(PositionDeleteV3Test, ConcurrentRemovalRejectsPreparedDeletes) {
  SetRetries(1);
  auto mock = std::make_shared<::testing::NiceMock<MockCatalog>>();
  ON_CALL(*mock, LoadTable(::testing::_)).WillByDefault([this](const auto& name) {
    return catalog_->LoadTable(name);
  });
  EXPECT_CALL(*mock, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillOnce([this](const auto&, const auto&,
                       const auto&) -> Result<std::shared_ptr<Table>> {
        ICEBERG_ASSIGN_OR_RAISE(auto removal, table_->NewDeleteFiles());
        removal->DeleteFile(data_file_);
        ICEBERG_RETURN_UNEXPECTED(removal->Commit());
        return CommitFailed("concurrent removal committed");
      });
  ICEBERG_UNWRAP_OR_FAIL(
      auto competing,
      Table::Make(table_->name(), table_->metadata(),
                  std::string(table_->metadata_file_location()), file_io_, mock));
  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(competing));
  update->Delete(data_file_->file_path, 1);
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_TRUE(Tasks().empty());
  EXPECT_TRUE(PuffinFiles().empty());
}

TEST_F(PositionDeleteV3Test, CommitRetryReusesWrittenDV) {
  SetRetries(1);
  FailCommits(1);
  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(table_));
  update->Delete(data_file_->file_path, 1);
  ASSERT_THAT(update->Commit(), IsOk());
  EXPECT_EQ(PuffinFiles().size(), 1);
}

TEST_F(PositionDeleteV3Test, CleanupFailureIsReportedAndRetried) {
  auto mock = std::make_shared<::testing::NiceMock<MockCatalog>>();
  EXPECT_CALL(*mock, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillOnce(::testing::Return(CommitFailed("injected commit failure")))
      .WillOnce([this](const auto& name, const auto& requirements, const auto& updates) {
        return catalog_->UpdateTable(name, requirements, updates);
      });
  auto io = std::static_pointer_cast<arrow::ArrowFileSystemFileIO>(file_io_);
  auto failing_io = std::make_shared<FailOncePuffinDeleteFileIO>(io->fs());
  ICEBERG_UNWRAP_OR_FAIL(
      auto failing,
      Table::Make(table_->name(), table_->metadata(),
                  std::string(table_->metadata_file_location()), failing_io, mock));
  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(failing));
  update->Delete(data_file_->file_path, 1);
  auto status = update->Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("injected cleanup failure"));
  const auto retained = PuffinFiles();
  ASSERT_EQ(retained.size(), 1);
  ASSERT_THAT(update->Commit(), IsOk());
  const auto committed = PuffinFiles();
  ASSERT_EQ(committed.size(), 1);
  EXPECT_NE(committed.front(), retained.front());
  EXPECT_EQ(failing_io->delete_attempts, 2);
}

TEST_F(PositionDeleteV3Test, CommitStateUnknownPreservesOutput) {
  auto mock = std::make_shared<::testing::NiceMock<MockCatalog>>();
  EXPECT_CALL(*mock, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillOnce(::testing::Return(CommitStateUnknown("injected unknown state")));
  ICEBERG_UNWRAP_OR_FAIL(
      auto unknown,
      Table::Make(table_->name(), table_->metadata(),
                  std::string(table_->metadata_file_location()), file_io_, mock));
  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(unknown));
  update->Delete(data_file_->file_path, 1);
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kCommitStateUnknown));
  const auto retained = PuffinFiles();
  ASSERT_EQ(retained.size(), 1);
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kInvalidArgument));
  EXPECT_EQ(PuffinFiles(), retained);
}

}  // namespace
}  // namespace iceberg
