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

#include <algorithm>
#include <cstdint>
#include <format>
#include <iterator>
#include <memory>
#include <span>
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
#include "iceberg/data/delete_loader.h"
#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/update/delete_files.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/row_delta.h"
#include "iceberg/update/update_partition_spec.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

namespace {

struct RoutingCase {
  int8_t format_version;
  bool unpartitioned;
  FileFormatType expected_format;
};

class FailOncePuffinDeleteFileIO : public arrow::ArrowFileSystemFileIO {
 public:
  explicit FailOncePuffinDeleteFileIO(
      std::shared_ptr<::arrow::fs::FileSystem> file_system)
      : ArrowFileSystemFileIO(std::move(file_system)) {}

  Status DeleteFile(const std::string& file_location) override {
    if (!file_location.ends_with(".puffin")) {
      return ArrowFileSystemFileIO::DeleteFile(file_location);
    }

    puffin_delete_attempts.push_back(file_location);
    if (fail_next_puffin_delete_) {
      fail_next_puffin_delete_ = false;
      return IOError("injected cleanup failure for {}", file_location);
    }
    return ArrowFileSystemFileIO::DeleteFile(file_location);
  }

  std::vector<std::string> puffin_delete_attempts;

 private:
  bool fail_next_puffin_delete_ = true;
};

class PositionDeleteUpdateTest : public MinimalUpdateTestBase,
                                 public ::testing::WithParamInterface<RoutingCase> {
 protected:
  static void SetUpTestSuite() {
    avro::RegisterAll();
    parquet::RegisterAll();
  }

  int8_t format_version() const override { return GetParam().format_version; }

  void SetUp() override {
    MinimalUpdateTestBase::SetUp();
    if (GetParam().unpartitioned) {
      RegisterUnpartitionedTable();
    }
    if (GetParam().format_version == 2) {
      ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
      properties->Set(TableProperties::kDeleteParquetCompression.key(), "uncompressed");
      ASSERT_THAT(properties->Commit(), IsOk());
      ASSERT_THAT(table_->Refresh(), IsOk());
    }
    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(data_file_, WriteDataFile());
    AppendDataFile();
  }

  void RegisterUnpartitionedTable() {
    ICEBERG_UNWRAP_OR_FAIL(
        auto metadata, ReadTableMetadataFromResource("TableMetadataV3ValidMinimal.json"));
    metadata->location = table_location_;
    metadata->partition_specs = {PartitionSpec::Unpartitioned()};
    metadata->default_spec_id = PartitionSpec::kInitialSpecId;

    const auto metadata_location =
        std::format("{}/metadata/00001-{}.metadata.json", table_location_,
                    Uuid::GenerateV7().ToString());
    ASSERT_THAT(TableMetadataUtil::Write(*file_io_, metadata_location, *metadata),
                IsOk());
    ASSERT_THAT(catalog_->DropTable(table_ident_, /*purge=*/false), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(table_,
                           catalog_->RegisterTable(table_ident_, metadata_location));
  }

  Result<std::shared_ptr<DataFile>> WriteDataFile() {
    ICEBERG_ASSIGN_OR_RAISE(auto schema, table_->schema());
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        DataWriter::Make({
            .path = table_location_ + "/data/file.parquet",
            .schema = schema,
            .spec = spec_,
            .partition = GetParam().unpartitioned ? PartitionValues{}
                                                  : PartitionValues({Literal::Long(10)}),
            .format = FileFormatType::kParquet,
            .io = file_io_,
            .properties = {{"write.parquet.compression-codec", "uncompressed"}},
        }));
    ArrowSchema c_schema{};
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema, &c_schema));
    auto type = ::arrow::ImportType(&c_schema).ValueOrDie();
    std::string json = "[";
    for (int64_t pos = 0; pos < 10; ++pos) {
      if (pos != 0) json += ",";
      json += std::format("[10,{},{}]", pos, pos * 10);
    }
    json += "]";
    auto array = ::arrow::json::ArrayFromJSONString(type, json).ValueOrDie();
    ArrowArray c_array{};
    auto status = ::arrow::ExportArray(*array, &c_array);
    if (!status.ok()) return UnknownError(status.ToString());
    ICEBERG_RETURN_UNEXPECTED(writer->Write(&c_array));
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
    return metadata.data_files.front();
  }

  Result<std::vector<int64_t>> ReadSurvivingRows() {
    ICEBERG_ASSIGN_OR_RAISE(table_, catalog_->LoadTable(table_ident_));
    ICEBERG_ASSIGN_OR_RAISE(auto schema, table_->schema());
    ICEBERG_ASSIGN_OR_RAISE(auto task, CurrentTask());
    ICEBERG_ASSIGN_OR_RAISE(auto reader, FileScanTaskReader::Make({
                                             .io = file_io_,
                                             .table_schema = schema,
                                             .schemas = table_->metadata()->schemas,
                                             .projected_schema = schema,
                                         }));
    ICEBERG_ASSIGN_OR_RAISE(auto stream, reader->Open(*task));
    auto batches = ::arrow::ImportRecordBatchReader(&stream).ValueOrDie();
    std::vector<int64_t> rows;
    while (auto batch = batches->Next().ValueOrDie()) {
      auto values = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
      for (int64_t i = 0; i < values->length(); ++i) rows.push_back(values->Value(i));
    }
    return rows;
  }

  void AppendDataFile() {
    ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
    append->AppendFile(data_file_);
    ASSERT_THAT(append->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
  }

  Result<std::shared_ptr<FileScanTask>> CurrentTask() {
    ICEBERG_ASSIGN_OR_RAISE(auto builder, table_->NewScan());
    ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Build());
    ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan->PlanFiles());
    ICEBERG_CHECK(tasks.size() == 1, "Expected one file scan task, found {}",
                  tasks.size());
    return tasks.front();
  }

  Result<PositionDeleteIndex> LoadPositions(const FileScanTask& task) {
    std::vector<std::shared_ptr<DataFile>> deletes;
    std::ranges::copy_if(task.delete_files(), std::back_inserter(deletes),
                         [](const auto& file) {
                           return file->content == DataFile::Content::kPositionDeletes;
                         });
    DeleteLoader loader(file_io_);
    return loader.LoadPositionDeletes(deletes, task.data_file()->file_path);
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<DataFile> data_file_;
};

TEST_P(PositionDeleteUpdateTest, RoutesAndCommitsPositionDeletes) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(table_));
  update->Delete(data_file_->file_path, 7).Delete(data_file_->file_path, 2);
  ASSERT_THAT(update->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto task, CurrentTask());
  ASSERT_EQ(task->delete_files().size(), 1);
  const auto& delete_file = task->delete_files().front();
  EXPECT_EQ(delete_file->file_format, GetParam().expected_format);
  EXPECT_EQ(delete_file->partition_spec_id, data_file_->partition_spec_id);
  EXPECT_EQ(delete_file->partition, data_file_->partition);

  ICEBERG_UNWRAP_OR_FAIL(auto positions, LoadPositions(*task));
  EXPECT_EQ(positions.Cardinality(), 2);
  EXPECT_TRUE(positions.IsDeleted(2));
  EXPECT_TRUE(positions.IsDeleted(7));
  if (GetParam().format_version == 2) {
    auto delete_schema = std::make_shared<Schema>(std::vector<SchemaField>{
        MetadataColumns::kDeleteFilePath, MetadataColumns::kDeleteFilePos});
    ICEBERG_UNWRAP_OR_FAIL(auto reader,
                           ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                                       {.path = delete_file->file_path,
                                                        .io = file_io_,
                                                        .projection = delete_schema}));
    ICEBERG_UNWRAP_OR_FAIL(auto batch, reader->Next());
    ASSERT_TRUE(batch.has_value());

    ArrowSchema arrow_schema;
    ASSERT_THAT(ToArrowSchema(*delete_schema, &arrow_schema), IsOk());
    auto arrow_type = ::arrow::ImportType(&arrow_schema).ValueOrDie();
    auto rows = ::arrow::ImportArray(&batch.value(), arrow_type).ValueOrDie();
    auto struct_rows = std::static_pointer_cast<::arrow::StructArray>(rows);
    auto positions = std::static_pointer_cast<::arrow::Int64Array>(struct_rows->field(1));
    ASSERT_EQ(positions->length(), 2);
    EXPECT_EQ(positions->Value(0), 2);
    EXPECT_EQ(positions->Value(1), 7);
  }
}

TEST_P(PositionDeleteUpdateTest, ReopenedTablePreservesBothDeletes) {
  ICEBERG_UNWRAP_OR_FAIL(auto first, PositionDeleteUpdate::Make(table_));
  first->Delete(data_file_->file_path, 2).Delete(data_file_->file_path, 7);
  ASSERT_THAT(first->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto rows, ReadSurvivingRows());
  EXPECT_THAT(rows, ::testing::ElementsAre(0, 1, 3, 4, 5, 6, 8, 9));

  if (GetParam().format_version == 3 && !GetParam().unpartitioned) {
    ICEBERG_UNWRAP_OR_FAIL(auto evolution, table_->NewUpdatePartitionSpec());
    evolution->RemoveField("x");
    ASSERT_THAT(evolution->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
  }
  ICEBERG_UNWRAP_OR_FAIL(auto second, PositionDeleteUpdate::Make(table_));
  second->Delete(data_file_->file_path, 4);
  ASSERT_THAT(second->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(rows, ReadSurvivingRows());
  EXPECT_THAT(rows, ::testing::ElementsAre(0, 1, 3, 5, 6, 8, 9));
}

TEST_P(PositionDeleteUpdateTest, ConcurrentRemovalRejectsPreparedDeletes) {
  ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
  properties->Set(TableProperties::kCommitMinRetryWaitMs.key(), "1")
      .Set(TableProperties::kCommitMaxRetryWaitMs.key(), "1");
  ASSERT_THAT(properties->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  auto mock = std::make_shared<::testing::NiceMock<MockCatalog>>();
  ON_CALL(*mock, LoadTable(::testing::_))
      .WillByDefault(
          [this](const TableIdentifier& name) { return catalog_->LoadTable(name); });
  EXPECT_CALL(*mock, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .Times(1)
      .WillOnce([this](const auto&, const auto&,
                       const auto&) -> Result<std::shared_ptr<Table>> {
        ICEBERG_ASSIGN_OR_RAISE(auto removal, table_->NewDeleteFiles());
        removal->DeleteFile(data_file_);
        ICEBERG_RETURN_UNEXPECTED(removal->Commit());
        return CommitFailed("concurrent removal committed");
      });
  ICEBERG_UNWRAP_OR_FAIL(
      auto competing_table,
      Table::Make(table_->name(), table_->metadata(),
                  std::string(table_->metadata_file_location()), file_io_, mock));
  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(competing_table));
  update->Delete(data_file_->file_path, 2);
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto builder, reloaded->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

INSTANTIATE_TEST_SUITE_P(
    FormatAndPartitioning, PositionDeleteUpdateTest,
    ::testing::Values(RoutingCase{.format_version = 2,
                                  .unpartitioned = false,
                                  .expected_format = FileFormatType::kParquet},
                      RoutingCase{.format_version = 3,
                                  .unpartitioned = false,
                                  .expected_format = FileFormatType::kPuffin},
                      RoutingCase{.format_version = 3,
                                  .unpartitioned = true,
                                  .expected_format = FileFormatType::kPuffin}));

class PositionDeleteV3Test : public MinimalUpdateTestBase {
 protected:
  static void SetUpTestSuite() {
    avro::RegisterAll();
    parquet::RegisterAll();
  }

  int8_t format_version() const override { return 3; }

  void SetUp() override {
    MinimalUpdateTestBase::SetUp();
    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());
    data_file_ = MakeDataFile();
    AppendDataFile();
  }

  std::shared_ptr<DataFile> MakeDataFile() const {
    auto file = std::make_shared<DataFile>();
    file->content = DataFile::Content::kData;
    file->file_path = table_location_ + "/data/file.parquet";
    file->file_format = FileFormatType::kParquet;
    file->partition = PartitionValues({Literal::Long(10)});
    file->file_size_in_bytes = 1024;
    file->record_count = 10;
    file->partition_spec_id = spec_->spec_id();
    return file;
  }

  void AppendDataFile() {
    ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
    append->AppendFile(data_file_);
    ASSERT_THAT(append->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
  }

  Result<std::shared_ptr<FileScanTask>> CurrentTask(const std::shared_ptr<Table>& table) {
    ICEBERG_ASSIGN_OR_RAISE(auto builder, table->NewScan());
    ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Build());
    ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan->PlanFiles());
    ICEBERG_CHECK(tasks.size() == 1, "Expected one file scan task, found {}",
                  tasks.size());
    return tasks.front();
  }

  Result<PositionDeleteIndex> LoadPositions(const FileScanTask& task) {
    DeleteLoader loader(file_io_);
    return loader.LoadPositionDeletes(task.delete_files(), task.data_file()->file_path);
  }

  Result<std::shared_ptr<DataFile>> WritePositionDeletes(
      std::span<const int64_t> positions) {
    const auto path = table_location_ + "/data/existing-position-deletes.parquet";
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        PositionDeleteWriter::Make(PositionDeleteWriterOptions{
            .path = path,
            .schema = schema_,
            .spec = spec_,
            .partition = data_file_->partition,
            .format = FileFormatType::kParquet,
            .io = file_io_,
            .properties = {{"write.parquet.compression-codec", "uncompressed"}},
        }));
    for (int64_t pos : positions) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteDelete(data_file_->file_path, pos));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto result, writer->Metadata());
    ICEBERG_CHECK(result.data_files.size() == 1,
                  "Expected one position delete file, found {}",
                  result.data_files.size());
    return result.data_files.front();
  }

  Result<std::vector<ManifestEntry>> CurrentDeleteEntries() {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    SnapshotReader cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, cache.DeleteManifests(file_io_));
    std::vector<ManifestEntry> entries;
    for (const auto& manifest : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto spec, table_->metadata()->PartitionSpecById(manifest.partition_spec_id));
      ICEBERG_ASSIGN_OR_RAISE(
          auto reader,
          ManifestReader::Make(manifest, file_io_, schema_, std::move(spec)));
      ICEBERG_ASSIGN_OR_RAISE(auto manifest_entries, reader->Entries());
      entries.insert(entries.end(), std::make_move_iterator(manifest_entries.begin()),
                     std::make_move_iterator(manifest_entries.end()));
    }
    return entries;
  }

  void ConfigureRetries(int32_t retries) {
    ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
    properties->Set(TableProperties::kCommitNumRetries.key(), std::to_string(retries))
        .Set(TableProperties::kCommitMinRetryWaitMs.key(), "1")
        .Set(TableProperties::kCommitMaxRetryWaitMs.key(), "1")
        .Set(TableProperties::kCommitTotalRetryTimeMs.key(), "1000");
    ASSERT_THAT(properties->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
  }

  void FailCommit(std::unexpected<Error> failure) {
    auto mock = std::make_shared<::testing::NiceMock<MockCatalog>>();
    EXPECT_CALL(*mock, UpdateTable(::testing::_, ::testing::_, ::testing::_))
        .Times(1)
        .WillOnce(::testing::Return(std::move(failure)));
    ICEBERG_UNWRAP_OR_FAIL(
        table_, Table::Make(table_->name(), table_->metadata(),
                            std::string(table_->metadata_file_location()), file_io_,
                            std::move(mock)));
  }

  std::vector<std::string> PuffinFiles() {
    auto arrow_io = std::dynamic_pointer_cast<arrow::ArrowFileSystemFileIO>(file_io_);
    EXPECT_NE(arrow_io, nullptr);
    ::arrow::fs::FileSelector selector;
    selector.base_dir = table_location_ + "/data";
    selector.recursive = true;
    auto infos = arrow_io->fs()->GetFileInfo(selector);
    EXPECT_TRUE(infos.ok()) << infos.status().ToString();
    std::vector<std::string> paths;
    if (!infos.ok()) {
      return paths;
    }
    for (const auto& info : *infos) {
      if (info.path().ends_with(".puffin")) {
        paths.push_back(info.path());
      }
    }
    return paths;
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> data_file_;
};

TEST_F(PositionDeleteV3Test, SecondDeleteMergesAndSupersedesPreviousDV) {
  ICEBERG_UNWRAP_OR_FAIL(auto first, PositionDeleteUpdate::Make(table_));
  first->Delete(data_file_->file_path, 1);
  ASSERT_THAT(first->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto old_task, CurrentTask(table_));
  const auto old_dv = old_task->delete_files().front();

  ICEBERG_UNWRAP_OR_FAIL(auto second, PositionDeleteUpdate::Make(table_));
  second->Delete(data_file_->file_path, 3);
  ASSERT_THAT(second->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto task, CurrentTask(table_));
  ASSERT_EQ(task->delete_files().size(), 1);
  EXPECT_NE(task->delete_files().front()->file_path, old_dv->file_path);
  ICEBERG_UNWRAP_OR_FAIL(auto positions, LoadPositions(*task));
  EXPECT_EQ(positions.Cardinality(), 2);
  EXPECT_TRUE(positions.IsDeleted(1));
  EXPECT_TRUE(positions.IsDeleted(3));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, CurrentDeleteEntries());
  EXPECT_TRUE(std::ranges::any_of(entries, [&old_dv](const ManifestEntry& entry) {
    return entry.status == ManifestStatus::kDeleted && entry.data_file != nullptr &&
           entry.data_file->file_path == old_dv->file_path &&
           entry.data_file->content_offset == old_dv->content_offset;
  }));
}

TEST_F(PositionDeleteV3Test, MergesAndSupersedesFileScopedParquetDelete) {
  RegisterTableFromResource("TableMetadataV2ValidMinimal.json");
  ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
  ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());
  data_file_ = MakeDataFile();
  AppendDataFile();

  const std::vector<int64_t> existing_positions{1, 3};
  ICEBERG_UNWRAP_OR_FAIL(auto old_delete, WritePositionDeletes(existing_positions));
  ASSERT_EQ(old_delete->referenced_data_file, data_file_->file_path);
  ICEBERG_UNWRAP_OR_FAIL(auto row_delta, table_->NewRowDelta());
  row_delta->AddDeletes(old_delete);
  ASSERT_THAT(row_delta->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
  properties->Set(TableProperties::kFormatVersion.key(), "3");
  ASSERT_THAT(properties->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(table_));
  update->Delete(data_file_->file_path, 5);
  ASSERT_THAT(update->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto task, CurrentTask(table_));
  ASSERT_EQ(task->delete_files().size(), 1);
  EXPECT_EQ(task->delete_files().front()->file_format, FileFormatType::kPuffin);
  ICEBERG_UNWRAP_OR_FAIL(auto positions, LoadPositions(*task));
  EXPECT_EQ(positions.Cardinality(), 3);
  EXPECT_TRUE(positions.IsDeleted(1));
  EXPECT_TRUE(positions.IsDeleted(3));
  EXPECT_TRUE(positions.IsDeleted(5));

  ICEBERG_UNWRAP_OR_FAIL(auto entries, CurrentDeleteEntries());
  EXPECT_TRUE(std::ranges::any_of(entries, [&old_delete](const ManifestEntry& entry) {
    return entry.status == ManifestStatus::kDeleted && entry.data_file != nullptr &&
           entry.data_file->file_path == old_delete->file_path;
  }));
}

TEST_F(PositionDeleteV3Test, UsesTargetDataFileSpecAfterPartitionEvolution) {
  ICEBERG_UNWRAP_OR_FAIL(auto spec_update, table_->NewUpdatePartitionSpec());
  spec_update->RemoveField("x");
  ASSERT_THAT(spec_update->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ASSERT_NE(table_->metadata()->default_spec_id, data_file_->partition_spec_id);

  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(table_));
  update->Delete(data_file_->file_path, 4);
  ASSERT_THAT(update->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto task, CurrentTask(table_));
  const auto& dv = task->delete_files().front();
  EXPECT_EQ(dv->partition_spec_id, data_file_->partition_spec_id);
  EXPECT_EQ(dv->partition, data_file_->partition);
}

TEST_F(PositionDeleteV3Test, CommitRetryReusesWrittenDV) {
  ConfigureRetries(1);
  FailCommits(1);
  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(table_));
  update->Delete(data_file_->file_path, 5);
  ASSERT_THAT(update->Commit(), IsOk());
  EXPECT_EQ(PuffinFiles().size(), 1);
}

TEST_F(PositionDeleteV3Test, FailedCommitCleansWrittenDV) {
  ConfigureRetries(0);
  FailCommit(CommitFailed("injected failure"));
  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(table_));
  update->Delete(data_file_->file_path, 6);
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kCommitFailed));
  EXPECT_TRUE(PuffinFiles().empty());
}

TEST_F(PositionDeleteV3Test, CleanupFailureIsReportedAndRetried) {
  ConfigureRetries(0);
  auto mock_catalog = std::make_shared<::testing::NiceMock<MockCatalog>>();
  int update_calls = 0;
  ON_CALL(*mock_catalog, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault(
          [this, &update_calls](
              const TableIdentifier& identifier,
              const std::vector<std::unique_ptr<TableRequirement>>& requirements,
              const std::vector<std::unique_ptr<TableUpdate>>& updates)
              -> Result<std::shared_ptr<Table>> {
            if (++update_calls == 1) {
              return CommitFailed("injected commit failure");
            }
            return catalog_->UpdateTable(identifier, requirements, updates);
          });
  auto arrow_io = std::dynamic_pointer_cast<arrow::ArrowFileSystemFileIO>(file_io_);
  ASSERT_NE(arrow_io, nullptr);
  auto failing_io = std::make_shared<FailOncePuffinDeleteFileIO>(arrow_io->fs());
  ICEBERG_UNWRAP_OR_FAIL(auto mock_table,
                         Table::Make(table_->name(), table_->metadata(),
                                     std::string(table_->metadata_file_location()),
                                     failing_io, mock_catalog));

  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(mock_table));
  update->Delete(data_file_->file_path, 6);
  auto first_status = update->Commit();
  EXPECT_THAT(first_status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(first_status, HasErrorMessage("injected commit failure"));
  EXPECT_THAT(first_status, HasErrorMessage("injected cleanup failure"));
  ASSERT_EQ(PuffinFiles().size(), 1);
  ASSERT_EQ(failing_io->puffin_delete_attempts.size(), 1);
  const auto retained_path = failing_io->puffin_delete_attempts.front();

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_EQ(update_calls, 2);
  EXPECT_EQ(PuffinFiles().size(), 1);
  ASSERT_EQ(failing_io->puffin_delete_attempts.size(), 2);
  EXPECT_EQ(std::ranges::count(failing_io->puffin_delete_attempts, retained_path), 2);
}

TEST_F(PositionDeleteV3Test, CommitStateUnknownRelinquishesOutputOwnership) {
  ConfigureRetries(1);
  FailCommit(CommitStateUnknown("injected unknown state"));
  ICEBERG_UNWRAP_OR_FAIL(auto update, PositionDeleteUpdate::Make(table_));
  update->Delete(data_file_->file_path, 6);
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kCommitStateUnknown));
  ASSERT_EQ(PuffinFiles().size(), 1);
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kInvalidArgument));
  EXPECT_EQ(PuffinFiles().size(), 1);
}

}  // namespace

}  // namespace iceberg
