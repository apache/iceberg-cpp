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

#include "iceberg/data/compaction_executor.h"

#include <array>
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <utility>
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
#include "iceberg/compaction_planner.h"
#include "iceberg/data/data_writer.h"
#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/data/position_delete_update.h"
#include "iceberg/file_writer.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

using Row = std::array<int64_t, 5>;

class FailingDeleteFileIO : public FileIO {
 public:
  explicit FailingDeleteFileIO(std::shared_ptr<FileIO> delegate)
      : delegate_(std::move(delegate)) {}

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override {
    return delegate_->NewInputFile(std::move(file_location));
  }

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location,
                                                  size_t length) override {
    return delegate_->NewInputFile(std::move(file_location), length);
  }

  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string file_location) override {
    return delegate_->NewOutputFile(std::move(file_location));
  }

  Status DeleteFile(const std::string& file_location) override {
    if (fail_compacted_deletes_ &&
        file_location.find("compacted-") != std::string::npos) {
      return IOError("injected cleanup failure: {}", file_location);
    }
    return delegate_->DeleteFile(file_location);
  }

  void AllowCompactedDeletes() { fail_compacted_deletes_ = false; }

 private:
  std::shared_ptr<FileIO> delegate_;
  bool fail_compacted_deletes_ = true;
};

class FailBeforeCreateFileIO : public FileIO {
 public:
  explicit FailBeforeCreateFileIO(std::shared_ptr<FileIO> delegate)
      : delegate_(std::move(delegate)),
        fail_before_create_(std::make_shared<bool>(true)) {}

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override {
    return delegate_->NewInputFile(std::move(file_location));
  }

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location,
                                                  size_t length) override {
    return delegate_->NewInputFile(std::move(file_location), length);
  }

  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string file_location) override {
    ICEBERG_ASSIGN_OR_RAISE(auto output, delegate_->NewOutputFile(file_location));
    if (!IsCompacted(file_location)) {
      return output;
    }
    return std::unique_ptr<OutputFile>(new FailingOutputFile(
        std::move(output), std::move(file_location), fail_before_create_));
  }

  Status DeleteFile(const std::string& file_location) override {
    return delegate_->DeleteFile(file_location);
  }

 private:
  class FailingOutputFile : public OutputFile {
   public:
    FailingOutputFile(std::unique_ptr<OutputFile> delegate, std::string location,
                      std::shared_ptr<bool> fail_before_create)
        : delegate_(std::move(delegate)),
          location_(std::move(location)),
          fail_before_create_(std::move(fail_before_create)) {}

    std::string_view location() const override { return location_; }

    Result<std::unique_ptr<PositionOutputStream>> Create() override {
      return CreateOrFail([this] { return delegate_->Create(); });
    }

    Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
      return CreateOrFail([this] { return delegate_->CreateOrOverwrite(); });
    }

   private:
    template <typename CreateFn>
    Result<std::unique_ptr<PositionOutputStream>> CreateOrFail(CreateFn&& create) {
      if (*fail_before_create_) {
        *fail_before_create_ = false;
        return IOError("injected output creation failure before materialization");
      }
      return std::forward<CreateFn>(create)();
    }

    std::unique_ptr<OutputFile> delegate_;
    std::string location_;
    std::shared_ptr<bool> fail_before_create_;
  };

  static bool IsCompacted(std::string_view location) {
    return location.find("compacted-") != std::string_view::npos;
  }

  std::shared_ptr<FileIO> delegate_;
  std::shared_ptr<bool> fail_before_create_;
};

class FailAfterCreateFileIO : public FileIO {
 public:
  explicit FailAfterCreateFileIO(std::shared_ptr<FileIO> delegate)
      : delegate_(std::move(delegate)),
        fail_after_create_(std::make_shared<bool>(true)) {}

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override {
    return delegate_->NewInputFile(std::move(file_location));
  }

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location,
                                                  size_t length) override {
    return delegate_->NewInputFile(std::move(file_location), length);
  }

  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string file_location) override {
    ICEBERG_ASSIGN_OR_RAISE(auto output, delegate_->NewOutputFile(file_location));
    if (!IsCompacted(file_location)) {
      return output;
    }
    return std::unique_ptr<OutputFile>(new FailingOutputFile(
        std::move(output), std::move(file_location), fail_after_create_));
  }

  Status DeleteFile(const std::string& file_location) override {
    return delegate_->DeleteFile(file_location);
  }

 private:
  class FailingOutputFile : public OutputFile {
   public:
    FailingOutputFile(std::unique_ptr<OutputFile> delegate, std::string location,
                      std::shared_ptr<bool> fail_after_create)
        : delegate_(std::move(delegate)),
          location_(std::move(location)),
          fail_after_create_(std::move(fail_after_create)) {}

    std::string_view location() const override { return location_; }

    Result<std::unique_ptr<PositionOutputStream>> Create() override {
      ICEBERG_ASSIGN_OR_RAISE(auto stream, delegate_->Create());
      return Track(std::move(stream));
    }

    Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
      ICEBERG_ASSIGN_OR_RAISE(auto stream, delegate_->CreateOrOverwrite());
      return Track(std::move(stream));
    }

   private:
    Result<std::unique_ptr<PositionOutputStream>> Track(
        std::unique_ptr<PositionOutputStream> stream) {
      if (*fail_after_create_) {
        *fail_after_create_ = false;
        return IOError("injected writer failure after output creation");
      }
      return stream;
    }

    std::unique_ptr<OutputFile> delegate_;
    std::string location_;
    std::shared_ptr<bool> fail_after_create_;
  };

  static bool IsCompacted(std::string_view location) {
    return location.find("compacted-") != std::string_view::npos;
  }

  std::shared_ptr<FileIO> delegate_;
  std::shared_ptr<bool> fail_after_create_;
};

class CompactionExecutorTest : public MinimalUpdateTestBase {
 protected:
  static void SetUpTestSuite() {
    avro::RegisterAll();
    parquet::RegisterAll();
  }

  int8_t format_version() const override { return 3; }

  void SetUp() override {
    MinimalUpdateTestBase::SetUp();
    ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
    properties->Set(TableProperties::kParquetCompression.key(), "uncompressed")
        .Set(TableProperties::kDeleteParquetCompression.key(), "uncompressed")
        .Set(TableProperties::kCommitNumRetries.key(), "0");
    ASSERT_THAT(properties->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());
    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    auto arrow_io = std::dynamic_pointer_cast<arrow::ArrowFileSystemFileIO>(file_io_);
    ASSERT_NE(arrow_io, nullptr);
    ASSERT_TRUE(arrow_io->fs()->CreateDir(table_location_ + "/data/x=10").ok());
    ASSERT_TRUE(arrow_io->fs()->CreateDir(table_location_ + "/data/x=20").ok());
  }

  Result<std::shared_ptr<DataFile>> WriteDataFile(std::string_view name,
                                                  std::string_view json,
                                                  int64_t partition = 10) {
    const auto path = std::format("{}/data/{}", table_location_, name);
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        DataWriter::Make({
            .path = path,
            .schema = schema_,
            .spec = spec_,
            .partition = PartitionValues({Literal::Long(partition)}),
            .format = FileFormatType::kParquet,
            .io = file_io_,
            .properties = {{"write.parquet.compression-codec", "uncompressed"}},
        }));

    ArrowSchema c_schema{};
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema_, &c_schema));
    auto arrow_type = ::arrow::ImportType(&c_schema);
    if (!arrow_type.ok()) {
      return UnknownError(arrow_type.status().ToString());
    }
    auto array = ::arrow::json::ArrayFromJSONString(
        ::arrow::struct_(arrow_type.ValueOrDie()->fields()), std::string(json));
    if (!array.ok()) {
      return UnknownError(array.status().ToString());
    }

    ArrowArray c_array{};
    auto export_status = ::arrow::ExportArray(*array.ValueOrDie(), &c_array);
    if (!export_status.ok()) {
      return UnknownError(export_status.ToString());
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Write(&c_array));
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
    ICEBERG_CHECK(metadata.data_files.size() == 1, "Expected one data file, found {}",
                  metadata.data_files.size());
    return metadata.data_files.front();
  }

  Status Append(const std::vector<std::shared_ptr<DataFile>>& files) {
    ICEBERG_ASSIGN_OR_RAISE(auto append, table_->NewFastAppend());
    for (const auto& file : files) {
      append->AppendFile(file);
    }
    ICEBERG_RETURN_UNEXPECTED(append->Commit());
    return table_->Refresh();
  }

  Result<std::vector<std::shared_ptr<FileScanTask>>> Tasks(
      const std::shared_ptr<Table>& table) {
    ICEBERG_ASSIGN_OR_RAISE(auto builder, table->NewScan());
    ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Build());
    return scan->PlanFiles();
  }

  Result<CompactionPlan> Plan(const std::vector<std::shared_ptr<FileScanTask>>& tasks) {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    ICEBERG_ASSIGN_OR_RAISE(
        auto plan, CompactionPlanner::Plan(snapshot->snapshot_id, tasks,
                                           CompactionPlannerConfig{
                                               .target_file_size_bytes = 1024 * 1024,
                                               .min_file_size_ratio = 1,
                                               .max_file_size_ratio = 2,
                                               .min_input_files = 1,
                                           }));
    return plan;
  }

  std::shared_ptr<Schema> LineageProjection() {
    std::vector<SchemaField> fields(schema_->fields().begin(), schema_->fields().end());
    fields.push_back(MetadataColumns::kRowId);
    fields.push_back(MetadataColumns::kLastUpdatedSequenceNumber);
    return std::make_shared<Schema>(std::move(fields), schema_->schema_id());
  }

  Result<std::vector<Row>> ReadRows(const FileScanTask& task) {
    ICEBERG_ASSIGN_OR_RAISE(auto reader, FileScanTaskReader::Make({
                                             .io = file_io_,
                                             .table_schema = schema_,
                                             .schemas = table_->metadata()->schemas,
                                             .projected_schema = LineageProjection(),
                                         }));
    ICEBERG_ASSIGN_OR_RAISE(auto stream, reader->Open(task));
    auto batch_reader = ::arrow::ImportRecordBatchReader(&stream);
    if (!batch_reader.ok()) {
      return UnknownError(batch_reader.status().ToString());
    }

    std::vector<Row> rows;
    while (true) {
      auto batch_result = batch_reader.ValueOrDie()->Next();
      if (!batch_result.ok()) {
        return UnknownError(batch_result.status().ToString());
      }
      auto batch = batch_result.ValueOrDie();
      if (batch == nullptr) {
        break;
      }
      ICEBERG_CHECK(batch->num_columns() == 5, "Expected five projected columns");
      std::array<std::shared_ptr<::arrow::Int64Array>, 5> columns;
      for (size_t i = 0; i < columns.size(); ++i) {
        columns[i] = std::static_pointer_cast<::arrow::Int64Array>(batch->column(i));
      }
      for (int64_t row = 0; row < batch->num_rows(); ++row) {
        Row values;
        for (size_t column = 0; column < columns.size(); ++column) {
          ICEBERG_CHECK(!columns[column]->IsNull(row),
                        "Compaction row lineage column is null");
          values[column] = columns[column]->Value(row);
        }
        rows.push_back(values);
      }
    }
    return rows;
  }

  Result<std::vector<ManifestEntry>> LiveDeleteEntries() {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    SnapshotCache cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, cache.DeleteManifests(file_io_));
    std::vector<ManifestEntry> result;
    for (const auto& manifest : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto spec, table_->metadata()->PartitionSpecById(manifest.partition_spec_id));
      ICEBERG_ASSIGN_OR_RAISE(
          auto reader,
          ManifestReader::Make(manifest, file_io_, schema_, std::move(spec)));
      ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());
      result.insert(result.end(), std::make_move_iterator(entries.begin()),
                    std::make_move_iterator(entries.end()));
    }
    return result;
  }

  std::vector<std::string> CompactedFiles() {
    auto arrow_io = std::dynamic_pointer_cast<arrow::ArrowFileSystemFileIO>(file_io_);
    EXPECT_NE(arrow_io, nullptr);
    ::arrow::fs::FileSelector selector;
    selector.base_dir = table_location_ + "/data";
    selector.recursive = true;
    auto infos = arrow_io->fs()->GetFileInfo(selector);
    EXPECT_TRUE(infos.ok()) << infos.status().ToString();
    std::vector<std::string> result;
    if (!infos.ok()) {
      return result;
    }
    for (const auto& info : *infos) {
      if (info.path().find("compacted-") != std::string::npos) {
        result.push_back(info.path());
      }
    }
    return result;
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
};

class CompactionExecutorV2UpgradeTest : public CompactionExecutorTest {
 protected:
  int8_t format_version() const override { return 2; }
};

TEST_F(CompactionExecutorTest, AppliesDeletesAndPreservesRowLineage) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      WriteDataFile("input.parquet",
                    R"([[10, 100, 1000], [10, 200, 2000], [10, 300, 3000]])"));
  ASSERT_THAT(Append({data_file}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto deletes, PositionDeleteUpdate::Make(table_));
  deletes->Delete(data_file->file_path, 1);
  ASSERT_THAT(deletes->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  ASSERT_EQ(tasks.size(), 1);
  ICEBERG_UNWRAP_OR_FAIL(auto expected_rows, ReadRows(*tasks.front()));
  ICEBERG_UNWRAP_OR_FAIL(auto base_snapshot, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto plan, Plan(tasks));

  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(table_));
  ASSERT_THAT(executor->Execute(plan), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto rewritten_tasks, Tasks(table_));
  ASSERT_EQ(rewritten_tasks.size(), 1);
  const auto& rewritten = rewritten_tasks.front();
  EXPECT_NE(rewritten->data_file()->file_path, data_file->file_path);
  EXPECT_EQ(rewritten->data_file()->record_count, 2);
  EXPECT_EQ(rewritten->data_file()->data_sequence_number, base_snapshot->sequence_number);
  EXPECT_TRUE(rewritten->delete_files().empty());
  ICEBERG_UNWRAP_OR_FAIL(auto actual_rows, ReadRows(*rewritten));
  EXPECT_EQ(actual_rows, expected_rows);
  ICEBERG_UNWRAP_OR_FAIL(auto live_deletes, LiveDeleteEntries());
  EXPECT_TRUE(live_deletes.empty());
}

TEST_F(CompactionExecutorTest, KeepsSharedPuffinForUncompactedDataFile) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto first,
      WriteDataFile("first.parquet", R"([[10, 100, 1000], [10, 200, 2000]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second,
      WriteDataFile("second.parquet", R"([[10, 300, 3000], [10, 400, 4000]])"));
  ASSERT_THAT(Append({first, second}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto deletes, PositionDeleteUpdate::Make(table_));
  deletes->Delete(first->file_path, 0).Delete(second->file_path, 1);
  ASSERT_THAT(deletes->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  auto first_task = std::ranges::find_if(tasks, [&](const auto& task) {
    return task->data_file()->file_path == first->file_path;
  });
  auto second_task = std::ranges::find_if(tasks, [&](const auto& task) {
    return task->data_file()->file_path == second->file_path;
  });
  ASSERT_NE(first_task, tasks.end());
  ASSERT_NE(second_task, tasks.end());
  ASSERT_EQ((*first_task)->delete_files().size(), 1);
  ASSERT_EQ((*second_task)->delete_files().size(), 1);
  const auto shared_puffin = (*first_task)->delete_files().front()->file_path;
  ASSERT_EQ(shared_puffin, (*second_task)->delete_files().front()->file_path);

  ICEBERG_UNWRAP_OR_FAIL(auto plan,
                         Plan(std::vector<std::shared_ptr<FileScanTask>>{*first_task}));
  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(table_));
  ASSERT_THAT(executor->Execute(plan), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto rewritten_tasks, Tasks(table_));
  auto remaining = std::ranges::find_if(rewritten_tasks, [&](const auto& task) {
    return task->data_file()->file_path == second->file_path;
  });
  ASSERT_NE(remaining, rewritten_tasks.end());
  ASSERT_EQ((*remaining)->delete_files().size(), 1);
  EXPECT_EQ((*remaining)->delete_files().front()->file_path, shared_puffin);
  auto arrow_io = std::dynamic_pointer_cast<arrow::ArrowFileSystemFileIO>(file_io_);
  ASSERT_NE(arrow_io, nullptr);
  auto info = arrow_io->fs()->GetFileInfo(shared_puffin);
  ASSERT_TRUE(info.ok()) << info.status().ToString();
  EXPECT_EQ(info->type(), ::arrow::fs::FileType::File);

  ICEBERG_UNWRAP_OR_FAIL(auto live_deletes, LiveDeleteEntries());
  ASSERT_EQ(live_deletes.size(), 1);
  EXPECT_EQ(live_deletes.front().data_file->referenced_data_file, second->file_path);
}

TEST_F(CompactionExecutorTest, FailedCommitCleansRewrittenDataFile) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      WriteDataFile("input.parquet", R"([[10, 100, 1000], [10, 200, 2000]])"));
  ASSERT_THAT(Append({data_file}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto plan, Plan(tasks));

  auto mock_catalog = std::make_shared<::testing::NiceMock<MockCatalog>>();
  ON_CALL(*mock_catalog, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault([](const TableIdentifier&,
                        const std::vector<std::unique_ptr<TableRequirement>>&,
                        const std::vector<std::unique_ptr<TableUpdate>>&)
                         -> Result<std::shared_ptr<Table>> {
        return CommitFailed("injected failure");
      });
  ICEBERG_UNWRAP_OR_FAIL(auto mock_table,
                         Table::Make(table_->name(), table_->metadata(),
                                     std::string(table_->metadata_file_location()),
                                     table_->io(), mock_catalog));
  ON_CALL(*mock_catalog, LoadTable(::testing::_))
      .WillByDefault(
          [&mock_table](const TableIdentifier&) -> Result<std::shared_ptr<Table>> {
            return mock_table;
          });

  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(mock_table));
  EXPECT_THAT(executor->Execute(plan), IsError(ErrorKind::kCommitFailed));
  EXPECT_TRUE(CompactedFiles().empty());
}

TEST_F(CompactionExecutorTest, CommitStateUnknownRelinquishesOutputOwnership) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      WriteDataFile("input.parquet", R"([[10, 100, 1000], [10, 200, 2000]])"));
  ASSERT_THAT(Append({data_file}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto plan, Plan(tasks));

  auto mock_catalog = std::make_shared<::testing::NiceMock<MockCatalog>>();
  int update_calls = 0;
  ON_CALL(*mock_catalog, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault(
          [&update_calls](const TableIdentifier&,
                          const std::vector<std::unique_ptr<TableRequirement>>&,
                          const std::vector<std::unique_ptr<TableUpdate>>&)
              -> Result<std::shared_ptr<Table>> {
            ++update_calls;
            return CommitStateUnknown("injected unknown state");
          });
  ICEBERG_UNWRAP_OR_FAIL(auto mock_table,
                         Table::Make(table_->name(), table_->metadata(),
                                     std::string(table_->metadata_file_location()),
                                     table_->io(), mock_catalog));
  ON_CALL(*mock_catalog, LoadTable(::testing::_))
      .WillByDefault(
          [&mock_table](const TableIdentifier&) -> Result<std::shared_ptr<Table>> {
            return mock_table;
          });

  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(mock_table));
  EXPECT_THAT(executor->Execute(plan), IsError(ErrorKind::kCommitStateUnknown));
  ASSERT_EQ(CompactedFiles().size(), 1);

  EXPECT_THAT(executor->Execute(plan), IsError(ErrorKind::kInvalidArgument));
  EXPECT_EQ(update_calls, 1);
  EXPECT_EQ(CompactedFiles().size(), 1);
}

TEST_F(CompactionExecutorTest, RejectsDuplicateSourceFilesBeforeWriting) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      WriteDataFile("input.parquet", R"([[10, 100, 1000], [10, 200, 2000]])"));
  ASSERT_THAT(Append({data_file}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto plan, Plan(tasks));
  ASSERT_EQ(plan.groups.size(), 1);
  ASSERT_EQ(plan.groups.front().files.size(), 1);

  auto within_group = plan;
  within_group.groups.front().files.push_back(within_group.groups.front().files.front());
  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(table_));
  EXPECT_THAT(executor->Execute(within_group), IsError(ErrorKind::kInvalidArgument));
  EXPECT_TRUE(CompactedFiles().empty());

  auto across_groups = plan;
  across_groups.groups.push_back(across_groups.groups.front());
  EXPECT_THAT(executor->Execute(across_groups), IsError(ErrorKind::kInvalidArgument));
  EXPECT_TRUE(CompactedFiles().empty());
}

TEST_F(CompactionExecutorTest, RejectsPlanAfterNewDeleteSnapshot) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      WriteDataFile("input.parquet",
                    R"([[10, 100, 1000], [10, 200, 2000], [10, 300, 3000]])"));
  ASSERT_THAT(Append({data_file}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto plan, Plan(tasks));

  ICEBERG_UNWRAP_OR_FAIL(auto deletes, PositionDeleteUpdate::Make(table_));
  deletes->Delete(data_file->file_path, 1);
  ASSERT_THAT(deletes->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(table_));
  EXPECT_THAT(executor->Execute(plan), IsError(ErrorKind::kValidationFailed));
  EXPECT_TRUE(CompactedFiles().empty());

  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto current_tasks, Tasks(table_));
  ASSERT_EQ(current_tasks.size(), 1);
  ASSERT_EQ(current_tasks.front()->delete_files().size(), 1);
  ICEBERG_UNWRAP_OR_FAIL(auto rows, ReadRows(*current_tasks.front()));
  EXPECT_EQ(rows.size(), 2);
}

TEST_F(CompactionExecutorTest, MultiFileMultiGroupCommitIsAtomicOnFailure) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto first,
      WriteDataFile("first.parquet", R"([[10, 100, 1000], [10, 200, 2000]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second,
      WriteDataFile("second.parquet", R"([[10, 300, 3000], [10, 400, 4000]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto third,
      WriteDataFile("third.parquet", R"([[20, 500, 5000], [20, 600, 6000]])", 20));
  ASSERT_THAT(Append({first, second, third}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto plan, Plan(tasks));
  ASSERT_EQ(plan.groups.size(), 2);
  ASSERT_EQ(plan.groups.front().files.size(), 2);

  auto mock_catalog = std::make_shared<::testing::NiceMock<MockCatalog>>();
  int update_calls = 0;
  ON_CALL(*mock_catalog, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault(
          [&update_calls](const TableIdentifier&,
                          const std::vector<std::unique_ptr<TableRequirement>>&,
                          const std::vector<std::unique_ptr<TableUpdate>>&)
              -> Result<std::shared_ptr<Table>> {
            ++update_calls;
            return CommitFailed("injected failure");
          });
  ICEBERG_UNWRAP_OR_FAIL(auto mock_table,
                         Table::Make(table_->name(), table_->metadata(),
                                     std::string(table_->metadata_file_location()),
                                     table_->io(), mock_catalog));
  ON_CALL(*mock_catalog, LoadTable(::testing::_))
      .WillByDefault(
          [&mock_table](const TableIdentifier&) -> Result<std::shared_ptr<Table>> {
            return mock_table;
          });

  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(mock_table));
  EXPECT_THAT(executor->Execute(plan), IsError(ErrorKind::kCommitFailed));
  EXPECT_EQ(update_calls, 1);
  EXPECT_TRUE(CompactedFiles().empty());

  ICEBERG_UNWRAP_OR_FAIL(auto current_tasks, Tasks(table_));
  EXPECT_EQ(current_tasks.size(), 3);
}

TEST_F(CompactionExecutorTest, CleanupFailureRetainsOutputForExplicitRetry) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      WriteDataFile("input.parquet", R"([[10, 100, 1000], [10, 200, 2000]])"));
  ASSERT_THAT(Append({data_file}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto plan, Plan(tasks));

  auto failing_io = std::make_shared<FailingDeleteFileIO>(table_->io());
  auto mock_catalog = std::make_shared<::testing::NiceMock<MockCatalog>>();
  ON_CALL(*mock_catalog, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault([](const TableIdentifier&,
                        const std::vector<std::unique_ptr<TableRequirement>>&,
                        const std::vector<std::unique_ptr<TableUpdate>>&)
                         -> Result<std::shared_ptr<Table>> {
        return CommitFailed("injected commit failure");
      });
  ICEBERG_UNWRAP_OR_FAIL(auto mock_table,
                         Table::Make(table_->name(), table_->metadata(),
                                     std::string(table_->metadata_file_location()),
                                     failing_io, mock_catalog));
  ON_CALL(*mock_catalog, LoadTable(::testing::_))
      .WillByDefault(
          [&mock_table](const TableIdentifier&) -> Result<std::shared_ptr<Table>> {
            return mock_table;
          });

  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(mock_table));
  auto status = executor->Execute(plan);
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("injected commit failure"));
  EXPECT_THAT(status, HasErrorMessage("injected cleanup failure"));
  ASSERT_EQ(CompactedFiles().size(), 1);

  failing_io->AllowCompactedDeletes();
  EXPECT_THAT(executor->Cleanup(), IsOk());
  EXPECT_TRUE(CompactedFiles().empty());
}

TEST_F(CompactionExecutorTest, WriterFailureBeforeFileCreationLeavesExecutorReusable) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      WriteDataFile("input.parquet", R"([[10, 100, 1000], [10, 200, 2000]])"));
  ASSERT_THAT(Append({data_file}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto plan, Plan(tasks));

  ICEBERG_UNWRAP_OR_FAIL(auto invalid_properties, table_->NewUpdateProperties());
  invalid_properties->Set(WriterProperties::kParquetMaxRowGroupRows.key(), "0");
  ASSERT_THAT(invalid_properties->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(table_));
  auto status = executor->Execute(plan);
  EXPECT_THAT(status, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(status, HasErrorMessage("Parquet max row group rows"));
  EXPECT_TRUE(CompactedFiles().empty());
  EXPECT_THAT(executor->Cleanup(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto valid_properties, table_->NewUpdateProperties());
  valid_properties->Set(WriterProperties::kParquetMaxRowGroupRows.key(), "100");
  ASSERT_THAT(valid_properties->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ASSERT_THAT(executor->Execute(plan), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto rewritten_tasks, Tasks(table_));
  ASSERT_EQ(rewritten_tasks.size(), 1);
  EXPECT_NE(rewritten_tasks.front()->data_file()->file_path, data_file->file_path);
}

TEST_F(CompactionExecutorTest, OutputCreateFailureCleansAndRemainsReusable) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      WriteDataFile("input.parquet", R"([[10, 100, 1000], [10, 200, 2000]])"));
  ASSERT_THAT(Append({data_file}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto plan, Plan(tasks));

  auto writer_io = std::make_shared<FailBeforeCreateFileIO>(table_->io());
  ICEBERG_UNWRAP_OR_FAIL(
      auto writer_table,
      Table::Make(table_->name(), table_->metadata(),
                  std::string(table_->metadata_file_location()), writer_io, catalog_));
  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(writer_table));

  auto status = executor->Execute(plan);
  EXPECT_THAT(status, IsError(ErrorKind::kIOError));
  EXPECT_THAT(status, HasErrorMessage("output creation failure before materialization"));
  EXPECT_TRUE(CompactedFiles().empty());
  EXPECT_THAT(executor->Cleanup(), IsOk());

  ASSERT_THAT(executor->Execute(plan), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto rewritten_tasks, Tasks(table_));
  ASSERT_EQ(rewritten_tasks.size(), 1);
  EXPECT_NE(rewritten_tasks.front()->data_file()->file_path, data_file->file_path);
}

TEST_F(CompactionExecutorTest, WriterFailureAfterFileCreationCleansAndRemainsReusable) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      WriteDataFile("input.parquet", R"([[10, 100, 1000], [10, 200, 2000]])"));
  ASSERT_THAT(Append({data_file}), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto plan, Plan(tasks));

  auto writer_io = std::make_shared<FailAfterCreateFileIO>(table_->io());
  ICEBERG_UNWRAP_OR_FAIL(
      auto writer_table,
      Table::Make(table_->name(), table_->metadata(),
                  std::string(table_->metadata_file_location()), writer_io, catalog_));
  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(writer_table));

  auto status = executor->Execute(plan);
  EXPECT_THAT(status, IsError(ErrorKind::kIOError));
  EXPECT_THAT(status, HasErrorMessage("injected writer failure after output creation"));
  EXPECT_TRUE(CompactedFiles().empty());
  EXPECT_THAT(executor->Cleanup(), IsOk());

  ASSERT_THAT(executor->Execute(plan), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto rewritten_tasks, Tasks(table_));
  ASSERT_EQ(rewritten_tasks.size(), 1);
  EXPECT_NE(rewritten_tasks.front()->data_file()->file_path, data_file->file_path);
}

TEST_F(CompactionExecutorV2UpgradeTest,
       RemovesParquetDeleteAndAssignsUpgradedRowLineage) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto data_file,
      WriteDataFile("input.parquet",
                    R"([[10, 100, 1000], [10, 200, 2000], [10, 300, 3000]])"));
  ASSERT_THAT(Append({data_file}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto deletes, PositionDeleteUpdate::Make(table_));
  deletes->Delete(data_file->file_path, 1);
  ASSERT_THAT(deletes->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto v2_tasks, Tasks(table_));
  ASSERT_EQ(v2_tasks.size(), 1);
  ASSERT_EQ(v2_tasks.front()->delete_files().size(), 1);
  EXPECT_EQ(v2_tasks.front()->delete_files().front()->file_format,
            FileFormatType::kParquet);

  ICEBERG_UNWRAP_OR_FAIL(auto properties, table_->NewUpdateProperties());
  properties->Set(TableProperties::kFormatVersion.key(), "3");
  ASSERT_THAT(properties->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto tasks, Tasks(table_));
  ASSERT_EQ(tasks.size(), 1);
  ASSERT_FALSE(tasks.front()->data_file()->first_row_id.has_value());
  ASSERT_TRUE(tasks.front()->data_file()->data_sequence_number.has_value());
  const auto original_sequence = *tasks.front()->data_file()->data_sequence_number;
  ICEBERG_UNWRAP_OR_FAIL(auto plan, Plan(tasks));

  ICEBERG_UNWRAP_OR_FAIL(auto executor, CompactionExecutor::Make(table_));
  ASSERT_THAT(executor->Execute(plan), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto rewritten_tasks, Tasks(table_));
  ASSERT_EQ(rewritten_tasks.size(), 1);
  ASSERT_TRUE(rewritten_tasks.front()->data_file()->first_row_id.has_value());
  const auto first_row_id = *rewritten_tasks.front()->data_file()->first_row_id;
  ICEBERG_UNWRAP_OR_FAIL(auto rows, ReadRows(*rewritten_tasks.front()));
  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0][3], first_row_id);
  EXPECT_EQ(rows[1][3], first_row_id + 1);
  EXPECT_EQ(rows[0][4], original_sequence);
  EXPECT_EQ(rows[1][4], original_sequence);
  EXPECT_TRUE(rewritten_tasks.front()->delete_files().empty());
  ICEBERG_UNWRAP_OR_FAIL(auto live_deletes, LiveDeleteEntries());
  EXPECT_TRUE(live_deletes.empty());
}

}  // namespace
}  // namespace iceberg
