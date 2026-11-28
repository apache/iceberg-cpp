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

// End-to-end tests that plan real tables with PlanTasksStream() and read them with
// ParallelScanReader, compared with PlanFilesStream() and the serial task reader.

#include <algorithm>
#include <cstdint>
#include <format>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/data/data_writer.h"
#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/data/parallel_scan_reader.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/data_table_scan.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/scan_test_base.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

// A row read from a scan. Lineage fields are -1 when not projected.
struct Row {
  int32_t id = 0;
  std::string data;
  int64_t row_id = -1;
  int64_t pos = -1;

  auto operator<=>(const Row&) const = default;
};

std::string DataValue(int32_t id) { return std::format("value-{}", id % 10); }

// Converts an Arrow C stream into sorted rows, or returns the stream's error.
Result<std::vector<Row>> CollectRows(ArrowArrayStream* stream) {
  auto reader_result = ::arrow::ImportRecordBatchReader(stream);
  if (!reader_result.ok()) {
    return IOError("Failed to import stream: {}", reader_result.status().ToString());
  }
  std::vector<Row> rows;
  for (auto next : **reader_result) {
    if (!next.ok()) {
      return IOError("{}", next.status().message());
    }
    const auto& batch = **next;
    // Optional columns may be absent from the projection, but a present column must
    // have the expected type.
    auto column = [&batch]<typename ArrayType>(
                      const char* name,
                      bool required) -> Result<std::shared_ptr<ArrayType>> {
      auto array = batch.GetColumnByName(name);
      if (array == nullptr) {
        if (required) {
          return Invalid("Result has no column {}", name);
        }
        return nullptr;
      }
      auto typed = std::dynamic_pointer_cast<ArrayType>(array);
      if (typed == nullptr) {
        return Invalid("Column {} has unexpected type {}", name,
                       array->type()->ToString());
      }
      return typed;
    };
    ICEBERG_ASSIGN_OR_RAISE(auto ids,
                            column.template operator()<::arrow::Int32Array>("id", true));
    ICEBERG_ASSIGN_OR_RAISE(
        auto data, column.template operator()<::arrow::StringArray>("data", false));
    ICEBERG_ASSIGN_OR_RAISE(
        auto row_ids, column.template operator()<::arrow::Int64Array>("_row_id", false));
    ICEBERG_ASSIGN_OR_RAISE(
        auto positions, column.template operator()<::arrow::Int64Array>("_pos", false));
    for (int64_t index = 0; index < batch.num_rows(); ++index) {
      Row row;
      row.id = ids->Value(index);
      if (data != nullptr) {
        row.data = data->GetString(index);
      }
      if (row_ids != nullptr) {
        row.row_id = row_ids->Value(index);
      }
      if (positions != nullptr) {
        row.pos = positions->Value(index);
      }
      rows.push_back(std::move(row));
    }
  }
  std::ranges::sort(rows);
  return rows;
}

}  // namespace

class ParallelTableScanTest : public ScanTestBase {
 protected:
  test::ThreadPoolExecutor executor_{4};

  static constexpr int64_t kSnapshotId = 1000L;
  static constexpr int64_t kSequenceNumber = 1L;

  void SetUp() override {
    ScanTestBase::SetUp();
    parquet::RegisterAll();
  }

  // A written data file and the ids it holds, in file order.
  struct WrittenFile {
    std::shared_ptr<DataFile> data_file;
    int32_t first_id = 0;
    int32_t rows = 0;
  };

  Result<WrittenFile> WriteDataFile(const std::string& path, int32_t first_id,
                                    int32_t rows, int64_t max_row_group_rows = 1 << 20,
                                    FileFormatType format = FileFormatType::kParquet) {
    std::string json = "[";
    for (int32_t row = 0; row < rows; ++row) {
      const int32_t id = first_id + row;
      json += std::format("{}[{},\"{}\"]", row == 0 ? "" : ",", id, DataValue(id));
    }
    json += "]";

    ArrowSchema c_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema_, &c_schema));
    auto arrow_type = ::arrow::ImportType(&c_schema);
    if (!arrow_type.ok()) {
      return Invalid("{}", arrow_type.status().ToString());
    }
    auto array = ::arrow::json::ArrayFromJSONString(
        ::arrow::struct_((*arrow_type)->fields()), json);
    if (!array.ok()) {
      return Invalid("{}", array.status().ToString());
    }
    ArrowArray c_array{};
    if (!::arrow::ExportArray(**array, &c_array).ok()) {
      return Invalid("Failed to export a batch for {}", path);
    }

    std::unordered_map<std::string, std::string> properties;
    if (format == FileFormatType::kParquet) {
      properties["write.parquet.max-row-group-rows"] = std::to_string(max_row_group_rows);
    }
    ICEBERG_ASSIGN_OR_RAISE(auto writer, DataWriter::Make({.path = path,
                                                           .schema = schema_,
                                                           .spec = unpartitioned_spec_,
                                                           .partition = PartitionValues{},
                                                           .format = format,
                                                           .io = file_io_,
                                                           .properties = properties}));
    auto write_status = writer->Write(&c_array);
    if (c_array.release != nullptr) {
      c_array.release(&c_array);
    }
    ICEBERG_RETURN_UNEXPECTED(write_status);
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto result, writer->Metadata());
    if (result.data_files.size() != 1) {
      return Invalid("Expected one data file for {}", path);
    }
    return WrittenFile{
        .data_file = result.data_files[0], .first_id = first_id, .rows = rows};
  }

  Result<std::shared_ptr<DataFile>> WritePositionDeletes(
      const std::string& path, const std::string& data_path,
      const std::vector<int64_t>& positions) {
    ICEBERG_ASSIGN_OR_RAISE(auto writer, PositionDeleteWriter::Make({
                                             .path = path,
                                             .schema = schema_,
                                             .spec = unpartitioned_spec_,
                                             .partition = PartitionValues{},
                                             .format = FileFormatType::kParquet,
                                             .io = file_io_,
                                         }));
    for (int64_t position : positions) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteDelete(data_path, position));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto result, writer->Metadata());
    return result.data_files[0];
  }

  // Reads the canonical row-group starts that the Parquet reader assigns to ranges.
  Result<std::vector<int64_t>> RowGroupStarts(const DataFile& data_file) {
    ICEBERG_ASSIGN_OR_RAISE(auto input,
                            arrow::OpenArrowInputStream(file_io_, data_file.file_path));
    auto metadata = ::parquet::ReadMetaData(input);
    std::vector<int64_t> starts;
    for (int group = 0; group < metadata->num_row_groups(); ++group) {
      auto column = metadata->RowGroup(group)->ColumnChunk(0);
      int64_t start = column->data_page_offset();
      if (column->has_dictionary_page() && column->dictionary_page_offset() > 0 &&
          column->dictionary_page_offset() < start) {
        start = column->dictionary_page_offset();
      }
      starts.push_back(start);
    }
    return starts;
  }

  std::shared_ptr<TableMetadata> MakeMetadata(
      const std::vector<WrittenFile>& files,
      std::vector<std::shared_ptr<DataFile>> delete_files = {}) {
    std::vector<ManifestEntry> data_entries;
    for (const auto& file : files) {
      data_entries.push_back(MakeEntry(ManifestStatus::kAdded, kSnapshotId,
                                       kSequenceNumber, file.data_file));
    }
    std::vector<ManifestFile> manifests;
    manifests.push_back(WriteDataManifest(GetParam(), kSnapshotId,
                                          std::move(data_entries), unpartitioned_spec_));
    if (!delete_files.empty()) {
      std::vector<ManifestEntry> delete_entries;
      for (auto& file : delete_files) {
        delete_entries.push_back(MakeEntry(ManifestStatus::kAdded, kSnapshotId,
                                           kSequenceNumber, std::move(file)));
      }
      manifests.push_back(WriteDeleteManifest(
          GetParam(), kSnapshotId, std::move(delete_entries), unpartitioned_spec_));
    }
    auto manifest_list = ScanTestBase::WriteManifestList(
        GetParam(), kSnapshotId, /*parent_snapshot_id=*/0L, kSequenceNumber, manifests);
    auto snapshot = std::make_shared<Snapshot>(
        Snapshot{.snapshot_id = kSnapshotId,
                 .parent_snapshot_id = std::nullopt,
                 .sequence_number = kSequenceNumber,
                 .timestamp_ms = TimePointMsFromUnixMs(1609459200000L),
                 .manifest_list = manifest_list,
                 .schema_id = schema_->schema_id()});
    auto metadata =
        ScanTestBase::MakeTableMetadata({snapshot}, kSnapshotId, {}, unpartitioned_spec_);
    if (GetParam() >= 3) {
      int64_t total_rows = 0;
      for (const auto& file : files) {
        total_rows += file.rows;
      }
      metadata->next_row_id = total_rows;
    }
    return metadata;
  }

  struct ScanSettings {
    int64_t target_split_size = 0;
    std::shared_ptr<Expression> filter;
    bool case_sensitive = true;
    std::shared_ptr<Schema> projection;
  };

  Result<std::unique_ptr<DataTableScan>> BuildScan(
      const std::shared_ptr<TableMetadata>& metadata, const ScanSettings& settings) {
    ICEBERG_ASSIGN_OR_RAISE(auto builder, MakeScanBuilder<DataTableScan>(metadata));
    if (settings.target_split_size > 0) {
      builder->Option("read.split.target-size",
                      std::to_string(settings.target_split_size));
    }
    builder->CaseSensitive(settings.case_sensitive);
    if (settings.filter != nullptr) {
      builder->Filter(settings.filter);
    }
    return builder->Build();
  }

  FileScanTaskReader::Options ReaderOptions(const ScanSettings& settings) const {
    return {.io = file_io_,
            .table_schema = schema_,
            .projected_schema = settings.projection ? settings.projection : schema_,
            .case_sensitive = settings.case_sensitive,
            .apply_residuals = true};
  }

  // PlanFilesStream() and the serial task reader, one task at a time.
  Result<std::vector<Row>> ReadSerially(const std::shared_ptr<TableMetadata>& metadata,
                                        const ScanSettings& settings) {
    ICEBERG_ASSIGN_OR_RAISE(auto scan, BuildScan(metadata, settings));
    ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan->PlanFilesStream());
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            FileScanTaskReader::Make(ReaderOptions(settings)));
    std::vector<Row> rows;
    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto task, tasks->Next());
      if (!task.has_value()) {
        break;
      }
      ICEBERG_ASSIGN_OR_RAISE(auto stream, reader->Open(**task));
      ICEBERG_ASSIGN_OR_RAISE(auto task_rows, CollectRows(&stream));
      rows.insert(rows.end(), task_rows.begin(), task_rows.end());
    }
    std::ranges::sort(rows);
    return rows;
  }

  // PlanTasksStream() and the parallel reader.
  Result<std::vector<Row>> ReadInParallel(const std::shared_ptr<TableMetadata>& metadata,
                                          const ScanSettings& settings, int32_t workers,
                                          int64_t max_queued_batches = 4,
                                          int64_t max_queued_bytes = 1 << 20) {
    ICEBERG_ASSIGN_OR_RAISE(auto scan, BuildScan(metadata, settings));
    ICEBERG_ASSIGN_OR_RAISE(auto groups, scan->PlanTasksStream());
    ICEBERG_ASSIGN_OR_RAISE(
        auto stream, ParallelScanReader::Open(std::move(groups),
                                              {.reader_options = ReaderOptions(settings),
                                               .executor = &executor_,
                                               .worker_count = workers,
                                               .max_queued_batches = max_queued_batches,
                                               .max_queued_bytes = max_queued_bytes}));
    return CollectRows(&stream);
  }

  Result<std::vector<std::shared_ptr<FileScanTaskGroup>>> PlanGroups(
      const std::shared_ptr<TableMetadata>& metadata, const ScanSettings& settings) {
    ICEBERG_ASSIGN_OR_RAISE(auto scan, BuildScan(metadata, settings));
    return scan->PlanTasks();
  }

  static size_t CountSplitTasks(
      const std::vector<std::shared_ptr<FileScanTaskGroup>>& groups) {
    size_t count = 0;
    for (const auto& group : groups) {
      for (const auto& task : group->tasks()) {
        count += task->is_split() ? 1 : 0;
      }
    }
    return count;
  }

  static std::vector<Row> ExpectedRows(const std::vector<WrittenFile>& files,
                                       const std::unordered_set<int32_t>& deleted = {}) {
    std::vector<Row> rows;
    for (const auto& file : files) {
      for (int32_t row = 0; row < file.rows; ++row) {
        const int32_t id = file.first_id + row;
        if (!deleted.contains(id)) {
          rows.push_back(Row{.id = id, .data = DataValue(id)});
        }
      }
    }
    std::ranges::sort(rows);
    return rows;
  }

  // Checks parallel results against independent expectations and the serial path,
  // for one and several workers.
  void ExpectParallelMatches(const std::shared_ptr<TableMetadata>& metadata,
                             const ScanSettings& settings,
                             const std::vector<Row>& expected) {
    ICEBERG_UNWRAP_OR_FAIL(auto serial, ReadSerially(metadata, settings));
    EXPECT_EQ(serial, expected);
    for (int32_t workers : {1, 4}) {
      SCOPED_TRACE(std::format("workers={}", workers));
      ICEBERG_UNWRAP_OR_FAIL(auto parallel, ReadInParallel(metadata, settings, workers));
      EXPECT_EQ(parallel, expected);
    }
  }
};

TEST_P(ParallelTableScanTest, LargeMultiRowGroupFilesWithManifestOffsets) {
  std::vector<WrittenFile> files;
  for (int file = 0; file < 3; ++file) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto written, WriteDataFile(std::format("large-{}.parquet", file), file * 2000,
                                    2000, /*max_row_group_rows=*/200));
    ASSERT_EQ(written.data_file->split_offsets.size(), 10U);
    files.push_back(std::move(written));
  }
  auto metadata = MakeMetadata(files);
  ScanSettings settings{.target_split_size = files[0].data_file->file_size_in_bytes / 4};
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanGroups(metadata, settings));
  EXPECT_GE(CountSplitTasks(groups), 9U);
  ExpectParallelMatches(metadata, settings, ExpectedRows(files));
}

TEST_P(ParallelTableScanTest, MissingManifestOffsetsUseFixedRanges) {
  ICEBERG_UNWRAP_OR_FAIL(auto written, WriteDataFile("no-offsets.parquet", 0, 3000,
                                                     /*max_row_group_rows=*/250));
  written.data_file->split_offsets.clear();
  std::vector<WrittenFile> files{written};
  auto metadata = MakeMetadata(files);
  ScanSettings settings{.target_split_size = written.data_file->file_size_in_bytes / 5};
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanGroups(metadata, settings));
  EXPECT_GE(CountSplitTasks(groups), 5U);
  ExpectParallelMatches(metadata, settings, ExpectedRows(files));
}

TEST_P(ParallelTableScanTest, ManifestOffsetsThatMissCanonicalStarts) {
  ICEBERG_UNWRAP_OR_FAIL(auto written, WriteDataFile("shifted.parquet", 0, 2400,
                                                     /*max_row_group_rows=*/200));
  ICEBERG_UNWRAP_OR_FAIL(auto starts, RowGroupStarts(*written.data_file));
  ASSERT_EQ(starts.size(), 12U);
  // Valid (increasing, in-bounds) offsets that fall inside row groups, so that
  // planned ranges disagree with the starts the reader assigns rows by.
  std::vector<int64_t> shifted;
  for (size_t group = 0; group < starts.size(); ++group) {
    const int64_t end = group + 1 < starts.size() ? starts[group + 1]
                                                  : written.data_file->file_size_in_bytes;
    shifted.push_back(starts[group] + (end - starts[group]) / 2);
  }
  written.data_file->split_offsets = shifted;
  std::vector<WrittenFile> files{written};
  auto metadata = MakeMetadata(files);
  ScanSettings settings{.target_split_size = written.data_file->file_size_in_bytes / 6};
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanGroups(metadata, settings));
  EXPECT_GE(CountSplitTasks(groups), 3U);
  ExpectParallelMatches(metadata, settings, ExpectedRows(files));
}

TEST_P(ParallelTableScanTest, OneLargeRowGroupWithoutOffsets) {
  ICEBERG_UNWRAP_OR_FAIL(auto written, WriteDataFile("single-group.parquet", 0, 2000));
  ICEBERG_UNWRAP_OR_FAIL(auto starts, RowGroupStarts(*written.data_file));
  ASSERT_EQ(starts.size(), 1U);
  written.data_file->split_offsets.clear();
  std::vector<WrittenFile> files{written};
  auto metadata = MakeMetadata(files);
  // Several ranges, only one of which holds the row group's start.
  ScanSettings settings{.target_split_size = written.data_file->file_size_in_bytes / 4};
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanGroups(metadata, settings));
  EXPECT_GE(CountSplitTasks(groups), 4U);
  ExpectParallelMatches(metadata, settings, ExpectedRows(files));
}

TEST_P(ParallelTableScanTest, ManySmallFiles) {
  std::vector<WrittenFile> files;
  for (int file = 0; file < 60; ++file) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto written,
        WriteDataFile(std::format("small-{}.parquet", file), file * 10, 10));
    files.push_back(std::move(written));
  }
  auto metadata = MakeMetadata(files);
  ScanSettings settings;
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanGroups(metadata, settings));
  // Small files are packed together.
  EXPECT_LT(groups.size(), files.size());
  ExpectParallelMatches(metadata, settings, ExpectedRows(files));
}

TEST_P(ParallelTableScanTest, MixedParquetAndAvroFiles) {
  std::vector<WrittenFile> files;
  for (int file = 0; file < 6; ++file) {
    const auto format = file % 2 == 0 ? FileFormatType::kParquet : FileFormatType::kAvro;
    const auto suffix = format == FileFormatType::kParquet ? "parquet" : "avro";
    ICEBERG_UNWRAP_OR_FAIL(
        auto written, WriteDataFile(std::format("mixed-{}.{}", file, suffix), file * 500,
                                    500, /*max_row_group_rows=*/100, format));
    files.push_back(std::move(written));
  }
  auto metadata = MakeMetadata(files);
  ScanSettings settings{.target_split_size = files[0].data_file->file_size_in_bytes / 3};
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanGroups(metadata, settings));
  for (const auto& group : groups) {
    for (const auto& task : group->tasks()) {
      if (task->data_file()->file_format == FileFormatType::kAvro) {
        EXPECT_FALSE(task->is_split());
      }
    }
  }
  ExpectParallelMatches(metadata, settings, ExpectedRows(files));
}

TEST_P(ParallelTableScanTest, CaseInsensitiveResidualFilter) {
  std::vector<WrittenFile> files;
  for (int file = 0; file < 2; ++file) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto written, WriteDataFile(std::format("filtered-{}.parquet", file), file * 1000,
                                    1000, /*max_row_group_rows=*/100));
    files.push_back(std::move(written));
  }
  auto metadata = MakeMetadata(files);
  ScanSettings settings{
      .target_split_size = files[0].data_file->file_size_in_bytes / 4,
      .filter = Expressions::And(Expressions::GreaterThanOrEqual("ID", Literal::Int(450)),
                                 Expressions::Equal("Data", Literal::String("value-3"))),
      .case_sensitive = false};
  std::vector<Row> expected;
  for (const auto& row : ExpectedRows(files)) {
    if (row.id >= 450 && row.data == "value-3") {
      expected.push_back(row);
    }
  }
  ASSERT_FALSE(expected.empty());
  ExpectParallelMatches(metadata, settings, expected);
}

TEST_P(ParallelTableScanTest, PlannedProjectionOmitsFoldedResidualFields) {
  ICEBERG_UNWRAP_OR_FAIL(auto file, WriteDataFile("folded.parquet", 0, 25));
  auto metadata = MakeMetadata({file});
  for (bool case_sensitive : {true, false}) {
    for (bool combined : {false, true}) {
      SCOPED_TRACE(case_sensitive);
      SCOPED_TRACE(combined);
      ICEBERG_UNWRAP_OR_FAIL(auto builder, MakeScanBuilder<DataTableScan>(metadata));
      auto filter = Expressions::NotNull(case_sensitive ? "id" : "ID");
      std::shared_ptr<Expression> residual = filter;
      if (combined) {
        residual = Expressions::And(
            filter, Expressions::Equal("data", Literal::String("value-3")));
      }
      builder->Select({"data"}).CaseSensitive(case_sensitive).Filter(residual);
      ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
      ICEBERG_UNWRAP_OR_FAIL(auto projection, scan->schema());
      ASSERT_EQ(projection->fields().size(), 1);
      ASSERT_EQ(projection->fields()[0].name(), "data");
      ICEBERG_UNWRAP_OR_FAIL(auto groups, scan->PlanTasksStream());
      FileScanTaskReader::Options reader_options{.io = file_io_,
                                                 .table_schema = schema_,
                                                 .projected_schema = projection,
                                                 .case_sensitive = case_sensitive};
      ICEBERG_UNWRAP_OR_FAIL(
          auto stream,
          ParallelScanReader::Open(std::move(groups), {.reader_options = reader_options,
                                                       .executor = &executor_,
                                                       .worker_count = 4,
                                                       .max_queued_batches = 1,
                                                       .max_queued_bytes = 1 << 20}));
      int64_t rows = 0;
      while (true) {
        ArrowArray batch{};
        const int code = stream.get_next(&stream, &batch);
        EXPECT_EQ(code, 0) << stream.get_last_error(&stream);
        if (code != 0 || batch.release == nullptr) {
          break;
        }
        EXPECT_EQ(batch.n_children, 1);
        rows += batch.length;
        batch.release(&batch);
      }
      stream.release(&stream);
      EXPECT_EQ(rows, combined ? 3 : 25);
    }
  }
}

TEST_P(ParallelTableScanTest, PositionDeletesApplyToEverySplit) {
  if (GetParam() < 2) {
    GTEST_SKIP() << "Delete files require format version 2";
  }
  ICEBERG_UNWRAP_OR_FAIL(auto written, WriteDataFile("with-deletes.parquet", 0, 2000,
                                                     /*max_row_group_rows=*/200));
  std::vector<int64_t> positions;
  std::unordered_set<int32_t> deleted;
  for (int32_t id = 3; id < 2000; id += 7) {
    positions.push_back(id);
    deleted.insert(id);
  }
  ICEBERG_UNWRAP_OR_FAIL(
      auto deletes,
      WritePositionDeletes("deletes.parquet", written.data_file->file_path, positions));
  std::vector<WrittenFile> files{written};
  auto metadata = MakeMetadata(files, {deletes});
  ScanSettings settings{.target_split_size = written.data_file->file_size_in_bytes / 4};
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanGroups(metadata, settings));
  // Row groups are merged toward the target, so the file has at least two splits.
  ASSERT_GE(CountSplitTasks(groups), 2U);
  for (const auto& group : groups) {
    for (const auto& task : group->tasks()) {
      EXPECT_EQ(task->delete_files().size(), 1U);
    }
  }
  ExpectParallelMatches(metadata, settings, ExpectedRows(files, deleted));
}

TEST_P(ParallelTableScanTest, RowLineageAndPositionsAreAbsolute) {
  if (GetParam() < 3) {
    GTEST_SKIP() << "Row lineage requires format version 3";
  }
  std::vector<WrittenFile> files;
  for (int file = 0; file < 2; ++file) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto written, WriteDataFile(std::format("lineage-{}.parquet", file), file * 1200,
                                    1200, /*max_row_group_rows=*/150));
    files.push_back(std::move(written));
  }
  auto metadata = MakeMetadata(files);
  auto projection = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               MetadataColumns::kRowId, MetadataColumns::kRowPosition});
  ScanSettings settings{.target_split_size = files[0].data_file->file_size_in_bytes / 4,
                        .projection = projection};

  // The manifest assigns first row ids in file order.
  std::map<int32_t, int64_t> first_row_ids;
  int64_t next_row_id = 0;
  for (const auto& file : files) {
    first_row_ids[file.first_id] = next_row_id;
    next_row_id += file.rows;
  }
  std::vector<Row> expected;
  for (const auto& file : files) {
    for (int32_t row = 0; row < file.rows; ++row) {
      expected.push_back(Row{.id = file.first_id + row,
                             .row_id = first_row_ids[file.first_id] + row,
                             .pos = row});
    }
  }
  std::ranges::sort(expected);
  ExpectParallelMatches(metadata, settings, expected);
}

TEST_P(ParallelTableScanTest, EarlyReleaseBetweenCallbacks) {
  std::vector<WrittenFile> files;
  for (int file = 0; file < 4; ++file) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto written, WriteDataFile(std::format("early-{}.parquet", file), file * 1000,
                                    1000, /*max_row_group_rows=*/100));
    files.push_back(std::move(written));
  }
  auto metadata = MakeMetadata(files);
  ScanSettings settings{.target_split_size = files[0].data_file->file_size_in_bytes / 4};
  ICEBERG_UNWRAP_OR_FAIL(auto scan, BuildScan(metadata, settings));
  ICEBERG_UNWRAP_OR_FAIL(auto groups, scan->PlanTasksStream());
  ICEBERG_UNWRAP_OR_FAIL(
      auto stream, ParallelScanReader::Open(std::move(groups),
                                            {.reader_options = ReaderOptions(settings),
                                             .executor = &executor_,
                                             .worker_count = 4,
                                             .max_queued_batches = 1,
                                             .max_queued_bytes = 1 << 20}));
  ArrowArray first{};
  ASSERT_EQ(stream.get_next(&stream, &first), 0);
  ASSERT_NE(first.release, nullptr);
  stream.release(&stream);
  // The caller still owns the returned batch.
  EXPECT_GT(first.length, 0);
  first.release(&first);
}

INSTANTIATE_TEST_SUITE_P(ParallelTableScanVersions, ParallelTableScanTest,
                         testing::Values(1, 2, 3));

}  // namespace iceberg
