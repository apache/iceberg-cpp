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

#include "iceberg/inspect/files_table.h"

#include <chrono>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include "iceberg/constants.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/inspect/metadata_table_scan_test_base.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

class FilesTableTest : public MetadataTableScanTestBase {};

TEST_P(FilesTableTest, Scan) {
  auto snapshot = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"s3://bucket/data.parquet", PartitionValues(Literal::Int(7))}},
      partitioned_spec_);
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10, {}, partitioned_spec_));

  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto files_stream, files->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto files_batches, ReadAllBatches(std::move(files_stream)));
  ASSERT_EQ(files_batches.size(), 1);
  ASSERT_EQ(files_batches[0]->num_rows(), 1);
  auto paths = std::static_pointer_cast<::arrow::StringArray>(
      files_batches[0]->GetColumnByName("file_path"));
  auto formats = std::static_pointer_cast<::arrow::StringArray>(
      files_batches[0]->GetColumnByName("file_format"));
  auto spec_ids = std::static_pointer_cast<::arrow::Int32Array>(
      files_batches[0]->GetColumnByName("spec_id"));
  auto file_partitions = std::static_pointer_cast<::arrow::StructArray>(
      files_batches[0]->GetColumnByName("partition"));
  auto partition_values =
      std::static_pointer_cast<::arrow::Int32Array>(file_partitions->field(0));
  EXPECT_EQ(paths->GetString(0), "s3://bucket/data.parquet");
  EXPECT_EQ(formats->GetString(0), "PARQUET");
  EXPECT_EQ(spec_ids->Value(0), partitioned_spec_->spec_id());
  EXPECT_EQ(partition_values->Value(0), 7);
}

TEST_P(FilesTableTest, SupportsTimeTravelForSnapshotScopedTables) {
  auto first =
      MakeAppendSnapshot(GetParam(), 1, std::nullopt, 1, {"s3://bucket/first.parquet"});
  auto second = MakeAppendSnapshot(GetParam(), 2, 1, 2, {"s3://bucket/second.parquet"});
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first, second}, 2));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         files->Scan(SnapshotSelection{.snapshot = int64_t{1}}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  auto paths = std::static_pointer_cast<::arrow::StringArray>(
      batches[0]->GetColumnByName("file_path"));
  ASSERT_EQ(paths->length(), 1);
  EXPECT_EQ(paths->GetString(0), "s3://bucket/first.parquet");
}

TEST_P(FilesTableTest, RejectsCombiningNamedRefsWithTimeTravel) {
  auto first = MakeAppendSnapshot(GetParam(), 1, std::nullopt, 1, {"first.parquet"});
  auto second = MakeAppendSnapshot(GetParam(), 2, 1, 2, {"second.parquet"});
  ICEBERG_UNWRAP_OR_FAIL(auto branch, SnapshotRef::MakeBranch(1));
  ICEBERG_UNWRAP_OR_FAIL(auto tag, SnapshotRef::MakeTag(1));
  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs;
  refs.emplace("dev", std::move(branch));
  refs.emplace("release", std::move(tag));
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first, second}, 2, std::move(refs)));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table, MetadataTable::Make<FilesTable>(table));
  for (const std::string ref_name : {"dev", "release"}) {
    for (const SnapshotSelection selection :
         {SnapshotSelection{.snapshot = int64_t{1}, .ref_name = ref_name},
          SnapshotSelection{.snapshot = first->timestamp_ms, .ref_name = ref_name}}) {
      auto result = metadata_table->Scan(selection);
      ASSERT_FALSE(result.has_value());
      EXPECT_EQ(result.error().kind, ErrorKind::kInvalidArgument);
    }
  }

  // A named ref alone selects its head; main is a no-op in Java's useRef.
  for (const std::string ref_name : {"dev", "release", "main"}) {
    SnapshotSelection selection{.ref_name = ref_name};
    if (ref_name == "main") {
      selection.snapshot = int64_t{1};
    }
    ICEBERG_UNWRAP_OR_FAIL(auto stream, metadata_table->Scan(selection));
    ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0]->num_rows(), 1);
    auto paths = std::static_pointer_cast<::arrow::StringArray>(
        batches[0]->GetColumnByName("file_path"));
    EXPECT_EQ(paths->GetString(0), "first.parquet");
  }
}

TEST_P(FilesTableTest, RejectsUnknownReference) {
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({}, kInvalidSnapshotId));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table, MetadataTable::Make<FilesTable>(table));
  EXPECT_THAT(metadata_table->Scan(SnapshotSelection{.ref_name = "missing"}),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_P(FilesTableTest, ScansRefreshedSchemaAndPartitionSpec) {
  auto first = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"old.parquet", PartitionValues(Literal::Int(7))}}, partitioned_spec_);
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first}, 10, {}, partitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  auto old_files_schema = files->schema();
  ICEBERG_UNWRAP_OR_FAIL(auto old_stream, files->Scan());
  auto old_reader = ::arrow::ImportRecordBatchReader(&old_stream);
  ASSERT_TRUE(old_reader.ok()) << old_reader.status().ToString();

  ASSERT_THAT(RefreshTableSchemaAndPartitionSpec(table, first), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto files_stream, files->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto files_batches, ReadAllBatches(std::move(files_stream)));
  ASSERT_EQ(files_batches.size(), 1);
  ASSERT_EQ(files_batches[0]->num_rows(), 2);
  auto file_partition = std::static_pointer_cast<::arrow::StructArray>(
      files_batches[0]->GetColumnByName("partition"));
  ASSERT_EQ(file_partition->num_fields(), 2);
  auto ids = std::static_pointer_cast<::arrow::Int32Array>(file_partition->field(1));
  EXPECT_EQ(ids->Value(0), 1);
  EXPECT_EQ(ids->Value(1), 2);
  auto metrics = std::static_pointer_cast<::arrow::StructArray>(
      files_batches[0]->GetColumnByName("readable_metrics"));
  EXPECT_NE(metrics->GetFieldByName("extra"), nullptr);
  ICEBERG_UNWRAP_OR_FAIL(auto extra,
                         files->schema()->FindFieldByName("readable_metrics.extra"));
  ASSERT_TRUE(extra.has_value());
  EXPECT_NE(files->schema(), old_files_schema);

  // Streams created before refresh retain their original metadata and schema.
  auto batches = (*old_reader)->ToRecordBatches();
  ASSERT_TRUE(batches.ok()) << batches.status().ToString();
  ASSERT_EQ(batches->size(), 1);
  EXPECT_EQ((*batches)[0]->num_rows(), 1);
  auto old_partition = std::static_pointer_cast<::arrow::StructArray>(
      (*batches)[0]->GetColumnByName("partition"));
  EXPECT_EQ(old_partition->num_fields(), 1);
}

TEST_P(FilesTableTest, MainTimestampSelectionUsesSnapshotLogAfterRollback) {
  auto first =
      MakeAppendSnapshot(GetParam(), 1, std::nullopt, 1, {"s3://bucket/first.parquet"});
  auto second = MakeAppendSnapshot(GetParam(), 2, 1, 2, {"s3://bucket/second.parquet"});
  auto third = MakeAppendSnapshot(GetParam(), 3, 2, 3, {"s3://bucket/third.parquet"});
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first, second, third}, 1));
  table->metadata()->snapshot_log = {
      SnapshotLogEntry{.timestamp_ms = first->timestamp_ms, .snapshot_id = 1},
      SnapshotLogEntry{.timestamp_ms = second->timestamp_ms, .snapshot_id = 2},
      SnapshotLogEntry{.timestamp_ms = third->timestamp_ms, .snapshot_id = 3},
      SnapshotLogEntry{.timestamp_ms = third->timestamp_ms + std::chrono::milliseconds(1),
                       .snapshot_id = 1},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         files->Scan(SnapshotSelection{.snapshot = third->timestamp_ms}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  auto paths = std::static_pointer_cast<::arrow::StringArray>(
      batches[0]->GetColumnByName("file_path"));
  ASSERT_EQ(paths->length(), 1);
  EXPECT_EQ(paths->GetString(0), "s3://bucket/third.parquet");
}

TEST_P(FilesTableTest, FilesSchemaIncludesReadableMetrics) {
  ICEBERG_UNWRAP_OR_FAIL(auto table,
                         MakeTable({}, kInvalidSnapshotId, {}, partitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto spec_id_field,
                         files->schema()->FindFieldById(DataFile::kSpecIdFieldId));
  ASSERT_TRUE(spec_id_field.has_value());
  EXPECT_TRUE(spec_id_field->get().optional());

  ICEBERG_UNWRAP_OR_FAIL(auto readable_metrics,
                         files->schema()->FindFieldByName("readable_metrics"));
  ASSERT_TRUE(readable_metrics.has_value());
  EXPECT_TRUE(readable_metrics->get().optional());
  auto metrics_type =
      std::static_pointer_cast<StructType>(readable_metrics->get().type());
  ASSERT_EQ(metrics_type->fields().size(), 2);
  EXPECT_EQ(metrics_type->fields()[0].name(), "data");
  EXPECT_EQ(metrics_type->fields()[1].name(), "id");
  for (const auto& field : metrics_type->fields()) {
    auto column_metrics = std::static_pointer_cast<StructType>(field.type());
    EXPECT_EQ(column_metrics->fields().size(), 6);
  }
}

TEST_P(FilesTableTest, SupportsLegacyVoidPartitionEvolution) {
  ICEBERG_UNWRAP_OR_FAIL(auto older_spec,
                         PartitionSpec::Make(1, {PartitionField(2, 1000, "old_bucket",
                                                                Transform::Bucket(16))}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto latest_spec,
      PartitionSpec::Make(2, {PartitionField(2, 1000, "new_name", Transform::Void())}));
  auto metadata = MakeTableMetadata({}, kInvalidSnapshotId);
  metadata->partition_specs = {
      std::shared_ptr<PartitionSpec>(std::move(older_spec)),
      std::shared_ptr<PartitionSpec>(std::move(latest_spec)),
  };
  metadata->default_spec_id = 2;
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      Table::Make(TableIdentifier{.ns = Namespace{.levels = {"db"}}, .name = "table"},
                  std::move(metadata), "s3://bucket/metadata.json", file_io_, catalog_));

  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto partition_field,
                         files->schema()->FindFieldById(DataFile::kPartitionFieldId));
  ASSERT_TRUE(partition_field.has_value());
  auto partition_type =
      std::static_pointer_cast<StructType>(partition_field->get().type());
  ASSERT_EQ(partition_type->fields().size(), 1);
  EXPECT_EQ(partition_type->fields()[0].name(), "new_name");
  EXPECT_EQ(partition_type->fields()[0].type()->type_id(), TypeId::kInt);
}

TEST_P(FilesTableTest, FilesScanReadsManifestsOnDemand) {
  std::vector<ManifestEntry> entries;
  for (int64_t index = 0; index < MetadataTable::kBatchSize; ++index) {
    entries.push_back(MakeEntry(ManifestStatus::kAdded, 10, 1,
                                MakeDataFile(std::format("data-{}.parquet", index))));
  }
  auto first_manifest = WriteDataManifest(GetParam(), 10, std::move(entries));
  auto second_manifest = WriteDataManifest(
      GetParam(), 10,
      {MakeEntry(ManifestStatus::kAdded, 10, 1, MakeDataFile("last.parquet"))});
  auto snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 10,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1000),
      .manifest_list =
          WriteManifestList(GetParam(), 10, 0, 1, {first_manifest, second_manifest}),
  });
  ASSERT_THAT(file_io_->DeleteFile(second_manifest.manifest_path), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, files->Scan());
  auto imported = ::arrow::ImportRecordBatchReader(&stream);
  ASSERT_TRUE(imported.ok()) << imported.status().ToString();
  auto reader = *imported;
  // The stream owns everything it needs, independent of the source table's lifetime.
  files.reset();
  table.reset();
  auto first = reader->Next();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_NE(*first, nullptr);
  EXPECT_EQ((*first)->num_rows(), MetadataTable::kBatchSize);
  // Opening the absent second manifest is deferred until the next batch.
  EXPECT_FALSE(reader->Next().ok());
}

TEST_P(FilesTableTest, FilesScanSkipsEmptyAndDeletedEntries) {
  auto first = WriteDataManifest(
      GetParam(), 10,
      {MakeEntry(ManifestStatus::kAdded, 10, 1, MakeDataFile("first.parquet"))});
  auto empty = WriteDataManifest(GetParam(), 10, {});
  auto last = WriteDataManifest(
      GetParam(), 10,
      {MakeEntry(ManifestStatus::kDeleted, 10, 1, MakeDataFile("deleted.parquet")),
       MakeEntry(ManifestStatus::kExisting, 10, 1, MakeDataFile("last.parquet"))});
  auto snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 10,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1000),
      .manifest_list = WriteManifestList(GetParam(), 10, 0, 1, {first, empty, last}),
  });
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, files->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_EQ(batches[0]->num_rows(), 2);
  auto paths = std::static_pointer_cast<::arrow::StringArray>(
      batches[0]->GetColumnByName("file_path"));
  EXPECT_EQ(paths->GetString(0), "first.parquet");
  EXPECT_EQ(paths->GetString(1), "last.parquet");
}

TEST_P(FilesTableTest, ScansDecimalPartitionsAndReadableBounds) {
  const int128_t large = (static_cast<int128_t>(1) << 80) + 123;
  const std::vector<int128_t> values{123, -456, large, -large, 0};
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeDecimalPartitionTable(values));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, files->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_EQ(batches[0]->num_rows(), values.size());
  auto partition = std::static_pointer_cast<::arrow::StructArray>(
      batches[0]->GetColumnByName("partition"));
  auto amounts = std::static_pointer_cast<::arrow::Decimal128Array>(partition->field(0));
  auto metrics = std::static_pointer_cast<::arrow::StructArray>(
      batches[0]->GetColumnByName("readable_metrics"));
  auto amount_metrics =
      std::static_pointer_cast<::arrow::StructArray>(metrics->GetFieldByName("amount"));
  auto lower = std::static_pointer_cast<::arrow::Decimal128Array>(
      amount_metrics->GetFieldByName("lower_bound"));
  auto upper = std::static_pointer_cast<::arrow::Decimal128Array>(
      amount_metrics->GetFieldByName("upper_bound"));
  for (size_t index = 0; index < values.size(); ++index) {
    ICEBERG_UNWRAP_OR_FAIL(auto expected, Decimal(values[index]).ToString(2));
    EXPECT_EQ(amounts->FormatValue(index), expected);
    EXPECT_EQ(lower->FormatValue(index), expected);
    EXPECT_EQ(upper->FormatValue(index), expected);
  }
}

TEST_P(FilesTableTest, ScanEmpty) {
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({}, kInvalidSnapshotId));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, metadata_table->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  EXPECT_TRUE(batches.empty());
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, FilesTableTest, ::testing::Values(2, 3));

}  // namespace
}  // namespace iceberg
