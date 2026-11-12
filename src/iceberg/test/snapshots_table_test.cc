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

#include "iceberg/inspect/snapshots_table.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/constants.h"
#include "iceberg/inspect/metadata_scan_task_reader.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/test/arrow_test_util.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/metadata_table_test_base.h"

namespace iceberg {
namespace {

std::vector<std::pair<std::string, std::string>> GetMapEntries(
    const std::shared_ptr<::arrow::MapArray>& map_array, int64_t row) {
  auto keys = std::static_pointer_cast<::arrow::StringArray>(map_array->keys());
  auto values = std::static_pointer_cast<::arrow::StringArray>(map_array->items());
  std::vector<std::pair<std::string, std::string>> entries;
  entries.reserve(map_array->value_length(row));
  const auto offset = map_array->value_offset(row);
  for (int64_t index = offset; index < offset + map_array->value_length(row); ++index) {
    entries.emplace_back(keys->GetString(index), values->GetString(index));
  }
  return entries;
}

}  // namespace

class SnapshotsTableTest : public MetadataTableTestBase {
 protected:
  void SetUp() override {
    MetadataTableTestBase::SetUp();

    auto [snap1, snap2] = MakeTestSnapshots();
    ICEBERG_UNWRAP_OR_FAIL(
        table_, MakeTableWithSnapshots({snap1, snap2}, /*current_snapshot_id=*/2));

    ICEBERG_UNWRAP_OR_FAIL(snapshots_table_, MetadataTable::Make<SnapshotsTable>(table_));
  }

  std::unique_ptr<MetadataTable> snapshots_table_;
};

TEST_F(SnapshotsTableTest, Construct) {
  EXPECT_EQ(snapshots_table_->kind(), MetadataTable::Kind::kSnapshots);
  EXPECT_EQ(snapshots_table_->source_table(), table_);
  EXPECT_NE(snapshots_table_->schema(), nullptr);
}

TEST_F(SnapshotsTableTest, Scan) {
  // Scan the snapshots table once and verify all columns of the result.
  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*snapshots_table_));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  const auto& batch = batches.front();

  // Row and column counts.
  EXPECT_EQ(batch->num_rows(), 2);
  EXPECT_EQ(batch->num_columns(), 6);

  // Column 0: committed_at (timestamptz) — microseconds since epoch.
  auto committed_at = std::static_pointer_cast<::arrow::TimestampArray>(batch->column(0));
  EXPECT_EQ(committed_at->Value(0), 1234567890000LL * 1000);
  EXPECT_EQ(committed_at->Value(1), 9876543210000LL * 1000);

  // Column 1: snapshot_id (long) — returned in storage order.
  auto snapshot_ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
  EXPECT_EQ(snapshot_ids->Value(0), 1);
  EXPECT_EQ(snapshot_ids->Value(1), 2);

  // Column 2: parent_id (long) — first snapshot has no parent.
  auto parent_ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(2));
  EXPECT_TRUE(parent_ids->IsNull(0));
  EXPECT_FALSE(parent_ids->IsNull(1));
  EXPECT_EQ(parent_ids->Value(1), 1);

  // Column 3: operation (string).
  auto operations = std::static_pointer_cast<::arrow::StringArray>(batch->column(3));
  EXPECT_EQ(operations->GetString(0), "append");
  EXPECT_EQ(operations->GetString(1), "append");

  // Column 4: manifest_list (string).
  auto manifest_lists = std::static_pointer_cast<::arrow::StringArray>(batch->column(4));
  EXPECT_EQ(manifest_lists->GetString(0), "file:/tmp/manifest1.avro");
  EXPECT_EQ(manifest_lists->GetString(1), "file:/tmp/manifest2.avro");

  // Column 5: summary (map<string,string>) excludes the separate operation field.
  auto summaries = std::static_pointer_cast<::arrow::MapArray>(batch->column(5));
  EXPECT_FALSE(summaries->IsNull(0));
  EXPECT_FALSE(summaries->IsNull(1));
  EXPECT_EQ(summaries->value_length(0), 10);
  EXPECT_EQ(summaries->value_length(1), 10);

  auto first_summary = GetMapEntries(summaries, 0);
  EXPECT_THAT(
      first_summary,
      ::testing::Not(::testing::Contains(::testing::Pair("operation", "append"))));
  EXPECT_THAT(first_summary, ::testing::Contains(::testing::Pair("total-records", "1")));

  auto second_summary = GetMapEntries(summaries, 1);
  EXPECT_THAT(
      second_summary,
      ::testing::Not(::testing::Contains(::testing::Pair("operation", "append"))));
  EXPECT_THAT(second_summary, ::testing::Contains(::testing::Pair("total-records", "2")));
}

TEST_F(SnapshotsTableTest, ScanSelectedColumns) {
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         ScanColumns(*snapshots_table_, {"operation", "snapshot_id"}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  const auto& batch = batches.front();
  EXPECT_EQ(batch->num_rows(), 2);
  ASSERT_EQ(batch->num_columns(), 2);
  EXPECT_EQ(batch->column_name(0), "snapshot_id");
  EXPECT_EQ(batch->column_name(1), "operation");
  ASSERT_EQ(batch->column(0)->type_id(), ::arrow::Type::INT64);
  ASSERT_EQ(batch->column(1)->type_id(), ::arrow::Type::STRING);

  auto snapshot_ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(0));
  EXPECT_EQ(snapshot_ids->Value(0), 1);
  EXPECT_EQ(snapshot_ids->Value(1), 2);
  auto operations = std::static_pointer_cast<::arrow::StringArray>(batch->column(1));
  EXPECT_EQ(operations->GetString(0), "append");
  EXPECT_EQ(operations->GetString(1), "append");
}

TEST_F(SnapshotsTableTest, ScanSelectedColumnsIgnoresUnknownNames) {
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         ScanColumns(*snapshots_table_, {"parent_id", "missing"}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_EQ(batches.front()->num_columns(), 1);
  EXPECT_EQ(batches.front()->column_name(0), "parent_id");
}

TEST_F(SnapshotsTableTest, ScanSelectedColumnsCaseSensitivity) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto insensitive_stream,
      ScanColumns(*snapshots_table_, {"SNAPSHOT_ID"}, /*case_sensitive=*/false));
  ICEBERG_UNWRAP_OR_FAIL(auto insensitive_batches,
                         ReadAllBatches(std::move(insensitive_stream)));
  ASSERT_EQ(insensitive_batches.size(), 1);
  ASSERT_EQ(insensitive_batches.front()->num_columns(), 1);
  EXPECT_EQ(insensitive_batches.front()->column_name(0), "snapshot_id");

  ICEBERG_UNWRAP_OR_FAIL(auto sensitive_stream,
                         ScanColumns(*snapshots_table_, {"SNAPSHOT_ID"}));
  ICEBERG_UNWRAP_OR_FAIL(auto sensitive_batches,
                         ReadAllBatches(std::move(sensitive_stream)));
  ASSERT_EQ(sensitive_batches.size(), 1);
  EXPECT_EQ(sensitive_batches.front()->num_columns(), 0);
}

TEST_F(SnapshotsTableTest, ScanAllColumnsSelection) {
  // Selecting `*` returns the same columns as a scan without a selection.
  ICEBERG_UNWRAP_OR_FAIL(auto stream, ScanColumns(*snapshots_table_, {"*"}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  EXPECT_EQ(batches.front()->num_rows(), 2);
  EXPECT_EQ(batches.front()->num_columns(), 6);
}

TEST_F(SnapshotsTableTest, ScanBuildsRepeatedly) {
  // A builder can be built more than once; each build returns a new stream.
  ICEBERG_UNWRAP_OR_FAIL(auto builder, snapshots_table_->NewScan());
  builder->Select({"snapshot_id"});
  for (int read = 0; read < 2; ++read) {
    ICEBERG_UNWRAP_OR_FAIL(auto stream, BuildStream(*builder));
    ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
    ASSERT_EQ(batches.size(), 1);
    EXPECT_EQ(batches.front()->num_rows(), 2);
    ASSERT_EQ(batches.front()->num_columns(), 1);
    EXPECT_EQ(batches.front()->column_name(0), "snapshot_id");
  }
}

TEST_F(SnapshotsTableTest, ScanRejectsTimeTravel) {
  ICEBERG_UNWRAP_OR_FAIL(auto builder, snapshots_table_->NewScan());
  EXPECT_THAT(builder->UseSnapshot(1).Build(), IsError(ErrorKind::kNotSupported));

  ICEBERG_UNWRAP_OR_FAIL(auto ref_builder, snapshots_table_->NewScan());
  EXPECT_THAT(ref_builder->UseRef("dev").Build(), IsError(ErrorKind::kNotSupported));

  ICEBERG_UNWRAP_OR_FAIL(auto time_builder, snapshots_table_->NewScan());
  EXPECT_THAT(time_builder->AsOfTime(1).Build(), IsError(ErrorKind::kNotSupported));
}

TEST_F(SnapshotsTableTest, ScanProjectedColumns) {
  // Project() reorders columns and keeps the whole summary map.
  auto projection = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(
          6, "summary",
          std::make_shared<MapType>(SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeRequired(8, "value", string()))),
      SchemaField::MakeRequired(2, "snapshot_id", int64())});
  ICEBERG_UNWRAP_OR_FAIL(auto builder, snapshots_table_->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto stream, BuildStream(builder->Project(projection)));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  const auto& batch = batches.front();
  EXPECT_EQ(batch->num_rows(), 2);
  ASSERT_EQ(batch->num_columns(), 2);
  EXPECT_EQ(batch->column_name(0), "summary");
  EXPECT_EQ(batch->column_name(1), "snapshot_id");
  auto summaries = std::static_pointer_cast<::arrow::MapArray>(batch->column(0));
  EXPECT_THAT(GetMapEntries(summaries, 1),
              ::testing::Contains(::testing::Pair("total-records", "2")));
  auto snapshot_ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
  EXPECT_EQ(snapshot_ids->Value(0), 1);
  EXPECT_EQ(snapshot_ids->Value(1), 2);
}

TEST_F(SnapshotsTableTest, ScanRejectsInvalidProjection) {
  // An optional column cannot be projected as required.
  auto required_parent = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(3, "parent_id", int64())});
  ICEBERG_UNWRAP_OR_FAIL(auto builder, snapshots_table_->NewScan());
  EXPECT_THAT(builder->Project(required_parent).Build(),
              IsError(ErrorKind::kInvalidArgument));

  // A column must keep its type.
  auto string_id = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(2, "snapshot_id", string())});
  ICEBERG_UNWRAP_OR_FAIL(auto type_builder, snapshots_table_->NewScan());
  EXPECT_THAT(type_builder->Project(string_id).Build(),
              IsError(ErrorKind::kNotSupported));

  // Nested fields are not top-level columns.
  auto summary_key = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(7, "key", string())});
  ICEBERG_UNWRAP_OR_FAIL(auto nested_builder, snapshots_table_->NewScan());
  EXPECT_THAT(nested_builder->Project(summary_key).Build(),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(SnapshotsTableTest, ScanNoColumns) {
  ICEBERG_UNWRAP_OR_FAIL(auto stream, ScanColumns(*snapshots_table_, {}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  EXPECT_EQ(batches.front()->num_rows(), 2);
  EXPECT_EQ(batches.front()->num_columns(), 0);
}

TEST_F(SnapshotsTableTest, ScanSelectedMapColumn) {
  ICEBERG_UNWRAP_OR_FAIL(auto stream, ScanColumns(*snapshots_table_, {"summary.value"}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  const auto& batch = batches.front();
  ASSERT_EQ(batch->num_columns(), 1);
  EXPECT_EQ(batch->column_name(0), "summary");
  ASSERT_EQ(batch->column(0)->type_id(), ::arrow::Type::MAP);
  auto summaries = std::static_pointer_cast<::arrow::MapArray>(batch->column(0));
  EXPECT_THAT(GetMapEntries(summaries, 0),
              ::testing::Contains(::testing::Pair("total-records", "1")));
  EXPECT_THAT(GetMapEntries(summaries, 1),
              ::testing::Contains(::testing::Pair("total-records", "2")));

  // A map cannot be projected without its values.
  ICEBERG_UNWRAP_OR_FAIL(auto builder, snapshots_table_->NewScan());
  EXPECT_THAT(builder->Select({"summary.key"}).Build(),
              IsError(ErrorKind::kValidationFailed));
}

TEST_F(SnapshotsTableTest, ScanEmptySnapshotList) {
  // A table with zero snapshots should return zero rows.
  ICEBERG_UNWRAP_OR_FAIL(
      auto empty_table,
      MakeTableWithSnapshots({}, /*current_snapshot_id=*/kInvalidSnapshotId));

  ICEBERG_UNWRAP_OR_FAIL(snapshots_table_,
                         MetadataTable::Make<SnapshotsTable>(empty_table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*snapshots_table_));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  EXPECT_TRUE(batches.empty());
}

TEST_F(SnapshotsTableTest, ScanSkipsNullSnapshots) {
  auto [snap1, snap2] = MakeTestSnapshots();
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTableWithSnapshots({snap1, nullptr, snap2},
                                                            /*current_snapshot_id=*/2));
  ICEBERG_UNWRAP_OR_FAIL(auto snapshots_table,
                         MetadataTable::Make<SnapshotsTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*snapshots_table));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  EXPECT_EQ(batches.front()->num_rows(), 2);
}

TEST_F(SnapshotsTableTest, ScanEmptySummary) {
  auto [missing_summary, operation_only_summary] = MakeTestSnapshots();
  missing_summary->summary.clear();
  missing_summary->manifest_list.clear();
  operation_only_summary->summary = {
      {SnapshotSummaryFields::kOperation, DataOperation::kAppend}};
  ICEBERG_UNWRAP_OR_FAIL(auto table,
                         MakeTableWithSnapshots({missing_summary, operation_only_summary},
                                                /*current_snapshot_id=*/2));
  ICEBERG_UNWRAP_OR_FAIL(auto snapshots_table,
                         MetadataTable::Make<SnapshotsTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*snapshots_table));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  auto summaries =
      std::static_pointer_cast<::arrow::MapArray>(batches.front()->column(5));
  EXPECT_TRUE(summaries->IsNull(0));
  auto manifests =
      std::static_pointer_cast<::arrow::StringArray>(batches.front()->column(4));
  EXPECT_TRUE(manifests->IsNull(0));
  EXPECT_FALSE(manifests->IsNull(1));
  EXPECT_FALSE(summaries->IsNull(1));
  EXPECT_EQ(summaries->value_length(1), 0);
}

TEST_F(SnapshotsTableTest, ScanReturnsMultipleBatches) {
  auto snapshot = MakeTestSnapshots().first;
  std::vector<std::shared_ptr<Snapshot>> snapshots(
      MetadataScanTaskReader::Options{}.batch_size + 1, snapshot);
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTableWithSnapshots(std::move(snapshots),
                                                            /*current_snapshot_id=*/1));
  ICEBERG_UNWRAP_OR_FAIL(auto snapshots_table,
                         MetadataTable::Make<SnapshotsTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*snapshots_table));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 2);
  EXPECT_EQ(batches[0]->num_rows(), MetadataScanTaskReader::Options{}.batch_size);
  EXPECT_EQ(batches[1]->num_rows(), 1);
}

TEST_F(SnapshotsTableTest, ScanUsesMetadataCapturedAtCreation) {
  auto snapshot = MakeTestSnapshots().first;
  std::vector<std::shared_ptr<Snapshot>> snapshots(
      MetadataScanTaskReader::Options{}.batch_size + 1, snapshot);
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTableWithSnapshots(std::move(snapshots),
                                                            /*current_snapshot_id=*/1));
  ICEBERG_UNWRAP_OR_FAIL(auto snapshots_table,
                         MetadataTable::Make<SnapshotsTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*snapshots_table));
  auto reader_result = ::arrow::ImportRecordBatchReader(&stream);
  ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
  auto reader = std::move(reader_result).ValueUnsafe();

  auto first_batch_result = reader->Next();
  ASSERT_TRUE(first_batch_result.ok()) << first_batch_result.status().ToString();
  ASSERT_NE(*first_batch_result, nullptr);
  EXPECT_EQ((*first_batch_result)->num_rows(),
            MetadataScanTaskReader::Options{}.batch_size);

  ICEBERG_UNWRAP_OR_FAIL(
      auto refreshed_table,
      MakeTableWithSnapshots({}, /*current_snapshot_id=*/kInvalidSnapshotId,
                             "s3://bucket/refreshed-meta.json"));
  EXPECT_CALL(*catalog_, LoadTable(table->name()))
      .WillOnce(::testing::Return(refreshed_table));
  ASSERT_THAT(table->Refresh(), IsOk());

  auto second_batch_result = reader->Next();
  ASSERT_TRUE(second_batch_result.ok()) << second_batch_result.status().ToString();
  ASSERT_NE(*second_batch_result, nullptr);
  EXPECT_EQ((*second_batch_result)->num_rows(), 1);
}

TEST_F(SnapshotsTableTest, CanonicalSchema) {
  auto expected = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "committed_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeOptional(4, "operation", string()),
      SchemaField::MakeOptional(5, "manifest_list", string()),
      SchemaField::MakeOptional(
          6, "summary",
          std::make_shared<MapType>(SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeRequired(8, "value", string())))});
  EXPECT_EQ(*SnapshotsTable::CanonicalSchema(), *expected);
  EXPECT_EQ(*snapshots_table_->schema(), *expected);
}

TEST_F(SnapshotsTableTest, PlannedTaskRetainsProjectionAndIndependentReadState) {
  ICEBERG_UNWRAP_OR_FAIL(auto builder, snapshots_table_->NewScan());
  builder->Select({"snapshot_id", "operation"});
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  ASSERT_EQ(tasks.size(), 1);
  ICEBERG_UNWRAP_OR_FAIL(auto reader, MetadataScanTaskReader::Make({.batch_size = 1}));
  ICEBERG_UNWRAP_OR_FAIL(auto first, reader->Open(*tasks.front()));
  ICEBERG_UNWRAP_OR_FAIL(auto second, reader->Open(*tasks.front()));
  // Streams retain their own input and cursors after the planning objects are gone.
  tasks.clear();
  scan.reset();
  reader.reset();
  builder.reset();
  snapshots_table_.reset();
  table_.reset();
  Schema selected({SchemaField::MakeRequired(2, "snapshot_id", int64()),
                   SchemaField::MakeOptional(4, "operation", string())});
  ICEBERG_UNWRAP_OR_FAIL(auto expected, TableFromJson(selected, R"([
    {"snapshot_id":1,"operation":"append"},
    {"snapshot_id":2,"operation":"append"}
  ])"));
  ICEBERG_UNWRAP_OR_FAIL(auto actual, ReadArrowTable(std::move(first)));
  EXPECT_TRUE(TablesEqual(*expected, *actual));
  ASSERT_EQ(actual->column(0)->num_chunks(), 2);
  EXPECT_EQ(actual->column(0)->chunk(0)->length(), 1);
  EXPECT_EQ(actual->column(0)->chunk(1)->length(), 1);
  ICEBERG_UNWRAP_OR_FAIL(auto repeated, ReadArrowTable(std::move(second)));
  EXPECT_TRUE(TablesEqual(*expected, *repeated));
}

}  // namespace iceberg
