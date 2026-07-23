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

#include "iceberg/inspect/metadata_table.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/metadata_table_test_base.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

std::shared_ptr<Schema> MakeSnapshotsSchema() {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "committed_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeOptional(4, "operation", string()),
      SchemaField::MakeOptional(5, "manifest_list", string()),
      SchemaField::MakeOptional(
          6, "summary",
          std::make_shared<MapType>(SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeRequired(8, "value", string())))});
}

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

    ICEBERG_UNWRAP_OR_FAIL(snapshots_table_,
                           MetadataTable::Make(table_, MetadataTable::Kind::kSnapshots));
  }

  std::unique_ptr<MetadataTable> snapshots_table_;
};

TEST_F(SnapshotsTableTest, Construct) {
  EXPECT_EQ(snapshots_table_->kind(), MetadataTable::Kind::kSnapshots);
  EXPECT_EQ(snapshots_table_->source_table(), table_);
  EXPECT_EQ(snapshots_table_->name().name, "test_table.snapshots");
  EXPECT_EQ(snapshots_table_->name().ns.levels, (std::vector<std::string>{"db"}));
  EXPECT_NE(snapshots_table_->schema(), nullptr);
}

TEST_F(SnapshotsTableTest, SchemaMatchesIcebergSchema) {
  EXPECT_TRUE(*snapshots_table_->schema() == *MakeSnapshotsSchema());
}

TEST_F(SnapshotsTableTest, Scan) {
  // Scan the snapshots table once and verify all columns of the result.
  ICEBERG_UNWRAP_OR_FAIL(auto array, snapshots_table_->Scan());
  auto batch = FinishAndImport(std::move(array), *snapshots_table_->schema());

  // Row and column counts.
  EXPECT_EQ(batch->num_rows(), 2);
  EXPECT_EQ(batch->num_columns(), 6);

  // Column 0: committed_at (timestamptz) — microseconds since epoch.
  auto committed_at = std::static_pointer_cast<::arrow::TimestampArray>(batch->column(0));
  EXPECT_EQ(committed_at->Value(0), 1234567890000 * 1000);
  EXPECT_EQ(committed_at->Value(1), 9876543210000 * 1000);

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

  // Column 5: summary (map<string,string>) — each summary has 11 entries
  // (10 data + 1 operation).
  auto summaries = std::static_pointer_cast<::arrow::MapArray>(batch->column(5));
  EXPECT_FALSE(summaries->IsNull(0));
  EXPECT_FALSE(summaries->IsNull(1));
  EXPECT_EQ(summaries->value_length(0), 11);
  EXPECT_EQ(summaries->value_length(1), 11);

  auto first_summary = GetMapEntries(summaries, 0);
  EXPECT_THAT(first_summary, ::testing::Contains(::testing::Pair("operation", "append")));
  EXPECT_THAT(first_summary, ::testing::Contains(::testing::Pair("total-records", "1")));

  auto second_summary = GetMapEntries(summaries, 1);
  EXPECT_THAT(second_summary,
              ::testing::Contains(::testing::Pair("operation", "append")));
  EXPECT_THAT(second_summary, ::testing::Contains(::testing::Pair("total-records", "2")));
}

TEST_F(SnapshotsTableTest, ScanSnapshotSelectionIgnored) {
  // SnapshotsTable always returns all snapshots regardless of selection.
  SnapshotSelection sel{.snapshot_id = 999};
  ICEBERG_UNWRAP_OR_FAIL(auto array, snapshots_table_->Scan(sel));
  auto batch = FinishAndImport(std::move(array), *snapshots_table_->schema());
  // Should still return all 2 snapshots, not filtered to snapshot 999.
  EXPECT_EQ(batch->num_rows(), 2);
}

TEST_F(SnapshotsTableTest, ScanEmptySnapshotList) {
  // A table with zero snapshots should return zero rows.
  ICEBERG_UNWRAP_OR_FAIL(auto empty_table,
                         MakeTableWithSnapshots({}, /*current_snapshot_id=*/-1));

  ICEBERG_UNWRAP_OR_FAIL(
      snapshots_table_,
      MetadataTable::Make(empty_table, MetadataTable::Kind::kSnapshots));

  ICEBERG_UNWRAP_OR_FAIL(auto array, snapshots_table_->Scan(std::nullopt));
  auto batch = FinishAndImport(std::move(array), *snapshots_table_->schema());
  EXPECT_EQ(batch->num_rows(), 0);
  EXPECT_EQ(batch->num_columns(), 6);
}

}  // namespace iceberg
