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

#include "iceberg/inspect/history_table.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/inspect/metadata_scan_task_reader.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/arrow_test_util.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/metadata_table_test_base.h"
#include "iceberg/type.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

class HistoryTableTest : public MetadataTableTestBase {
 protected:
  void SetUp() override {
    MetadataTableTestBase::SetUp();
    metadata_->snapshots = {
        std::make_shared<Snapshot>(Snapshot{.snapshot_id = 1}),
        std::make_shared<Snapshot>(Snapshot{.snapshot_id = 2, .parent_snapshot_id = 1}),
        std::make_shared<Snapshot>(Snapshot{.snapshot_id = 3, .parent_snapshot_id = 1})};
    metadata_->current_snapshot_id = 3;
    // Roll back from 2 to 1, include an expired snapshot (4), then move to 3.
    metadata_->snapshot_log = {
        {.timestamp_ms = TimePointMsFromUnixMs(1000), .snapshot_id = 1},
        {.timestamp_ms = TimePointMsFromUnixMs(2000), .snapshot_id = 2},
        {.timestamp_ms = TimePointMsFromUnixMs(2500), .snapshot_id = 1},
        {.timestamp_ms = TimePointMsFromUnixMs(2700), .snapshot_id = 4},
        {.timestamp_ms = TimePointMsFromUnixMs(3000), .snapshot_id = 3}};
    ICEBERG_UNWRAP_OR_FAIL(metadata_table_, table_->Inspect(MetadataTableKind::kHistory));
  }

  static std::shared_ptr<Schema> ExpectedSchema() {
    return std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(1, "made_current_at", timestamp_tz()),
        SchemaField::MakeRequired(2, "snapshot_id", int64()),
        SchemaField::MakeOptional(3, "parent_id", int64()),
        SchemaField::MakeRequired(4, "is_current_ancestor", boolean())});
  }

  std::unique_ptr<MetadataTable> metadata_table_;
};

TEST_F(HistoryTableTest, CanonicalSchema) {
  const auto expected = ExpectedSchema();
  EXPECT_EQ(*HistoryTable::CanonicalSchema(), *expected);
  EXPECT_EQ(*metadata_table_->schema(), *expected);
}

TEST_F(HistoryTableTest, Read) {
  ICEBERG_UNWRAP_OR_FAIL(auto builder, metadata_table_->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  ASSERT_EQ(tasks.size(), 1);
  ICEBERG_UNWRAP_OR_FAIL(auto expected, TableFromJson(*ExpectedSchema(), R"([
    {"made_current_at":1000000,"snapshot_id":1,"parent_id":null,"is_current_ancestor":true},
    {"made_current_at":2000000,"snapshot_id":2,"parent_id":1,"is_current_ancestor":false},
    {"made_current_at":2500000,"snapshot_id":1,"parent_id":null,"is_current_ancestor":true},
    {"made_current_at":2700000,"snapshot_id":4,"parent_id":null,"is_current_ancestor":false},
    {"made_current_at":3000000,"snapshot_id":3,"parent_id":1,"is_current_ancestor":true}
  ])"));
  for (int64_t batch_size : {1, 2}) {
    SCOPED_TRACE(batch_size);
    ICEBERG_UNWRAP_OR_FAIL(auto reader,
                           MetadataScanTaskReader::Make({.batch_size = batch_size}));
    ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(*tasks.front()));
    ICEBERG_UNWRAP_OR_FAIL(auto actual, ReadArrowTable(std::move(stream)));
    EXPECT_TRUE(TablesEqual(*expected, *actual));
  }
}

TEST_F(HistoryTableTest, Project) {
  ICEBERG_UNWRAP_OR_FAIL(auto builder, metadata_table_->NewScan());
  builder->Select({"snapshot_id", "is_current_ancestor"});
  ICEBERG_UNWRAP_OR_FAIL(auto selected_scan, builder->Build());

  auto projection = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(3, "parent_id", int64()),
                               SchemaField::MakeRequired(2, "snapshot_id", int64())});
  builder->Project(projection);
  ICEBERG_UNWRAP_OR_FAIL(auto projected_scan, builder->Build());

  Schema selection({SchemaField::MakeRequired(2, "snapshot_id", int64()),
                    SchemaField::MakeRequired(4, "is_current_ancestor", boolean())});
  ICEBERG_UNWRAP_OR_FAIL(auto expected, TableFromJson(selection, R"([
    {"snapshot_id":1,"is_current_ancestor":true},
    {"snapshot_id":2,"is_current_ancestor":false},
    {"snapshot_id":1,"is_current_ancestor":true},
    {"snapshot_id":4,"is_current_ancestor":false},
    {"snapshot_id":3,"is_current_ancestor":true}
  ])"));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, selected_scan->ToArrowArrayStream());
  ICEBERG_UNWRAP_OR_FAIL(auto actual, ReadArrowTable(std::move(stream)));
  EXPECT_TRUE(TablesEqual(*expected, *actual));

  ICEBERG_UNWRAP_OR_FAIL(auto reordered, TableFromJson(*projection, R"([
    {"parent_id":null,"snapshot_id":1},
    {"parent_id":1,"snapshot_id":2},
    {"parent_id":null,"snapshot_id":1},
    {"parent_id":null,"snapshot_id":4},
    {"parent_id":1,"snapshot_id":3}
  ])"));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, projected_scan->PlanTasks());
  ASSERT_EQ(tasks.size(), 1);
  ICEBERG_UNWRAP_OR_FAIL(auto reader, MetadataScanTaskReader::Make({.batch_size = 2}));
  ICEBERG_UNWRAP_OR_FAIL(auto projected_stream, reader->Open(*tasks.front()));
  ICEBERG_UNWRAP_OR_FAIL(auto projected, ReadArrowTable(std::move(projected_stream)));
  EXPECT_TRUE(TablesEqual(*reordered, *projected));
}

TEST_F(HistoryTableTest, CyclicAncestryTerminates) {
  metadata_->snapshots.front()->parent_snapshot_id = 3;
  ICEBERG_UNWRAP_OR_FAIL(auto builder, metadata_table_->NewScan());
  builder->Select({"snapshot_id", "is_current_ancestor"});
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->ToArrowArrayStream());
  ICEBERG_UNWRAP_OR_FAIL(auto actual, ReadArrowTable(std::move(stream)));
  Schema schema({SchemaField::MakeRequired(2, "snapshot_id", int64()),
                 SchemaField::MakeRequired(4, "is_current_ancestor", boolean())});
  ICEBERG_UNWRAP_OR_FAIL(auto expected, TableFromJson(schema, R"([
    {"snapshot_id":1,"is_current_ancestor":true},
    {"snapshot_id":2,"is_current_ancestor":false},
    {"snapshot_id":1,"is_current_ancestor":true},
    {"snapshot_id":4,"is_current_ancestor":false},
    {"snapshot_id":3,"is_current_ancestor":true}
  ])"));
  EXPECT_TRUE(TablesEqual(*expected, *actual));
}

}  // namespace iceberg
