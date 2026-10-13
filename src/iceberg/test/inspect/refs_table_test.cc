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

#include "iceberg/inspect/refs_table.h"

#include <cstdint>
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
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/metadata_table_test_base.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

class RefsTableTest : public MetadataTableTestBase,
                      public ::testing::WithParamInterface<int8_t> {
 protected:
  Result<std::shared_ptr<Table>> MakeTable(
      std::vector<std::shared_ptr<Snapshot>> snapshots, int64_t current_snapshot_id,
      std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs = {}) {
    auto metadata = std::make_shared<TableMetadata>(*metadata_);
    metadata->format_version = GetParam();
    metadata->snapshots = std::move(snapshots);
    metadata->current_snapshot_id = current_snapshot_id;
    metadata->refs = std::move(refs);
    return Table::Make(
        TableIdentifier{.ns = Namespace{.levels = {"db"}}, .name = "table"},
        std::move(metadata), "s3://bucket/metadata.json", io_, catalog_);
  }
};

TEST_P(RefsTableTest, ScansBranchesAndTagsAsRefs) {
  ICEBERG_UNWRAP_OR_FAIL(auto main_ref, SnapshotRef::MakeBranch(2));
  ICEBERG_UNWRAP_OR_FAIL(auto dev_ref, SnapshotRef::MakeBranch(1, 3, 2000, 1000));
  ICEBERG_UNWRAP_OR_FAIL(auto release_ref, SnapshotRef::MakeTag(1, 5000));
  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs;
  refs.emplace("main", std::move(main_ref));
  refs.emplace("dev", std::move(dev_ref));
  refs.emplace("release", std::move(release_ref));

  auto first = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 1,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1000),
      .manifest_list = "unused-1.avro",
  });
  auto second = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 2,
      .parent_snapshot_id = 1,
      .sequence_number = 2,
      .timestamp_ms = TimePointMsFromUnixMs(2000),
      .manifest_list = "unused-2.avro",
  });
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first, second}, 2, std::move(refs)));

  ICEBERG_UNWRAP_OR_FAIL(auto refs_table, MetadataTable::Make<RefsTable>(table));
  EXPECT_EQ(refs_table->kind(), MetadataTable::Kind::kRefs);
  EXPECT_FALSE(refs_table->supports_time_travel());
  Schema expected_schema({
      SchemaField::MakeRequired(1, "name", string()),
      SchemaField::MakeRequired(2, "type", string()),
      SchemaField::MakeRequired(3, "snapshot_id", int64()),
      SchemaField::MakeOptional(4, "max_reference_age_in_ms", int64()),
      SchemaField::MakeOptional(5, "min_snapshots_to_keep", int32()),
      SchemaField::MakeOptional(6, "max_snapshot_age_in_ms", int64()),
  });
  EXPECT_EQ(*refs_table->schema(), expected_schema);
  ICEBERG_UNWRAP_OR_FAIL(auto stream, refs_table->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_EQ(batches[0]->num_rows(), 3);
  auto names = std::static_pointer_cast<::arrow::StringArray>(batches[0]->column(0));
  auto types = std::static_pointer_cast<::arrow::StringArray>(batches[0]->column(1));
  auto ids = std::static_pointer_cast<::arrow::Int64Array>(batches[0]->column(2));
  auto ref_age = std::static_pointer_cast<::arrow::Int64Array>(batches[0]->column(3));
  auto min_snapshots =
      std::static_pointer_cast<::arrow::Int32Array>(batches[0]->column(4));
  auto snapshot_age =
      std::static_pointer_cast<::arrow::Int64Array>(batches[0]->column(5));
  EXPECT_EQ(names->GetString(0), "dev");
  EXPECT_EQ(types->GetString(0), "BRANCH");
  EXPECT_EQ(ids->Value(0), 1);
  EXPECT_EQ(ref_age->Value(0), 1000);
  EXPECT_EQ(min_snapshots->Value(0), 3);
  EXPECT_EQ(snapshot_age->Value(0), 2000);
  EXPECT_EQ(names->GetString(1), "main");
  EXPECT_EQ(types->GetString(1), "BRANCH");
  EXPECT_EQ(ids->Value(1), 2);
  EXPECT_TRUE(ref_age->IsNull(1));
  EXPECT_TRUE(min_snapshots->IsNull(1));
  EXPECT_TRUE(snapshot_age->IsNull(1));
  EXPECT_EQ(names->GetString(2), "release");
  EXPECT_EQ(types->GetString(2), "TAG");
  EXPECT_EQ(ids->Value(2), 1);
  EXPECT_EQ(ref_age->Value(2), 5000);
  EXPECT_TRUE(min_snapshots->IsNull(2));
  EXPECT_TRUE(snapshot_age->IsNull(2));
}

TEST_P(RefsTableTest, ScanEmpty) {
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({}, kInvalidSnapshotId));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table, MetadataTable::Make<RefsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, metadata_table->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  EXPECT_TRUE(batches.empty());
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, RefsTableTest, ::testing::Values(2, 3));

}  // namespace
}  // namespace iceberg
