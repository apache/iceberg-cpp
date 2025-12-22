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

#include <chrono>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/util/snapshot_util_internal.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

namespace {

// Schema for testing: id (int32), data (string)
std::shared_ptr<Schema> CreateTestSchema() {
  auto field1 = SchemaField::MakeRequired(1, "id", int32());
  auto field2 = SchemaField::MakeRequired(2, "data", string());
  return std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 0);
}

// Helper to create a snapshot
std::shared_ptr<Snapshot> CreateSnapshot(int64_t snapshot_id,
                                         std::optional<int64_t> parent_id,
                                         int64_t sequence_number,
                                         TimePointMs timestamp_ms) {
  return std::make_shared<Snapshot>(
      Snapshot{.snapshot_id = snapshot_id,
               .parent_snapshot_id = parent_id,
               .sequence_number = sequence_number,
               .timestamp_ms = timestamp_ms,
               .manifest_list =
                   "s3://bucket/manifest-list-" + std::to_string(snapshot_id) + ".avro",
               .summary = {},
               .schema_id = 0});
}

// Helper to create table metadata with snapshots
std::shared_ptr<TableMetadata> CreateTableMetadataWithSnapshots(
    int64_t base_snapshot_id, int64_t main1_snapshot_id, int64_t main2_snapshot_id,
    int64_t branch_snapshot_id, int64_t fork0_snapshot_id, int64_t fork1_snapshot_id,
    int64_t fork2_snapshot_id, TimePointMs base_timestamp) {
  auto metadata = std::make_shared<TableMetadata>();
  metadata->format_version = 2;
  metadata->table_uuid = "test-uuid-1234";
  metadata->location = "s3://bucket/test";
  metadata->last_sequence_number = 10;
  metadata->last_updated_ms = base_timestamp + std::chrono::milliseconds(3000);
  metadata->last_column_id = 2;
  metadata->current_schema_id = 0;
  metadata->schemas.push_back(CreateTestSchema());
  metadata->default_spec_id = PartitionSpec::kInitialSpecId;
  metadata->last_partition_id = 0;
  metadata->current_snapshot_id = main2_snapshot_id;
  metadata->default_sort_order_id = SortOrder::kInitialSortOrderId;
  metadata->sort_orders.push_back(SortOrder::Unsorted());
  metadata->next_row_id = TableMetadata::kInitialRowId;
  metadata->properties = TableProperties::default_properties();

  // Create snapshots: base -> main1 -> main2
  auto base_snapshot = CreateSnapshot(base_snapshot_id, std::nullopt, 1, base_timestamp);
  auto main1_snapshot = CreateSnapshot(main1_snapshot_id, base_snapshot_id, 2,
                                       base_timestamp + std::chrono::milliseconds(1000));
  auto main2_snapshot = CreateSnapshot(main2_snapshot_id, main1_snapshot_id, 3,
                                       base_timestamp + std::chrono::milliseconds(2000));

  // Branch snapshot (from base)
  auto branch_snapshot = CreateSnapshot(branch_snapshot_id, base_snapshot_id, 4,
                                        base_timestamp + std::chrono::milliseconds(1500));

  // Fork branch snapshots: fork0 -> fork1 -> fork2 (fork0 will be expired)
  auto fork0_snapshot = CreateSnapshot(fork0_snapshot_id, base_snapshot_id, 5,
                                       base_timestamp + std::chrono::milliseconds(500));
  auto fork1_snapshot = CreateSnapshot(fork1_snapshot_id, fork0_snapshot_id, 6,
                                       base_timestamp + std::chrono::milliseconds(2500));
  auto fork2_snapshot = CreateSnapshot(fork2_snapshot_id, fork1_snapshot_id, 7,
                                       base_timestamp + std::chrono::milliseconds(3000));

  metadata->snapshots = {base_snapshot,   main1_snapshot, main2_snapshot,
                         branch_snapshot, fork1_snapshot, fork2_snapshot};
  // Note: fork0 is expired, so it's not in the snapshots list

  // Snapshot log
  metadata->snapshot_log = {
      {.timestamp_ms = base_timestamp, .snapshot_id = base_snapshot_id},
      {.timestamp_ms = base_timestamp + std::chrono::milliseconds(1000),
       .snapshot_id = main1_snapshot_id},
      {.timestamp_ms = base_timestamp + std::chrono::milliseconds(2000),
       .snapshot_id = main2_snapshot_id},
  };

  // Create refs
  std::string branch_name = "b1";
  metadata->refs[branch_name] = std::make_shared<SnapshotRef>(
      SnapshotRef{.snapshot_id = branch_snapshot_id, .retention = SnapshotRef::Branch{}});

  std::string fork_branch = "fork";
  metadata->refs[fork_branch] = std::make_shared<SnapshotRef>(
      SnapshotRef{.snapshot_id = fork2_snapshot_id, .retention = SnapshotRef::Branch{}});

  return metadata;
}

// Helper to extract snapshot IDs from a vector of snapshots
std::vector<int64_t> ExtractSnapshotIds(
    const std::vector<std::shared_ptr<Snapshot>>& snapshots) {
  std::vector<int64_t> ids;
  ids.reserve(snapshots.size());
  for (const auto& snapshot : snapshots) {
    ids.push_back(snapshot->snapshot_id);
  }
  return ids;
}

}  // namespace

class SnapshotUtilTest : public ::testing::Test {
 protected:
  void SetUp() override {
    base_timestamp_ = TimePointMs{std::chrono::milliseconds(1000000000000)};
    base_snapshot_id_ = 100;
    main1_snapshot_id_ = 101;
    main2_snapshot_id_ = 102;
    branch_snapshot_id_ = 200;
    fork0_snapshot_id_ = 300;
    fork1_snapshot_id_ = 301;
    fork2_snapshot_id_ = 302;

    auto metadata = CreateTableMetadataWithSnapshots(
        base_snapshot_id_, main1_snapshot_id_, main2_snapshot_id_, branch_snapshot_id_,
        fork0_snapshot_id_, fork1_snapshot_id_, fork2_snapshot_id_, base_timestamp_);

    TableIdentifier table_ident{.ns = {}, .name = "test"};
    auto io = std::make_shared<MockFileIO>();
    auto catalog = std::make_shared<MockCatalog>();
    table_ = std::move(Table::Make(table_ident, std::move(metadata),
                                   "s3://bucket/test/metadata.json", io, catalog)
                           .value());
  }

  TimePointMs base_timestamp_;
  int64_t base_snapshot_id_;
  int64_t main1_snapshot_id_;
  int64_t main2_snapshot_id_;
  int64_t branch_snapshot_id_;
  int64_t fork0_snapshot_id_;
  int64_t fork1_snapshot_id_;
  int64_t fork2_snapshot_id_;
  std::shared_ptr<Table> table_;
};

TEST_F(SnapshotUtilTest, IsParentAncestorOf) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto result1,
      SnapshotUtil::IsParentAncestorOf(*table_, main1_snapshot_id_, base_snapshot_id_));
  EXPECT_TRUE(result1);

  ICEBERG_UNWRAP_OR_FAIL(
      auto result2,
      SnapshotUtil::IsParentAncestorOf(*table_, branch_snapshot_id_, main1_snapshot_id_));
  EXPECT_FALSE(result2);

  // fork2's parent is fork1, fork1's parent is fork0 (expired)
  ICEBERG_UNWRAP_OR_FAIL(
      auto result3,
      SnapshotUtil::IsParentAncestorOf(*table_, fork2_snapshot_id_, fork0_snapshot_id_));
  EXPECT_TRUE(result3);
}

TEST_F(SnapshotUtilTest, IsAncestorOf) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto result1,
      SnapshotUtil::IsAncestorOf(*table_, main1_snapshot_id_, base_snapshot_id_));
  EXPECT_TRUE(result1);

  ICEBERG_UNWRAP_OR_FAIL(
      auto result2,
      SnapshotUtil::IsAncestorOf(*table_, branch_snapshot_id_, main1_snapshot_id_));
  EXPECT_FALSE(result2);

  // fork2 -> fork1 -> fork0 (expired, not in snapshots)
  ICEBERG_UNWRAP_OR_FAIL(
      auto result3,
      SnapshotUtil::IsAncestorOf(*table_, fork2_snapshot_id_, fork0_snapshot_id_));
  EXPECT_FALSE(result3);  // fork0 is expired, so not found

  // Test with current snapshot
  ICEBERG_UNWRAP_OR_FAIL(auto result4,
                         SnapshotUtil::IsAncestorOf(*table_, main1_snapshot_id_));
  EXPECT_TRUE(result4);

  ICEBERG_UNWRAP_OR_FAIL(auto result5,
                         SnapshotUtil::IsAncestorOf(*table_, branch_snapshot_id_));
  EXPECT_FALSE(result5);
}

TEST_F(SnapshotUtilTest, CurrentAncestors) {
  ICEBERG_UNWRAP_OR_FAIL(auto ancestors, SnapshotUtil::CurrentAncestors(*table_));
  auto ids = ExtractSnapshotIds(ancestors);
  EXPECT_EQ(ids, std::vector<int64_t>(
                     {main2_snapshot_id_, main1_snapshot_id_, base_snapshot_id_}));

  ICEBERG_UNWRAP_OR_FAIL(auto ancestor_ids, SnapshotUtil::CurrentAncestorIds(*table_));
  EXPECT_EQ(ancestor_ids, std::vector<int64_t>({main2_snapshot_id_, main1_snapshot_id_,
                                                base_snapshot_id_}));
}

TEST_F(SnapshotUtilTest, OldestAncestor) {
  ICEBERG_UNWRAP_OR_FAIL(auto oldest, SnapshotUtil::OldestAncestor(*table_));
  ASSERT_TRUE(oldest.has_value());
  EXPECT_EQ(oldest.value()->snapshot_id, base_snapshot_id_);

  ICEBERG_UNWRAP_OR_FAIL(auto oldest_of_main2,
                         SnapshotUtil::OldestAncestorOf(*table_, main2_snapshot_id_));
  ASSERT_TRUE(oldest_of_main2.has_value());
  EXPECT_EQ(oldest_of_main2.value()->snapshot_id, base_snapshot_id_);

  ICEBERG_UNWRAP_OR_FAIL(auto oldest_after,
                         SnapshotUtil::OldestAncestorAfter(
                             *table_, base_timestamp_ + std::chrono::milliseconds(1)));
  ASSERT_TRUE(oldest_after.has_value());
  EXPECT_EQ(oldest_after.value()->snapshot_id, main1_snapshot_id_);
}

TEST_F(SnapshotUtilTest, SnapshotsBetween) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto snapshot_ids,
      SnapshotUtil::SnapshotIdsBetween(*table_, base_snapshot_id_, main2_snapshot_id_));
  EXPECT_EQ(snapshot_ids, std::vector<int64_t>({main2_snapshot_id_, main1_snapshot_id_}));

  ICEBERG_UNWRAP_OR_FAIL(
      auto ancestors_between1,
      SnapshotUtil::AncestorsBetween(*table_, main2_snapshot_id_, main1_snapshot_id_));
  auto ids1 = ExtractSnapshotIds(ancestors_between1);
  EXPECT_EQ(ids1, std::vector<int64_t>({main2_snapshot_id_}));

  ICEBERG_UNWRAP_OR_FAIL(
      auto ancestors_between2,
      SnapshotUtil::AncestorsBetween(*table_, main2_snapshot_id_, branch_snapshot_id_));
  auto ids2 = ExtractSnapshotIds(ancestors_between2);
  EXPECT_EQ(ids2, std::vector<int64_t>(
                      {main2_snapshot_id_, main1_snapshot_id_, base_snapshot_id_}));
}

TEST_F(SnapshotUtilTest, AncestorsOf) {
  // Test ancestors of fork2: fork2 -> fork1 (fork0 is expired, not in snapshots)
  ICEBERG_UNWRAP_OR_FAIL(auto ancestors,
                         SnapshotUtil::AncestorsOf(*table_, fork2_snapshot_id_));
  auto ids = ExtractSnapshotIds(ancestors);
  EXPECT_EQ(ids, std::vector<int64_t>({fork2_snapshot_id_, fork1_snapshot_id_}));
}

TEST_F(SnapshotUtilTest, SchemaForRef) {
  ICEBERG_UNWRAP_OR_FAIL(auto initial_schema, table_->schema());
  ASSERT_NE(initial_schema, nullptr);

  // Test with null/empty ref (main branch)
  ICEBERG_UNWRAP_OR_FAIL(auto schema1, SnapshotUtil::SchemaFor(*table_, ""));
  EXPECT_EQ(schema1->fields().size(), initial_schema->fields().size());

  // Test with non-existing ref
  ICEBERG_UNWRAP_OR_FAIL(auto schema2,
                         SnapshotUtil::SchemaFor(*table_, "non-existing-ref"));
  EXPECT_EQ(schema2->fields().size(), initial_schema->fields().size());

  // Test with main branch
  ICEBERG_UNWRAP_OR_FAIL(
      auto schema3,
      SnapshotUtil::SchemaFor(*table_, std::string(SnapshotRef::kMainBranch)));
  EXPECT_EQ(schema3->fields().size(), initial_schema->fields().size());
}

TEST_F(SnapshotUtilTest, SchemaForBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto initial_schema, table_->schema());
  ASSERT_NE(initial_schema, nullptr);

  std::string branch = "b1";
  ICEBERG_UNWRAP_OR_FAIL(auto schema, SnapshotUtil::SchemaFor(*table_, branch));
  // Branch should return current schema (not snapshot schema)
  EXPECT_EQ(schema->fields().size(), initial_schema->fields().size());
}

TEST_F(SnapshotUtilTest, SchemaForTag) {
  // Create a tag pointing to base snapshot
  auto metadata = table_->metadata();
  std::string tag = "tag1";
  metadata->refs[tag] = std::make_shared<SnapshotRef>(
      SnapshotRef{.snapshot_id = base_snapshot_id_, .retention = SnapshotRef::Tag{}});

  ICEBERG_UNWRAP_OR_FAIL(auto initial_schema, table_->schema());
  ASSERT_NE(initial_schema, nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto schema, SnapshotUtil::SchemaFor(*table_, tag));
  // Tag should return the schema of the snapshot it points to
  // Since base snapshot has schema_id = 0, it should return the same schema
  EXPECT_EQ(schema->fields().size(), initial_schema->fields().size());
}

TEST_F(SnapshotUtilTest, SnapshotAfter) {
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_after,
                         SnapshotUtil::SnapshotAfter(*table_, base_snapshot_id_));
  EXPECT_EQ(snapshot_after->snapshot_id, main1_snapshot_id_);

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_after_main1,
                         SnapshotUtil::SnapshotAfter(*table_, main1_snapshot_id_));
  EXPECT_EQ(snapshot_after_main1->snapshot_id, main2_snapshot_id_);
}

TEST_F(SnapshotUtilTest, SnapshotIdAsOfTime) {
  // Test with timestamp before any snapshot
  auto early_timestamp = base_timestamp_ - std::chrono::milliseconds(1000);
  auto snapshot_id = SnapshotUtil::OptionalSnapshotIdAsOfTime(*table_, early_timestamp);
  EXPECT_FALSE(snapshot_id.has_value());

  // Test with timestamp at base snapshot
  auto snapshot_id1 = SnapshotUtil::OptionalSnapshotIdAsOfTime(*table_, base_timestamp_);
  ASSERT_TRUE(snapshot_id1.has_value());
  EXPECT_EQ(snapshot_id1.value(), base_snapshot_id_);

  // Test with timestamp between main1 and main2
  auto mid_timestamp = base_timestamp_ + std::chrono::milliseconds(1500);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_id2,
                         SnapshotUtil::SnapshotIdAsOfTime(*table_, mid_timestamp));
  EXPECT_EQ(snapshot_id2, main1_snapshot_id_);
}

TEST_F(SnapshotUtilTest, LatestSnapshot) {
  // Test main branch
  ICEBERG_UNWRAP_OR_FAIL(
      auto main_snapshot,
      SnapshotUtil::LatestSnapshot(*table_, std::string(SnapshotRef::kMainBranch)));
  EXPECT_EQ(main_snapshot->snapshot_id, main2_snapshot_id_);

  // Test branch
  ICEBERG_UNWRAP_OR_FAIL(auto branch_snapshot,
                         SnapshotUtil::LatestSnapshot(*table_, "b1"));
  EXPECT_EQ(branch_snapshot->snapshot_id, branch_snapshot_id_);

  // Test non-existing branch
  ICEBERG_UNWRAP_OR_FAIL(auto non_existing,
                         SnapshotUtil::LatestSnapshot(*table_, "non-existing"));
  // Should return current snapshot
  EXPECT_EQ(non_existing->snapshot_id, main2_snapshot_id_);
}

}  // namespace iceberg
