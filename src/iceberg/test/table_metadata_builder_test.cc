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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

namespace {

// Helper function to create a simple schema for testing
std::shared_ptr<Schema> CreateTestSchema() {
  auto field1 = SchemaField::MakeRequired(1, "id", int32());
  auto field2 = SchemaField::MakeRequired(2, "data", string());
  auto field3 = SchemaField::MakeRequired(3, "ts", timestamp());
  return std::make_shared<Schema>(std::vector<SchemaField>{field1, field2, field3}, 0);
}

// Helper function to create base metadata for tests
std::unique_ptr<TableMetadata> CreateBaseMetadata() {
  auto metadata = std::make_unique<TableMetadata>();
  metadata->format_version = 2;
  metadata->table_uuid = "test-uuid-1234";
  metadata->location = "s3://bucket/test";
  metadata->last_sequence_number = 0;
  metadata->last_updated_ms = TimePointMs{std::chrono::milliseconds(1000)};
  metadata->last_column_id = 3;
  metadata->current_schema_id = 0;
  metadata->schemas.push_back(CreateTestSchema());
  metadata->default_spec_id = PartitionSpec::kInitialSpecId;
  metadata->last_partition_id = 0;
  metadata->current_snapshot_id = Snapshot::kInvalidSnapshotId;
  metadata->default_sort_order_id = SortOrder::kInitialSortOrderId;
  metadata->sort_orders.push_back(SortOrder::Unsorted());
  metadata->next_row_id = TableMetadata::kInitialRowId;
  return metadata;
}

}  // namespace

// test construction of TableMetadataBuilder
TEST(TableMetadataBuilderTest, BuildFromEmpty) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  ASSERT_NE(builder, nullptr);

  builder->AssignUUID("new-uuid-5678");

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_NE(metadata, nullptr);

  EXPECT_EQ(metadata->format_version, 2);
  EXPECT_EQ(metadata->last_sequence_number, TableMetadata::kInitialSequenceNumber);
  EXPECT_EQ(metadata->default_spec_id, PartitionSpec::kInitialSpecId);
  EXPECT_EQ(metadata->default_sort_order_id, SortOrder::kInitialSortOrderId);
  EXPECT_EQ(metadata->current_snapshot_id, Snapshot::kInvalidSnapshotId);
}

TEST(TableMetadataBuilderTest, BuildFromExisting) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  ASSERT_NE(builder, nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_NE(metadata, nullptr);

  EXPECT_EQ(metadata->format_version, 2);
  EXPECT_EQ(metadata->table_uuid, "test-uuid-1234");
  EXPECT_EQ(metadata->location, "s3://bucket/test");
}

// Test AssignUUID method
TEST(TableMetadataBuilderTest, AssignUUID) {
  // Assign UUID for new table
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID("new-uuid-5678");
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "new-uuid-5678");

  // Update existing table's UUID
  auto base = CreateBaseMetadata();
  builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->AssignUUID("updated-uuid-9999");
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "updated-uuid-9999");

  // Empty UUID should fail
  builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID("");
  ASSERT_THAT(builder->Build(), HasErrorMessage("Cannot assign empty UUID"));

  // Assign same UUID (no-op)
  base = CreateBaseMetadata();
  builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->AssignUUID("test-uuid-1234");
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "test-uuid-1234");

  // Auto-generate UUID
  builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID();
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_FALSE(metadata->table_uuid.empty());

  // Case insensitive comparison
  base = CreateBaseMetadata();
  base->table_uuid = "TEST-UUID-ABCD";
  builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->AssignUUID("test-uuid-abcd");  // Different case - should be no-op
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "TEST-UUID-ABCD");  // Original case preserved
}

// Test AddSortOrder
TEST(TableMetadataBuilderTest, AddSortOrderBasic) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  auto schema = CreateTestSchema();

  // 1. Add unsorted - should reuse existing unsorted order
  builder->AddSortOrder(SortOrder::Unsorted());
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->sort_orders.size(), 1);
  EXPECT_TRUE(metadata->sort_orders[0]->is_unsorted());

  // 2. Add basic sort order
  builder = TableMetadataBuilder::BuildFrom(base.get());
  SortField field1(1, Transform::Identity(), SortDirection::kAscending,
                   NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto order1,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{field1}));
  builder->AddSortOrder(std::move(order1));
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  ASSERT_EQ(metadata->sort_orders.size(), 2);
  EXPECT_EQ(metadata->sort_orders[1]->order_id(), 1);

  // 3. Add duplicate - should be idempotent
  builder = TableMetadataBuilder::BuildFrom(base.get());
  ICEBERG_UNWRAP_OR_FAIL(auto order2,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{field1}));
  ICEBERG_UNWRAP_OR_FAIL(auto order3,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{field1}));
  builder->AddSortOrder(std::move(order2));
  builder->AddSortOrder(std::move(order3));  // Duplicate
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  ASSERT_EQ(metadata->sort_orders.size(), 2);  // Only one added

  // 4. Add multiple different orders + verify ID reassignment
  builder = TableMetadataBuilder::BuildFrom(base.get());
  SortField field2(2, Transform::Identity(), SortDirection::kDescending,
                   NullOrder::kLast);
  // User provides ID=99, Builder should reassign to ID=1
  ICEBERG_UNWRAP_OR_FAIL(auto order4,
                         SortOrder::Make(*schema, 99, std::vector<SortField>{field1}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto order5, SortOrder::Make(*schema, 2, std::vector<SortField>{field1, field2}));
  builder->AddSortOrder(std::move(order4));
  builder->AddSortOrder(std::move(order5));
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  ASSERT_EQ(metadata->sort_orders.size(), 3);
  EXPECT_EQ(metadata->sort_orders[1]->order_id(), 1);  // Reassigned from 99
  EXPECT_EQ(metadata->sort_orders[2]->order_id(), 2);
}

TEST(TableMetadataBuilderTest, AddSortOrderInvalid) {
  auto base = CreateBaseMetadata();
  auto schema = CreateTestSchema();

  // 1. Invalid field ID
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  SortField invalid_field(999, Transform::Identity(), SortDirection::kAscending,
                          NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto order1,
                         SortOrder::Make(1, std::vector<SortField>{invalid_field}));
  builder->AddSortOrder(std::move(order1));
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kCommitFailed));
  ASSERT_THAT(builder->Build(),
              HasErrorMessage("Cannot find source column for sort field"));

  // 2. Invalid transform (Day transform on string type)
  builder = TableMetadataBuilder::BuildFrom(base.get());
  SortField invalid_transform(2, Transform::Day(), SortDirection::kAscending,
                              NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto order2,
                         SortOrder::Make(1, std::vector<SortField>{invalid_transform}));
  builder->AddSortOrder(std::move(order2));
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kCommitFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("Invalid source type"));

  // 3. Without schema
  builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID("test-uuid");
  SortField field(1, Transform::Identity(), SortDirection::kAscending, NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto order3,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{field}));
  builder->AddSortOrder(std::move(order3));
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kCommitFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("Cannot find current schema"));
}

// Test applying TableUpdate to builder
TEST(TableMetadataBuilderTest, ApplyUpdate) {
  // Use base metadata with schema for all update tests
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Apply AssignUUID update
  table::AssignUUID uuid_update("apply-uuid");
  uuid_update.ApplyTo(*builder);

  // Apply AddSortOrder update
  auto schema = CreateTestSchema();
  SortField sort_field(1, Transform::Identity(), SortDirection::kAscending,
                       NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order_unique,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{sort_field}));
  auto sort_order = std::shared_ptr<SortOrder>(std::move(sort_order_unique));

  table::AddSortOrder sort_order_update(sort_order);
  sort_order_update.ApplyTo(*builder);

  // TODO(Li Feiyang): Apply more build methods once they are implemented

  // Verify all updates were applied
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "apply-uuid");
  ASSERT_EQ(metadata->sort_orders.size(), 2);
  EXPECT_EQ(metadata->sort_orders[1]->order_id(), 1);
}

}  // namespace iceberg
