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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
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
  metadata->properties = TableProperties::default_properties();
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
  EXPECT_TRUE(metadata->metadata_log.empty());
}

TEST(TableMetadataBuilderTest, BuildFromExisting) {
  auto base = CreateBaseMetadata();
  std::string base_metadata_location = "s3://bucket/test/00010-xxx.metadata.json";
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->SetPreviousMetadataLocation(base_metadata_location);
  ASSERT_NE(builder, nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_NE(metadata, nullptr);

  EXPECT_EQ(metadata->format_version, 2);
  EXPECT_EQ(metadata->table_uuid, "test-uuid-1234");
  EXPECT_EQ(metadata->location, "s3://bucket/test");
  ASSERT_EQ(1, metadata->metadata_log.size());
  EXPECT_EQ(base_metadata_location, metadata->metadata_log[0].metadata_file);
  EXPECT_EQ(base->last_updated_ms, metadata->metadata_log[0].timestamp_ms);
}

TEST(TableMetadataBuilderTest, BuildupMetadataLog) {
  auto base = CreateBaseMetadata();
  std::string base_metadata_location = "s3://bucket/test/00010-xxx.metadata.json";
  base->metadata_log = {
      {.timestamp_ms = TimePointMs{std::chrono::milliseconds(100)},
       .metadata_file = "s3://bucket/test/00000-aaa.metadata.json"},
      {.timestamp_ms = TimePointMs{std::chrono::milliseconds(200)},
       .metadata_file = "s3://bucket/test/00001-bbb.metadata.json"},
  };

  {
    // Base metadata_log size less than max size
    base->properties.Set(TableProperties::kMetadataPreviousVersionsMax, 3);
    auto builder = TableMetadataBuilder::BuildFrom(base.get());
    builder->SetPreviousMetadataLocation(base_metadata_location);
    ASSERT_NE(builder, nullptr);
    ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
    EXPECT_EQ(3, metadata->metadata_log.size());
    EXPECT_EQ(base->metadata_log[0].metadata_file,
              metadata->metadata_log[0].metadata_file);
    EXPECT_EQ(base->metadata_log[1].metadata_file,
              metadata->metadata_log[1].metadata_file);
    EXPECT_EQ(base->last_updated_ms, metadata->metadata_log[2].timestamp_ms);
    EXPECT_EQ(base_metadata_location, metadata->metadata_log[2].metadata_file);
  }

  {
    // Base metadata_log size greater than max size
    base->properties.Set(TableProperties::kMetadataPreviousVersionsMax, 2);
    auto builder = TableMetadataBuilder::BuildFrom(base.get());
    builder->SetPreviousMetadataLocation(base_metadata_location);
    ASSERT_NE(builder, nullptr);
    ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
    EXPECT_EQ(2, metadata->metadata_log.size());
    EXPECT_EQ(base->metadata_log[1].metadata_file,
              metadata->metadata_log[0].metadata_file);
    EXPECT_EQ(base->last_updated_ms, metadata->metadata_log[1].timestamp_ms);
    EXPECT_EQ(base_metadata_location, metadata->metadata_log[1].metadata_file);
  }
}

// Test AssignUUID
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

TEST(TableMetadataBuilderTest, SetProperties) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->SetProperties({{"key1", "value1"}, {"key2", "value2"}});

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->properties.configs().size(), 2);
  EXPECT_EQ(metadata->properties.configs().at("key1"), "value1");
  EXPECT_EQ(metadata->properties.configs().at("key2"), "value2");

  // Update existing property and add new one
  builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->SetProperties({{"key1", "value1"}});
  builder->SetProperties({{"key1", "new_value1"}, {"key3", "value3"}});

  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->properties.configs().size(), 2);
  EXPECT_EQ(metadata->properties.configs().at("key1"), "new_value1");
  EXPECT_EQ(metadata->properties.configs().at("key3"), "value3");
}

TEST(TableMetadataBuilderTest, RemoveProperties) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->SetProperties({{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}});
  builder->RemoveProperties({"key2", "key4"});  // key4 does not exist

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->properties.configs().size(), 2);
  EXPECT_EQ(metadata->properties.configs().at("key1"), "value1");
  EXPECT_EQ(metadata->properties.configs().at("key3"), "value3");
}

TEST(TableMetadataBuilderTest, UpgradeFormatVersion) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(1);
  builder->UpgradeFormatVersion(2);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->format_version, 2);

  // Unsupported format version should fail
  builder = TableMetadataBuilder::BuildFromEmpty(3);
  builder->UpgradeFormatVersion(4);
  EXPECT_THAT(builder->Build(),
              HasErrorMessage("Cannot upgrade table to unsupported format version"));

  // Downgrade should fail
  builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->UpgradeFormatVersion(1);
  EXPECT_THAT(builder->Build(), HasErrorMessage("Cannot downgrade"));
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
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("Cannot find source column"));

  // 2. Invalid transform (Day transform on string type)
  builder = TableMetadataBuilder::BuildFrom(base.get());
  SortField invalid_transform(2, Transform::Day(), SortDirection::kAscending,
                              NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto order2,
                         SortOrder::Make(1, std::vector<SortField>{invalid_transform}));
  builder->AddSortOrder(std::move(order2));
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("Invalid source type"));

  // 3. Without schema
  builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID("test-uuid");
  SortField field(1, Transform::Identity(), SortDirection::kAscending, NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto order3,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{field}));
  builder->AddSortOrder(std::move(order3));
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("Schema with ID"));
}

// Test SetDefaultSortOrder
TEST(TableMetadataBuilderTest, SetDefaultSortOrderBasic) {
  auto base = CreateBaseMetadata();
  auto schema = CreateTestSchema();

  // 1. Set default sort order by SortOrder object
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  SortField field1(1, Transform::Identity(), SortDirection::kAscending,
                   NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto order1_unique,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{field1}));
  auto order1 = std::shared_ptr<SortOrder>(std::move(order1_unique));
  builder->SetDefaultSortOrder(order1);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->sort_orders.size(), 2);
  EXPECT_EQ(metadata->default_sort_order_id, 1);
  EXPECT_EQ(metadata->sort_orders[1]->order_id(), 1);

  // 2. Set default sort order by order ID
  builder = TableMetadataBuilder::BuildFrom(base.get());
  SortField field2(1, Transform::Identity(), SortDirection::kAscending,
                   NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto order2_unique,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{field2}));
  auto order2 = std::shared_ptr<SortOrder>(std::move(order2_unique));
  builder->AddSortOrder(order2);
  builder->SetDefaultSortOrder(1);
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->default_sort_order_id, 1);

  // 3. Set default sort order using -1 (last added)
  builder = TableMetadataBuilder::BuildFrom(base.get());
  SortField field3(2, Transform::Identity(), SortDirection::kDescending,
                   NullOrder::kLast);
  ICEBERG_UNWRAP_OR_FAIL(auto order3_unique,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{field3}));
  auto order3 = std::shared_ptr<SortOrder>(std::move(order3_unique));
  builder->AddSortOrder(order3);
  builder->SetDefaultSortOrder(-1);  // Use last added
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->default_sort_order_id, 1);

  // 4. Setting same order is no-op
  builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->SetDefaultSortOrder(0);
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->default_sort_order_id, 0);
}

TEST(TableMetadataBuilderTest, SetDefaultSortOrderInvalid) {
  auto base = CreateBaseMetadata();

  // Try to use -1 (last added) when no order has been added
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->SetDefaultSortOrder(-1);
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("no sort order has been added"));
}

}  // namespace iceberg
