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
  metadata->partition_specs.push_back(PartitionSpec::Unpartitioned());
  metadata->default_spec_id = PartitionSpec::kInitialSpecId;
  metadata->last_partition_id = 0;
  metadata->current_snapshot_id = Snapshot::kInvalidSnapshotId;
  metadata->default_sort_order_id = SortOrder::kUnsortedOrderId;
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

  auto schema = CreateTestSchema();
  builder->SetCurrentSchema(schema, schema->HighestFieldId().value());
  builder->SetDefaultSortOrder(SortOrder::Unsorted());
  builder->SetDefaultPartitionSpec(PartitionSpec::Unpartitioned());
  builder->AssignUUID("new-uuid-5678");

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_NE(metadata, nullptr);

  EXPECT_EQ(metadata->format_version, 2);
  EXPECT_EQ(metadata->last_sequence_number, TableMetadata::kInitialSequenceNumber);
  EXPECT_EQ(metadata->default_spec_id, PartitionSpec::kInitialSpecId);
  EXPECT_EQ(metadata->default_sort_order_id, SortOrder::kUnsortedOrderId);
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
  auto schema = CreateTestSchema();
  builder->SetCurrentSchema(schema, schema->HighestFieldId().value());
  builder->SetDefaultSortOrder(SortOrder::Unsorted());
  builder->SetDefaultPartitionSpec(PartitionSpec::Unpartitioned());
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
  builder->SetCurrentSchema(schema, schema->HighestFieldId().value());
  builder->SetDefaultSortOrder(SortOrder::Unsorted());
  builder->SetDefaultPartitionSpec(PartitionSpec::Unpartitioned());
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
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->SetProperties({{"key1", "value1"}, {"key2", "value2"}});

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->properties.configs().size(), 2);
  EXPECT_EQ(metadata->properties.configs().at("key1"), "value1");
  EXPECT_EQ(metadata->properties.configs().at("key2"), "value2");

  // Update existing property and add new one
  builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->SetProperties({{"key1", "value1"}});
  builder->SetProperties({{"key1", "new_value1"}, {"key3", "value3"}});

  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->properties.configs().size(), 2);
  EXPECT_EQ(metadata->properties.configs().at("key1"), "new_value1");
  EXPECT_EQ(metadata->properties.configs().at("key3"), "value3");
}

TEST(TableMetadataBuilderTest, RemoveProperties) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->SetProperties({{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}});
  builder->RemoveProperties({"key2", "key4"});  // key4 does not exist

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->properties.configs().size(), 2);
  EXPECT_EQ(metadata->properties.configs().at("key1"), "value1");
  EXPECT_EQ(metadata->properties.configs().at("key3"), "value3");
}

TEST(TableMetadataBuilderTest, UpgradeFormatVersion) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(1);
  auto schema = CreateTestSchema();
  builder->SetCurrentSchema(schema, schema->HighestFieldId().value());
  builder->SetDefaultSortOrder(SortOrder::Unsorted());
  builder->SetDefaultPartitionSpec(PartitionSpec::Unpartitioned());
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

// Test AddSchema
TEST(TableMetadataBuilderTest, AddSchemaBasic) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // 1. Add a new schema
  auto field1 = SchemaField::MakeRequired(4, "new_field1", int64());
  auto field2 = SchemaField::MakeRequired(5, "new_field2", float64());
  auto new_schema = std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 1);
  builder->AddSchema(new_schema);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 1);
  EXPECT_EQ(metadata->last_column_id, 5);

  // 2. Add duplicate schema - should be idempotent
  builder = TableMetadataBuilder::BuildFrom(base.get());
  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 1);
  auto schema2 = std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 2);
  builder->AddSchema(schema1);
  builder->AddSchema(schema2);  // Same fields, should reuse ID
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);  // Only one new schema added

  // 3. Add multiple different schemas
  builder = TableMetadataBuilder::BuildFrom(base.get());
  auto field3 = SchemaField::MakeRequired(6, "field3", string());
  auto schema3 = std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 1);
  auto schema4 = std::make_shared<Schema>(std::vector<SchemaField>{field1, field3}, 2);
  builder->AddSchema(schema3);
  builder->AddSchema(schema4);
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 3);
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 1);
  EXPECT_EQ(metadata->schemas[2]->schema_id().value(), 2);
  EXPECT_EQ(metadata->last_column_id, 6);
}

TEST(TableMetadataBuilderTest, AddSchemaWithDuplicateColumnIds) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add schema with column ID that already exists - should still work
  auto field1 =
      SchemaField::MakeRequired(2, "duplicate_id", int64());  // ID 2 already exists
  auto schema_with_duplicate =
      std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);
  builder->AddSchema(schema_with_duplicate);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  // Should work - AddSchema automatically uses max(existing, highest_in_schema)
  ASSERT_EQ(metadata->schemas.size(), 2);
  EXPECT_EQ(metadata->last_column_id, 3);  // Should remain 3 (from base metadata)
}

TEST(TableMetadataBuilderTest, AddSchemaWithHigherColumnIds) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add schema with higher column IDs
  auto field1 = SchemaField::MakeRequired(10, "high_id1", int64());
  auto field2 = SchemaField::MakeRequired(15, "high_id2", string());
  auto new_schema = std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 1);
  builder->AddSchema(new_schema);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);
  EXPECT_EQ(metadata->last_column_id, 15);  // Should be updated to highest field ID
}

TEST(TableMetadataBuilderTest, AddSchemaEmptyFields) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add schema with no fields
  auto empty_schema = std::make_shared<Schema>(std::vector<SchemaField>{}, 1);
  builder->AddSchema(empty_schema);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);
  EXPECT_EQ(metadata->last_column_id, 3);  // Should remain unchanged
}

TEST(TableMetadataBuilderTest, AddSchemaIdempotent) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add the same schema twice
  auto field1 = SchemaField::MakeRequired(4, "field1", int64());
  auto field2 = SchemaField::MakeRequired(5, "field2", string());
  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 1);
  auto schema2 = std::make_shared<Schema>(std::vector<SchemaField>{field1, field2},
                                          2);  // Different ID but same fields

  builder->AddSchema(schema1);
  builder->AddSchema(schema2);  // Should reuse existing schema

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);  // Only one new schema should be added
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 1);
}

TEST(TableMetadataBuilderTest, AddSchemaMultipleDifferent) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add multiple different schemas
  auto field1 = SchemaField::MakeRequired(4, "field1", int64());
  auto field2 = SchemaField::MakeRequired(5, "field2", string());
  auto field3 = SchemaField::MakeRequired(6, "field3", float64());

  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);
  auto schema2 = std::make_shared<Schema>(std::vector<SchemaField>{field2}, 2);
  auto schema3 = std::make_shared<Schema>(std::vector<SchemaField>{field3}, 3);

  builder->AddSchema(schema1);
  builder->AddSchema(schema2);
  builder->AddSchema(schema3);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 4);  // Original + 3 new
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 1);
  EXPECT_EQ(metadata->schemas[2]->schema_id().value(), 2);
  EXPECT_EQ(metadata->schemas[3]->schema_id().value(), 3);
  EXPECT_EQ(metadata->last_column_id, 6);
}

// Test SetCurrentSchema
TEST(TableMetadataBuilderTest, SetCurrentSchemaBasic) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // 1. Set current schema by Schema object with explicit last_column_id
  auto field1 = SchemaField::MakeRequired(4, "new_field", int64());
  auto new_schema = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);
  builder->SetCurrentSchema(new_schema, 4);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);
  EXPECT_EQ(metadata->current_schema_id.value(), 1);
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 1);
  EXPECT_EQ(metadata->last_column_id, 4);

  // 2. Set current schema by schema ID
  builder = TableMetadataBuilder::BuildFrom(base.get());
  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);
  builder->AddSchema(schema1);
  builder->SetCurrentSchema(1);
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->current_schema_id.value(), 1);

  // 3. Set current schema using -1 (last added)
  builder = TableMetadataBuilder::BuildFrom(base.get());
  auto field2 = SchemaField::MakeRequired(5, "another_field", float64());
  auto schema2 = std::make_shared<Schema>(std::vector<SchemaField>{field2}, 2);
  builder->AddSchema(schema2);
  builder->SetCurrentSchema(-1);  // Use last added
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->current_schema_id.value(), 1);

  // 4. Setting same schema is no-op
  builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->SetCurrentSchema(0);
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->current_schema_id.value(), 0);
}

TEST(TableMetadataBuilderTest, SetCurrentSchemaWithInvalidLastColumnId) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Try to set current schema with last_column_id lower than existing
  auto field1 = SchemaField::MakeRequired(4, "new_field", int64());
  auto new_schema = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);
  builder->SetCurrentSchema(new_schema, 2);  // 2 < 3 (existing last_column_id)
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("Invalid last column ID"));
}

TEST(TableMetadataBuilderTest, SetCurrentSchemaUpdatesLastColumnId) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Set current schema with higher last_column_id
  auto field1 = SchemaField::MakeRequired(4, "new_field1", int64());
  auto field2 = SchemaField::MakeRequired(8, "new_field2", string());
  auto new_schema = std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 1);
  builder->SetCurrentSchema(new_schema, 10);  // Higher than field IDs
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->current_schema_id.value(), 1);
  EXPECT_EQ(metadata->last_column_id, 10);
}

// Additional comprehensive tests for AddSchema
TEST(TableMetadataBuilderTest, AddSchemaWithNestedTypes) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add schema with nested struct type
  auto inner_field1 = SchemaField::MakeRequired(10, "inner_id", int64());
  auto inner_field2 = SchemaField::MakeRequired(11, "inner_name", string());
  auto struct_type =
      std::make_shared<StructType>(std::vector<SchemaField>{inner_field1, inner_field2});
  auto struct_field = SchemaField::MakeRequired(4, "nested_struct", struct_type);

  auto new_schema = std::make_shared<Schema>(std::vector<SchemaField>{struct_field}, 1);
  builder->AddSchema(new_schema);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());

  ASSERT_EQ(metadata->schemas.size(), 2);
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 1);
  EXPECT_EQ(metadata->last_column_id, 11);  // Should be highest nested field ID
}

TEST(TableMetadataBuilderTest, AddSchemaWithListAndMapTypes) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add schema with list type
  auto list_type = std::make_shared<ListType>(10, int32(), true);
  auto list_field = SchemaField::MakeRequired(4, "int_list", list_type);

  // Add schema with map type
  auto key_field = SchemaField::MakeRequired(11, "key", string());
  auto value_field = SchemaField::MakeRequired(12, "value", int64());
  auto map_type = std::make_shared<MapType>(key_field, value_field);
  auto map_field = SchemaField::MakeRequired(5, "string_int_map", map_type);

  auto new_schema =
      std::make_shared<Schema>(std::vector<SchemaField>{list_field, map_field}, 1);
  builder->AddSchema(new_schema);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());

  ASSERT_EQ(metadata->schemas.size(), 2);
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 1);
  EXPECT_EQ(metadata->last_column_id, 12);  // Should be highest field ID including nested
}

TEST(TableMetadataBuilderTest, AddSchemaSequentialIds) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add multiple schemas and verify ID assignment
  auto field1 = SchemaField::MakeRequired(4, "field1", int64());
  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);

  auto field2 = SchemaField::MakeRequired(5, "field2", string());
  auto schema2 = std::make_shared<Schema>(std::vector<SchemaField>{field2}, 2);

  auto field3 = SchemaField::MakeRequired(6, "field3", float64());
  auto schema3 = std::make_shared<Schema>(std::vector<SchemaField>{field3}, 3);

  builder->AddSchema(schema1);
  builder->AddSchema(schema2);
  builder->AddSchema(schema3);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 4);  // Original + 3 new

  // Verify sequential schema IDs
  EXPECT_EQ(metadata->schemas[0]->schema_id().value(), 0);  // Original
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 1);
  EXPECT_EQ(metadata->schemas[2]->schema_id().value(), 2);
  EXPECT_EQ(metadata->schemas[3]->schema_id().value(), 3);
}

TEST(TableMetadataBuilderTest, AddSchemaWithOptionalFields) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add schema with mix of required and optional fields
  auto required_field = SchemaField::MakeRequired(4, "required_field", int64());
  auto optional_field = SchemaField::MakeOptional(5, "optional_field", string());

  auto new_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{required_field, optional_field}, 1);
  builder->AddSchema(new_schema);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());

  ASSERT_EQ(metadata->schemas.size(), 2);
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 1);

  // Verify field properties
  const auto& fields = metadata->schemas[1]->fields();
  ASSERT_EQ(fields.size(), 2);
  EXPECT_FALSE(fields[0].optional());  // required_field
  EXPECT_TRUE(fields[1].optional());   // optional_field
}

// Additional comprehensive tests for SetCurrentSchema
TEST(TableMetadataBuilderTest, SetCurrentSchemaInvalidId) {
  auto base = CreateBaseMetadata();

  // 1. Try to use -1 (last added) when no schema has been added
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->SetCurrentSchema(-1);
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("no schema has been added"));

  // 2. Try to set non-existent schema ID
  builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->SetCurrentSchema(999);
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("unknown schema: 999"));
}

TEST(TableMetadataBuilderTest, SetCurrentSchemaWithPartitionSpecRebuild) {
  auto base = CreateBaseMetadata();

  // Add a partition spec to the base metadata
  auto schema = CreateTestSchema();
  ICEBERG_UNWRAP_OR_FAIL(
      auto spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          {PartitionField(1, 1000, "id_bucket", Transform::Bucket(16))},
                          1000));
  base->partition_specs.push_back(std::move(spec));

  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add and set a new schema - should rebuild partition specs
  auto field1 = SchemaField::MakeRequired(4, "new_id", int64());
  auto field2 = SchemaField::MakeRequired(5, "new_data", string());
  auto new_schema = std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 1);
  builder->SetCurrentSchema(new_schema, 5);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());

  // Verify schema was set
  EXPECT_EQ(metadata->current_schema_id.value(), 1);
  EXPECT_EQ(metadata->last_column_id, 5);

  // Verify partition specs were rebuilt (they should still exist)
  ASSERT_EQ(metadata->partition_specs.size(), 2);
}

TEST(TableMetadataBuilderTest, SetCurrentSchemaMultipleOperations) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add multiple schemas
  auto field1 = SchemaField::MakeRequired(4, "field1", int64());
  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);

  auto field2 = SchemaField::MakeRequired(5, "field2", string());
  auto schema2 = std::make_shared<Schema>(std::vector<SchemaField>{field2}, 2);

  builder->AddSchema(schema1);
  builder->AddSchema(schema2);

  // Set current to first added schema
  builder->SetCurrentSchema(1);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->current_schema_id.value(), 1);

  // Change current to second schema
  builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->SetCurrentSchema(2);
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->current_schema_id.value(), 2);

  // Change back to original schema
  builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->SetCurrentSchema(0);
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->current_schema_id.value(), 0);
}

TEST(TableMetadataBuilderTest, SetCurrentSchemaLastAddedTracking) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add multiple schemas
  auto field1 = SchemaField::MakeRequired(4, "field1", int64());
  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);

  auto field2 = SchemaField::MakeRequired(5, "field2", string());
  auto schema2 = std::make_shared<Schema>(std::vector<SchemaField>{field2}, 2);

  builder->AddSchema(schema1);
  builder->AddSchema(schema2);  // This becomes the "last added"

  // Use -1 to set current to last added schema
  builder->SetCurrentSchema(-1);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->current_schema_id.value(), 2);  // Should be schema2
}

TEST(TableMetadataBuilderTest, AddSchemaAndSetCurrentCombined) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add a schema and immediately set it as current
  auto field1 = SchemaField::MakeRequired(4, "new_field", int64());
  auto new_schema = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);

  builder->AddSchema(new_schema);
  builder->SetCurrentSchema(-1);  // Set to last added

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);
  EXPECT_EQ(metadata->current_schema_id.value(), 1);
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 1);
  EXPECT_EQ(metadata->last_column_id, 4);
}

TEST(TableMetadataBuilderTest, SetCurrentSchemaInvalid) {
  auto base = CreateBaseMetadata();

  // 1. Try to use -1 (last added) when no schema has been added
  auto builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->SetCurrentSchema(-1);
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("no schema has been added"));

  // 2. Try to set non-existent schema ID
  builder = TableMetadataBuilder::BuildFrom(base.get());
  builder->SetCurrentSchema(999);
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("unknown schema: 999"));
}

// Test schema evolution: SetCurrentSchema should rebuild partition specs and sort orders
TEST(TableMetadataBuilderTest, SetCurrentSchemaRebuildsSpecsAndOrders) {
  auto base = CreateBaseMetadata();

  // Add a partition spec to the base metadata
  auto schema = CreateTestSchema();
  ICEBERG_UNWRAP_OR_FAIL(
      auto spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          {PartitionField(1, 1000, "id_bucket", Transform::Bucket(16))},
                          1000));
  base->partition_specs.push_back(std::move(spec));

  // Add a sort order to the base metadata
  SortField sort_field(1, Transform::Identity(), SortDirection::kAscending,
                       NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto order,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{sort_field}));
  base->sort_orders.push_back(std::move(order));
  base->default_sort_order_id = 1;

  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add and set a new schema
  std::vector<SchemaField> new_fields{schema->fields().begin(), schema->fields().end()};
  new_fields.push_back(SchemaField::MakeRequired(4, "new_id", int64()));
  new_fields.push_back(SchemaField::MakeRequired(5, "new_data", string()));
  auto new_schema = std::make_shared<Schema>(std::move(new_fields), 1);
  builder->SetCurrentSchema(new_schema, 5);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());

  // Verify schema was set
  EXPECT_EQ(metadata->current_schema_id.value(), 1);

  // Verify partition specs were rebuilt (they should still exist)
  ASSERT_EQ(metadata->partition_specs.size(), 2);

  // Verify sort orders were rebuilt (they should still exist)
  ASSERT_EQ(metadata->sort_orders.size(), 2);
}

// Test RemoveSchemas
TEST(TableMetadataBuilderTest, RemoveSchemasBasic) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add multiple schemas
  auto field1 = SchemaField::MakeRequired(4, "field1", int64());
  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);
  auto field2 = SchemaField::MakeRequired(5, "field2", float64());
  auto schema2 = std::make_shared<Schema>(std::vector<SchemaField>{field2}, 2);
  auto field3 = SchemaField::MakeRequired(6, "field3", string());
  auto schema3 = std::make_shared<Schema>(std::vector<SchemaField>{field3}, 3);

  builder->AddSchema(schema1);
  builder->AddSchema(schema2);
  builder->AddSchema(schema3);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 4);  // Original + 3 new

  // Remove one schema
  builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->RemoveSchemas({1});
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 3);
  EXPECT_EQ(metadata->schemas[0]->schema_id().value(), 0);
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 2);
  EXPECT_EQ(metadata->schemas[2]->schema_id().value(), 3);

  // Remove multiple schemas
  builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->RemoveSchemas({2, 3});
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 1);
  EXPECT_EQ(metadata->schemas[0]->schema_id().value(), Schema::kInitialSchemaId);
}

TEST(TableMetadataBuilderTest, RemoveSchemasCannotRemoveCurrent) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add a new schema
  auto field1 = SchemaField::MakeRequired(4, "field1", int64());
  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);
  builder->AddSchema(schema1);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);
  EXPECT_EQ(metadata->current_schema_id.value(), 0);

  // Try to remove current schema (ID 0)
  builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->RemoveSchemas({0});
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("Cannot remove current schema: 0"));

  // Try to remove current schema along with others
  builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->RemoveSchemas({0, 1});
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("Cannot remove current schema: 0"));
}

TEST(TableMetadataBuilderTest, RemoveSchemasNonExistent) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add one schema
  auto field1 = SchemaField::MakeRequired(4, "field1", int64());
  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);
  builder->AddSchema(schema1);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);

  // Try to remove non-existent schema - should be no-op
  builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->RemoveSchemas({999});
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);

  // Remove mix of existent and non-existent
  builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->RemoveSchemas({1, 999, 888});
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 1);
  EXPECT_EQ(metadata->schemas[0]->schema_id().value(), Schema::kInitialSchemaId);
}

TEST(TableMetadataBuilderTest, RemoveSchemasEmptySet) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add a schema
  auto field1 = SchemaField::MakeRequired(4, "field1", int64());
  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);
  builder->AddSchema(schema1);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);

  // Remove empty set - should be no-op
  builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->RemoveSchemas({});
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);
}

TEST(TableMetadataBuilderTest, RemoveSchemasAfterSchemaChange) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Add multiple schemas
  auto field1 = SchemaField::MakeRequired(4, "field1", int64());
  auto schema1 = std::make_shared<Schema>(std::vector<SchemaField>{field1}, 1);
  auto field2 = SchemaField::MakeRequired(5, "field2", float64());
  auto schema2 = std::make_shared<Schema>(std::vector<SchemaField>{field2}, 2);

  builder->AddSchema(schema1);
  builder->AddSchema(schema2);
  builder->SetCurrentSchema(1);  // Set schema1 as current

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 3);
  EXPECT_EQ(metadata->current_schema_id.value(), 1);

  // Now remove the old current schema (ID 0)
  builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->RemoveSchemas({0});
  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  ASSERT_EQ(metadata->schemas.size(), 2);
  EXPECT_EQ(metadata->current_schema_id.value(), 1);
  EXPECT_EQ(metadata->schemas[0]->schema_id().value(), 1);
  EXPECT_EQ(metadata->schemas[1]->schema_id().value(), 2);

  // Cannot remove the new current schema
  builder = TableMetadataBuilder::BuildFrom(metadata.get());
  builder->RemoveSchemas({1});
  ASSERT_THAT(builder->Build(), IsError(ErrorKind::kValidationFailed));
  ASSERT_THAT(builder->Build(), HasErrorMessage("Cannot remove current schema: 1"));
}

}  // namespace iceberg
