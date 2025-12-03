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

#include "iceberg/table_update.h"

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
#include "iceberg/table_requirement.h"
#include "iceberg/table_requirements.h"
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

// Helper function to generate requirements
std::vector<std::unique_ptr<TableRequirement>> GenerateRequirements(
    const TableUpdate& update, const TableMetadata* base) {
  TableUpdateContext context(base, /*is_replace=*/false);
  update.GenerateRequirements(context), IsOk();
  auto requirements = context.Build();
  EXPECT_THAT(requirements, IsOk());
  return std::move(requirements.value());
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

// Test AssignUUID
TEST(TableUpdateTest, AssignUUIDApplyUpdate) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Apply AssignUUID update
  table::AssignUUID uuid_update("apply-uuid");
  uuid_update.ApplyTo(*builder);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "apply-uuid");
}

TEST(TableUpdateTest, AssignUUIDGenerateRequirements) {
  table::AssignUUID update("new-uuid");

  // New table - no requirements (AssignUUID doesn't generate requirements)
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - no requirements
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  EXPECT_TRUE(existing_table_reqs.empty());
}

// Test UpgradeFormatVersion
TEST(TableUpdateTest, UpgradeFormatVersionGenerateRequirements) {
  table::UpgradeFormatVersion update(3);

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - no requirements
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  EXPECT_TRUE(existing_table_reqs.empty());
}

// Test AddSchema
TEST(TableUpdateTest, AddSchemaGenerateRequirements) {
  auto new_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(4, "new_col", string())}, 3);
  table::AddSchema update(new_schema, 3);

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - generates AssertLastAssignedFieldId requirement
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  ASSERT_EQ(existing_table_reqs.size(), 1);

  auto* assert_id =
      dynamic_cast<table::AssertLastAssignedFieldId*>(existing_table_reqs[0].get());
  ASSERT_NE(assert_id, nullptr);
  EXPECT_EQ(assert_id->last_assigned_field_id(), base->last_column_id);
}

// Test SetCurrentSchema
TEST(TableUpdateTest, SetCurrentSchemaGenerateRequirements) {
  table::SetCurrentSchema update(1);

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - generates AssertCurrentSchemaID requirement
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  ASSERT_EQ(existing_table_reqs.size(), 1);

  auto* assert_id =
      dynamic_cast<table::AssertCurrentSchemaID*>(existing_table_reqs[0].get());
  ASSERT_NE(assert_id, nullptr);
  EXPECT_EQ(assert_id->schema_id(), base->current_schema_id);
}

// Test AddPartitionSpec
TEST(TableUpdateTest, AddPartitionSpecGenerateRequirements) {
  PartitionField partition_field(1, 1, "id_identity", Transform::Identity());
  ICEBERG_UNWRAP_OR_FAIL(auto spec_unique, PartitionSpec::Make(1, {partition_field}));
  auto spec = std::shared_ptr<PartitionSpec>(std::move(spec_unique));
  table::AddPartitionSpec update(spec);

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - generates AssertLastAssignedPartitionId requirement
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  ASSERT_EQ(existing_table_reqs.size(), 1);

  auto* assert_id =
      dynamic_cast<table::AssertLastAssignedPartitionId*>(existing_table_reqs[0].get());
  ASSERT_NE(assert_id, nullptr);
  EXPECT_EQ(assert_id->last_assigned_partition_id(), base->last_partition_id);
}

// Test SetDefaultPartitionSpec
TEST(TableUpdateTest, SetDefaultPartitionSpecGenerateRequirements) {
  table::SetDefaultPartitionSpec update(1);

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - generates AssertDefaultSpecID requirement
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  ASSERT_EQ(existing_table_reqs.size(), 1);

  auto* assert_id =
      dynamic_cast<table::AssertDefaultSpecID*>(existing_table_reqs[0].get());
  ASSERT_NE(assert_id, nullptr);
  EXPECT_EQ(assert_id->spec_id(), base->default_spec_id);
}

// Test RemovePartitionSpecs
TEST(TableUpdateTest, RemovePartitionSpecsGenerateRequirements) {
  table::RemovePartitionSpecs update(std::vector<int>{1});

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table without snapshot - one requirement
  auto base = CreateBaseMetadata();
  auto existing_table_reqs_no_snapshot = GenerateRequirements(update, base.get());
  ASSERT_EQ(existing_table_reqs_no_snapshot.size(), 1);
  auto* assert_spec_id =
      dynamic_cast<table::AssertDefaultSpecID*>(existing_table_reqs_no_snapshot[0].get());
  ASSERT_NE(assert_spec_id, nullptr);
  EXPECT_EQ(assert_spec_id->spec_id(), base->default_spec_id);

  // Existing table with a non-main branch - two requirements
  base->current_snapshot_id = 12345;
  auto snapshot = std::make_shared<Snapshot>();
  snapshot->snapshot_id = 12345;
  snapshot->sequence_number = 1;
  base->snapshots.push_back(snapshot);
  const std::string dev_branch = "dev";
  base->refs.emplace(dev_branch,
                     std::make_shared<SnapshotRef>(SnapshotRef{
                         .snapshot_id = 12345, .retention = SnapshotRef::Branch{}}));

  auto existing_table_reqs_with_snapshot = GenerateRequirements(update, base.get());
  ASSERT_EQ(existing_table_reqs_with_snapshot.size(), 2);

  auto* req1 =
      dynamic_cast<table::AssertDefaultSpecID*>(existing_table_reqs_no_snapshot[0].get());
  EXPECT_EQ(req1->spec_id(), base->default_spec_id);
  auto* req2 = dynamic_cast<table::AssertRefSnapshotID*>(
      existing_table_reqs_with_snapshot[1].get());
  EXPECT_EQ(req2->ref_name(), dev_branch);
  EXPECT_TRUE(req2->snapshot_id().has_value());
  EXPECT_EQ(req2->snapshot_id().value(), base->current_snapshot_id);
}

// Test RemoveSchemas
TEST(TableUpdateTest, RemoveSchemasGenerateRequirements) {
  table::RemoveSchemas update({1});

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table without snapshot - one requirement
  auto base = CreateBaseMetadata();
  auto existing_table_reqs_no_snapshot = GenerateRequirements(update, base.get());
  ASSERT_EQ(existing_table_reqs_no_snapshot.size(), 1);
  auto* assert_schema_id = dynamic_cast<table::AssertCurrentSchemaID*>(
      existing_table_reqs_no_snapshot[0].get());
  ASSERT_NE(assert_schema_id, nullptr);
  EXPECT_EQ(assert_schema_id->schema_id(), base->current_schema_id);

  // Existing table with a non-main branch - two requirements
  base->current_snapshot_id = 12345;
  auto snapshot = std::make_shared<Snapshot>();
  snapshot->snapshot_id = 12345;
  snapshot->sequence_number = 1;
  base->snapshots.push_back(snapshot);
  const std::string dev_branch = "dev";
  base->refs.emplace(dev_branch,
                     std::make_shared<SnapshotRef>(SnapshotRef{
                         .snapshot_id = 12345, .retention = SnapshotRef::Branch{}}));

  auto existing_table_reqs_with_snapshot = GenerateRequirements(update, base.get());
  ASSERT_EQ(existing_table_reqs_with_snapshot.size(), 2);

  // The order is not guaranteed.
  bool found_schema_id_req = false;
  bool found_branch_req = false;
  for (const auto& req : existing_table_reqs_with_snapshot) {
    if (auto* r = dynamic_cast<table::AssertCurrentSchemaID*>(req.get())) {
      EXPECT_EQ(r->schema_id(), base->current_schema_id);
      found_schema_id_req = true;
    } else if (auto* r = dynamic_cast<table::AssertRefSnapshotID*>(req.get())) {
      EXPECT_EQ(r->ref_name(), dev_branch);
      EXPECT_TRUE(r->snapshot_id().has_value());
      EXPECT_EQ(r->snapshot_id().value(), base->current_snapshot_id);
      found_branch_req = true;
    }
  }
  EXPECT_TRUE(found_schema_id_req);
  EXPECT_TRUE(found_branch_req);
}

// Test AddSortOrder
TEST(TableUpdateTest, ApplyAddSortOrderUpdate) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Apply AddSortOrder update
  auto schema = CreateTestSchema();
  SortField sort_field(1, Transform::Identity(), SortDirection::kAscending,
                       NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order_unique,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{sort_field}));
  auto sort_order = std::shared_ptr<SortOrder>(std::move(sort_order_unique));
  table::AddSortOrder sort_order_update(sort_order);
  sort_order_update.ApplyTo(*builder);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_EQ(metadata->sort_orders.size(), 2);
  EXPECT_EQ(metadata->sort_orders[1]->order_id(), 1);
}

TEST(TableUpdateTest, AddSortOrderGenerateRequirements) {
  auto schema = CreateTestSchema();
  SortField sort_field(1, Transform::Identity(), SortDirection::kAscending,
                       NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(std::shared_ptr<SortOrder> sort_order,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{sort_field}));
  table::AddSortOrder update(sort_order);

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - no requirements
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  EXPECT_TRUE(existing_table_reqs.empty());
}

// Test SetDefaultSortOrder
TEST(TableUpdateTest, ApplySetDefaultSortOrderUpdate) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  auto schema = CreateTestSchema();
  SortField sort_field(1, Transform::Identity(), SortDirection::kAscending,
                       NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order_unique,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{sort_field}));
  auto sort_order = std::shared_ptr<SortOrder>(std::move(sort_order_unique));
  builder->AddSortOrder(sort_order);

  table::SetDefaultSortOrder set_default_update(1);
  set_default_update.ApplyTo(*builder);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->default_sort_order_id, 1);
}

TEST(TableUpdateTest, SetDefaultSortOrderGenerateRequirements) {
  table::SetDefaultSortOrder update(1);

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - generates AssertDefaultSortOrderID requirement
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  ASSERT_EQ(existing_table_reqs.size(), 1);

  // Verify it's the correct requirement type
  auto* assert_sort_order =
      dynamic_cast<table::AssertDefaultSortOrderID*>(existing_table_reqs[0].get());
  ASSERT_NE(assert_sort_order, nullptr);
  EXPECT_EQ(assert_sort_order->sort_order_id(), base->default_sort_order_id);
}

// Test AddSnapshot
TEST(TableUpdateTest, AddSnapshotGenerateRequirements) {
  auto snapshot = std::make_shared<Snapshot>();
  table::AddSnapshot update(snapshot);

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - no requirements
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  EXPECT_TRUE(existing_table_reqs.empty());
}

// Test RemoveSnapshotRef
TEST(TableUpdateTest, RemoveSnapshotRefGenerateRequirements) {
  table::RemoveSnapshotRef update("my-branch");

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - no requirements
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  EXPECT_TRUE(existing_table_reqs.empty());
}

// Test SetProperties
TEST(TableUpdateTest, SetPropertiesGenerateRequirements) {
  table::SetProperties update({{"key", "value"}});

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - no requirements
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  EXPECT_TRUE(existing_table_reqs.empty());
}

// Test RemoveProperties
TEST(TableUpdateTest, RemovePropertiesGenerateRequirements) {
  table::RemoveProperties update({"key"});

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - no requirements
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  EXPECT_TRUE(existing_table_reqs.empty());
}

// Test SetLocation
TEST(TableUpdateTest, SetLocationGenerateRequirements) {
  table::SetLocation update("s3://new/location");

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - no requirements
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  EXPECT_TRUE(existing_table_reqs.empty());
}

}  // namespace iceberg
