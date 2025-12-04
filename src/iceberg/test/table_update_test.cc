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

#include <functional>
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

// Parameter struct for testing GenerateRequirements behavior
struct GenerateRequirementsTestParam {
  std::string test_name;
  std::function<std::unique_ptr<TableUpdate>()> update_factory;
  // Expected number of requirements for existing table (new table always expects 0)
  size_t expected_existing_table_count;
  // Optional validator function to check the generated requirements
  std::function<void(const std::vector<std::unique_ptr<TableRequirement>>&,
                     const TableMetadata*)>
      validator;
};

class GenerateRequirementsTest
    : public ::testing::TestWithParam<GenerateRequirementsTestParam> {};

TEST_P(GenerateRequirementsTest, GeneratesExpectedRequirements) {
  const auto& param = GetParam();
  auto update = param.update_factory();

  // New table - always no requirements
  auto new_table_reqs = GenerateRequirements(*update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - check expected count
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(*update, base.get());
  ASSERT_EQ(existing_table_reqs.size(), param.expected_existing_table_count);

  // Validate the requirements if validator is provided
  if (param.validator) {
    param.validator(existing_table_reqs, base.get());
  }
}

INSTANTIATE_TEST_SUITE_P(
    TableUpdateGenerateRequirements, GenerateRequirementsTest,
    ::testing::Values(
        // Updates that generate no requirements
        GenerateRequirementsTestParam{
            .test_name = "AssignUUID",
            .update_factory =
                [] { return std::make_unique<table::AssignUUID>("new-uuid"); },
            .expected_existing_table_count = 0,
            .validator = nullptr},
        GenerateRequirementsTestParam{
            .test_name = "UpgradeFormatVersion",
            .update_factory =
                [] { return std::make_unique<table::UpgradeFormatVersion>(3); },
            .expected_existing_table_count = 0,
            .validator = nullptr},
        GenerateRequirementsTestParam{
            .test_name = "AddSortOrder",
            .update_factory =
                [] {
                  auto schema = CreateTestSchema();
                  SortField sort_field(1, Transform::Identity(),
                                       SortDirection::kAscending, NullOrder::kFirst);
                  auto sort_order =
                      SortOrder::Make(*schema, 1, std::vector<SortField>{sort_field})
                          .value();
                  return std::make_unique<table::AddSortOrder>(std::move(sort_order));
                },
            .expected_existing_table_count = 0,
            .validator = nullptr},
        GenerateRequirementsTestParam{.test_name = "AddSnapshot",
                                      .update_factory =
                                          [] {
                                            auto snapshot = std::make_shared<Snapshot>();
                                            return std::make_unique<table::AddSnapshot>(
                                                snapshot);
                                          },
                                      .expected_existing_table_count = 0,
                                      .validator = nullptr},
        GenerateRequirementsTestParam{
            .test_name = "RemoveSnapshotRef",
            .update_factory =
                [] { return std::make_unique<table::RemoveSnapshotRef>("my-branch"); },
            .expected_existing_table_count = 0,
            .validator = nullptr},
        GenerateRequirementsTestParam{
            .test_name = "SetProperties",
            .update_factory =
                [] {
                  return std::make_unique<table::SetProperties>(
                      std::unordered_map<std::string, std::string>{{"key", "value"}});
                },
            .expected_existing_table_count = 0,
            .validator = nullptr},
        GenerateRequirementsTestParam{
            .test_name = "RemoveProperties",
            .update_factory =
                [] {
                  return std::make_unique<table::RemoveProperties>(
                      std::vector<std::string>{"key"});
                },
            .expected_existing_table_count = 0,
            .validator = nullptr},
        GenerateRequirementsTestParam{
            .test_name = "SetLocation",
            .update_factory =
                [] { return std::make_unique<table::SetLocation>("s3://new/location"); },
            .expected_existing_table_count = 0,
            .validator = nullptr},

        // Updates that generate single requirement for existing tables
        GenerateRequirementsTestParam{
            .test_name = "AddSchema",
            .update_factory =
                [] {
                  auto new_schema = std::make_shared<Schema>(
                      std::vector<SchemaField>{
                          SchemaField::MakeRequired(4, "new_col", string())},
                      3);
                  return std::make_unique<table::AddSchema>(new_schema, 3);
                },
            .expected_existing_table_count = 1,
            .validator =
                [](const std::vector<std::unique_ptr<TableRequirement>>& reqs,
                   const TableMetadata* base) {
                  auto* assert_id = dynamic_cast<const table::AssertLastAssignedFieldId*>(
                      reqs[0].get());
                  ASSERT_NE(assert_id, nullptr);
                  EXPECT_EQ(assert_id->last_assigned_field_id(), base->last_column_id);
                }},
        GenerateRequirementsTestParam{
            .test_name = "SetCurrentSchema",
            .update_factory = [] { return std::make_unique<table::SetCurrentSchema>(1); },
            .expected_existing_table_count = 1,
            .validator =
                [](const std::vector<std::unique_ptr<TableRequirement>>& reqs,
                   const TableMetadata* base) {
                  auto* assert_id =
                      dynamic_cast<const table::AssertCurrentSchemaID*>(reqs[0].get());
                  ASSERT_NE(assert_id, nullptr);
                  EXPECT_EQ(assert_id->schema_id(), base->current_schema_id);
                }},
        GenerateRequirementsTestParam{
            .test_name = "AddPartitionSpec",
            .update_factory =
                [] {
                  PartitionField partition_field(1, 1, "id_identity",
                                                 Transform::Identity());
                  auto spec = std::shared_ptr<PartitionSpec>(
                      PartitionSpec::Make(1, {partition_field}).value().release());
                  return std::make_unique<table::AddPartitionSpec>(spec);
                },
            .expected_existing_table_count = 1,
            .validator =
                [](const std::vector<std::unique_ptr<TableRequirement>>& reqs,
                   const TableMetadata* base) {
                  auto* assert_id =
                      dynamic_cast<const table::AssertLastAssignedPartitionId*>(
                          reqs[0].get());
                  ASSERT_NE(assert_id, nullptr);
                  EXPECT_EQ(assert_id->last_assigned_partition_id(),
                            base->last_partition_id);
                }},
        GenerateRequirementsTestParam{
            .test_name = "SetDefaultPartitionSpec",
            .update_factory =
                [] { return std::make_unique<table::SetDefaultPartitionSpec>(1); },
            .expected_existing_table_count = 1,
            .validator =
                [](const std::vector<std::unique_ptr<TableRequirement>>& reqs,
                   const TableMetadata* base) {
                  auto* assert_id =
                      dynamic_cast<const table::AssertDefaultSpecID*>(reqs[0].get());
                  ASSERT_NE(assert_id, nullptr);
                  EXPECT_EQ(assert_id->spec_id(), base->default_spec_id);
                }},
        GenerateRequirementsTestParam{
            .test_name = "SetDefaultSortOrder",
            .update_factory =
                [] { return std::make_unique<table::SetDefaultSortOrder>(1); },
            .expected_existing_table_count = 1,
            .validator =
                [](const std::vector<std::unique_ptr<TableRequirement>>& reqs,
                   const TableMetadata* base) {
                  auto* assert_sort_order =
                      dynamic_cast<const table::AssertDefaultSortOrderID*>(reqs[0].get());
                  ASSERT_NE(assert_sort_order, nullptr);
                  EXPECT_EQ(assert_sort_order->sort_order_id(),
                            base->default_sort_order_id);
                }},
        GenerateRequirementsTestParam{
            .test_name = "RemovePartitionSpecs",
            .update_factory =
                [] {
                  return std::make_unique<table::RemovePartitionSpecs>(
                      std::vector<int>{1});
                },
            .expected_existing_table_count = 1,
            .validator =
                [](const std::vector<std::unique_ptr<TableRequirement>>& reqs,
                   const TableMetadata* base) {
                  auto* assert_id =
                      dynamic_cast<const table::AssertDefaultSpecID*>(reqs[0].get());
                  ASSERT_NE(assert_id, nullptr);
                  EXPECT_EQ(assert_id->spec_id(), base->default_spec_id);
                }},
        GenerateRequirementsTestParam{
            .test_name = "RemoveSchemas",
            .update_factory =
                [] {
                  return std::make_unique<table::RemoveSchemas>(std::vector<int>{1});
                },
            .expected_existing_table_count = 1,
            .validator =
                [](const std::vector<std::unique_ptr<TableRequirement>>& reqs,
                   const TableMetadata* base) {
                  auto* assert_id =
                      dynamic_cast<const table::AssertCurrentSchemaID*>(reqs[0].get());
                  ASSERT_NE(assert_id, nullptr);
                  EXPECT_EQ(assert_id->schema_id(), base->current_schema_id);
                }}),
    [](const testing::TestParamInfo<GenerateRequirementsTestParam>& info) {
      return info.param.test_name;
    });

// Test AssignUUID ApplyTo
TEST(TableUpdateTest, AssignUUIDApplyUpdate) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Apply AssignUUID update
  table::AssignUUID uuid_update("apply-uuid");
  uuid_update.ApplyTo(*builder);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "apply-uuid");
}

// Test AddSortOrder ApplyTo
TEST(TableUpdateTest, AddSortOrderApplyUpdate) {
  auto base = CreateBaseMetadata();
  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Create a sort order
  auto schema = CreateTestSchema();
  SortField sort_field(1, Transform::Identity(), SortDirection::kAscending,
                       NullOrder::kFirst);
  auto sort_order = std::shared_ptr<SortOrder>(
      SortOrder::Make(*schema, 1, std::vector<SortField>{sort_field}).value().release());

  // Apply AddSortOrder update
  table::AddSortOrder add_sort_order(sort_order);
  add_sort_order.ApplyTo(*builder);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());

  // Verify the sort order was added
  ASSERT_EQ(metadata->sort_orders.size(), 2);  // unsorted + new order
  auto& added_order = metadata->sort_orders[1];
  EXPECT_EQ(added_order->order_id(), 1);
  EXPECT_EQ(added_order->fields().size(), 1);
  EXPECT_EQ(added_order->fields()[0].source_id(), 1);
  EXPECT_EQ(added_order->fields()[0].direction(), SortDirection::kAscending);
  EXPECT_EQ(added_order->fields()[0].null_order(), NullOrder::kFirst);
}

// Test SetDefaultSortOrder ApplyTo
TEST(TableUpdateTest, SetDefaultSortOrderApplyUpdate) {
  auto base = CreateBaseMetadata();

  // add a sort order to the base metadata
  auto schema = CreateTestSchema();
  SortField sort_field(1, Transform::Identity(), SortDirection::kDescending,
                       NullOrder::kLast);
  auto sort_order = std::shared_ptr<SortOrder>(
      SortOrder::Make(*schema, 1, std::vector<SortField>{sort_field}).value().release());
  base->sort_orders.push_back(sort_order);

  auto builder = TableMetadataBuilder::BuildFrom(base.get());

  // Apply SetDefaultSortOrder update to set the new sort order as default
  table::SetDefaultSortOrder set_default_sort_order(1);
  set_default_sort_order.ApplyTo(*builder);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());

  // Verify the default sort order was changed
  EXPECT_EQ(metadata->default_sort_order_id, 1);
}

}  // namespace iceberg
