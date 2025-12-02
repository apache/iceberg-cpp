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

#include "iceberg/table_requirements.h"

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/partition_spec.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

namespace {

// Helper function to create base metadata for tests
std::unique_ptr<TableMetadata> CreateBaseMetadata(
    const std::string& uuid = "test-uuid-1234") {
  auto metadata = std::make_unique<TableMetadata>();
  metadata->format_version = 2;
  metadata->table_uuid = uuid;
  metadata->location = "s3://bucket/test";
  metadata->last_sequence_number = 0;
  metadata->last_updated_ms = TimePointMs{std::chrono::milliseconds(1000)};
  metadata->last_column_id = 0;
  metadata->default_spec_id = PartitionSpec::kInitialSpecId;
  metadata->last_partition_id = 0;
  metadata->current_snapshot_id = Snapshot::kInvalidSnapshotId;
  metadata->default_sort_order_id = SortOrder::kInitialSortOrderId;
  metadata->next_row_id = TableMetadata::kInitialRowId;
  return metadata;
}

}  // namespace

TEST(TableRequirementsTest, EmptyUpdatesForCreateTable) {
  std::vector<std::unique_ptr<TableUpdate>> updates;

  auto result = TableRequirements::ForCreateTable(updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  ASSERT_EQ(requirements.size(), 1);

  // Should have only AssertDoesNotExist requirement
  auto* assert_does_not_exist =
      dynamic_cast<table::AssertDoesNotExist*>(requirements[0].get());
  EXPECT_NE(assert_does_not_exist, nullptr);
}

TEST(TableRequirementsTest, EmptyUpdatesForUpdateTable) {
  auto metadata = CreateBaseMetadata();
  std::vector<std::unique_ptr<TableUpdate>> updates;

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  ASSERT_EQ(requirements.size(), 1);

  // Should have only AssertUUID requirement
  auto* assert_uuid = dynamic_cast<table::AssertUUID*>(requirements[0].get());
  ASSERT_NE(assert_uuid, nullptr);
  EXPECT_EQ(assert_uuid->uuid(), metadata->table_uuid);
}

TEST(TableRequirementsTest, EmptyUpdatesForReplaceTable) {
  auto metadata = CreateBaseMetadata();
  std::vector<std::unique_ptr<TableUpdate>> updates;

  auto result = TableRequirements::ForReplaceTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  ASSERT_EQ(requirements.size(), 1);

  // Should have only AssertUUID requirement
  auto* assert_uuid = dynamic_cast<table::AssertUUID*>(requirements[0].get());
  ASSERT_NE(assert_uuid, nullptr);
  EXPECT_EQ(assert_uuid->uuid(), metadata->table_uuid);
}

TEST(TableRequirementsTest, TableAlreadyExists) {
  std::vector<std::unique_ptr<TableUpdate>> updates;

  auto result = TableRequirements::ForCreateTable(updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  ASSERT_EQ(requirements.size(), 1);

  // Validate against existing metadata (should fail)
  auto metadata = CreateBaseMetadata();
  auto status = requirements[0]->Validate(metadata.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("table already exists"));
}

TEST(TableRequirementsTest, TableDoesNotExist) {
  std::vector<std::unique_ptr<TableUpdate>> updates;

  auto result = TableRequirements::ForCreateTable(updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  ASSERT_EQ(requirements.size(), 1);

  // Validate against null metadata (should succeed)
  auto status = requirements[0]->Validate(nullptr);
  EXPECT_THAT(status, IsOk());
}

// Test for AssignUUID update
TEST(TableRequirementsTest, AssignUUID) {
  auto metadata = CreateBaseMetadata("original-uuid");
  std::vector<std::unique_ptr<TableUpdate>> updates;

  // Add multiple AssignUUID updates
  updates.push_back(std::make_unique<table::AssignUUID>(metadata->table_uuid));
  updates.push_back(std::make_unique<table::AssignUUID>("new-uuid-1"));
  updates.push_back(std::make_unique<table::AssignUUID>("new-uuid-2"));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // After deduplication: only 1 AssertUUID from ForUpdateTable
  ASSERT_EQ(requirements.size(), 1);

  // Should have AssertUUID requirement with original UUID
  auto* assert_uuid = dynamic_cast<table::AssertUUID*>(requirements[0].get());
  ASSERT_NE(assert_uuid, nullptr);
  EXPECT_EQ(assert_uuid->uuid(), "original-uuid");

  auto status = requirements[0]->Validate(metadata.get());
  EXPECT_THAT(status, IsOk());
}

TEST(TableRequirementsTest, AssignUUIDFailure) {
  auto metadata = CreateBaseMetadata("original-uuid");
  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::AssignUUID>(metadata->table_uuid));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // After deduplication: only 1 AssertUUID from ForUpdateTable
  ASSERT_EQ(requirements.size(), 1);

  // Create updated metadata with different UUID
  auto updated = CreateBaseMetadata("different-uuid");

  // Validate against updated metadata (should fail)
  auto status = requirements[0]->Validate(updated.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("UUID does not match"));
}

TEST(TableRequirementsTest, AssignUUIDForReplaceTable) {
  auto metadata = CreateBaseMetadata("original-uuid");
  std::vector<std::unique_ptr<TableUpdate>> updates;

  // Add multiple AssignUUID updates
  updates.push_back(std::make_unique<table::AssignUUID>(metadata->table_uuid));
  updates.push_back(std::make_unique<table::AssignUUID>("new-uuid-1"));
  updates.push_back(std::make_unique<table::AssignUUID>("new-uuid-2"));

  auto result = TableRequirements::ForReplaceTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // After deduplication: only 1 AssertUUID from ForReplaceTable
  ASSERT_EQ(requirements.size(), 1);

  // Should have AssertUUID requirement
  auto* assert_uuid = dynamic_cast<table::AssertUUID*>(requirements[0].get());
  ASSERT_NE(assert_uuid, nullptr);
  EXPECT_EQ(assert_uuid->uuid(), "original-uuid");

  // Validate against base metadata (should succeed)
  auto status = requirements[0]->Validate(metadata.get());
  EXPECT_THAT(status, IsOk());
}

}  // namespace iceberg
