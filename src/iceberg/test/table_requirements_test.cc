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
#include <ranges>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

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
  metadata->current_schema_id = Schema::kInitialSchemaId;
  metadata->default_spec_id = PartitionSpec::kInitialSpecId;
  metadata->last_partition_id = 0;
  metadata->current_snapshot_id = kInvalidSnapshotId;
  metadata->default_sort_order_id = SortOrder::kUnsortedOrderId;
  metadata->next_row_id = TableMetadata::kInitialRowId;
  return metadata;
}

// Helper function to create a simple schema for tests
std::shared_ptr<Schema> CreateTestSchema(int32_t schema_id = 0) {
  std::vector<SchemaField> fields;
  fields.emplace_back(SchemaField::MakeRequired(1, "id", int32()));
  return std::make_shared<Schema>(std::move(fields), schema_id);
}

// Helper function to count requirements of a specific type
template <typename T>
int CountRequirementsOfType(
    const std::vector<std::unique_ptr<TableRequirement>>& requirements) {
  return std::ranges::count_if(requirements, [](const auto& req) {
    return dynamic_cast<T*>(req.get()) != nullptr;
  });
}

// Helper function to add a branch to metadata
void AddBranch(TableMetadata& metadata, const std::string& name, int64_t snapshot_id) {
  auto ref = std::make_shared<SnapshotRef>();
  ref->snapshot_id = snapshot_id;
  ref->retention = SnapshotRef::Branch{};
  metadata.refs[name] = ref;
}

}  // namespace

// Empty Updates Tests

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

TEST(TableRequirementsTest, IsCreate) {
  // Should have only AssertDoesNotExist requirement
  std::vector<std::unique_ptr<TableRequirement>> requirements;
  requirements.push_back(std::make_unique<table::AssertDoesNotExist>());
  EXPECT_TRUE(TableRequirements::IsCreate(requirements));

  // Not only have AssertDoesNotExist requirement
  requirements.push_back(std::make_unique<table::AssertCurrentSchemaID>(0));
  auto res = TableRequirements::IsCreate(requirements);
  EXPECT_THAT(res, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(res,
              HasErrorMessage("Cannot have other requirements than AssertDoesNotExist"));
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

// Table Existence Tests

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

// AssignUUID Tests

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

// UpgradeFormatVersion Tests

TEST(TableRequirementsTest, UpgradeFormatVersion) {
  auto metadata = CreateBaseMetadata();
  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::UpgradeFormatVersion>(2));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // UpgradeFormatVersion doesn't add additional requirements
  ASSERT_EQ(requirements.size(), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

// AddSchema Tests

TEST(TableRequirementsTest, AddSchema) {
  auto metadata = CreateBaseMetadata();
  metadata->last_column_id = 1;
  std::vector<std::unique_ptr<TableUpdate>> updates;

  auto schema = CreateTestSchema();
  // Add multiple AddSchema updates
  updates.push_back(std::make_unique<table::AddSchema>(schema, 1));
  updates.push_back(std::make_unique<table::AddSchema>(schema, 1));
  updates.push_back(std::make_unique<table::AddSchema>(schema, 1));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Should have AssertUUID + AssertLastAssignedFieldId (deduplicated)
  ASSERT_EQ(requirements.size(), 2);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertLastAssignedFieldId>(requirements), 1);

  // Verify the last assigned field ID value
  auto* assert_field_id =
      dynamic_cast<table::AssertLastAssignedFieldId*>(requirements[1].get());
  ASSERT_NE(assert_field_id, nullptr);
  EXPECT_EQ(assert_field_id->last_assigned_field_id(), 1);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

TEST(TableRequirementsTest, AddSchemaFailure) {
  auto metadata = CreateBaseMetadata();
  metadata->last_column_id = 2;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  auto schema = CreateTestSchema();
  updates.push_back(std::make_unique<table::AddSchema>(schema, 2));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();

  // Create updated metadata with different last_column_id
  auto updated = CreateBaseMetadata();
  updated->last_column_id = 3;

  // Find and validate the AssertLastAssignedFieldId requirement
  for (const auto& req : requirements) {
    if (dynamic_cast<table::AssertLastAssignedFieldId*>(req.get()) != nullptr) {
      auto status = req->Validate(updated.get());
      EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
      EXPECT_THAT(status, HasErrorMessage("last assigned field ID does not match"));
      break;
    }
  }
}

// SetCurrentSchema Tests

TEST(TableRequirementsTest, SetCurrentSchema) {
  auto metadata = CreateBaseMetadata();
  metadata->current_schema_id = 3;
  std::vector<std::unique_ptr<TableUpdate>> updates;

  // Add multiple SetCurrentSchema updates
  updates.push_back(std::make_unique<table::SetCurrentSchema>(3));
  updates.push_back(std::make_unique<table::SetCurrentSchema>(4));
  updates.push_back(std::make_unique<table::SetCurrentSchema>(5));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Should have AssertUUID + AssertCurrentSchemaID (deduplicated)
  ASSERT_EQ(requirements.size(), 2);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertCurrentSchemaID>(requirements), 1);

  // Verify the current schema ID value
  auto* assert_schema_id =
      dynamic_cast<table::AssertCurrentSchemaID*>(requirements[1].get());
  ASSERT_NE(assert_schema_id, nullptr);
  EXPECT_EQ(assert_schema_id->schema_id(), 3);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

TEST(TableRequirementsTest, SetCurrentSchemaFailure) {
  auto metadata = CreateBaseMetadata();
  metadata->current_schema_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::SetCurrentSchema>(3));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();

  // Create updated metadata with different current_schema_id
  auto updated = CreateBaseMetadata();
  updated->current_schema_id = 4;

  // Find and validate the AssertCurrentSchemaID requirement
  for (const auto& req : requirements) {
    if (dynamic_cast<table::AssertCurrentSchemaID*>(req.get()) != nullptr) {
      auto status = req->Validate(updated.get());
      EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
      EXPECT_THAT(status, HasErrorMessage("current schema ID does not match"));
      break;
    }
  }
}

// AddPartitionSpec Tests

TEST(TableRequirementsTest, AddPartitionSpec) {
  auto metadata = CreateBaseMetadata();
  metadata->last_partition_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::AddPartitionSpec>(PartitionSpec::Unpartitioned()));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Should have AssertUUID + AssertLastAssignedPartitionId
  ASSERT_EQ(requirements.size(), 2);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertLastAssignedPartitionId>(requirements),
            1);

  // Verify the last assigned partition ID value
  auto* assert_partition_id =
      dynamic_cast<table::AssertLastAssignedPartitionId*>(requirements[1].get());
  ASSERT_NE(assert_partition_id, nullptr);
  EXPECT_EQ(assert_partition_id->last_assigned_partition_id(), 3);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

TEST(TableRequirementsTest, AddPartitionSpecFailure) {
  auto metadata = CreateBaseMetadata();
  metadata->last_partition_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::AddPartitionSpec>(PartitionSpec::Unpartitioned()));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();

  // Create updated metadata with different last_partition_id
  auto updated = CreateBaseMetadata();
  updated->last_partition_id = 4;

  // Find and validate the AssertLastAssignedPartitionId requirement
  for (const auto& req : requirements) {
    if (dynamic_cast<table::AssertLastAssignedPartitionId*>(req.get()) != nullptr) {
      auto status = req->Validate(updated.get());
      EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
      EXPECT_THAT(status, HasErrorMessage("last assigned partition ID does not match"));
      break;
    }
  }
}

// SetDefaultPartitionSpec Tests

TEST(TableRequirementsTest, SetDefaultPartitionSpec) {
  auto metadata = CreateBaseMetadata();
  metadata->default_spec_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  // Add multiple SetDefaultPartitionSpec updates
  updates.push_back(std::make_unique<table::SetDefaultPartitionSpec>(3));
  updates.push_back(std::make_unique<table::SetDefaultPartitionSpec>(4));
  updates.push_back(std::make_unique<table::SetDefaultPartitionSpec>(5));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Should have AssertUUID + AssertDefaultSpecID (deduplicated)
  ASSERT_EQ(requirements.size(), 2);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertDefaultSpecID>(requirements), 1);

  // Verify the default spec ID value
  auto* assert_spec_id = dynamic_cast<table::AssertDefaultSpecID*>(requirements[1].get());
  ASSERT_NE(assert_spec_id, nullptr);
  EXPECT_EQ(assert_spec_id->spec_id(), 3);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

TEST(TableRequirementsTest, SetDefaultPartitionSpecFailure) {
  auto metadata = CreateBaseMetadata();
  metadata->default_spec_id = PartitionSpec::kInitialSpecId;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::SetDefaultPartitionSpec>(PartitionSpec::kInitialSpecId));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();

  // Create updated metadata with different default_spec_id
  auto updated = CreateBaseMetadata();
  updated->default_spec_id = PartitionSpec::kInitialSpecId + 1;

  // Find and validate the AssertDefaultSpecID requirement
  for (const auto& req : requirements) {
    if (dynamic_cast<table::AssertDefaultSpecID*>(req.get()) != nullptr) {
      auto status = req->Validate(updated.get());
      EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
      EXPECT_THAT(status, HasErrorMessage("default partition spec changed"));
      break;
    }
  }
}

// RemovePartitionSpecs Tests

TEST(TableRequirementsTest, RemovePartitionSpecs) {
  auto metadata = CreateBaseMetadata();
  metadata->default_spec_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::RemovePartitionSpecs>(std::vector<int32_t>{1, 2}));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Should have AssertUUID + AssertDefaultSpecID
  ASSERT_EQ(requirements.size(), 2);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertDefaultSpecID>(requirements), 1);

  // Verify the default spec ID value
  auto* assert_spec_id = dynamic_cast<table::AssertDefaultSpecID*>(requirements[1].get());
  ASSERT_NE(assert_spec_id, nullptr);
  EXPECT_EQ(assert_spec_id->spec_id(), 3);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

TEST(TableRequirementsTest, RemovePartitionSpecsWithBranch) {
  auto metadata = CreateBaseMetadata();
  metadata->default_spec_id = 3;
  AddBranch(*metadata, "branch", 42);

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::RemovePartitionSpecs>(std::vector<int32_t>{1, 2}));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Should have AssertUUID + AssertDefaultSpecID + AssertRefSnapshotID
  ASSERT_EQ(requirements.size(), 3);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertDefaultSpecID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertRefSnapshotID>(requirements), 1);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

TEST(TableRequirementsTest, RemovePartitionSpecsWithSpecChangedFailure) {
  auto metadata = CreateBaseMetadata();
  metadata->default_spec_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::RemovePartitionSpecs>(std::vector<int32_t>{1, 2}));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();

  // Create updated metadata with different default_spec_id
  auto updated = CreateBaseMetadata();
  updated->default_spec_id = 4;

  // Find and validate the AssertDefaultSpecID requirement
  for (const auto& req : requirements) {
    if (dynamic_cast<table::AssertDefaultSpecID*>(req.get()) != nullptr) {
      auto status = req->Validate(updated.get());
      EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
      EXPECT_THAT(status, HasErrorMessage("default partition spec changed"));
      break;
    }
  }
}

TEST(TableRequirementsTest, RemovePartitionSpecsWithBranchChangedFailure) {
  auto metadata = CreateBaseMetadata();
  metadata->default_spec_id = 3;
  AddBranch(*metadata, "test", 42);

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::RemovePartitionSpecs>(std::vector<int32_t>{1, 2}));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();

  // Create updated metadata with changed branch
  auto updated = CreateBaseMetadata();
  updated->default_spec_id = 3;
  AddBranch(*updated, "test", 43);

  // Find and validate the AssertRefSnapshotID requirement
  for (const auto& req : requirements) {
    if (dynamic_cast<table::AssertRefSnapshotID*>(req.get()) != nullptr) {
      auto status = req->Validate(updated.get());
      EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
      EXPECT_THAT(status, HasErrorMessage("has changed"));
      break;
    }
  }
}

// RemoveSchemas Tests

TEST(TableRequirementsTest, RemoveSchemas) {
  auto metadata = CreateBaseMetadata();
  metadata->current_schema_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::RemoveSchemas>(std::unordered_set<int32_t>{1, 2}));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Should have AssertUUID + AssertCurrentSchemaID
  ASSERT_EQ(requirements.size(), 2);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertCurrentSchemaID>(requirements), 1);

  // Verify the current schema ID value
  auto* assert_schema_id =
      dynamic_cast<table::AssertCurrentSchemaID*>(requirements[1].get());
  ASSERT_NE(assert_schema_id, nullptr);
  EXPECT_EQ(assert_schema_id->schema_id(), 3);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

TEST(TableRequirementsTest, RemoveSchemasWithBranch) {
  auto metadata = CreateBaseMetadata();
  metadata->current_schema_id = 3;
  AddBranch(*metadata, "branch", 42);

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::RemoveSchemas>(std::unordered_set<int32_t>{1, 2}));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Should have AssertUUID + AssertCurrentSchemaID + AssertRefSnapshotID
  ASSERT_EQ(requirements.size(), 3);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertCurrentSchemaID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertRefSnapshotID>(requirements), 1);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

TEST(TableRequirementsTest, RemoveSchemasWithSchemaChangedFailure) {
  auto metadata = CreateBaseMetadata();
  metadata->current_schema_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::RemoveSchemas>(std::unordered_set<int32_t>{1, 2}));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();

  // Create updated metadata with different current_schema_id
  auto updated = CreateBaseMetadata();
  updated->current_schema_id = 4;

  // Find and validate the AssertCurrentSchemaID requirement
  for (const auto& req : requirements) {
    if (dynamic_cast<table::AssertCurrentSchemaID*>(req.get()) != nullptr) {
      auto status = req->Validate(updated.get());
      EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
      EXPECT_THAT(status, HasErrorMessage("current schema ID does not match"));
      break;
    }
  }
}

TEST(TableRequirementsTest, RemoveSchemasWithBranchChangedFailure) {
  auto metadata = CreateBaseMetadata();
  metadata->current_schema_id = 3;
  AddBranch(*metadata, "test", 42);

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::RemoveSchemas>(std::unordered_set<int32_t>{1, 2}));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();

  // Create updated metadata with changed branch
  auto updated = CreateBaseMetadata();
  updated->current_schema_id = 3;
  AddBranch(*updated, "test", 43);

  // Find and validate the AssertRefSnapshotID requirement
  for (const auto& req : requirements) {
    if (dynamic_cast<table::AssertRefSnapshotID*>(req.get()) != nullptr) {
      auto status = req->Validate(updated.get());
      EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
      EXPECT_THAT(status, HasErrorMessage("has changed"));
      break;
    }
  }
}

// AddSortOrder Tests

TEST(TableRequirementsTest, AddSortOrder) {
  auto metadata = CreateBaseMetadata();
  std::vector<std::unique_ptr<TableUpdate>> updates;

  updates.push_back(std::make_unique<table::AddSortOrder>(SortOrder::Unsorted()));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // AddSortOrder doesn't add additional requirements
  ASSERT_EQ(requirements.size(), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

// SetDefaultSortOrder Tests

TEST(TableRequirementsTest, SetDefaultSortOrder) {
  auto metadata = CreateBaseMetadata();
  metadata->default_sort_order_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  // Add multiple SetDefaultSortOrder updates
  updates.push_back(std::make_unique<table::SetDefaultSortOrder>(3));
  updates.push_back(std::make_unique<table::SetDefaultSortOrder>(4));
  updates.push_back(std::make_unique<table::SetDefaultSortOrder>(5));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Should have AssertUUID + AssertDefaultSortOrderID (deduplicated)
  ASSERT_EQ(requirements.size(), 2);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertDefaultSortOrderID>(requirements), 1);

  // Verify the default sort order ID value
  auto* assert_sort_order_id =
      dynamic_cast<table::AssertDefaultSortOrderID*>(requirements[1].get());
  ASSERT_NE(assert_sort_order_id, nullptr);
  EXPECT_EQ(assert_sort_order_id->sort_order_id(), 3);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

TEST(TableRequirementsTest, SetDefaultSortOrderFailure) {
  auto metadata = CreateBaseMetadata();
  metadata->default_sort_order_id = SortOrder::kUnsortedOrderId;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::SetDefaultSortOrder>(SortOrder::kUnsortedOrderId));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();

  // Create updated metadata with different default_sort_order_id
  auto updated = CreateBaseMetadata();
  updated->default_sort_order_id = SortOrder::kUnsortedOrderId + 1;

  // Find and validate the AssertDefaultSortOrderID requirement
  for (const auto& req : requirements) {
    if (dynamic_cast<table::AssertDefaultSortOrderID*>(req.get()) != nullptr) {
      auto status = req->Validate(updated.get());
      EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
      EXPECT_THAT(status, HasErrorMessage("default sort order changed"));
      break;
    }
  }
}

// AddSnapshot Tests

TEST(TableRequirementsTest, AddSnapshot) {
  auto metadata = CreateBaseMetadata();

  std::vector<std::unique_ptr<TableUpdate>> updates;
  auto snapshot = std::make_shared<Snapshot>();
  snapshot->snapshot_id = 1;
  snapshot->sequence_number = 1;
  snapshot->timestamp_ms = TimePointMs{std::chrono::milliseconds(1000)};
  snapshot->manifest_list = "s3://bucket/manifest_list";
  updates.push_back(std::make_unique<table::AddSnapshot>(snapshot));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  ASSERT_EQ(requirements.size(), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);

  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

// RemoveSnapshots Tests

TEST(TableRequirementsTest, RemoveSnapshots) {
  auto metadata = CreateBaseMetadata();

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::RemoveSnapshots>(std::vector<int64_t>{0}));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  ASSERT_EQ(requirements.size(), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);

  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

// SetSnapshotRef Tests

TEST(TableRequirementsTest, SetSnapshotRef) {
  constexpr int64_t kSnapshotId = 14;
  const std::string kRefName = "branch";

  auto metadata = CreateBaseMetadata();
  AddBranch(*metadata, kRefName, kSnapshotId);

  // Multiple updates to same ref should deduplicate
  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::SetSnapshotRef>(kRefName, kSnapshotId,
                                                            SnapshotRefType::kBranch));
  updates.push_back(std::make_unique<table::SetSnapshotRef>(kRefName, kSnapshotId + 1,
                                                            SnapshotRefType::kBranch));
  updates.push_back(std::make_unique<table::SetSnapshotRef>(kRefName, kSnapshotId + 2,
                                                            SnapshotRefType::kBranch));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }

  ASSERT_EQ(requirements.size(), 2);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertRefSnapshotID>(requirements), 1);

  auto* assert_ref = dynamic_cast<table::AssertRefSnapshotID*>(requirements[1].get());
  ASSERT_NE(assert_ref, nullptr);
  EXPECT_EQ(assert_ref->snapshot_id(), kSnapshotId);
  EXPECT_EQ(assert_ref->ref_name(), kRefName);
}

// RemoveSnapshotRef Tests

TEST(TableRequirementsTest, RemoveSnapshotRef) {
  auto metadata = CreateBaseMetadata();

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::RemoveSnapshotRef>("branch"));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  ASSERT_EQ(requirements.size(), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);

  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

// SetAndRemoveProperties Tests

TEST(TableRequirementsTest, SetProperties) {
  auto metadata = CreateBaseMetadata();
  std::vector<std::unique_ptr<TableUpdate>> updates;

  std::unordered_map<std::string, std::string> props;
  props["test"] = "value";
  updates.push_back(std::make_unique<table::SetProperties>(props));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // SetProperties doesn't add additional requirements
  ASSERT_EQ(requirements.size(), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

TEST(TableRequirementsTest, RemoveProperties) {
  auto metadata = CreateBaseMetadata();
  std::vector<std::unique_ptr<TableUpdate>> updates;

  updates.push_back(
      std::make_unique<table::RemoveProperties>(std::unordered_set<std::string>{"test"}));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // RemoveProperties doesn't add additional requirements
  ASSERT_EQ(requirements.size(), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

// SetLocation Tests

TEST(TableRequirementsTest, SetLocation) {
  auto metadata = CreateBaseMetadata();
  std::vector<std::unique_ptr<TableUpdate>> updates;

  updates.push_back(std::make_unique<table::SetLocation>("s3://new-bucket/test"));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // SetLocation doesn't add additional requirements
  ASSERT_EQ(requirements.size(), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);

  // Validate against base metadata
  for (const auto& req : requirements) {
    EXPECT_THAT(req->Validate(metadata.get()), IsOk());
  }
}

// AssertRefSnapshotID Tests

TEST(TableRequirementsTest, AssertRefSnapshotIDSuccess) {
  auto metadata = CreateBaseMetadata();
  AddBranch(*metadata, "branch", 14);

  table::AssertRefSnapshotID requirement("branch", 14);
  auto status = requirement.Validate(metadata.get());
  EXPECT_THAT(status, IsOk());
}

TEST(TableRequirementsTest, AssertRefSnapshotIDCreatedConcurrently) {
  auto metadata = CreateBaseMetadata();
  AddBranch(*metadata, "random_branch", 14);

  // Requirement expects ref doesn't exist (nullopt snapshot_id)
  table::AssertRefSnapshotID requirement("random_branch", std::nullopt);
  auto status = requirement.Validate(metadata.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("was created concurrently"));
}

TEST(TableRequirementsTest, AssertRefSnapshotIDMissing) {
  auto metadata = CreateBaseMetadata();
  // No branch added

  // Requirement expects a snapshot ID that doesn't exist
  table::AssertRefSnapshotID requirement("random_branch", 14);
  auto status = requirement.Validate(metadata.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("is missing"));
}

TEST(TableRequirementsTest, AssertRefSnapshotIDChanged) {
  auto metadata = CreateBaseMetadata();
  AddBranch(*metadata, "random_branch", 15);

  // Requirement expects snapshot ID 14, but actual is 15
  table::AssertRefSnapshotID requirement("random_branch", 14);
  auto status = requirement.Validate(metadata.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("has changed"));
}

// Replace Table Tests (less restrictive than Update)

TEST(TableRequirementsTest, ReplaceTableDoesNotRequireCurrentSchemaID) {
  auto metadata = CreateBaseMetadata();
  metadata->current_schema_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::SetCurrentSchema>(5));

  auto result = TableRequirements::ForReplaceTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Replace table should NOT add AssertCurrentSchemaID
  EXPECT_EQ(CountRequirementsOfType<table::AssertCurrentSchemaID>(requirements), 0);
}

TEST(TableRequirementsTest, ReplaceTableDoesNotRequireDefaultSpecID) {
  auto metadata = CreateBaseMetadata();
  metadata->default_spec_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::SetDefaultPartitionSpec>(5));

  auto result = TableRequirements::ForReplaceTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Replace table should NOT add AssertDefaultSpecID
  EXPECT_EQ(CountRequirementsOfType<table::AssertDefaultSpecID>(requirements), 0);
}

TEST(TableRequirementsTest, ReplaceTableDoesNotRequireDefaultSortOrderID) {
  auto metadata = CreateBaseMetadata();
  metadata->default_sort_order_id = 3;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::SetDefaultSortOrder>(5));

  auto result = TableRequirements::ForReplaceTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Replace table should NOT add AssertDefaultSortOrderID
  EXPECT_EQ(CountRequirementsOfType<table::AssertDefaultSortOrderID>(requirements), 0);
}

TEST(TableRequirementsTest, ReplaceTableDoesNotAddBranchRequirements) {
  auto metadata = CreateBaseMetadata();
  metadata->current_schema_id = 3;
  AddBranch(*metadata, "branch", 42);

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(
      std::make_unique<table::RemoveSchemas>(std::unordered_set<int32_t>{1, 2}));

  auto result = TableRequirements::ForReplaceTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Replace table should NOT add AssertRefSnapshotID for branches
  EXPECT_EQ(CountRequirementsOfType<table::AssertRefSnapshotID>(requirements), 0);
}

// Combined Updates Tests

TEST(TableRequirementsTest, MultipleUpdatesDeduplication) {
  auto metadata = CreateBaseMetadata();
  metadata->last_column_id = 1;
  metadata->current_schema_id = 0;

  std::vector<std::unique_ptr<TableUpdate>> updates;
  auto schema = CreateTestSchema();
  // Add multiple AddSchema updates - should only generate one requirement
  updates.push_back(std::make_unique<table::AddSchema>(schema, 1));
  updates.push_back(std::make_unique<table::AddSchema>(schema, 1));
  // Add multiple SetCurrentSchema updates - should only generate one requirement
  updates.push_back(std::make_unique<table::SetCurrentSchema>(0));
  updates.push_back(std::make_unique<table::SetCurrentSchema>(1));

  auto result = TableRequirements::ForUpdateTable(*metadata, updates);
  ASSERT_THAT(result, IsOk());

  auto& requirements = result.value();
  // Should have: 1 AssertUUID + 1 AssertLastAssignedFieldId + 1 AssertCurrentSchemaID
  ASSERT_EQ(requirements.size(), 3);
  EXPECT_EQ(CountRequirementsOfType<table::AssertUUID>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertLastAssignedFieldId>(requirements), 1);
  EXPECT_EQ(CountRequirementsOfType<table::AssertCurrentSchemaID>(requirements), 1);
}

}  // namespace iceberg
