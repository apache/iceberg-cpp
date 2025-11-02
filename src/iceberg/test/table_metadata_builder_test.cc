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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "iceberg/partition_spec.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// Helper functions to reduce test boilerplate
namespace {

// Generate requirements and return them
std::vector<std::unique_ptr<TableRequirement>> GenerateRequirements(
    const TableUpdate& update, const TableMetadata* base) {
  TableUpdateContext context(base, /*is_replace=*/false);
  EXPECT_THAT(update.GenerateRequirements(context), IsOk());

  auto requirements = context.Build();
  EXPECT_THAT(requirements, IsOk());
  return std::move(requirements.value());
}

}  // namespace

// Test fixture for TableMetadataBuilder tests
class TableMetadataBuilderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a base metadata for update tests
    base_metadata_ = std::make_unique<TableMetadata>();
    base_metadata_->format_version = 2;
    base_metadata_->table_uuid = "test-uuid-1234";
    base_metadata_->location = "s3://bucket/test";
    base_metadata_->last_sequence_number = 0;
    base_metadata_->last_updated_ms = TimePointMs{std::chrono::milliseconds(1000)};
    base_metadata_->last_column_id = 0;
    base_metadata_->default_spec_id = PartitionSpec::kInitialSpecId;
    base_metadata_->last_partition_id = 0;
    base_metadata_->current_snapshot_id = Snapshot::kInvalidSnapshotId;
    base_metadata_->default_sort_order_id = SortOrder::kInitialSortOrderId;
    base_metadata_->next_row_id = TableMetadata::kInitialRowId;
  }

  std::unique_ptr<TableMetadata> base_metadata_;
};

// ============================================================================
// TableMetadataBuilder - Basic Construction Tests
// ============================================================================

TEST_F(TableMetadataBuilderTest, BuildFromEmpty) {
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

TEST_F(TableMetadataBuilderTest, BuildFromExisting) {
  auto builder = TableMetadataBuilder::BuildFrom(base_metadata_.get());
  ASSERT_NE(builder, nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_NE(metadata, nullptr);

  EXPECT_EQ(metadata->format_version, 2);
  EXPECT_EQ(metadata->table_uuid, "test-uuid-1234");
  EXPECT_EQ(metadata->location, "s3://bucket/test");
}

// ============================================================================
// TableMetadataBuilder - AssignUUID Tests
// ============================================================================

TEST_F(TableMetadataBuilderTest, AssignUUIDForNewTable) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID("new-uuid-5678");

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "new-uuid-5678");
}

TEST_F(TableMetadataBuilderTest, AssignUUIDAndUpdateExisting) {
  auto builder = TableMetadataBuilder::BuildFrom(base_metadata_.get());
  builder->AssignUUID("updated-uuid-9999");

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "updated-uuid-9999");
}

TEST_F(TableMetadataBuilderTest, AssignUUIDWithEmptyUUID) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID("");

  ASSERT_THAT(builder->Build(), HasErrorMessage("Cannot assign empty UUID"));
}

TEST_F(TableMetadataBuilderTest, AssignUUIDWithSameUUID) {
  auto builder = TableMetadataBuilder::BuildFrom(base_metadata_.get());
  builder->AssignUUID("test-uuid-1234");  // Same UUID

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "test-uuid-1234");
}

TEST_F(TableMetadataBuilderTest, AssignUUIDWithAutoGenerate) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID();  // Auto-generate

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_FALSE(metadata->table_uuid.empty());
}

TEST_F(TableMetadataBuilderTest, AssignUUIDAndCaseInsensitiveComparison) {
  base_metadata_->table_uuid = "TEST-UUID-ABCD";
  auto builder = TableMetadataBuilder::BuildFrom(base_metadata_.get());
  builder->AssignUUID("test-uuid-abcd");  // Different case - should be no-op

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "TEST-UUID-ABCD");  // Original case preserved
}

// ============================================================================
// TableUpdate - ApplyTo Tests
// ============================================================================

TEST_F(TableMetadataBuilderTest, TableUpdateWithAssignUUID) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);

  table::AssignUUID update("apply-uuid");
  update.ApplyTo(*builder);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "apply-uuid");
}

// ============================================================================
// TableUpdate - GenerateRequirements Tests
// ============================================================================

TEST_F(TableMetadataBuilderTest,
       TableUpdateWithAssignUUIDAndGenerateRequirementsForNewTable) {
  table::AssignUUID update("new-uuid");

  auto requirements = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(requirements.empty());  // No requirements for new table
}

TEST_F(TableMetadataBuilderTest,
       TableUpdateWithAssignUUIDAndGenerateRequirementsForExistingTable) {
  table::AssignUUID update("new-uuid");

  auto requirements = GenerateRequirements(update, base_metadata_.get());
  EXPECT_EQ(requirements.size(), 1);  // Should generate AssertUUID requirement
}

TEST_F(TableMetadataBuilderTest,
       TableUpdateWithAssignUUIDAndGenerateRequirementsWithEmptyUUID) {
  base_metadata_->table_uuid = "";
  table::AssignUUID update("new-uuid");

  auto requirements = GenerateRequirements(update, base_metadata_.get());
  EXPECT_TRUE(requirements.empty());  // No requirement when base has no UUID
}

// ============================================================================
// TableRequirement - Validate Tests
// ============================================================================

TEST_F(TableMetadataBuilderTest, TableRequirementAssertUUIDSuccess) {
  table::AssertUUID requirement("test-uuid-1234");

  ASSERT_THAT(requirement.Validate(base_metadata_.get()), IsOk());
}

TEST_F(TableMetadataBuilderTest, TableRequirementAssertUUIDMismatch) {
  table::AssertUUID requirement("wrong-uuid");

  auto status = requirement.Validate(base_metadata_.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("UUID does not match"));
}

TEST_F(TableMetadataBuilderTest, TableRequirementAssertUUIDNullBase) {
  table::AssertUUID requirement("any-uuid");

  auto status = requirement.Validate(nullptr);
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("metadata is missing"));
}

TEST_F(TableMetadataBuilderTest, TableRequirementAssertUUIDCaseInsensitive) {
  base_metadata_->table_uuid = "TEST-UUID-1234";
  table::AssertUUID requirement("test-uuid-1234");

  ASSERT_THAT(requirement.Validate(base_metadata_.get()), IsOk());
}

TEST_F(TableMetadataBuilderTest, TableRequirementAssertCurrentSchemaIDSuccess) {
  base_metadata_->current_schema_id = 5;
  table::AssertCurrentSchemaID requirement(5);

  ASSERT_THAT(requirement.Validate(base_metadata_.get()), IsOk());
}

TEST_F(TableMetadataBuilderTest, TableRequirementAssertCurrentSchemaIDMismatch) {
  base_metadata_->current_schema_id = 5;
  table::AssertCurrentSchemaID requirement(10);

  auto status = requirement.Validate(base_metadata_.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("schema ID does not match"));
}

TEST_F(TableMetadataBuilderTest, TableRequirementAssertCurrentSchemaIDNullBase) {
  table::AssertCurrentSchemaID requirement(5);

  auto status = requirement.Validate(nullptr);
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("metadata is missing"));
}

TEST_F(TableMetadataBuilderTest, TableRequirementAssertCurrentSchemaIDNotSet) {
  base_metadata_->current_schema_id = std::nullopt;
  table::AssertCurrentSchemaID requirement(5);

  auto status = requirement.Validate(base_metadata_.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("schema ID is not set"));
}

// ============================================================================
// Integration Tests - End-to-End Workflow
// ============================================================================

TEST_F(TableMetadataBuilderTest, IntegrationCreateTableWithUUID) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID("integration-test-uuid");

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "integration-test-uuid");
  EXPECT_EQ(metadata->format_version, 2);
}

TEST_F(TableMetadataBuilderTest, IntegrationOptimisticConcurrencyControl) {
  table::AssignUUID update("new-uuid");

  // Generate and validate requirements
  auto requirements = GenerateRequirements(update, base_metadata_.get());
  for (const auto& req : requirements) {
    auto val_status = req->Validate(base_metadata_.get());
    ASSERT_THAT(val_status, IsOk()) << "Requirement validation failed";
  }

  // Apply update and build
  auto builder = TableMetadataBuilder::BuildFrom(base_metadata_.get());
  update.ApplyTo(*builder);

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  ASSERT_NE(metadata, nullptr);
}

}  // namespace iceberg
