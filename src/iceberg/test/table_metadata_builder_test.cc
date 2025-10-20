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

#include <gtest/gtest.h>

#include "gmock/gmock-matchers.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// Helper functions to reduce test boilerplate
namespace {

// Build and assert success, returning the metadata
std::unique_ptr<TableMetadata> BuildAndExpectSuccess(TableMetadataBuilder& builder) {
  auto result = builder.Build();
  EXPECT_TRUE(result.has_value()) << "Build failed: " << result.error().message;
  if (!result.has_value()) {
    return nullptr;
  }
  return std::move(result.value());
}

// Build and expect failure with message containing substring
void BuildAndExpectFailure(TableMetadataBuilder& builder,
                           const std::string& expected_message_substr) {
  auto result = builder.Build();
  ASSERT_FALSE(result.has_value()) << "Build should have failed";
  EXPECT_TRUE(result.error().message.find(expected_message_substr) != std::string::npos)
      << "Expected error message to contain: " << expected_message_substr
      << "\nActual error: " << result.error().message;
}

// Generate requirements and return them
std::vector<std::unique_ptr<TableRequirement>> GenerateRequirements(
    const TableUpdate& update, const TableMetadata* base) {
  TableUpdateContext context(base, false);
  auto status = update.GenerateRequirements(context);
  EXPECT_TRUE(status.has_value())
      << "GenerateRequirements failed: " << status.error().message;

  auto requirements = context.Build();
  EXPECT_TRUE(requirements.has_value());

  if (!requirements.has_value()) {
    return {};
  }
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
    base_metadata_->default_spec_id = TableMetadata::kInitialSpecId;
    base_metadata_->last_partition_id = 0;
    base_metadata_->current_snapshot_id = TableMetadata::kInvalidSnapshotId;
    base_metadata_->default_sort_order_id = TableMetadata::kInitialSortOrderId;
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

  auto metadata = BuildAndExpectSuccess(*builder);
  ASSERT_NE(metadata, nullptr);

  EXPECT_EQ(metadata->format_version, 2);
  EXPECT_EQ(metadata->last_sequence_number, TableMetadata::kInitialSequenceNumber);
  EXPECT_EQ(metadata->default_spec_id, TableMetadata::kInitialSpecId);
  EXPECT_EQ(metadata->default_sort_order_id, TableMetadata::kInitialSortOrderId);
  EXPECT_EQ(metadata->current_snapshot_id, TableMetadata::kInvalidSnapshotId);
}

TEST_F(TableMetadataBuilderTest, BuildFromExisting) {
  auto builder = TableMetadataBuilder::BuildFrom(base_metadata_.get());
  ASSERT_NE(builder, nullptr);

  auto metadata = BuildAndExpectSuccess(*builder);
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

  auto metadata = BuildAndExpectSuccess(*builder);
  EXPECT_EQ(metadata->table_uuid, "new-uuid-5678");
}

TEST_F(TableMetadataBuilderTest, AssignUUIDAndUpdateExisting) {
  auto builder = TableMetadataBuilder::BuildFrom(base_metadata_.get());
  builder->AssignUUID("updated-uuid-9999");

  auto metadata = BuildAndExpectSuccess(*builder);
  EXPECT_EQ(metadata->table_uuid, "updated-uuid-9999");
}

TEST_F(TableMetadataBuilderTest, AssignUUIDWithEmptyUUID) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID("");

  BuildAndExpectFailure(*builder, "Cannot assign null or empty UUID");
}

TEST_F(TableMetadataBuilderTest, AssignUUIDWithSameUUID) {
  auto builder = TableMetadataBuilder::BuildFrom(base_metadata_.get());
  builder->AssignUUID("test-uuid-1234");  // Same UUID

  auto metadata = BuildAndExpectSuccess(*builder);
  EXPECT_EQ(metadata->table_uuid, "test-uuid-1234");
}

TEST_F(TableMetadataBuilderTest, AssignUUIDWithAutoGenerate) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID();  // Auto-generate

  auto metadata = BuildAndExpectSuccess(*builder);
  EXPECT_FALSE(metadata->table_uuid.empty());
}

TEST_F(TableMetadataBuilderTest, AssignUUIDAndCaseInsensitiveComparison) {
  base_metadata_->table_uuid = "TEST-UUID-ABCD";
  auto builder = TableMetadataBuilder::BuildFrom(base_metadata_.get());
  builder->AssignUUID("test-uuid-abcd");  // Different case - should be no-op

  auto metadata = BuildAndExpectSuccess(*builder);
  EXPECT_EQ(metadata->table_uuid, "TEST-UUID-ABCD");  // Original case preserved
}

// ============================================================================
// TableMetadataBuilder - DiscardChanges Tests
// ============================================================================

TEST_F(TableMetadataBuilderTest, DiscardChangesAndResetsToBase) {
  auto builder = TableMetadataBuilder::BuildFrom(base_metadata_.get());
  builder->AssignUUID("new-uuid");
  builder->DiscardChanges();

  auto metadata = BuildAndExpectSuccess(*builder);
  EXPECT_EQ(metadata->table_uuid, "test-uuid-1234");  // Original UUID
}

// ============================================================================
// TableUpdate - ApplyTo Tests
// ============================================================================

TEST_F(TableMetadataBuilderTest, TableUpdateWithAssignUUID) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);

  table::AssignUUID update("apply-uuid");
  update.ApplyTo(*builder);

  auto metadata = BuildAndExpectSuccess(*builder);
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

  auto status = requirement.Validate(base_metadata_.get());

  EXPECT_TRUE(status.has_value()) << "Validation failed: " << status.error().message;
}

TEST_F(TableMetadataBuilderTest, TableRequirementAssertUUIDMismatch) {
  table::AssertUUID requirement("wrong-uuid");

  auto status = requirement.Validate(base_metadata_.get());

  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kCommitFailed);
  EXPECT_TRUE(status.error().message.find("UUID does not match") != std::string::npos);
}

TEST_F(TableMetadataBuilderTest, TableRequirementAssertUUIDNullBase) {
  table::AssertUUID requirement("any-uuid");

  auto status = requirement.Validate(nullptr);

  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kCommitFailed);
  EXPECT_TRUE(status.error().message.find("metadata is missing") != std::string::npos);
}

TEST_F(TableMetadataBuilderTest, TableRequirementAssertUUIDCaseInsensitive) {
  base_metadata_->table_uuid = "TEST-UUID-1234";
  table::AssertUUID requirement("test-uuid-1234");

  auto status = requirement.Validate(base_metadata_.get());

  EXPECT_TRUE(status.has_value())
      << "Validation should succeed with case-insensitive match";
}

// ============================================================================
// Integration Tests - End-to-End Workflow
// ============================================================================

TEST_F(TableMetadataBuilderTest, IntegrationCreateTableWithUUID) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->AssignUUID("integration-test-uuid");

  auto metadata = BuildAndExpectSuccess(*builder);
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

  auto metadata = BuildAndExpectSuccess(*builder);
  ASSERT_NE(metadata, nullptr);
}

}  // namespace iceberg
