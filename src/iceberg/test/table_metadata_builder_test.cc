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

#include "iceberg/partition_spec.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

namespace {

// Helper function to create base metadata for tests
std::unique_ptr<TableMetadata> CreateBaseMetadata() {
  auto metadata = std::make_unique<TableMetadata>();
  metadata->format_version = 2;
  metadata->table_uuid = "test-uuid-1234";
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

TEST(TableMetadataBuilderTest, SetProperties) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->SetProperties({{"key1", "value1"}, {"key2", "value2"}});

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->properties.size(), 2);
  EXPECT_EQ(metadata->properties["key1"], "value1");
  EXPECT_EQ(metadata->properties["key2"], "value2");

  // Update existing property and add new one
  builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->SetProperties({{"key1", "value1"}});
  builder->SetProperties({{"key1", "new_value1"}, {"key3", "value3"}});

  ICEBERG_UNWRAP_OR_FAIL(metadata, builder->Build());
  EXPECT_EQ(metadata->properties.size(), 2);
  EXPECT_EQ(metadata->properties["key1"], "new_value1");
  EXPECT_EQ(metadata->properties["key3"], "value3");
}

TEST(TableMetadataBuilderTest, RemoveProperties) {
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  builder->SetProperties({{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}});
  builder->RemoveProperties({"key2", "key4"});  // key4 does not exist

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->properties.size(), 2);
  EXPECT_EQ(metadata->properties["key1"], "value1");
  EXPECT_EQ(metadata->properties["key3"], "value3");
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

// Test applying TableUpdate to builder
TEST(TableMetadataBuilderTest, ApplyUpdate) {
  // Apply AssignUUID update
  auto builder = TableMetadataBuilder::BuildFromEmpty(2);
  table::AssignUUID update("apply-uuid");
  update.ApplyTo(*builder);
  // TODO(Li Feiyang): Add more update and `apply` once other build methods are
  // implemented

  ICEBERG_UNWRAP_OR_FAIL(auto metadata, builder->Build());
  EXPECT_EQ(metadata->table_uuid, "apply-uuid");
}

}  // namespace iceberg
