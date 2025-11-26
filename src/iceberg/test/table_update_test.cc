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
  EXPECT_THAT(update.GenerateRequirements(context), IsOk());

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

TEST(TableUpdateTest, AssignUUIDGenerateRequirements) {
  table::AssignUUID update("new-uuid");

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - should generate AssertUUID requirement
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  EXPECT_EQ(existing_table_reqs.size(), 1);

  // Existing table with empty UUID - no requirements
  base->table_uuid = "";
  auto empty_uuid_reqs = GenerateRequirements(update, base.get());
  EXPECT_TRUE(empty_uuid_reqs.empty());
}

TEST(TableUpdateTest, AddSortOrderGenerateRequirements) {
  auto schema = CreateTestSchema();
  SortField sort_field(1, Transform::Identity(), SortDirection::kAscending,
                       NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order_unique,
                         SortOrder::Make(*schema, 1, std::vector<SortField>{sort_field}));
  auto sort_order = std::shared_ptr<SortOrder>(std::move(sort_order_unique));
  table::AddSortOrder update(sort_order);

  // New table - no requirements
  auto new_table_reqs = GenerateRequirements(update, nullptr);
  EXPECT_TRUE(new_table_reqs.empty());

  // Existing table - no requirements (AddSortOrder doesn't generate requirements)
  auto base = CreateBaseMetadata();
  auto existing_table_reqs = GenerateRequirements(update, base.get());
  EXPECT_TRUE(existing_table_reqs.empty());
}

}  // namespace iceberg
