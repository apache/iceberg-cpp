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

#include "iceberg/table_requirement.h"

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

TEST(TableRequirementTest, AssertUUID) {
  auto base = std::make_unique<TableMetadata>();
  base->table_uuid = "test-uuid-1234";

  // Success - UUID matches
  table::AssertUUID requirement("test-uuid-1234");
  ASSERT_THAT(requirement.Validate(base.get()), IsOk());

  // UUID mismatch
  table::AssertUUID wrong_uuid("wrong-uuid");
  auto status = wrong_uuid.Validate(base.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("UUID does not match"));

  // Null base metadata
  table::AssertUUID any_uuid("any-uuid");
  status = any_uuid.Validate(nullptr);
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("metadata is missing"));

  // Case insensitive UUID comparison
  base->table_uuid = "TEST-UUID-1234";
  table::AssertUUID lowercase_uuid("test-uuid-1234");
  ASSERT_THAT(lowercase_uuid.Validate(base.get()), IsOk());
}

TEST(TableRequirementTest, AssertCurrentSchemaID) {
  auto base = std::make_unique<TableMetadata>();
  base->current_schema_id = 5;

  // Success - schema ID matches
  table::AssertCurrentSchemaID requirement(5);
  ASSERT_THAT(requirement.Validate(base.get()), IsOk());

  // Schema ID mismatch
  table::AssertCurrentSchemaID wrong_id(10);
  auto status = wrong_id.Validate(base.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("schema ID does not match"));

  // Null base metadata
  table::AssertCurrentSchemaID req_for_null(5);
  status = req_for_null.Validate(nullptr);
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("metadata is missing"));

  // Schema ID not set
  base->current_schema_id = std::nullopt;
  table::AssertCurrentSchemaID req_for_nullopt(5);
  status = req_for_nullopt.Validate(base.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("schema ID is not set"));
}

TEST(TableRequirementTest, AssertDoesNotExist) {
  // Success - table does not exist (null metadata)
  table::AssertDoesNotExist requirement;
  ASSERT_THAT(requirement.Validate(nullptr), IsOk());

  // Table already exists
  auto base = std::make_unique<TableMetadata>();
  auto status = requirement.Validate(base.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("table already exists"));
}

TEST(TableRequirementTest, AssertRefSnapshotID) {
  auto base = std::make_unique<TableMetadata>();
  auto ref = std::make_shared<SnapshotRef>();
  ref->snapshot_id = 100;
  ref->retention = SnapshotRef::Branch{};
  base->refs["main"] = ref;

  // Success - ref snapshot ID matches
  table::AssertRefSnapshotID requirement("main", 100);
  ASSERT_THAT(requirement.Validate(base.get()), IsOk());

  // Snapshot ID mismatch
  table::AssertRefSnapshotID wrong_id("main", 200);
  auto status = wrong_id.Validate(base.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("has changed"));

  // Ref missing
  table::AssertRefSnapshotID missing_ref("missing-ref", 100);
  status = missing_ref.Validate(base.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("is missing"));

  // Ref should not exist and doesn't (nullopt snapshot ID)
  table::AssertRefSnapshotID nonexistent("nonexistent", std::nullopt);
  ASSERT_THAT(nonexistent.Validate(base.get()), IsOk());

  // Ref should not exist but does (nullopt snapshot ID but ref exists)
  table::AssertRefSnapshotID exists_but_shouldnt("main", std::nullopt);
  status = exists_but_shouldnt.Validate(base.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("created concurrently"));
}

TEST(TableRequirementTest, AssertLastAssignedFieldId) {
  auto base = std::make_unique<TableMetadata>();
  base->last_column_id = 10;

  // Success - field ID matches
  table::AssertLastAssignedFieldId requirement(10);
  ASSERT_THAT(requirement.Validate(base.get()), IsOk());

  // Field ID mismatch
  table::AssertLastAssignedFieldId wrong_id(15);
  auto status = wrong_id.Validate(base.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("last assigned field ID does not match"));

  // Null base metadata (should succeed)
  table::AssertLastAssignedFieldId req_for_null(10);
  EXPECT_THAT(req_for_null.Validate(nullptr), IsOk());
}

TEST(TableRequirementTest, AssertLastAssignedPartitionId) {
  auto base = std::make_unique<TableMetadata>();
  base->last_partition_id = 5;

  // Success - partition ID matches
  table::AssertLastAssignedPartitionId requirement(5);
  ASSERT_THAT(requirement.Validate(base.get()), IsOk());

  // Partition ID mismatch
  table::AssertLastAssignedPartitionId wrong_id(8);
  auto status = wrong_id.Validate(base.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("last assigned partition ID does not match"));

  // Null base metadata
  table::AssertLastAssignedPartitionId req_for_null(5);
  status = req_for_null.Validate(nullptr);
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("metadata is missing"));
}

TEST(TableRequirementTest, AssertDefaultSpecID) {
  auto base = std::make_unique<TableMetadata>();
  base->default_spec_id = 3;

  // Success - spec ID matches
  table::AssertDefaultSpecID requirement(3);
  ASSERT_THAT(requirement.Validate(base.get()), IsOk());

  // Spec ID mismatch
  table::AssertDefaultSpecID wrong_id(7);
  auto status = wrong_id.Validate(base.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("spec changed"));
}

TEST(TableRequirementTest, AssertDefaultSortOrderID) {
  auto base = std::make_unique<TableMetadata>();
  base->default_sort_order_id = 2;

  // Success - sort order ID matches
  table::AssertDefaultSortOrderID requirement(2);
  ASSERT_THAT(requirement.Validate(base.get()), IsOk());

  // Sort order ID mismatch
  table::AssertDefaultSortOrderID wrong_id(4);
  auto status = wrong_id.Validate(base.get());
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("sort order changed"));
}

}  // namespace iceberg
