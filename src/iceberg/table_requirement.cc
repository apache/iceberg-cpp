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

#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/string_util.h"

namespace iceberg::table {

Status AssertDoesNotExist::Validate(const TableMetadata* base) const {
  // Validate that the table does not exist

  if (base != nullptr) {
    return CommitFailed("Requirement failed: table already exists");
  }

  return {};
}

Status AssertUUID::Validate(const TableMetadata* base) const {
  // Validate that the table UUID matches the expected value

  if (base == nullptr) {
    return CommitFailed("Requirement failed: current table metadata is missing");
  }

  if (!StringUtils::EqualsIgnoreCase(base->table_uuid, uuid_)) {
    return CommitFailed(
        "Requirement failed: table UUID does not match (expected='{}', actual='{}')",
        uuid_, base->table_uuid);
  }

  return {};
}

Status AssertRefSnapshotID::Validate(const TableMetadata* base) const {
  // Validate that a reference (branch or tag) points to the expected snapshot ID
  // Matches Java implementation logic

  auto it = base->refs.find(ref_name_);
  if (it != base->refs.end()) {
    // Reference exists
    if (!snapshot_id_.has_value()) {
      // A null snapshot ID means the ref should not exist already
      return CommitFailed("Requirement failed: reference '{}' was created concurrently",
                          ref_name_);
    } else if (snapshot_id_.value() != it->second->snapshot_id) {
      return CommitFailed(
          "Requirement failed: reference '{}' has changed: expected id {} != {}",
          ref_name_, snapshot_id_.value(), it->second->snapshot_id);
    }
  } else if (snapshot_id_.has_value()) {
    // Reference does not exist but snapshot_id is specified
    return CommitFailed("Requirement failed: branch or tag '{}' is missing, expected {}",
                        ref_name_, snapshot_id_.value());
  }

  return {};
}

Status AssertLastAssignedFieldId::Validate(const TableMetadata* base) const {
  // Validate that the last assigned field ID matches the expected value
  // Allows base to be null for new tables

  if (base && base->last_column_id != last_assigned_field_id_) {
    return CommitFailed(
        "Requirement failed: last assigned field ID does not match (expected={}, "
        "actual={})",
        last_assigned_field_id_, base->last_column_id);
  }

  return {};
}

Status AssertCurrentSchemaID::Validate(const TableMetadata* base) const {
  // Validate that the current schema ID matches the one used when the metadata was read

  if (base == nullptr) {
    return CommitFailed("Requirement failed: current table metadata is missing");
  }

  if (!base->current_schema_id.has_value()) {
    return CommitFailed(
        "Requirement failed: current schema ID is not set in table metadata");
  }

  if (base->current_schema_id.value() != schema_id_) {
    return CommitFailed(
        "Requirement failed: current schema ID does not match (expected={}, actual={})",
        schema_id_, base->current_schema_id.value());
  }

  return {};
}

Status AssertLastAssignedPartitionId::Validate(const TableMetadata* base) const {
  // Validate that the last assigned partition ID matches the expected value
  // Allows base to be null for new tables

  if (base && base->last_partition_id != last_assigned_partition_id_) {
    return CommitFailed(
        "Requirement failed: last assigned partition ID does not match (expected={}, "
        "actual={})",
        last_assigned_partition_id_, base->last_partition_id);
  }

  return {};
}

Status AssertDefaultSpecID::Validate(const TableMetadata* base) const {
  // Validate that the default partition spec ID matches the expected value
  // Matches Java implementation - assumes base is never null

  if (base->default_spec_id != spec_id_) {
    return CommitFailed(
        "Requirement failed: default partition spec changed: expected id {} != {}",
        spec_id_, base->default_spec_id);
  }

  return {};
}

Status AssertDefaultSortOrderID::Validate(const TableMetadata* base) const {
  // Validate that the default sort order ID matches the expected value
  // Matches Java implementation - assumes base is never null

  if (base->default_sort_order_id != sort_order_id_) {
    return CommitFailed(
        "Requirement failed: default sort order changed: expected id {} != {}",
        sort_order_id_, base->default_sort_order_id);
  }

  return {};
}

}  // namespace iceberg::table
