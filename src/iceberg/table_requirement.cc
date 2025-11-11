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
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg::table {

Status AssertDoesNotExist::Validate(const TableMetadata* base) const {
  if (base != nullptr) {
    return CommitFailed("Requirement failed: table already exists");
  }

  return {};
}

Status AssertUUID::Validate(const TableMetadata* base) const {
  if (base == nullptr) {
    return CommitFailed("Requirement failed: current table metadata is missing");
  }

  if (!StringUtils::EqualsIgnoreCase(base->table_uuid, uuid_)) {
    return CommitFailed(
        "Requirement failed: table UUID does not match, expected {} != {}", uuid_,
        base->table_uuid);
  }

  return {};
}

Status AssertRefSnapshotID::Validate(const TableMetadata* base) const {
  if (base == nullptr) {
    return CommitFailed("Requirement failed: current table metadata is missing");
  }

  if (auto ref = base->refs.find(ref_name_); ref != base->refs.cend()) {
    const auto& snapshot_ref = ref->second;
    ICEBERG_DCHECK(snapshot_ref != nullptr, "Snapshot reference is null");
    std::string_view type =
        snapshot_ref->type() == SnapshotRefType::kBranch ? "branch" : "tag";
    if (!snapshot_id_.has_value()) {
      // A null snapshot ID means the ref should not exist already
      return CommitFailed("Requirement failed: {} '{}' was created concurrently", type,
                          ref_name_);
    } else if (snapshot_id_.value() != snapshot_ref->snapshot_id) {
      return CommitFailed("Requirement failed: {} '{}' has changed: expected id {} != {}",
                          type, ref_name_, snapshot_id_.value(),
                          snapshot_ref->snapshot_id);
    }
  } else if (snapshot_id_.has_value()) {
    return CommitFailed("Requirement failed: branch or tag '{}' is missing, expected {}",
                        ref_name_, snapshot_id_.value());
  }

  return {};
}

Status AssertLastAssignedFieldId::Validate(const TableMetadata* base) const {
  if (base && base->last_column_id != last_assigned_field_id_) {
    return CommitFailed(
        "Requirement failed: last assigned field ID does not match, expected {} != {}",
        last_assigned_field_id_, base->last_column_id);
  }

  return {};
}

Status AssertCurrentSchemaID::Validate(const TableMetadata* base) const {
  if (base == nullptr) {
    return CommitFailed("Requirement failed: current table metadata is missing");
  }

  if (!base->current_schema_id.has_value()) {
    return CommitFailed(
        "Requirement failed: current schema ID is not set in table metadata");
  }

  if (base->current_schema_id.value() != schema_id_) {
    return CommitFailed(
        "Requirement failed: current schema ID does not match, expected {} != {}",
        schema_id_, base->current_schema_id.value());
  }

  return {};
}

Status AssertLastAssignedPartitionId::Validate(const TableMetadata* base) const {
  if (base == nullptr) {
    return CommitFailed("Requirement failed: current table metadata is missing");
  }

  if (base && base->last_partition_id != last_assigned_partition_id_) {
    return CommitFailed(
        "Requirement failed: last assigned partition ID does not match, expected {} != "
        "{}",
        last_assigned_partition_id_, base->last_partition_id);
  }

  return {};
}

Status AssertDefaultSpecID::Validate(const TableMetadata* base) const {
  if (base == nullptr) {
    return CommitFailed("Requirement failed: current table metadata is missing");
  }

  if (base->default_spec_id != spec_id_) {
    return CommitFailed(
        "Requirement failed: default partition spec changed, expected id {} != {}",
        spec_id_, base->default_spec_id);
  }

  return {};
}

Status AssertDefaultSortOrderID::Validate(const TableMetadata* base) const {
  if (base == nullptr) {
    return CommitFailed("Requirement failed: current table metadata is missing");
  }

  if (base->default_sort_order_id != sort_order_id_) {
    return CommitFailed(
        "Requirement failed: default sort order changed: expected id {} != {}",
        sort_order_id_, base->default_sort_order_id);
  }

  return {};
}

}  // namespace iceberg::table
