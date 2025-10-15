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

#include "iceberg/update_requirement.h"

#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

Status AssertTableDoesNotExist::Validate(const TableMetadata* base) const {
  if (base != nullptr) {
    return CommitFailed("Requirement failed: table already exists");
  }
  return {};
}

Status AssertTableUUID::Validate(const TableMetadata* base) const {
  if (base == nullptr) {
    return CommitFailed("Requirement failed: table does not exist");
  }

  if (uuid_.empty()) {
    return CommitFailed("Requirement failed: expected non-empty UUID");
  }

  if (!StringUtils::EqualsIgnoreCase(uuid_, base->table_uuid)) {
    return CommitFailed("Requirement failed: UUID does not match: expected {} != {}",
                        base->table_uuid, uuid_);
  }

  return {};
}

Status AssertRefSnapshotID::Validate(const TableMetadata* base) const {
  if (base == nullptr) {
    return CommitFailed("Requirement failed: table does not exist");
  }

  // Find the reference in the table metadata
  auto it = base->refs.find(ref_name_);

  if (it != base->refs.end()) {
    // Reference exists
    const auto& ref = it->second;
    std::string type = (ref->type() == SnapshotRefType::kBranch) ? "branch" : "tag";

    if (!snapshot_id_.has_value()) {
      // A null snapshot ID means the ref should not exist already
      return CommitFailed("Requirement failed: {} {} was created concurrently", type,
                          ref_name_);
    }
    if (snapshot_id_.value() != ref->snapshot_id) {
      return CommitFailed("Requirement failed: {} {} has changed: expected id {} != {}",
                          type, ref_name_, snapshot_id_.value(), ref->snapshot_id);
    }
  } else {
    // Reference does not exist
    if (snapshot_id_.has_value()) {
      return CommitFailed("Requirement failed: branch or tag {} is missing, expected {}",
                          ref_name_, snapshot_id_.value());
    }
  }

  return {};
}

Status AssertLastAssignedFieldId::Validate(const TableMetadata* base) const {
  if (base != nullptr && base->last_column_id != last_assigned_field_id_) {
    return CommitFailed(
        "Requirement failed: last assigned field id changed: expected id {} != {}",
        last_assigned_field_id_, base->last_column_id);
  }
  return {};
}

Status AssertCurrentSchemaID::Validate(const TableMetadata* base) const {
  if (base == nullptr) {
    return CommitFailed("Requirement failed: table does not exist");
  }

  if (!base->current_schema_id.has_value()) {
    return CommitFailed("Requirement failed: table has no current schema");
  }

  if (schema_id_ != base->current_schema_id.value()) {
    return CommitFailed(
        "Requirement failed: current schema changed: expected id {} != {}", schema_id_,
        base->current_schema_id.value());
  }

  return {};
}

Status AssertLastAssignedPartitionId::Validate(const TableMetadata* base) const {
  if (base != nullptr && base->last_partition_id != last_assigned_partition_id_) {
    return CommitFailed(
        "Requirement failed: last assigned partition id changed: expected id {} != {}",
        last_assigned_partition_id_, base->last_partition_id);
  }
  return {};
}

Status AssertDefaultSpecID::Validate(const TableMetadata* base) const {
  if (base == nullptr) {
    return CommitFailed("Requirement failed: table does not exist");
  }

  if (spec_id_ != base->default_spec_id) {
    return CommitFailed(
        "Requirement failed: default partition spec changed: expected id {} != {}",
        spec_id_, base->default_spec_id);
  }

  return {};
}

Status AssertDefaultSortOrderID::Validate(const TableMetadata* base) const {
  if (base == nullptr) {
    return CommitFailed("Requirement failed: table does not exist");
  }

  if (sort_order_id_ != base->default_sort_order_id) {
    return CommitFailed(
        "Requirement failed: default sort order changed: expected id {} != {}",
        sort_order_id_, base->default_sort_order_id);
  }

  return {};
}

}  // namespace iceberg
