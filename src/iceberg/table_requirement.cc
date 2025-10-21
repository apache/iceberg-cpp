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

#include "iceberg/table_metadata.h"
#include "iceberg/util/string_util.h"

namespace iceberg::table {

Status AssertDoesNotExist::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertDoesNotExist::Validate not implemented");
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
  return NotImplemented("AssertTableRefSnapshotID::Validate not implemented");
}

Status AssertLastAssignedFieldId::Validate(const TableMetadata* base) const {
  return NotImplemented(
      "AssertCurrentTableLastAssignedFieldId::Validate not implemented");
}

Status AssertCurrentSchemaID::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertCurrentTableSchemaID::Validate not implemented");
}

Status AssertLastAssignedPartitionId::Validate(const TableMetadata* base) const {
  return NotImplemented(
      "AssertCurrentTableLastAssignedPartitionId::Validate not implemented");
}

Status AssertDefaultSpecID::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertDefaultTableSpecID::Validate not implemented");
}

Status AssertDefaultSortOrderID::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertDefaultTableSortOrderID::Validate not implemented");
}

}  // namespace iceberg::table
