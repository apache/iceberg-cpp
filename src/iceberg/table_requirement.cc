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

namespace iceberg {

Status AssertTableDoesNotExist::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertTableDoesNotExist::Validate not implemented");
}

Status AssertTableUUID::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertTableUUID::Validate not implemented");
}

Status AssertRefSnapshotID::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertRefSnapshotID::Validate not implemented");
}

Status AssertLastAssignedFieldId::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertLastAssignedFieldId::Validate not implemented");
}

Status AssertCurrentSchemaID::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertCurrentSchemaID::Validate not implemented");
}

Status AssertLastAssignedPartitionId::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertLastAssignedPartitionId::Validate not implemented");
}

Status AssertDefaultSpecID::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertDefaultSpecID::Validate not implemented");
}

Status AssertDefaultSortOrderID::Validate(const TableMetadata* base) const {
  return NotImplemented("AssertDefaultSortOrderID::Validate not implemented");
}

}  // namespace iceberg
