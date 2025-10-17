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

#include "iceberg/exception.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirements.h"

namespace iceberg {

// AssignTableUUID

void AssignTableUUID::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status AssignTableUUID::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("AssignTableUUID::GenerateRequirements not implemented");
}

// UpgradeTableFormatVersion

void UpgradeTableFormatVersion::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status UpgradeTableFormatVersion::GenerateRequirements(
    TableUpdateContext& context) const {
  return NotImplemented(
      "UpgradeTableFormatVersion::GenerateRequirements not implemented");
}

// AddTableSchema

void AddTableSchema::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status AddTableSchema::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("AddTableSchema::GenerateRequirements not implemented");
}

// SetCurrentTableSchema

void SetCurrentTableSchema::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetCurrentTableSchema::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("SetCurrentTableSchema::GenerateRequirements not implemented");
}

// AddTablePartitionSpec

void AddTablePartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status AddTablePartitionSpec::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("AddTablePartitionSpec::GenerateRequirements not implemented");
}

// SetDefaultTablePartitionSpec

void SetDefaultTablePartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetDefaultTablePartitionSpec::GenerateRequirements(
    TableUpdateContext& context) const {
  return NotImplemented(
      "SetDefaultTablePartitionSpec::GenerateRequirements not implemented");
}

// RemoveTablePartitionSpecs

void RemoveTablePartitionSpecs::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status RemoveTablePartitionSpecs::GenerateRequirements(
    TableUpdateContext& context) const {
  return NotImplemented(
      "RemoveTablePartitionSpecs::GenerateRequirements not implemented");
}

// RemoveTableSchemas

void RemoveTableSchemas::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status RemoveTableSchemas::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("RemoveTableSchemas::GenerateRequirements not implemented");
}

// AddTableSortOrder

void AddTableSortOrder::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status AddTableSortOrder::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("AddTableSortOrder::GenerateRequirements not implemented");
}

// SetDefaultTableSortOrder

void SetDefaultTableSortOrder::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetDefaultTableSortOrder::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("SetDefaultTableSortOrder::GenerateRequirements not implemented");
}

// AddTableSnapshot

void AddTableSnapshot::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status AddTableSnapshot::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("AddTableSnapshot::GenerateRequirements not implemented");
}

// RemoveTableSnapshots

void RemoveTableSnapshots::ApplyTo(TableMetadataBuilder& builder) const {}

Status RemoveTableSnapshots::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("RemoveTableSnapshots::GenerateRequirements not implemented");
}

// RemoveTableSnapshotRef

void RemoveTableSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status RemoveTableSnapshotRef::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("RemoveTableSnapshotRef::GenerateRequirements not implemented");
}

// SetTableSnapshotRef

void SetTableSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetTableSnapshotRef::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("SetTableSnapshotRef::GenerateRequirements not implemented");
}

// SetTableProperties

void SetTableProperties::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetTableProperties::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("SetTableProperties::GenerateRequirements not implemented");
}

// RemoveTableProperties

void RemoveTableProperties::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status RemoveTableProperties::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("RemoveTableProperties::GenerateRequirements not implemented");
}

// SetTableLocation

void SetTableLocation::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetTableLocation::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("SetTableLocation::GenerateRequirements not implemented");
}

}  // namespace iceberg
