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

namespace iceberg::table {

// AssignUUID

void AssignUUID::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status AssignUUID::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("AssignTableUUID::GenerateRequirements not implemented");
}

// UpgradeFormatVersion

void UpgradeFormatVersion::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status UpgradeFormatVersion::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented(
      "UpgradeTableFormatVersion::GenerateRequirements not implemented");
}

// AddSchema

void AddSchema::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status AddSchema::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("AddTableSchema::GenerateRequirements not implemented");
}

// SetCurrentSchema

void SetCurrentSchema::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetCurrentSchema::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("SetCurrentTableSchema::GenerateRequirements not implemented");
}

// AddPartitionSpec

void AddPartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status AddPartitionSpec::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("AddTablePartitionSpec::GenerateRequirements not implemented");
}

// SetDefaultPartitionSpec

void SetDefaultPartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetDefaultPartitionSpec::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented(
      "SetDefaultTablePartitionSpec::GenerateRequirements not implemented");
}

// RemovePartitionSpecs

void RemovePartitionSpecs::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status RemovePartitionSpecs::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented(
      "RemoveTablePartitionSpecs::GenerateRequirements not implemented");
}

// RemoveSchemas

void RemoveSchemas::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status RemoveSchemas::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("RemoveTableSchemas::GenerateRequirements not implemented");
}

// AddSortOrder

void AddSortOrder::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status AddSortOrder::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("AddTableSortOrder::GenerateRequirements not implemented");
}

// SetDefaultSortOrder

void SetDefaultSortOrder::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetDefaultSortOrder::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("SetDefaultTableSortOrder::GenerateRequirements not implemented");
}

// AddSnapshot

void AddSnapshot::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status AddSnapshot::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("AddTableSnapshot::GenerateRequirements not implemented");
}

// RemoveSnapshots

void RemoveSnapshots::ApplyTo(TableMetadataBuilder& builder) const {}

Status RemoveSnapshots::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("RemoveTableSnapshots::GenerateRequirements not implemented");
}

// RemoveSnapshotRef

void RemoveSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status RemoveSnapshotRef::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("RemoveTableSnapshotRef::GenerateRequirements not implemented");
}

// SetSnapshotRef

void SetSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetSnapshotRef::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("SetTableSnapshotRef::GenerateRequirements not implemented");
}

// SetProperties

void SetProperties::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetProperties::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("SetTableProperties::GenerateRequirements not implemented");
}

// RemoveProperties

void RemoveProperties::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status RemoveProperties::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("RemoveTableProperties::GenerateRequirements not implemented");
}

// SetLocation

void SetLocation::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Status SetLocation::GenerateRequirements(TableUpdateContext& context) const {
  return NotImplemented("SetTableLocation::GenerateRequirements not implemented");
}

}  // namespace iceberg::table
