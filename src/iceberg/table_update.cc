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

#include "iceberg/table_metadata.h"
#include "iceberg/table_requirements.h"

namespace iceberg {

// AssignUUID

void AssignUUID::ApplyTo(TableMetadataBuilder& builder) const {}

Status AssignUUID::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("AssignUUID::GenerateRequirements not implemented");
}

// UpgradeFormatVersion

void UpgradeFormatVersion::ApplyTo(TableMetadataBuilder& builder) const {}

Status UpgradeFormatVersion::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("UpgradeFormatVersion::GenerateRequirements not implemented");
}

// AddSchema

void AddSchema::ApplyTo(TableMetadataBuilder& builder) const {}

Status AddSchema::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("AddSchema::GenerateRequirements not implemented");
}

// SetCurrentSchema

void SetCurrentSchema::ApplyTo(TableMetadataBuilder& builder) const {}

Status SetCurrentSchema::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("SetCurrentSchema::GenerateRequirements not implemented");
}

// AddPartitionSpec

void AddPartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {}

Status AddPartitionSpec::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("AddPartitionSpec::GenerateRequirements not implemented");
}

// SetDefaultPartitionSpec

void SetDefaultPartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {}

Status SetDefaultPartitionSpec::GenerateRequirements(
    MetadataUpdateContext& context) const {
  return NotImplemented("SetDefaultPartitionSpec::GenerateRequirements not implemented");
}

// RemovePartitionSpecs

void RemovePartitionSpecs::ApplyTo(TableMetadataBuilder& builder) const {}

Status RemovePartitionSpecs::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("RemovePartitionSpecs::GenerateRequirements not implemented");
}

// RemoveSchemas

void RemoveSchemas::ApplyTo(TableMetadataBuilder& builder) const {}

Status RemoveSchemas::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("RemoveSchemas::GenerateRequirements not implemented");
}

// AddSortOrder

void AddSortOrder::ApplyTo(TableMetadataBuilder& builder) const {}

Status AddSortOrder::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("AddSortOrder::GenerateRequirements not implemented");
}

// SetDefaultSortOrder

void SetDefaultSortOrder::ApplyTo(TableMetadataBuilder& builder) const {}

Status SetDefaultSortOrder::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("SetDefaultSortOrder::GenerateRequirements not implemented");
}

// AddSnapshot

void AddSnapshot::ApplyTo(TableMetadataBuilder& builder) const {}

Status AddSnapshot::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("AddSnapshot::GenerateRequirements not implemented");
}

// RemoveSnapshots

void RemoveSnapshots::ApplyTo(TableMetadataBuilder& builder) const {}

Status RemoveSnapshots::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("RemoveSnapshots::GenerateRequirements not implemented");
}

// RemoveSnapshotRef

void RemoveSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {}

Status RemoveSnapshotRef::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("RemoveSnapshotRef::GenerateRequirements not implemented");
}

// SetSnapshotRef

void SetSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {}

Status SetSnapshotRef::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("SetSnapshotRef::GenerateRequirements not implemented");
}

// SetProperties

void SetProperties::ApplyTo(TableMetadataBuilder& builder) const {}

Status SetProperties::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("SetProperties::GenerateRequirements not implemented");
}

// RemoveProperties

void RemoveProperties::ApplyTo(TableMetadataBuilder& builder) const {}

Status RemoveProperties::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("RemoveProperties::GenerateRequirements not implemented");
}

// SetLocation

void SetLocation::ApplyTo(TableMetadataBuilder& builder) const {}

Status SetLocation::GenerateRequirements(MetadataUpdateContext& context) const {
  return NotImplemented("SetLocation::GenerateRequirements not implemented");
}

}  // namespace iceberg
