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
  builder.AssignUUID(uuid_);
}

void AssignUUID::GenerateRequirements(TableUpdateContext& context) const {
  // AssignUUID does not generate additional requirements.
}

// UpgradeFormatVersion

void UpgradeFormatVersion::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

void UpgradeFormatVersion::GenerateRequirements(TableUpdateContext& context) const {
  // UpgradeFormatVersion doesn't generate any requirements
}

// AddSchema

void AddSchema::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

void AddSchema::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireLastAssignedFieldIdUnchanged();
}

// SetCurrentSchema

void SetCurrentSchema::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

void SetCurrentSchema::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireCurrentSchemaIdUnchanged();
}

// AddPartitionSpec

void AddPartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

void AddPartitionSpec::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireLastAssignedPartitionIdUnchanged();
}

// SetDefaultPartitionSpec

void SetDefaultPartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

void SetDefaultPartitionSpec::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireDefaultSpecIdUnchanged();
}

// RemovePartitionSpecs

void RemovePartitionSpecs::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

void RemovePartitionSpecs::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireDefaultSpecIdUnchanged();
  context.RequireNoBranchesChanged();
}

// RemoveSchemas

void RemoveSchemas::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

void RemoveSchemas::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireCurrentSchemaIdUnchanged();
  context.RequireNoBranchesChanged();
}

// AddSortOrder

void AddSortOrder::ApplyTo(TableMetadataBuilder& builder) const {
  builder.AddSortOrder(sort_order_);
}

void AddSortOrder::GenerateRequirements(TableUpdateContext& context) const {
  // AddSortOrder doesn't generate any requirements
}

// SetDefaultSortOrder

void SetDefaultSortOrder::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetDefaultSortOrder(sort_order_id_);
}

void SetDefaultSortOrder::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireDefaultSortOrderIdUnchanged();
}

// AddSnapshot

void AddSnapshot::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

void AddSnapshot::GenerateRequirements(TableUpdateContext& context) const {
  // AddSnapshot doesn't generate any requirements
}

// RemoveSnapshots

void RemoveSnapshots::ApplyTo(TableMetadataBuilder& builder) const {}

void RemoveSnapshots::GenerateRequirements(TableUpdateContext& context) const {
  // RemoveSnapshots doesn't generate any requirements
}

// RemoveSnapshotRef

void RemoveSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

void RemoveSnapshotRef::GenerateRequirements(TableUpdateContext& context) const {
  // RemoveSnapshotRef doesn't generate any requirements
}

// SetSnapshotRef

void SetSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

void SetSnapshotRef::GenerateRequirements(TableUpdateContext& context) const {
  bool added = context.AddChangedRef(ref_name_);
  if (added && context.base() != nullptr && !context.is_replace()) {
    const auto& refs = context.base()->refs;
    auto it = refs.find(ref_name_);
    // Require that the ref does not exist (nullopt) or is the same as the base snapshot
    std::optional<int64_t> base_snapshot_id =
        (it != refs.end()) ? std::make_optional(it->second->snapshot_id) : std::nullopt;
    context.AddRequirement(
        std::make_unique<table::AssertRefSnapshotID>(ref_name_, base_snapshot_id));
  }
}

// SetProperties

void SetProperties::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetProperties(updated_);
}

void SetProperties::GenerateRequirements(TableUpdateContext& context) const {
  // SetProperties doesn't generate any requirements
}

// RemoveProperties

void RemoveProperties::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemoveProperties(removed_);
}

void RemoveProperties::GenerateRequirements(TableUpdateContext& context) const {
  // RemoveProperties doesn't generate any requirements
}

// SetLocation

void SetLocation::ApplyTo(TableMetadataBuilder& builder) const {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

void SetLocation::GenerateRequirements(TableUpdateContext& context) const {
  // SetLocation doesn't generate any requirements
}

}  // namespace iceberg::table
