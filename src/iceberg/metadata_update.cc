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

#include "iceberg/metadata_update.h"

#include "iceberg/table_metadata.h"
#include "iceberg/update_requirements.h"

namespace iceberg {

// AssignUUID

void AssignUUID::ApplyTo(TableMetadataBuilder& builder) const {
  builder.AssignUUID(uuid_);
}

void AssignUUID::GenerateRequirements(UpdateRequirementsContext& /*context*/) const {
  // AssignUUID doesn't generate any requirements
}

// UpgradeFormatVersion

void UpgradeFormatVersion::ApplyTo(TableMetadataBuilder& builder) const {
  builder.UpgradeFormatVersion(format_version_);
}

void UpgradeFormatVersion::GenerateRequirements(
    UpdateRequirementsContext& /*context*/) const {
  // UpgradeFormatVersion doesn't generate any requirements
}

// AddSchema

void AddSchema::ApplyTo(TableMetadataBuilder& builder) const {
  builder.AddSchema(schema_);
}

void AddSchema::GenerateRequirements(UpdateRequirementsContext& context) const {
  if (context.base() != nullptr) {
    context.AddRequirement(
        std::make_unique<AssertLastAssignedFieldId>(context.base()->last_column_id));
  }
}

// SetCurrentSchema

void SetCurrentSchema::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetCurrentSchema(schema_id_);
}

void SetCurrentSchema::GenerateRequirements(UpdateRequirementsContext& context) const {
  // Require current schema not changed
  if (context.base() != nullptr && !context.is_replace()) {
    if (context.base()->current_schema_id.has_value()) {
      context.AddRequirement(std::make_unique<AssertCurrentSchemaID>(
          context.base()->current_schema_id.value()));
    }
  }
}

// AddPartitionSpec

void AddPartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {
  builder.AddPartitionSpec(spec_);
}

void AddPartitionSpec::GenerateRequirements(UpdateRequirementsContext& context) const {
  if (context.base() != nullptr) {
    context.AddRequirement(std::make_unique<AssertLastAssignedPartitionId>(
        context.base()->last_partition_id));
  }
}

// SetDefaultPartitionSpec

void SetDefaultPartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetDefaultPartitionSpec(spec_id_);
}

void SetDefaultPartitionSpec::GenerateRequirements(
    UpdateRequirementsContext& context) const {
  // Require default partition spec not changed
  if (context.base() != nullptr && !context.is_replace()) {
    context.AddRequirement(
        std::make_unique<AssertDefaultSpecID>(context.base()->default_spec_id));
  }
}

// RemovePartitionSpecs

void RemovePartitionSpecs::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemovePartitionSpecs(spec_ids_);
}

void RemovePartitionSpecs::GenerateRequirements(
    UpdateRequirementsContext& context) const {
  // Require default partition spec not changed
  if (context.base() != nullptr && !context.is_replace()) {
    context.AddRequirement(
        std::make_unique<AssertDefaultSpecID>(context.base()->default_spec_id));
  }

  // Require that no branches have changed
  if (context.base() != nullptr && !context.is_replace()) {
    for (const auto& [name, ref] : context.base()->refs) {
      if (ref->type() == SnapshotRefType::kBranch && name != "main") {
        context.AddRequirement(
            std::make_unique<AssertRefSnapshotID>(name, ref->snapshot_id));
      }
    }
  }
}

// RemoveSchemas

void RemoveSchemas::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemoveSchemas(schema_ids_);
}

void RemoveSchemas::GenerateRequirements(UpdateRequirementsContext& context) const {
  // Require current schema not changed
  if (context.base() != nullptr && !context.is_replace()) {
    if (context.base()->current_schema_id.has_value()) {
      context.AddRequirement(std::make_unique<AssertCurrentSchemaID>(
          context.base()->current_schema_id.value()));
    }
  }

  // Require that no branches have changed
  if (context.base() != nullptr && !context.is_replace()) {
    for (const auto& [name, ref] : context.base()->refs) {
      if (ref->type() == SnapshotRefType::kBranch && name != "main") {
        context.AddRequirement(
            std::make_unique<AssertRefSnapshotID>(name, ref->snapshot_id));
      }
    }
  }
}

// AddSortOrder

void AddSortOrder::ApplyTo(TableMetadataBuilder& builder) const {
  builder.AddSortOrder(sort_order_);
}

void AddSortOrder::GenerateRequirements(UpdateRequirementsContext& /*context*/) const {
  // AddSortOrder doesn't generate any requirements
}

// SetDefaultSortOrder

void SetDefaultSortOrder::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetDefaultSortOrder(sort_order_id_);
}

void SetDefaultSortOrder::GenerateRequirements(UpdateRequirementsContext& context) const {
  if (context.base() != nullptr && !context.is_replace()) {
    context.AddRequirement(std::make_unique<AssertDefaultSortOrderID>(
        context.base()->default_sort_order_id));
  }
}

// AddSnapshot

void AddSnapshot::ApplyTo(TableMetadataBuilder& builder) const {
  builder.AddSnapshot(snapshot_);
}

void AddSnapshot::GenerateRequirements(UpdateRequirementsContext& /*context*/) const {
  // AddSnapshot doesn't generate any requirements
}

// RemoveSnapshots

void RemoveSnapshots::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemoveSnapshots(snapshot_ids_);
}

void RemoveSnapshots::GenerateRequirements(UpdateRequirementsContext& /*context*/) const {
  // RemoveSnapshots doesn't generate any requirements
}

// RemoveSnapshotRef

void RemoveSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemoveRef(ref_name_);
}

void RemoveSnapshotRef::GenerateRequirements(
    UpdateRequirementsContext& /*context*/) const {
  // RemoveSnapshotRef doesn't generate any requirements
}

// SetSnapshotRef

void SetSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {
  // Create a SnapshotRef based on the type
  std::shared_ptr<SnapshotRef> ref;

  if (type_ == SnapshotRefType::kBranch) {
    SnapshotRef::Branch branch;
    branch.min_snapshots_to_keep = min_snapshots_to_keep_;
    branch.max_snapshot_age_ms = max_snapshot_age_ms_;
    branch.max_ref_age_ms = max_ref_age_ms_;

    ref = std::make_shared<SnapshotRef>();
    ref->snapshot_id = snapshot_id_;
    ref->retention = branch;
  } else {
    SnapshotRef::Tag tag;
    tag.max_ref_age_ms = max_ref_age_ms_;

    ref = std::make_shared<SnapshotRef>();
    ref->snapshot_id = snapshot_id_;
    ref->retention = tag;
  }

  builder.SetRef(ref_name_, ref);
}

void SetSnapshotRef::GenerateRequirements(UpdateRequirementsContext& context) const {
  // Require that the ref is unchanged from the base
  if (context.base() != nullptr && !context.is_replace()) {
    // Find the reference in the base metadata
    auto it = context.base()->refs.find(ref_name_);
    std::optional<int64_t> base_snapshot_id;

    if (it != context.base()->refs.end()) {
      base_snapshot_id = it->second->snapshot_id;
    }

    // Require that the ref does not exist (nullopt) or is the same as the base snapshot
    context.AddRequirement(
        std::make_unique<AssertRefSnapshotID>(ref_name_, base_snapshot_id));
  }
}

// SetProperties

void SetProperties::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetProperties(updated_);
}

void SetProperties::GenerateRequirements(UpdateRequirementsContext& /*context*/) const {
  // SetProperties doesn't generate any requirements
}

// RemoveProperties

void RemoveProperties::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemoveProperties(removed_);
}

void RemoveProperties::GenerateRequirements(
    UpdateRequirementsContext& /*context*/) const {
  // RemoveProperties doesn't generate any requirements
}

// SetLocation

void SetLocation::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetLocation(location_);
}

void SetLocation::GenerateRequirements(UpdateRequirementsContext& /*context*/) const {
  // SetLocation doesn't generate any requirements
}

}  // namespace iceberg
