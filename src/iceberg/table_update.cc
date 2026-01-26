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
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/statistics_file.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirements.h"
#include "iceberg/util/checked_cast.h"

namespace iceberg {
TableUpdate::~TableUpdate() = default;
}

namespace iceberg::table {

// AssignUUID

void AssignUUID::ApplyTo(TableMetadataBuilder& builder) const {
  builder.AssignUUID(uuid_);
}

void AssignUUID::GenerateRequirements(TableUpdateContext& context) const {
  // AssignUUID does not generate additional requirements.
}

bool AssignUUID::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kAssignUUID) {
    return false;
  }
  const auto& other_assign = internal::checked_cast<const AssignUUID&>(other);
  return uuid_ == other_assign.uuid_;
}

std::unique_ptr<TableUpdate> AssignUUID::Clone() const {
  return std::make_unique<AssignUUID>(uuid_);
}

// UpgradeFormatVersion

void UpgradeFormatVersion::ApplyTo(TableMetadataBuilder& builder) const {
  builder.UpgradeFormatVersion(format_version_);
}

void UpgradeFormatVersion::GenerateRequirements(TableUpdateContext& context) const {
  // UpgradeFormatVersion doesn't generate any requirements
}

bool UpgradeFormatVersion::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kUpgradeFormatVersion) {
    return false;
  }
  const auto& other_upgrade = internal::checked_cast<const UpgradeFormatVersion&>(other);
  return format_version_ == other_upgrade.format_version_;
}

std::unique_ptr<TableUpdate> UpgradeFormatVersion::Clone() const {
  return std::make_unique<UpgradeFormatVersion>(format_version_);
}

// AddSchema

void AddSchema::ApplyTo(TableMetadataBuilder& builder) const {
  builder.AddSchema(schema_);
}

void AddSchema::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireLastAssignedFieldIdUnchanged();
}

bool AddSchema::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kAddSchema) {
    return false;
  }
  const auto& other_add = internal::checked_cast<const AddSchema&>(other);
  if (!schema_ != !other_add.schema_) {
    return false;
  }
  if (schema_ && !(*schema_ == *other_add.schema_)) {
    return false;
  }
  return last_column_id_ == other_add.last_column_id_;
}

std::unique_ptr<TableUpdate> AddSchema::Clone() const {
  return std::make_unique<AddSchema>(schema_, last_column_id_);
}

// SetCurrentSchema

void SetCurrentSchema::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetCurrentSchema(schema_id_);
}

void SetCurrentSchema::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireCurrentSchemaIdUnchanged();
}

bool SetCurrentSchema::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kSetCurrentSchema) {
    return false;
  }
  const auto& other_set = internal::checked_cast<const SetCurrentSchema&>(other);
  return schema_id_ == other_set.schema_id_;
}

std::unique_ptr<TableUpdate> SetCurrentSchema::Clone() const {
  return std::make_unique<SetCurrentSchema>(schema_id_);
}

// AddPartitionSpec

void AddPartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {
  builder.AddPartitionSpec(spec_);
}

void AddPartitionSpec::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireLastAssignedPartitionIdUnchanged();
}

bool AddPartitionSpec::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kAddPartitionSpec) {
    return false;
  }
  const auto& other_add = internal::checked_cast<const AddPartitionSpec&>(other);
  if (!spec_ != !other_add.spec_) {
    return false;
  }
  if (spec_ && *spec_ != *other_add.spec_) {
    return false;
  }
  return true;
}

std::unique_ptr<TableUpdate> AddPartitionSpec::Clone() const {
  return std::make_unique<AddPartitionSpec>(spec_);
}

// SetDefaultPartitionSpec

void SetDefaultPartitionSpec::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetDefaultPartitionSpec(spec_id_);
}

void SetDefaultPartitionSpec::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireDefaultSpecIdUnchanged();
}

bool SetDefaultPartitionSpec::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kSetDefaultPartitionSpec) {
    return false;
  }
  const auto& other_set = internal::checked_cast<const SetDefaultPartitionSpec&>(other);
  return spec_id_ == other_set.spec_id_;
}

std::unique_ptr<TableUpdate> SetDefaultPartitionSpec::Clone() const {
  return std::make_unique<SetDefaultPartitionSpec>(spec_id_);
}

// RemovePartitionSpecs

void RemovePartitionSpecs::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemovePartitionSpecs(spec_ids_);
}

void RemovePartitionSpecs::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireDefaultSpecIdUnchanged();
  context.RequireNoBranchesChanged();
}

bool RemovePartitionSpecs::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kRemovePartitionSpecs) {
    return false;
  }
  const auto& other_remove = internal::checked_cast<const RemovePartitionSpecs&>(other);
  return spec_ids_ == other_remove.spec_ids_;
}

std::unique_ptr<TableUpdate> RemovePartitionSpecs::Clone() const {
  return std::make_unique<RemovePartitionSpecs>(spec_ids_);
}

// RemoveSchemas

void RemoveSchemas::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemoveSchemas(schema_ids_);
}

void RemoveSchemas::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireCurrentSchemaIdUnchanged();
  context.RequireNoBranchesChanged();
}

bool RemoveSchemas::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kRemoveSchemas) {
    return false;
  }
  const auto& other_remove = internal::checked_cast<const RemoveSchemas&>(other);
  return schema_ids_ == other_remove.schema_ids_;
}

std::unique_ptr<TableUpdate> RemoveSchemas::Clone() const {
  return std::make_unique<RemoveSchemas>(schema_ids_);
}

// AddSortOrder

void AddSortOrder::ApplyTo(TableMetadataBuilder& builder) const {
  builder.AddSortOrder(sort_order_);
}

void AddSortOrder::GenerateRequirements(TableUpdateContext& context) const {
  // AddSortOrder doesn't generate any requirements
}

bool AddSortOrder::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kAddSortOrder) {
    return false;
  }
  const auto& other_add = internal::checked_cast<const AddSortOrder&>(other);
  if (!sort_order_ != !other_add.sort_order_) {
    return false;
  }
  if (sort_order_ && !(*sort_order_ == *other_add.sort_order_)) {
    return false;
  }
  return true;
}

std::unique_ptr<TableUpdate> AddSortOrder::Clone() const {
  return std::make_unique<AddSortOrder>(sort_order_);
}

// SetDefaultSortOrder

void SetDefaultSortOrder::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetDefaultSortOrder(sort_order_id_);
}

void SetDefaultSortOrder::GenerateRequirements(TableUpdateContext& context) const {
  context.RequireDefaultSortOrderIdUnchanged();
}

bool SetDefaultSortOrder::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kSetDefaultSortOrder) {
    return false;
  }
  const auto& other_set = internal::checked_cast<const SetDefaultSortOrder&>(other);
  return sort_order_id_ == other_set.sort_order_id_;
}

std::unique_ptr<TableUpdate> SetDefaultSortOrder::Clone() const {
  return std::make_unique<SetDefaultSortOrder>(sort_order_id_);
}

// AddSnapshot

void AddSnapshot::ApplyTo(TableMetadataBuilder& builder) const {
  builder.AddSnapshot(snapshot_);
}

void AddSnapshot::GenerateRequirements(TableUpdateContext& context) const {
  // AddSnapshot doesn't generate any requirements
}

bool AddSnapshot::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kAddSnapshot) {
    return false;
  }
  const auto& other_add = internal::checked_cast<const AddSnapshot&>(other);
  if (!snapshot_ != !other_add.snapshot_) {
    return false;
  }
  if (snapshot_ && *snapshot_ != *other_add.snapshot_) {
    return false;
  }
  return true;
}

std::unique_ptr<TableUpdate> AddSnapshot::Clone() const {
  return std::make_unique<AddSnapshot>(snapshot_);
}

// RemoveSnapshots

void RemoveSnapshots::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemoveSnapshots(snapshot_ids_);
}

void RemoveSnapshots::GenerateRequirements(TableUpdateContext& context) const {
  // RemoveSnapshots doesn't generate any requirements
}

bool RemoveSnapshots::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kRemoveSnapshots) {
    return false;
  }
  const auto& other_remove = internal::checked_cast<const RemoveSnapshots&>(other);
  return snapshot_ids_ == other_remove.snapshot_ids_;
}

std::unique_ptr<TableUpdate> RemoveSnapshots::Clone() const {
  return std::make_unique<RemoveSnapshots>(snapshot_ids_);
}

// RemoveSnapshotRef

void RemoveSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemoveRef(ref_name_);
}

void RemoveSnapshotRef::GenerateRequirements(TableUpdateContext& context) const {
  // RemoveSnapshotRef doesn't generate any requirements
}

bool RemoveSnapshotRef::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kRemoveSnapshotRef) {
    return false;
  }
  const auto& other_remove = internal::checked_cast<const RemoveSnapshotRef&>(other);
  return ref_name_ == other_remove.ref_name_;
}

std::unique_ptr<TableUpdate> RemoveSnapshotRef::Clone() const {
  return std::make_unique<RemoveSnapshotRef>(ref_name_);
}

// SetSnapshotRef

void SetSnapshotRef::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetBranchSnapshot(snapshot_id_, ref_name_);
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

bool SetSnapshotRef::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kSetSnapshotRef) {
    return false;
  }
  const auto& other_set = internal::checked_cast<const SetSnapshotRef&>(other);
  return ref_name_ == other_set.ref_name_ && snapshot_id_ == other_set.snapshot_id_ &&
         type_ == other_set.type_ &&
         min_snapshots_to_keep_ == other_set.min_snapshots_to_keep_ &&
         max_snapshot_age_ms_ == other_set.max_snapshot_age_ms_ &&
         max_ref_age_ms_ == other_set.max_ref_age_ms_;
}

std::unique_ptr<TableUpdate> SetSnapshotRef::Clone() const {
  return std::make_unique<SetSnapshotRef>(ref_name_, snapshot_id_, type_,
                                          min_snapshots_to_keep_, max_snapshot_age_ms_,
                                          max_ref_age_ms_);
}

// SetProperties

void SetProperties::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetProperties(updated_);
}

void SetProperties::GenerateRequirements(TableUpdateContext& context) const {
  // SetProperties doesn't generate any requirements
}

bool SetProperties::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kSetProperties) {
    return false;
  }
  const auto& other_set = internal::checked_cast<const SetProperties&>(other);
  return updated_ == other_set.updated_;
}

std::unique_ptr<TableUpdate> SetProperties::Clone() const {
  return std::make_unique<SetProperties>(updated_);
}

// RemoveProperties

void RemoveProperties::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemoveProperties(removed_);
}

void RemoveProperties::GenerateRequirements(TableUpdateContext& context) const {
  // RemoveProperties doesn't generate any requirements
}

bool RemoveProperties::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kRemoveProperties) {
    return false;
  }
  const auto& other_remove = internal::checked_cast<const RemoveProperties&>(other);
  return removed_ == other_remove.removed_;
}

std::unique_ptr<TableUpdate> RemoveProperties::Clone() const {
  return std::make_unique<RemoveProperties>(removed_);
}

// SetLocation

void SetLocation::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetLocation(location_);
}

void SetLocation::GenerateRequirements(TableUpdateContext& context) const {
  // SetLocation doesn't generate any requirements
}

bool SetLocation::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kSetLocation) {
    return false;
  }
  const auto& other_set = internal::checked_cast<const SetLocation&>(other);
  return location_ == other_set.location_;
}

std::unique_ptr<TableUpdate> SetLocation::Clone() const {
  return std::make_unique<SetLocation>(location_);
}

// SetStatistics

int64_t SetStatistics::snapshot_id() const { return statistics_file_->snapshot_id; }

void SetStatistics::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetStatistics(statistics_file_);
}

void SetStatistics::GenerateRequirements(TableUpdateContext& context) const {
  // SetStatistics doesn't generate any requirements
}

bool SetStatistics::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kSetStatistics) {
    return false;
  }
  const auto& other_set = internal::checked_cast<const SetStatistics&>(other);
  if (!statistics_file_ != !other_set.statistics_file_) {
    return false;
  }
  if (statistics_file_ && !(*statistics_file_ == *other_set.statistics_file_)) {
    return false;
  }
  return true;
}

std::unique_ptr<TableUpdate> SetStatistics::Clone() const {
  return std::make_unique<SetStatistics>(statistics_file_);
}

// RemoveStatistics

void RemoveStatistics::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemoveStatistics(snapshot_id_);
}

void RemoveStatistics::GenerateRequirements(TableUpdateContext& context) const {
  // RemoveStatistics doesn't generate any requirements
}

bool RemoveStatistics::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kRemoveStatistics) {
    return false;
  }
  const auto& other_remove = internal::checked_cast<const RemoveStatistics&>(other);
  return snapshot_id_ == other_remove.snapshot_id_;
}

std::unique_ptr<TableUpdate> RemoveStatistics::Clone() const {
  return std::make_unique<RemoveStatistics>(snapshot_id_);
}

// SetPartitionStatistics

int64_t SetPartitionStatistics::snapshot_id() const {
  return partition_statistics_file_->snapshot_id;
}

void SetPartitionStatistics::ApplyTo(TableMetadataBuilder& builder) const {
  builder.SetPartitionStatistics(partition_statistics_file_);
}

void SetPartitionStatistics::GenerateRequirements(TableUpdateContext& context) const {
  // SetPartitionStatistics doesn't generate any requirements
}

bool SetPartitionStatistics::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kSetPartitionStatistics) {
    return false;
  }
  const auto& other_set = internal::checked_cast<const SetPartitionStatistics&>(other);
  if (!partition_statistics_file_ != !other_set.partition_statistics_file_) {
    return false;
  }
  if (partition_statistics_file_ &&
      !(*partition_statistics_file_ == *other_set.partition_statistics_file_)) {
    return false;
  }
  return true;
}

std::unique_ptr<TableUpdate> SetPartitionStatistics::Clone() const {
  return std::make_unique<SetPartitionStatistics>(partition_statistics_file_);
}

// RemovePartitionStatistics

void RemovePartitionStatistics::ApplyTo(TableMetadataBuilder& builder) const {
  builder.RemovePartitionStatistics(snapshot_id_);
}

void RemovePartitionStatistics::GenerateRequirements(TableUpdateContext& context) const {
  // RemovePartitionStatistics doesn't generate any requirements
}

bool RemovePartitionStatistics::Equals(const TableUpdate& other) const {
  if (other.kind() != Kind::kRemovePartitionStatistics) {
    return false;
  }
  const auto& other_remove =
      internal::checked_cast<const RemovePartitionStatistics&>(other);
  return snapshot_id_ == other_remove.snapshot_id_;
}

std::unique_ptr<TableUpdate> RemovePartitionStatistics::Clone() const {
  return std::make_unique<RemovePartitionStatistics>(snapshot_id_);
}

}  // namespace iceberg::table
