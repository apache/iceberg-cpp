
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
#include "iceberg/transaction.h"

#include <memory>
#include <optional>

#include "iceberg/catalog.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/update/expire_snapshots.h"
#include "iceberg/update/pending_update.h"
#include "iceberg/update/snapshot_update.h"
#include "iceberg/update/update_location.h"
#include "iceberg/update/update_partition_spec.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/update/update_schema.h"
#include "iceberg/update/update_sort_order.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Transaction::Transaction(std::shared_ptr<Table> table, Kind kind, bool auto_commit,
                         std::unique_ptr<TableMetadataBuilder> metadata_builder)
    : table_(std::move(table)),
      kind_(kind),
      auto_commit_(auto_commit),
      metadata_builder_(std::move(metadata_builder)) {}

Transaction::~Transaction() = default;

Result<std::shared_ptr<Transaction>> Transaction::Make(std::shared_ptr<Table> table,
                                                       Kind kind, bool auto_commit) {
  if (!table || !table->catalog()) [[unlikely]] {
    return InvalidArgument("Table and catalog cannot be null");
  }

  std::unique_ptr<TableMetadataBuilder> metadata_builder;
  if (kind == Kind::kCreate) {
    metadata_builder = TableMetadataBuilder::BuildFromEmpty();
    std::ignore = metadata_builder->ApplyChangesForCreate(*table->metadata());
  } else {
    metadata_builder = TableMetadataBuilder::BuildFrom(table->metadata().get());
  }

  return std::shared_ptr<Transaction>(
      new Transaction(std::move(table), kind, auto_commit, std::move(metadata_builder)));
}

const TableMetadata* Transaction::base() const { return metadata_builder_->base(); }

const TableMetadata& Transaction::current() const { return metadata_builder_->current(); }

std::string Transaction::MetadataFileLocation(std::string_view filename) const {
  const auto metadata_location =
      current().properties.Get(TableProperties::kWriteMetadataLocation);
  if (metadata_location.empty()) {
    return std::format("{}/{}", LocationUtil::StripTrailingSlash(metadata_location),
                       filename);
  }
  return std::format("{}/metadata/{}", current().location, filename);
}

Status Transaction::AddUpdate(const std::shared_ptr<PendingUpdate>& update) {
  if (!last_update_committed_) {
    return InvalidArgument("Cannot add update when previous update is not committed");
  }
  pending_updates_.emplace_back(std::weak_ptr<PendingUpdate>(update));
  last_update_committed_ = false;
  return {};
}

Status Transaction::Apply(PendingUpdate& update) {
  switch (update.kind()) {
    case PendingUpdate::Kind::kUpdateProperties: {
      auto& update_properties = internal::checked_cast<UpdateProperties&>(update);
      ICEBERG_ASSIGN_OR_RAISE(auto result, update_properties.Apply());
      if (!result.updates.empty()) {
        metadata_builder_->SetProperties(std::move(result.updates));
      }
      if (!result.removals.empty()) {
        metadata_builder_->RemoveProperties(std::move(result.removals));
      }
      if (result.format_version.has_value()) {
        metadata_builder_->UpgradeFormatVersion(result.format_version.value());
      }
    } break;
    case PendingUpdate::Kind::kUpdateSortOrder: {
      auto& update_sort_order = internal::checked_cast<UpdateSortOrder&>(update);
      ICEBERG_ASSIGN_OR_RAISE(auto sort_order, update_sort_order.Apply());
      metadata_builder_->SetDefaultSortOrder(std::move(sort_order));
    } break;
    case PendingUpdate::Kind::kUpdatePartitionSpec: {
      auto& update_partition_spec = internal::checked_cast<UpdatePartitionSpec&>(update);
      ICEBERG_ASSIGN_OR_RAISE(auto result, update_partition_spec.Apply());
      if (result.set_as_default) {
        metadata_builder_->SetDefaultPartitionSpec(std::move(result.spec));
      } else {
        metadata_builder_->AddPartitionSpec(std::move(result.spec));
      }
    } break;
    case PendingUpdate::Kind::kUpdateSchema: {
      auto& update_schema = internal::checked_cast<UpdateSchema&>(update);
      ICEBERG_ASSIGN_OR_RAISE(auto result, update_schema.Apply());
      metadata_builder_->SetCurrentSchema(std::move(result.schema),
                                          result.new_last_column_id);
    } break;
    case PendingUpdate::Kind::kUpdateSnapshot: {
      const auto& base = metadata_builder_->current();

      auto& update_snapshot = internal::checked_cast<SnapshotUpdate&>(update);
      ICEBERG_ASSIGN_OR_RAISE(auto result, update_snapshot.Apply());

      // Create a temp builder to check if this is an empty update
      auto temp_update = TableMetadataBuilder::BuildFrom(&base);
      if (base.SnapshotById(result.snapshot->snapshot_id).has_value()) {
        // This is a rollback operation
        temp_update->SetBranchSnapshot(result.snapshot->snapshot_id,
                                       result.target_branch);
      } else if (result.stage_only) {
        temp_update->AddSnapshot(result.snapshot);
      } else {
        temp_update->SetBranchSnapshot(std::move(result.snapshot), result.target_branch);
      }

      if (temp_update->changes().empty()) {
        // Do not commit if the metadata has not changed. for example, this may happen
        // when setting the current snapshot to an ID that is already current. note that
        // this check uses identity.
        return {};
      }

      for (const auto& change : temp_update->changes()) {
        change->ApplyTo(*metadata_builder_);
      }

      // If the table UUID is missing, add it here. the UUID will be re-created each time
      // this operation retries to ensure that if a concurrent operation assigns the UUID,
      // this operation will not fail.
      if (base.table_uuid.empty()) {
        metadata_builder_->AssignUUID();
      }
    } break;
    case PendingUpdate::Kind::kExpireSnapshots: {
      auto& expire_snapshots = internal::checked_cast<ExpireSnapshots&>(update);
      ICEBERG_ASSIGN_OR_RAISE(auto result, expire_snapshots.Apply());
      if (!result.snapshot_ids_to_remove.empty()) {
        metadata_builder_->RemoveSnapshots(std::move(result.snapshot_ids_to_remove));
      }
      if (!result.refs_to_remove.empty()) {
        for (const auto& ref_name : result.refs_to_remove) {
          metadata_builder_->RemoveRef(ref_name);
        }
      }
      if (!result.partition_spec_ids_to_remove.empty()) {
        metadata_builder_->RemovePartitionSpecs(
            std::move(result.partition_spec_ids_to_remove));
      }
      if (!result.schema_ids_to_remove.empty()) {
        metadata_builder_->RemoveSchemas(std::move(result.schema_ids_to_remove));
      }
    } break;
    case PendingUpdate::Kind::kUpdateLocation: {
      auto& update_location = internal::checked_cast<UpdateLocation&>(update);
      ICEBERG_ASSIGN_OR_RAISE(auto location, update_location.Apply());
      metadata_builder_->SetLocation(location);
    } break;
    default:
      return NotSupported("Unsupported pending update: {}",
                          static_cast<int32_t>(update.kind()));
  }

  last_update_committed_ = true;

  if (auto_commit_) {
    ICEBERG_RETURN_UNEXPECTED(Commit());
  }

  return {};
}

Result<std::shared_ptr<Table>> Transaction::Commit() {
  if (committed_) {
    return Invalid("Transaction already committed");
  }
  if (!last_update_committed_) {
    return InvalidArgument(
        "Cannot commit transaction when previous update is not committed");
  }

  const auto& updates = metadata_builder_->changes();
  if (updates.empty()) {
    committed_ = true;
    return table_;
  }

  std::vector<std::unique_ptr<TableRequirement>> requirements;
  switch (kind_) {
    case Kind::kCreate: {
      ICEBERG_ASSIGN_OR_RAISE(requirements, TableRequirements::ForCreateTable(updates));
    } break;
    case Kind::kUpdate: {
      ICEBERG_ASSIGN_OR_RAISE(requirements, TableRequirements::ForUpdateTable(
                                                *metadata_builder_->base(), updates));

    } break;
  }

  // XXX: we should handle commit failure and retry here.
  auto commit_result =
      table_->catalog()->UpdateTable(table_->name(), requirements, updates);

  for (const auto& update : pending_updates_) {
    if (auto update_ptr = update.lock()) {
      std::ignore = update_ptr->Finalize(commit_result.has_value()
                                             ? std::nullopt
                                             : std::make_optional(commit_result.error()));
    }
  }

  ICEBERG_RETURN_UNEXPECTED(commit_result);

  // Mark as committed and update table reference
  committed_ = true;
  table_ = std::move(commit_result.value());

  return table_;
}

Result<std::shared_ptr<UpdatePartitionSpec>> Transaction::NewUpdatePartitionSpec() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdatePartitionSpec> update_spec,
                          UpdatePartitionSpec::Make(shared_from_this()));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_spec));
  return update_spec;
}

Result<std::shared_ptr<UpdateProperties>> Transaction::NewUpdateProperties() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateProperties> update_properties,
                          UpdateProperties::Make(shared_from_this()));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_properties));
  return update_properties;
}

Result<std::shared_ptr<UpdateSortOrder>> Transaction::NewUpdateSortOrder() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateSortOrder> update_sort_order,
                          UpdateSortOrder::Make(shared_from_this()));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_sort_order));
  return update_sort_order;
}

Result<std::shared_ptr<UpdateSchema>> Transaction::NewUpdateSchema() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateSchema> update_schema,
                          UpdateSchema::Make(shared_from_this()));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_schema));
  return update_schema;
}

Result<std::shared_ptr<ExpireSnapshots>> Transaction::NewExpireSnapshots() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<ExpireSnapshots> expire_snapshots,
                          ExpireSnapshots::Make(shared_from_this()));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(expire_snapshots));
  return expire_snapshots;
}

Result<std::shared_ptr<UpdateLocation>> Transaction::NewUpdateLocation() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateLocation> update_location,
                          UpdateLocation::Make(shared_from_this()));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_location));
  return update_location;
}

}  // namespace iceberg
