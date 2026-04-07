
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

#include <format>
#include <memory>
#include <optional>

#include "iceberg/catalog.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/statistics_file.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/update/expire_snapshots.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/pending_update.h"
#include "iceberg/update/set_snapshot.h"
#include "iceberg/update/snapshot_manager.h"
#include "iceberg/update/snapshot_update.h"
#include "iceberg/update/update_location.h"
#include "iceberg/update/update_partition_spec.h"
#include "iceberg/update/update_partition_statistics.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/update/update_schema.h"
#include "iceberg/update/update_snapshot_reference.h"
#include "iceberg/update/update_sort_order.h"
#include "iceberg/update/update_statistics.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/retry_util.h"

namespace iceberg {

// ---------------------------------------------------------------------------
// TransactionContext
// ---------------------------------------------------------------------------

TransactionContext::TransactionContext() = default;
TransactionContext::~TransactionContext() = default;

Result<std::shared_ptr<TransactionContext>> TransactionContext::Make(
    std::shared_ptr<Table> table, TransactionKind kind) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  auto ctx = std::make_shared<TransactionContext>();
  ctx->kind = kind;
  ctx->table = std::move(table);
  if (kind == TransactionKind::kCreate) {
    ctx->metadata_builder = TableMetadataBuilder::BuildFromEmpty();
    std::ignore = ctx->metadata_builder->ApplyChangesForCreate(*ctx->table->metadata());
  } else {
    ctx->metadata_builder = TableMetadataBuilder::BuildFrom(ctx->table->metadata().get());
  }
  return ctx;
}

const TableMetadata* TransactionContext::base() const { return metadata_builder->base(); }

const TableMetadata& TransactionContext::current() const {
  return metadata_builder->current();
}

std::string TransactionContext::MetadataFileLocation(std::string_view filename) const {
  const auto metadata_location =
      current().properties.Get(TableProperties::kWriteMetadataLocation);
  if (metadata_location.empty()) {
    return std::format("{}/metadata/{}", current().location, filename);
  }
  return std::format("{}/{}", LocationUtil::StripTrailingSlash(metadata_location),
                     filename);
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

Transaction::Transaction(std::shared_ptr<TransactionContext> ctx)
    : ctx_(std::move(ctx)) {}

Transaction::~Transaction() = default;

Result<std::shared_ptr<Transaction>> Transaction::Make(std::shared_ptr<Table> table,
                                                       TransactionKind kind) {
  ICEBERG_PRECHECK(table && table->catalog(), "Table and catalog cannot be null");
  ICEBERG_ASSIGN_OR_RAISE(auto ctx, TransactionContext::Make(std::move(table), kind));
  auto txn = std::shared_ptr<Transaction>(new Transaction(ctx));
  ctx->transaction = std::weak_ptr<Transaction>(txn);
  return txn;
}

Result<std::shared_ptr<Transaction>> Transaction::Make(
    std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(ctx != nullptr, "TransactionContext cannot be null");
  auto txn = std::shared_ptr<Transaction>(new Transaction(ctx));
  ctx->transaction = std::weak_ptr<Transaction>(txn);
  return txn;
}

const std::shared_ptr<Table>& Transaction::table() const { return ctx_->table; }

const TableMetadata* Transaction::base() const { return ctx_->base(); }

const TableMetadata& Transaction::current() const { return ctx_->current(); }

std::string Transaction::MetadataFileLocation(std::string_view filename) const {
  return ctx_->MetadataFileLocation(filename);
}

Status Transaction::AddUpdate(const std::shared_ptr<PendingUpdate>& update) {
  ICEBERG_CHECK(last_update_committed_,
                "Cannot add update when previous update is not committed");

  pending_updates_.push_back(update);
  last_update_committed_ = false;
  return {};
}

Status Transaction::Apply(PendingUpdate& update) {
  switch (update.kind()) {
    case PendingUpdate::Kind::kExpireSnapshots:
      ICEBERG_RETURN_UNEXPECTED(
          ApplyExpireSnapshots(internal::checked_cast<ExpireSnapshots&>(update)));
      break;
    case PendingUpdate::Kind::kSetSnapshot:
      ICEBERG_RETURN_UNEXPECTED(
          ApplySetSnapshot(internal::checked_cast<SetSnapshot&>(update)));
      break;
    case PendingUpdate::Kind::kUpdateLocation:
      ICEBERG_RETURN_UNEXPECTED(
          ApplyUpdateLocation(internal::checked_cast<UpdateLocation&>(update)));
      break;
    case PendingUpdate::Kind::kUpdatePartitionSpec:
      ICEBERG_RETURN_UNEXPECTED(
          ApplyUpdatePartitionSpec(internal::checked_cast<UpdatePartitionSpec&>(update)));
      break;
    case PendingUpdate::Kind::kUpdateProperties:
      ICEBERG_RETURN_UNEXPECTED(
          ApplyUpdateProperties(internal::checked_cast<UpdateProperties&>(update)));
      break;
    case PendingUpdate::Kind::kUpdateSchema:
      ICEBERG_RETURN_UNEXPECTED(
          ApplyUpdateSchema(internal::checked_cast<UpdateSchema&>(update)));
      break;
    case PendingUpdate::Kind::kUpdateSnapshot:
      ICEBERG_RETURN_UNEXPECTED(
          ApplyUpdateSnapshot(internal::checked_cast<SnapshotUpdate&>(update)));
      break;
    case PendingUpdate::Kind::kUpdateSnapshotReference:
      ICEBERG_RETURN_UNEXPECTED(ApplyUpdateSnapshotReference(
          internal::checked_cast<UpdateSnapshotReference&>(update)));
      break;
    case PendingUpdate::Kind::kUpdateSortOrder:
      ICEBERG_RETURN_UNEXPECTED(
          ApplyUpdateSortOrder(internal::checked_cast<UpdateSortOrder&>(update)));
      break;
    case PendingUpdate::Kind::kUpdateStatistics:
      ICEBERG_RETURN_UNEXPECTED(
          ApplyUpdateStatistics(internal::checked_cast<UpdateStatistics&>(update)));
      break;
    case PendingUpdate::Kind::kUpdatePartitionStatistics:
      ICEBERG_RETURN_UNEXPECTED(ApplyUpdatePartitionStatistics(
          internal::checked_cast<UpdatePartitionStatistics&>(update)));
      break;
    default:
      return NotSupported("Unsupported pending update: {}",
                          static_cast<int32_t>(update.kind()));
  }

  last_update_committed_ = true;

  return {};
}

Status Transaction::ApplyExpireSnapshots(ExpireSnapshots& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Apply());
  if (!result.snapshot_ids_to_remove.empty()) {
    ctx_->metadata_builder->RemoveSnapshots(std::move(result.snapshot_ids_to_remove));
  }
  if (!result.refs_to_remove.empty()) {
    for (const auto& ref_name : result.refs_to_remove) {
      ctx_->metadata_builder->RemoveRef(ref_name);
    }
  }
  if (!result.partition_spec_ids_to_remove.empty()) {
    ctx_->metadata_builder->RemovePartitionSpecs(
        std::move(result.partition_spec_ids_to_remove));
  }
  if (!result.schema_ids_to_remove.empty()) {
    ctx_->metadata_builder->RemoveSchemas(std::move(result.schema_ids_to_remove));
  }
  return {};
}

Status Transaction::ApplySetSnapshot(SetSnapshot& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id, update.Apply());
  ctx_->metadata_builder->SetBranchSnapshot(snapshot_id,
                                            std::string(SnapshotRef::kMainBranch));
  return {};
}

Status Transaction::ApplyUpdateLocation(UpdateLocation& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto location, update.Apply());
  ctx_->metadata_builder->SetLocation(location);
  return {};
}

Status Transaction::ApplyUpdatePartitionSpec(UpdatePartitionSpec& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Apply());
  if (result.set_as_default) {
    ctx_->metadata_builder->SetDefaultPartitionSpec(std::move(result.spec));
  } else {
    ctx_->metadata_builder->AddPartitionSpec(std::move(result.spec));
  }
  return {};
}

Status Transaction::ApplyUpdateProperties(UpdateProperties& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Apply());
  if (!result.updates.empty()) {
    ctx_->metadata_builder->SetProperties(std::move(result.updates));
  }
  if (!result.removals.empty()) {
    ctx_->metadata_builder->RemoveProperties(std::move(result.removals));
  }
  if (result.format_version.has_value()) {
    ctx_->metadata_builder->UpgradeFormatVersion(result.format_version.value());
  }
  return {};
}

Status Transaction::ApplyUpdateSchema(UpdateSchema& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Apply());
  ctx_->metadata_builder->SetCurrentSchema(std::move(result.schema),
                                           result.new_last_column_id);
  if (!result.updated_props.empty()) {
    ctx_->metadata_builder->SetProperties(result.updated_props);
  }

  return {};
}

Status Transaction::ApplyUpdateSnapshot(SnapshotUpdate& update) {
  const auto& base = ctx_->metadata_builder->current();

  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Apply());

  // Create a temp builder to check if this is an empty update
  auto temp_update = TableMetadataBuilder::BuildFrom(&base);
  if (base.SnapshotById(result.snapshot->snapshot_id).has_value()) {
    // This is a rollback operation
    temp_update->SetBranchSnapshot(result.snapshot->snapshot_id, result.target_branch);
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
    change->ApplyTo(*ctx_->metadata_builder);
  }

  // If the table UUID is missing, add it here. the UUID will be re-created each time
  // this operation retries to ensure that if a concurrent operation assigns the UUID,
  // this operation will not fail.
  if (base.table_uuid.empty()) {
    ctx_->metadata_builder->AssignUUID();
  }
  return {};
}

Status Transaction::ApplyUpdateSnapshotReference(UpdateSnapshotReference& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Apply());
  for (const auto& name : result.to_remove) {
    ctx_->metadata_builder->RemoveRef(name);
  }
  for (auto&& [name, ref] : result.to_set) {
    ctx_->metadata_builder->SetRef(std::move(name), std::move(ref));
  }
  return {};
}

Status Transaction::ApplyUpdateSortOrder(UpdateSortOrder& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto sort_order, update.Apply());
  ctx_->metadata_builder->SetDefaultSortOrder(std::move(sort_order));
  return {};
}

Status Transaction::ApplyUpdateStatistics(UpdateStatistics& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Apply());
  for (auto&& [_, stat_file] : result.to_set) {
    ctx_->metadata_builder->SetStatistics(std::move(stat_file));
  }
  for (const auto& snapshot_id : result.to_remove) {
    ctx_->metadata_builder->RemoveStatistics(snapshot_id);
  }
  return {};
}

Status Transaction::ApplyUpdatePartitionStatistics(UpdatePartitionStatistics& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Apply());
  for (auto&& [_, partition_stat_file] : result.to_set) {
    ctx_->metadata_builder->SetPartitionStatistics(std::move(partition_stat_file));
  }
  for (const auto& snapshot_id : result.to_remove) {
    ctx_->metadata_builder->RemovePartitionStatistics(snapshot_id);
  }
  return {};
}

Result<std::shared_ptr<Table>> Transaction::Commit() {
  ICEBERG_CHECK(!committed_, "Transaction already committed");
  ICEBERG_CHECK(last_update_committed_,
                "Cannot commit transaction when previous update is not committed");

  const auto& updates = ctx_->metadata_builder->changes();
  if (updates.empty()) {
    committed_ = true;
    return ctx_->table;
  }

  const auto& props = ctx_->table->properties();
  int32_t num_retries =
      CanRetry() ? static_cast<int32_t>(props.Get(TableProperties::kCommitNumRetries))
                 : 0;
  int32_t min_wait_ms = props.Get(TableProperties::kCommitMinRetryWaitMs);
  int32_t max_wait_ms = props.Get(TableProperties::kCommitMaxRetryWaitMs);
  int32_t total_timeout_ms = props.Get(TableProperties::kCommitTotalRetryTimeMs);

  bool is_first_attempt = true;
  auto commit_result =
      MakeCommitRetryRunner(num_retries, min_wait_ms, max_wait_ms, total_timeout_ms)
          .Run([this, &is_first_attempt]() -> Result<std::shared_ptr<Table>> {
            auto result = CommitOnce(is_first_attempt);
            is_first_attempt = false;
            return result;
          });

  for (const auto& update : pending_updates_) {
    std::ignore = update->Finalize(commit_result.has_value()
                                       ? std::nullopt
                                       : std::make_optional(commit_result.error()));
  }

  ICEBERG_RETURN_UNEXPECTED(commit_result);

  // Mark as committed and update table reference
  committed_ = true;
  ctx_->table = std::move(commit_result.value());

  return ctx_->table;
}

Result<std::shared_ptr<Table>> Transaction::CommitOnce(bool is_first_attempt) {
  std::vector<std::unique_ptr<TableRequirement>> requirements;

  switch (ctx_->kind) {
    case TransactionKind::kCreate: {
      ICEBERG_ASSIGN_OR_RAISE(requirements, TableRequirements::ForCreateTable(
                                                ctx_->metadata_builder->changes()));
    } break;
    case TransactionKind::kUpdate: {
      if (!is_first_attempt) {
        ICEBERG_RETURN_UNEXPECTED(ctx_->table->Refresh());
      }
      if (ctx_->metadata_builder->base() != ctx_->table->metadata().get()) {
        ctx_->metadata_builder =
            TableMetadataBuilder::BuildFrom(ctx_->table->metadata().get());
        for (const auto& update : pending_updates_) {
          ICEBERG_RETURN_UNEXPECTED(Apply(*update));
        }
      }
      ICEBERG_ASSIGN_OR_RAISE(requirements, TableRequirements::ForUpdateTable(
                                                *ctx_->metadata_builder->base(),
                                                ctx_->metadata_builder->changes()));
    } break;
  }

  return ctx_->table->catalog()->UpdateTable(ctx_->table->name(), requirements,
                                             ctx_->metadata_builder->changes());
}

bool Transaction::CanRetry() const {
  if (ctx_->kind == TransactionKind::kCreate) {
    return false;
  }
  for (const auto& update : pending_updates_) {
    if (!update->IsRetryable()) {
      return false;
    }
  }
  return true;
}

Result<std::shared_ptr<UpdatePartitionSpec>> Transaction::NewUpdatePartitionSpec() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdatePartitionSpec> update_spec,
                          UpdatePartitionSpec::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_spec));
  return update_spec;
}

Result<std::shared_ptr<UpdateProperties>> Transaction::NewUpdateProperties() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateProperties> update_properties,
                          UpdateProperties::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_properties));
  return update_properties;
}

Result<std::shared_ptr<UpdateSortOrder>> Transaction::NewUpdateSortOrder() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateSortOrder> update_sort_order,
                          UpdateSortOrder::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_sort_order));
  return update_sort_order;
}

Result<std::shared_ptr<UpdateSchema>> Transaction::NewUpdateSchema() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateSchema> update_schema,
                          UpdateSchema::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_schema));
  return update_schema;
}

Result<std::shared_ptr<ExpireSnapshots>> Transaction::NewExpireSnapshots() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<ExpireSnapshots> expire_snapshots,
                          ExpireSnapshots::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(expire_snapshots));
  return expire_snapshots;
}

Result<std::shared_ptr<UpdateLocation>> Transaction::NewUpdateLocation() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateLocation> update_location,
                          UpdateLocation::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_location));
  return update_location;
}

Result<std::shared_ptr<SetSnapshot>> Transaction::NewSetSnapshot() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<SetSnapshot> set_snapshot,
                          SetSnapshot::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(set_snapshot));
  return set_snapshot;
}

Result<std::shared_ptr<FastAppend>> Transaction::NewFastAppend() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<FastAppend> fast_append,
                          FastAppend::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(fast_append));
  return fast_append;
}

Result<std::shared_ptr<UpdateStatistics>> Transaction::NewUpdateStatistics() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateStatistics> update_statistics,
                          UpdateStatistics::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_statistics));
  return update_statistics;
}

Result<std::shared_ptr<UpdatePartitionStatistics>>
Transaction::NewUpdatePartitionStatistics() {
  ICEBERG_ASSIGN_OR_RAISE(
      std::shared_ptr<UpdatePartitionStatistics> update_partition_statistics,
      UpdatePartitionStatistics::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_partition_statistics));
  return update_partition_statistics;
}

Result<std::shared_ptr<UpdateSnapshotReference>>
Transaction::NewUpdateSnapshotReference() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateSnapshotReference> update_ref,
                          UpdateSnapshotReference::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_ref));
  return update_ref;
}

Result<std::shared_ptr<SnapshotManager>> Transaction::NewSnapshotManager() {
  // SnapshotManager has its own commit logic, so it is not added to the pending updates.
  return SnapshotManager::Make(shared_from_this());
}

}  // namespace iceberg
