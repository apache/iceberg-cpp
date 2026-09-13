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

#include <algorithm>
#include <format>
#include <memory>
#include <ranges>

#include "iceberg/catalog.h"
#include "iceberg/location_provider.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/statistics_file.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/update/delete_files.h"
#include "iceberg/update/expire_snapshots.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/merge_append.h"
#include "iceberg/update/overwrite_files.h"
#include "iceberg/update/pending_update.h"
#include "iceberg/update/replace_partitions.h"
#include "iceberg/update/rewrite_files.h"
#include "iceberg/update/row_delta.h"
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
namespace {

class ScopedTrue {
 public:
  explicit ScopedTrue(bool& value) : value_(value) { value_ = true; }
  ~ScopedTrue() { value_ = false; }

  ScopedTrue(const ScopedTrue&) = delete;
  ScopedTrue& operator=(const ScopedTrue&) = delete;

 private:
  bool& value_;
};

}  // namespace

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
  ctx->base_metadata_ = ctx->table->metadata();
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

Result<std::unique_ptr<LocationProvider>> TransactionContext::NewLocationProvider()
    const {
  return iceberg::LocationProvider::Make(current().location, current().properties);
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
  return std::shared_ptr<Transaction>(new Transaction(std::move(ctx)));
}

const std::shared_ptr<Table>& Transaction::table() const { return ctx_->table; }

const TableMetadata* Transaction::base() const { return ctx_->base(); }

const TableMetadata& Transaction::current() const { return ctx_->current(); }

std::string Transaction::MetadataFileLocation(std::string_view filename) const {
  return ctx_->MetadataFileLocation(filename);
}

Status Transaction::CheckActive() const {
  ICEBERG_CHECK(!ctx_->in_progress_, "Cannot reenter a transaction operation");
  ICEBERG_CHECK(
      state_ == TransactionState::kReady || state_ == TransactionState::kUpdatePending,
      "Transaction is terminal");
  return {};
}

Status Transaction::CheckReady() const {
  ICEBERG_CHECK(!ctx_->in_progress_, "Cannot reenter a transaction operation");
  ICEBERG_CHECK(state_ == TransactionState::kReady, "Transaction is not ready (state {})",
                static_cast<int>(state_));
  return {};
}

Status Transaction::AddUpdate(const std::shared_ptr<PendingUpdate>& update) {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_PRECHECK(update && update->ctx_.get() == ctx_.get(),
                   "Update must belong to this transaction context");
  ICEBERG_CHECK(update->phase_ == PendingUpdate::Phase::kMutable,
                "Update has already been used");
  pending_updates_.push_back(update);
  state_ = TransactionState::kUpdatePending;
  return {};
}

Status Transaction::Apply(PendingUpdate& update) {
  ICEBERG_CHECK(!ctx_->in_progress_, "Cannot reenter a transaction operation");
  ICEBERG_CHECK(state_ == TransactionState::kUpdatePending,
                "Transaction has no pending operation (state {})",
                static_cast<int>(state_));
  ICEBERG_CHECK(!pending_updates_.empty() && pending_updates_.back().get() == &update,
                "Update is not the current pending operation");
  ScopedTrue running(ctx_->in_progress_);
  Status status;
  try {
    update.phase_ = PendingUpdate::Phase::kFrozen;
    update.staged_ = true;
    status = update.Freeze();
    if (status) {
      status = ApplyRegistered(update);
    }
  } catch (const std::exception& e) {
    status = ValidationFailed("Update Apply threw: {}", e.what());
  } catch (...) {
    status = ValidationFailed("Update Apply threw an unknown exception");
  }
  if (!status) {
    SetTerminalState(TransactionState::kFailed);
    CleanupUpdates();
    return status;
  }
  state_ = TransactionState::kReady;
  return {};
}

Status Transaction::ReplayApply(PendingUpdate& update) {
  ICEBERG_CHECK(state_ == TransactionState::kReady && ctx_->in_progress_,
                "Replay requires an active transaction commit");
  ICEBERG_CHECK(std::ranges::any_of(pending_updates_,
                                    [&update](const auto& registered) {
                                      return registered.get() == &update;
                                    }),
                "Cannot replay an unregistered update");
  ICEBERG_CHECK(update.phase_ == PendingUpdate::Phase::kFrozen,
                "Cannot replay this update");
  update.staged_ = true;
  return ApplyRegistered(update);
}

Status Transaction::ApplyRegistered(PendingUpdate& update) {
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

  return ctx_->metadata_builder->CheckErrors();
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
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
  return {};
}

Status Transaction::ApplySetSnapshot(SetSnapshot& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id, update.Validate());
  ctx_->metadata_builder->SetBranchSnapshot(snapshot_id,
                                            std::string(SnapshotRef::kMainBranch));
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
  return {};
}

Status Transaction::ApplyUpdateLocation(UpdateLocation& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto location, update.Validate());
  ctx_->metadata_builder->SetLocation(location);
  return {};
}

Status Transaction::ApplyUpdatePartitionSpec(UpdatePartitionSpec& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Validate());
  if (result.set_as_default) {
    ctx_->metadata_builder->SetDefaultPartitionSpec(std::move(result.spec));
  } else {
    ctx_->metadata_builder->AddPartitionSpec(std::move(result.spec));
  }
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
  return {};
}

Status Transaction::ApplyUpdateProperties(UpdateProperties& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Validate());
  if (!result.updates.empty()) {
    ctx_->metadata_builder->SetProperties(std::move(result.updates));
  }
  if (!result.removals.empty()) {
    ctx_->metadata_builder->RemoveProperties(std::move(result.removals));
  }
  if (result.format_version.has_value()) {
    ctx_->metadata_builder->UpgradeFormatVersion(result.format_version.value());
  }
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
  return {};
}

Status Transaction::ApplyUpdateSchema(UpdateSchema& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Validate());
  ctx_->metadata_builder->SetCurrentSchema(std::move(result.schema),
                                           result.new_last_column_id);
  if (!result.updated_props.empty()) {
    ctx_->metadata_builder->SetProperties(result.updated_props);
  }
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());

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
  ICEBERG_RETURN_UNEXPECTED(temp_update->CheckErrors());

  if (temp_update->changes().empty()) {
    // Apply may already have written files. Consume this generation immediately,
    // while retaining its frozen intent for a possible replay against new metadata.
    update.Cleanup();
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
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
  return {};
}

Status Transaction::ApplyUpdateSnapshotReference(UpdateSnapshotReference& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Validate());
  for (const auto& name : result.to_remove) {
    ctx_->metadata_builder->RemoveRef(name);
  }
  for (auto&& [name, ref] : result.to_set) {
    ctx_->metadata_builder->SetRef(std::move(name), std::move(ref));
  }
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
  return {};
}

Status Transaction::ApplyUpdateSortOrder(UpdateSortOrder& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto sort_order, update.Validate());
  ctx_->metadata_builder->SetDefaultSortOrder(std::move(sort_order));
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
  return {};
}

Status Transaction::ApplyUpdateStatistics(UpdateStatistics& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Validate());
  for (auto&& [_, stat_file] : result.to_set) {
    ctx_->metadata_builder->SetStatistics(std::move(stat_file));
  }
  for (const auto& snapshot_id : result.to_remove) {
    ctx_->metadata_builder->RemoveStatistics(snapshot_id);
  }
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
  return {};
}

Status Transaction::ApplyUpdatePartitionStatistics(UpdatePartitionStatistics& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Validate());
  for (auto&& [_, partition_stat_file] : result.to_set) {
    ctx_->metadata_builder->SetPartitionStatistics(std::move(partition_stat_file));
  }
  for (const auto& snapshot_id : result.to_remove) {
    ctx_->metadata_builder->RemovePartitionStatistics(snapshot_id);
  }
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
  return {};
}

Result<std::shared_ptr<Table>> Transaction::Commit() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ScopedTrue running(ctx_->in_progress_);
  Result<std::shared_ptr<Table>> commit_result = ctx_->table;
  bool catalog_state_unknown = false;
  try {
    ConfigureExpirationCleanup();
    auto builder_status = ctx_->metadata_builder->CheckErrors();
    if (!builder_status) {
      commit_result = std::unexpected(builder_status.error());
    } else {
      const auto& props = ctx_->table->properties();
      const int32_t num_retries =
          CanRetry() ? static_cast<int32_t>(props.Get(TableProperties::kCommitNumRetries))
                     : 0;
      bool is_first_attempt = true;
      std::optional<Error> replay_error;
      commit_result =
          MakeCommitRetryRunner(num_retries,
                                props.Get(TableProperties::kCommitMinRetryWaitMs),
                                props.Get(TableProperties::kCommitMaxRetryWaitMs),
                                props.Get(TableProperties::kCommitTotalRetryTimeMs))
              .Run([this, &is_first_attempt, &replay_error,
                    &catalog_state_unknown]() -> Result<std::shared_ptr<Table>> {
                auto result =
                    CommitOnce(is_first_attempt, replay_error, catalog_state_unknown);
                is_first_attempt = false;
                // A replay failure is not another catalog conflict. Stop the
                // runner, then restore the original Apply error for the
                // caller below.
                if (replay_error) {
                  return ValidationFailed("Transaction replay failed");
                }
                return result;
              });
      if (replay_error) {
        commit_result = std::unexpected(std::move(*replay_error));
      }
    }
  } catch (const std::exception& e) {
    // CommitOnce catches exceptions at the catalog boundary separately.
    commit_result = ValidationFailed("Transaction preparation threw: {}", e.what());
  } catch (...) {
    commit_result =
        ValidationFailed("Transaction preparation threw an unknown exception");
  }

  if (!commit_result) {
    if (catalog_state_unknown) {
      SetTerminalState(TransactionState::kCommitStateUnknown);
    } else {
      SetTerminalState(TransactionState::kFailed);
      CleanupUpdates();
    }
    return commit_result;
  }

  ctx_->table = std::move(commit_result.value());
  SetTerminalState(TransactionState::kCommitted);
  FinalizeUpdates(*ctx_->table->metadata());
  return ctx_->table;
}

Status Transaction::Abort() {
  ICEBERG_CHECK(!ctx_->in_progress_, "Cannot reenter a transaction operation");
  if (state_ == TransactionState::kAborted) {
    return {};
  }
  ICEBERG_CHECK(state_ == TransactionState::kReady ||
                    state_ == TransactionState::kUpdatePending ||
                    state_ == TransactionState::kFailed,
                "Cannot abort a committed or unknown transaction");
  ScopedTrue running(ctx_->in_progress_);
  SetTerminalState(TransactionState::kAborted);
  CleanupUpdates();
  return {};
}

void Transaction::SetTerminalState(TransactionState state) {
  state_ = state;
  // Publish every marker before invoking even the first user callback.
  for (const auto& update : pending_updates_) {
    update->phase_ = PendingUpdate::Phase::kTerminal;
  }
}

void Transaction::CleanupUpdates() noexcept {
  for (const auto& update : pending_updates_) {
    update->Cleanup();
  }
}

void Transaction::FinalizeUpdates(const TableMetadata& committed) noexcept {
  for (const auto& update : pending_updates_) {
    update->FinalizeOnce(committed);
  }
}

void Transaction::ConfigureExpirationCleanup() {
  bool may_add_references = false;
  for (const auto& update : pending_updates_ | std::views::reverse) {
    if (update->kind() == PendingUpdate::Kind::kExpireSnapshots) {
      internal::checked_cast<ExpireSnapshots&>(*update).skip_physical_cleanup_ =
          may_add_references;
    }
    may_add_references |= update->MayAddFileReferences();
  }
}

Result<std::shared_ptr<Table>> Transaction::CommitOnce(bool is_first_attempt,
                                                       std::optional<Error>& replay_error,
                                                       bool& catalog_state_unknown) {
  std::vector<std::unique_ptr<TableRequirement>> requirements;
  if (ctx_->kind == TransactionKind::kUpdate) {
    if (!is_first_attempt) {
      ICEBERG_RETURN_UNEXPECTED(ctx_->table->Refresh());
    }
    if (!is_first_attempt ||
        ctx_->metadata_builder->base() != ctx_->table->metadata().get()) {
      ICEBERG_CHECK(CanRetry(),
                    "Cannot rebase a transaction containing a non-retryable update");
      CleanupUpdates();
      ctx_->metadata_builder =
          TableMetadataBuilder::BuildFrom(ctx_->table->metadata().get());
      ctx_->base_metadata_ = ctx_->table->metadata();
      for (const auto& update : pending_updates_) {
        Status applied;
        try {
          applied = ReplayApply(*update);
        } catch (const std::exception& e) {
          applied = ValidationFailed("Replay Apply threw: {}", e.what());
        } catch (...) {
          applied = ValidationFailed("Replay Apply threw an unknown exception");
        }
        if (!applied) {
          replay_error = applied.error();
          return std::unexpected(applied.error());
        }
      }
    }
    if (ctx_->metadata_builder->changes().empty()) {
      return ctx_->table;
    }
    ICEBERG_ASSIGN_OR_RAISE(requirements, TableRequirements::ForUpdateTable(
                                              *ctx_->metadata_builder->base(),
                                              ctx_->metadata_builder->changes()));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(requirements, TableRequirements::ForCreateTable(
                                              ctx_->metadata_builder->changes()));
  }

  // Only this boundary can turn an uncaught exception into an unknown commit.
  try {
    auto result = ctx_->table->catalog()->UpdateTable(ctx_->table->name(), requirements,
                                                      ctx_->metadata_builder->changes());
    catalog_state_unknown =
        !result && result.error().kind == ErrorKind::kCommitStateUnknown;
    return result;
  } catch (const std::exception& e) {
    catalog_state_unknown = true;
    return CommitStateUnknown("Catalog commit threw: {}", e.what());
  } catch (...) {
    catalog_state_unknown = true;
    return CommitStateUnknown("Catalog commit threw an unknown exception");
  }
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
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdatePartitionSpec> update_spec,
                          UpdatePartitionSpec::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_spec));
  return update_spec;
}

Result<std::shared_ptr<UpdateProperties>> Transaction::NewUpdateProperties() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateProperties> update_properties,
                          UpdateProperties::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_properties));
  return update_properties;
}

Result<std::shared_ptr<UpdateSortOrder>> Transaction::NewUpdateSortOrder() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateSortOrder> update_sort_order,
                          UpdateSortOrder::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_sort_order));
  return update_sort_order;
}

Result<std::shared_ptr<UpdateSchema>> Transaction::NewUpdateSchema() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateSchema> update_schema,
                          UpdateSchema::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_schema));
  return update_schema;
}

Result<std::shared_ptr<ExpireSnapshots>> Transaction::NewExpireSnapshots() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<ExpireSnapshots> expire_snapshots,
                          ExpireSnapshots::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(expire_snapshots));
  return expire_snapshots;
}

Result<std::shared_ptr<UpdateLocation>> Transaction::NewUpdateLocation() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateLocation> update_location,
                          UpdateLocation::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_location));
  return update_location;
}

Result<std::shared_ptr<SetSnapshot>> Transaction::NewSetSnapshot() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<SetSnapshot> set_snapshot,
                          SetSnapshot::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(set_snapshot));
  return set_snapshot;
}

Result<std::shared_ptr<FastAppend>> Transaction::NewFastAppend() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<FastAppend> fast_append,
                          FastAppend::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(fast_append));
  return fast_append;
}

Result<std::shared_ptr<MergeAppend>> Transaction::NewMergeAppend() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<MergeAppend> merge_append,
                          MergeAppend::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(merge_append));
  return merge_append;
}

Result<std::shared_ptr<DeleteFiles>> Transaction::NewDeleteFiles() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<DeleteFiles> delete_files,
                          DeleteFiles::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(delete_files));
  return delete_files;
}

Result<std::shared_ptr<RowDelta>> Transaction::NewRowDelta() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<RowDelta> row_delta,
                          RowDelta::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(row_delta));
  return row_delta;
}

Result<std::shared_ptr<OverwriteFiles>> Transaction::NewOverwrite() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<OverwriteFiles> overwrite,
                          OverwriteFiles::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(overwrite));
  return overwrite;
}

Result<std::shared_ptr<RewriteFiles>> Transaction::NewRewriteFiles() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<RewriteFiles> rewrite_files,
                          RewriteFiles::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(rewrite_files));
  return rewrite_files;
}

Result<std::shared_ptr<ReplacePartitions>> Transaction::NewReplacePartitions() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<ReplacePartitions> replace_partitions,
                          ReplacePartitions::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(replace_partitions));
  return replace_partitions;
}

Result<std::shared_ptr<UpdateStatistics>> Transaction::NewUpdateStatistics() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateStatistics> update_statistics,
                          UpdateStatistics::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_statistics));
  return update_statistics;
}

Result<std::shared_ptr<UpdatePartitionStatistics>>
Transaction::NewUpdatePartitionStatistics() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(
      std::shared_ptr<UpdatePartitionStatistics> update_partition_statistics,
      UpdatePartitionStatistics::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_partition_statistics));
  return update_partition_statistics;
}

Result<std::shared_ptr<UpdateSnapshotReference>>
Transaction::NewUpdateSnapshotReference() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateSnapshotReference> update_ref,
                          UpdateSnapshotReference::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_ref));
  return update_ref;
}

Result<std::shared_ptr<SnapshotManager>> Transaction::NewSnapshotManager() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  // SnapshotManager has its own commit logic, so it is not added to the pending updates.
  return SnapshotManager::Make(shared_from_this());
}

}  // namespace iceberg
