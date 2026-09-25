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
#include <iterator>
#include <memory>
#include <string>

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
#include "iceberg/update/rewrite_manifests.h"
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
#include "iceberg/util/error_util_internal.h"
#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/retry_util.h"

namespace iceberg {

namespace {

std::string FormatCommittedSnapshots(
    const std::vector<std::unique_ptr<TableUpdate>>& changes) {
  size_t snapshot_count = 0;
  for (const auto& change : changes) {
    snapshot_count += change->kind() == TableUpdate::Kind::kAddSnapshot;
  }
  if (snapshot_count == 0) {
    return {};
  }

  std::string detail;
  detail.reserve(32 + snapshot_count * 48);
  std::format_to(std::back_inserter(detail), ": committed snapshot{} ",
                 snapshot_count == 1 ? "" : "s");

  size_t formatted_count = 0;
  for (const auto& change : changes) {
    if (change->kind() != TableUpdate::Kind::kAddSnapshot) {
      continue;
    }
    const auto& snapshot =
        internal::checked_cast<const table::AddSnapshot&>(*change).snapshot();
    if (formatted_count++ > 0) {
      detail += ", ";
    }
    const auto operation = snapshot->summary.find(SnapshotSummaryFields::kOperation);
    std::format_to(std::back_inserter(detail), "{} (op={})", snapshot->snapshot_id,
                   operation != snapshot->summary.end() ? operation->second : "unknown");
  }
  return detail;
}

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

Status Transaction::CheckReady() const {
  ICEBERG_CHECK(state_ == TransactionState::kReady, "Transaction is not ready (state {})",
                static_cast<int>(state_));
  return {};
}

Status Transaction::AddUpdate(const std::shared_ptr<PendingUpdate>& update) {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  ICEBERG_PRECHECK(update && update->ctx_.get() == ctx_.get(),
                   "Update must belong to this transaction context");
  ICEBERG_CHECK(!update->commit_called_, "Update has already been committed");
  pending_updates_.push_back(update);
  state_ = TransactionState::kUpdatePending;
  return {};
}

Status Transaction::CommitUpdate(PendingUpdate& update) {
  ICEBERG_CHECK(state_ == TransactionState::kUpdatePending,
                "Transaction has no pending operation (state {})",
                static_cast<int>(state_));
  ICEBERG_CHECK(!pending_updates_.empty() && pending_updates_.back().get() == &update,
                "Update is not the current pending operation");
  update.commit_called_ = true;
  Status status;
  try {
    status = ApplyUpdate(update);
  } catch (const std::exception& e) {
    status = ValidationFailed("Update Apply threw: {}", e.what());
  } catch (...) {
    status = ValidationFailed("Update Apply threw an unknown exception");
  }
  if (!status) {
    state_ = TransactionState::kFailed;
    CleanupUpdates();
    return status;
  }
  state_ = TransactionState::kReady;
  return {};
}

Status Transaction::ReplayUpdates() {
  ICEBERG_CHECK(state_ == TransactionState::kReady,
                "Replay requires a ready transaction");
  for (const auto& update : pending_updates_) {
    ICEBERG_RETURN_UNEXPECTED(ApplyUpdate(*update));
  }
  return {};
}

Status Transaction::ApplyUpdate(PendingUpdate& update) {
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
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id, update.Apply());
  ctx_->metadata_builder->SetBranchSnapshot(snapshot_id,
                                            std::string(SnapshotRef::kMainBranch));
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
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
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
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
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
  return {};
}

Status Transaction::ApplyUpdateSchema(UpdateSchema& update) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Apply());
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
    // A no-op may still write temporary files. Clean them now; the update remains
    // registered so it can be replayed after a refresh.
    internal::LogAndIgnoreFailure("Update staging cleanup",
                                  [&update] { return update.CleanStaged(); });
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
  ICEBERG_ASSIGN_OR_RAISE(auto result, update.Apply());
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
  ICEBERG_ASSIGN_OR_RAISE(auto sort_order, update.Apply());
  ctx_->metadata_builder->SetDefaultSortOrder(std::move(sort_order));
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
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
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
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
  ICEBERG_RETURN_UNEXPECTED(ctx_->metadata_builder->CheckErrors());
  return {};
}

Result<std::shared_ptr<Table>> Transaction::Commit() {
  ICEBERG_RETURN_UNEXPECTED(CheckReady());
  Result<std::shared_ptr<Table>> commit_result = ctx_->table;
  int32_t attempt = 0;
  std::string last_error;
  try {
    auto builder_status = ctx_->metadata_builder->CheckErrors();
    if (!builder_status) {
      commit_result = std::unexpected(builder_status.error());
    } else {
      const auto& props = ctx_->table->properties();
      const int32_t num_retries =
          CanRetry() ? static_cast<int32_t>(props.Get(TableProperties::kCommitNumRetries))
                     : 0;
      bool is_first_attempt = true;
      commit_result =
          MakeCommitRetryRunner(num_retries,
                                props.Get(TableProperties::kCommitMinRetryWaitMs),
                                props.Get(TableProperties::kCommitMaxRetryWaitMs),
                                props.Get(TableProperties::kCommitTotalRetryTimeMs))
              .Run(
                  [this, &is_first_attempt, &attempt,
                   &last_error]() -> Result<std::shared_ptr<Table>> {
                    if (attempt > 1) {
                      ICEBERG_LOG_WARN(
                          "Retrying transaction commit for table {} (attempt {}) after: "
                          "{}",
                          ctx_->table->name().ToString(), attempt, last_error);
                    }
                    auto result = CommitOnce(is_first_attempt);
                    is_first_attempt = false;
                    if (!result) {
                      last_error = result.error().message;
                    }
                    return result;
                  },
                  &attempt);
    }
  } catch (const std::exception& e) {
    // CommitOnce handles catalog exceptions, so this failed before the commit.
    commit_result = ValidationFailed("Transaction preparation threw: {}", e.what());
  } catch (...) {
    commit_result =
        ValidationFailed("Transaction preparation threw an unknown exception");
  }

  if (commit_result && !ctx_->metadata_builder->changes().empty()) {
    if (attempt > 1) {
      ICEBERG_LOG_INFO("Transaction commit for table {} succeeded after {} attempts{}",
                       ctx_->table->name().ToString(), attempt,
                       FormatCommittedSnapshots(ctx_->metadata_builder->changes()));
    } else {
      ICEBERG_LOG_INFO("Transaction commit for table {} succeeded{}",
                       ctx_->table->name().ToString(),
                       FormatCommittedSnapshots(ctx_->metadata_builder->changes()));
    }
  }

  if (!commit_result) {
    if (commit_result.error().kind == ErrorKind::kCommitStateUnknown) {
      state_ = TransactionState::kCommitStateUnknown;
    } else {
      state_ = TransactionState::kFailed;
      CleanupUpdates();
    }
    return commit_result;
  }

  ctx_->table = std::move(commit_result.value());
  state_ = TransactionState::kCommitted;
  FinalizeUpdates(*ctx_->table->metadata());
  return ctx_->table;
}

Status Transaction::Abort() {
  if (state_ == TransactionState::kAborted) {
    return {};
  }
  ICEBERG_CHECK(state_ == TransactionState::kReady ||
                    state_ == TransactionState::kUpdatePending ||
                    state_ == TransactionState::kFailed,
                "Cannot abort a committed or unknown transaction");
  state_ = TransactionState::kAborted;
  CleanupUpdates();
  return {};
}

void Transaction::CleanupUpdates() noexcept {
  for (const auto& update : pending_updates_) {
    internal::LogAndIgnoreFailure("Update staging cleanup",
                                  [&update] { return update->CleanStaged(); });
  }
}

void Transaction::FinalizeUpdates(const TableMetadata& committed) noexcept {
  for (const auto& update : pending_updates_) {
    internal::LogAndIgnoreFailure("Update finalization", [&update, &committed] {
      return update->Finalize(committed);
    });
  }
}

Result<std::shared_ptr<Table>> Transaction::CommitOnce(bool is_first_attempt) {
  std::vector<std::unique_ptr<TableRequirement>> requirements;
  if (ctx_->kind == TransactionKind::kUpdate) {
    std::shared_ptr<TableMetadata> metadata_before_refresh;
    if (!is_first_attempt) {
      // Keep the builder's base alive while Refresh replaces the table metadata.
      metadata_before_refresh = ctx_->table->metadata();
      ICEBERG_RETURN_UNEXPECTED(ctx_->table->Refresh());
    }
    const bool metadata_changed =
        ctx_->metadata_builder->base() != ctx_->table->metadata().get();
    const bool standalone_retry = !is_first_attempt && !ctx_->transaction.has_value();
    if (metadata_changed || standalone_retry) {
      ICEBERG_CHECK(CanRetry(),
                    "Cannot rebase a transaction containing a non-retryable update");
      CleanupUpdates();
      ctx_->metadata_builder =
          TableMetadataBuilder::BuildFrom(ctx_->table->metadata().get());
      auto applied = ReplayUpdates();
      if (!applied) {
        return ValidationFailed("Transaction replay failed: {}", applied.error().message);
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

  // UpdateTable may throw after the catalog has committed the changes.
  try {
    return ctx_->table->catalog()->UpdateTable(ctx_->table->name(), requirements,
                                               ctx_->metadata_builder->changes());
  } catch (const std::exception& e) {
    return CommitStateUnknown("Catalog commit threw: {}", e.what());
  } catch (...) {
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

Result<std::shared_ptr<MergeAppend>> Transaction::NewMergeAppend() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<MergeAppend> merge_append,
                          MergeAppend::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(merge_append));
  return merge_append;
}

Result<std::shared_ptr<DeleteFiles>> Transaction::NewDeleteFiles() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<DeleteFiles> delete_files,
                          DeleteFiles::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(delete_files));
  return delete_files;
}

Result<std::shared_ptr<RowDelta>> Transaction::NewRowDelta() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<RowDelta> row_delta,
                          RowDelta::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(row_delta));
  return row_delta;
}

Result<std::shared_ptr<OverwriteFiles>> Transaction::NewOverwrite() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<OverwriteFiles> overwrite,
                          OverwriteFiles::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(overwrite));
  return overwrite;
}

Result<std::shared_ptr<RewriteFiles>> Transaction::NewRewriteFiles() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<RewriteFiles> rewrite_files,
                          RewriteFiles::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(rewrite_files));
  return rewrite_files;
}

Result<std::shared_ptr<ReplacePartitions>> Transaction::NewReplacePartitions() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<ReplacePartitions> replace_partitions,
                          ReplacePartitions::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(replace_partitions));
  return replace_partitions;
}

Result<std::shared_ptr<RewriteManifests>> Transaction::NewRewriteManifests() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<RewriteManifests> rewrite_manifests,
                          RewriteManifests::Make(ctx_->table->name().name, ctx_));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(rewrite_manifests));
  return rewrite_manifests;
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
