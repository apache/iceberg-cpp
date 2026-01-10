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

#include "iceberg/update/snapshot_manager.h"

#include <memory>
#include <string>

#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/update/update_snapshot_reference.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<SnapshotManager>> SnapshotManager::Make(
    const std::string& table_name, std::shared_ptr<Table> table) {
  if (table == nullptr) {
    return InvalidArgument("Table cannot be null");
  }
  if (table->metadata() == nullptr) {
    return InvalidArgument("Cannot manage snapshots: table {} does not exist",
                           table_name);
  }
  // Create a transaction first
  ICEBERG_ASSIGN_OR_RAISE(auto transaction,
                          Transaction::Make(table, Transaction::Kind::kUpdate,
                                            /*auto_commit=*/false));
  // Create SnapshotManager with the transaction (not external)
  auto manager = std::shared_ptr<SnapshotManager>(
      new SnapshotManager(std::move(transaction), false));
  return manager;
}

Result<std::shared_ptr<SnapshotManager>> SnapshotManager::Make(
    std::shared_ptr<Transaction> transaction) {
  if (transaction == nullptr) {
    return InvalidArgument("Invalid input transaction: null");
  }
  return std::shared_ptr<SnapshotManager>(
      new SnapshotManager(std::move(transaction), true));
}

SnapshotManager::SnapshotManager(std::shared_ptr<Transaction> transaction,
                                 bool is_external)
    : PendingUpdate(transaction), is_external_transaction_(is_external) {}

SnapshotManager::~SnapshotManager() = default;

SnapshotManager& SnapshotManager::Cherrypick(int64_t snapshot_id) {
  if (auto status = CommitIfRefUpdatesExist(); !status.has_value()) {
    return AddError(status.error());
  }
  // TODO(anyone): Implement cherrypick operation
  // This should create a new snapshot by applying changes from the given snapshot
  // For now, throw NotImplemented
  ICEBERG_BUILDER_CHECK(false, "Cherrypick operation not yet implemented");
  return *this;
}

SnapshotManager& SnapshotManager::SetCurrentSnapshot(int64_t snapshot_id) {
  if (auto status = CommitIfRefUpdatesExist(); !status.has_value()) {
    return AddError(status.error());
  }
  // Verify snapshot exists
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto snapshot,
                                   transaction_->current().SnapshotById(snapshot_id));
  // Set the main branch to point to this snapshot
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref,
                                   transaction_->NewUpdateSnapshotReference());
  update_ref->ReplaceBranch(std::string(SnapshotRef::kMainBranch), snapshot_id);
  if (auto status = update_ref->Commit(); !status.has_value()) {
    return AddError(status.error());
  }
  return *this;
}

SnapshotManager& SnapshotManager::RollbackToTime(TimePointMs timestamp_ms) {
  if (auto status = CommitIfRefUpdatesExist(); !status.has_value()) {
    return AddError(status.error());
  }
  // Find the last snapshot before the given timestamp
  const auto& snapshots = transaction_->current().snapshots;
  std::shared_ptr<Snapshot> target_snapshot = nullptr;
  for (const auto& snapshot : snapshots) {
    if (snapshot != nullptr && snapshot->timestamp_ms < timestamp_ms) {
      if (target_snapshot == nullptr ||
          snapshot->timestamp_ms > target_snapshot->timestamp_ms) {
        target_snapshot = snapshot;
      }
    }
  }
  ICEBERG_BUILDER_CHECK(target_snapshot != nullptr,
                        "Table has no old snapshot before timestamp {}", timestamp_ms);
  return SetCurrentSnapshot(target_snapshot->snapshot_id);
}

SnapshotManager& SnapshotManager::RollbackTo(int64_t snapshot_id) {
  if (auto status = CommitIfRefUpdatesExist(); !status.has_value()) {
    return AddError(status.error());
  }
  // Verify snapshot exists
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto snapshot,
                                   transaction_->current().SnapshotById(snapshot_id));
  // Get current snapshot
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto current_snapshot,
                                   transaction_->current().Snapshot());
  // Verify that the target snapshot is an ancestor of the current snapshot
  // TODO(anyone): Use SnapshotUtil::IsAncestorOf once we have access to Table
  // For now, we'll do a simple check by traversing parent_snapshot_id
  int64_t current_id = current_snapshot->snapshot_id;
  bool found = false;
  while (current_id != -1) {
    if (current_id == snapshot_id) {
      found = true;
      break;
    }
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto current_snap,
                                     transaction_->current().SnapshotById(current_id));
    if (current_snap->parent_snapshot_id.has_value()) {
      current_id = current_snap->parent_snapshot_id.value();
    } else {
      break;
    }
  }
  ICEBERG_BUILDER_CHECK(found,
                        "Cannot rollback to {}: it is not an ancestor of the current "
                        "snapshot",
                        snapshot_id);
  return SetCurrentSnapshot(snapshot_id);
}

SnapshotManager& SnapshotManager::CreateBranch(const std::string& name) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto current_snapshot,
                                   transaction_->current().Snapshot());
  if (current_snapshot != nullptr) {
    return CreateBranch(name, current_snapshot->snapshot_id);
  }
  // Check if branch already exists
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  const auto& current_refs = transaction_->current().refs;
  ICEBERG_BUILDER_CHECK(current_refs.find(name) == current_refs.end(),
                        "Ref {} already exists", name);
  // Create an empty snapshot for the branch
  // TODO(anyone): Implement creating empty snapshot
  // For now, throw NotImplemented
  ICEBERG_BUILDER_CHECK(false, "Creating branch with empty snapshot not yet implemented");
  return *this;
}

SnapshotManager& SnapshotManager::CreateBranch(const std::string& name,
                                               int64_t snapshot_id) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->CreateBranch(name, snapshot_id);
  return *this;
}

SnapshotManager& SnapshotManager::CreateTag(const std::string& name,
                                            int64_t snapshot_id) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->CreateTag(name, snapshot_id);
  return *this;
}

SnapshotManager& SnapshotManager::RemoveBranch(const std::string& name) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->RemoveBranch(name);
  return *this;
}

SnapshotManager& SnapshotManager::RemoveTag(const std::string& name) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->RemoveTag(name);
  return *this;
}

SnapshotManager& SnapshotManager::ReplaceTag(const std::string& name,
                                             int64_t snapshot_id) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->ReplaceTag(name, snapshot_id);
  return *this;
}

SnapshotManager& SnapshotManager::ReplaceBranch(const std::string& name,
                                                int64_t snapshot_id) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->ReplaceBranch(name, snapshot_id);
  return *this;
}

SnapshotManager& SnapshotManager::ReplaceBranch(const std::string& from,
                                                const std::string& to) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->ReplaceBranch(from, to);
  return *this;
}

SnapshotManager& SnapshotManager::FastForwardBranch(const std::string& from,
                                                    const std::string& to) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->FastForward(from, to);
  return *this;
}

SnapshotManager& SnapshotManager::RenameBranch(const std::string& name,
                                               const std::string& new_name) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->RenameBranch(name, new_name);
  return *this;
}

SnapshotManager& SnapshotManager::SetMinSnapshotsToKeep(const std::string& branch_name,
                                                        int32_t min_snapshots_to_keep) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->SetMinSnapshotsToKeep(branch_name, min_snapshots_to_keep);
  return *this;
}

SnapshotManager& SnapshotManager::SetMaxSnapshotAgeMs(const std::string& branch_name,
                                                      int64_t max_snapshot_age_ms) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->SetMaxSnapshotAgeMs(branch_name, max_snapshot_age_ms);
  return *this;
}

SnapshotManager& SnapshotManager::SetMaxRefAgeMs(const std::string& name,
                                                 int64_t max_ref_age_ms) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto update_ref, UpdateSnapshotReferencesOperation());
  update_ref->SetMaxRefAgeMs(name, max_ref_age_ms);
  return *this;
}

Result<std::shared_ptr<Snapshot>> SnapshotManager::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CommitIfRefUpdatesExist());
  return transaction_->table()->current_snapshot();
}

Status SnapshotManager::Commit() {
  ICEBERG_RETURN_UNEXPECTED(CommitIfRefUpdatesExist());
  if (!is_external_transaction_) {
    ICEBERG_ASSIGN_OR_RAISE(auto updated_table, transaction_->Commit());
    // Create a new transaction with the updated table
    ICEBERG_ASSIGN_OR_RAISE(transaction_,
                            Transaction::Make(updated_table, Transaction::Kind::kUpdate,
                                              /*auto_commit=*/false));
    // Note: The base class transaction_ member is protected, so we can't update it
    // directly However, since we always use transaction_ through the base class member,
    // we need to ensure consistency. For now, we'll rely on the fact that all methods use
    // transaction_ from the base class.
  }
  return {};
}

Result<std::shared_ptr<UpdateSnapshotReference>>
SnapshotManager::UpdateSnapshotReferencesOperation() {
  if (update_snapshot_references_operation_ == nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(update_snapshot_references_operation_,
                            transaction_->NewUpdateSnapshotReference());
  }
  return update_snapshot_references_operation_;
}

Status SnapshotManager::CommitIfRefUpdatesExist() {
  if (update_snapshot_references_operation_ != nullptr) {
    ICEBERG_RETURN_UNEXPECTED(update_snapshot_references_operation_->Commit());
    update_snapshot_references_operation_ = nullptr;
  }
  return {};
}

}  // namespace iceberg
