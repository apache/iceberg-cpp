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
#include "iceberg/update/fast_append.h"
#include "iceberg/update/set_snapshot.h"
#include "iceberg/update/update_snapshot_reference.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

class AutoCommitGuard {
 public:
  AutoCommitGuard(std::shared_ptr<Transaction> transaction, bool auto_commit)
      : transaction_(std::move(transaction)), auto_commit_(auto_commit) {}

  ~AutoCommitGuard() {
    if (auto_commit_) {
      transaction_->EnableAutoCommit();
    } else {
      transaction_->DisableAutoCommit();
    }
  }

 private:
  std::shared_ptr<Transaction> transaction_;
  bool auto_commit_;
};

}  // namespace

Result<std::shared_ptr<SnapshotManager>> SnapshotManager::Make(
    std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(transaction != nullptr, "Invalid input transaction: null");
  return std::shared_ptr<SnapshotManager>(new SnapshotManager(std::move(transaction)));
}

SnapshotManager::SnapshotManager(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)),
      original_auto_commit_(transaction_->auto_commit_) {
  transaction_->DisableAutoCommit();
}

SnapshotManager::~SnapshotManager() = default;

SnapshotManager& SnapshotManager::Cherrypick(int64_t snapshot_id) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(CommitIfRefUpdatesExist());
  // TODO(anyone): Implement cherrypick operation
  ICEBERG_BUILDER_CHECK(false, "Cherrypick operation not yet implemented");
  return *this;
}

SnapshotManager& SnapshotManager::SetCurrentSnapshot(int64_t snapshot_id) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(CommitIfRefUpdatesExist());
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto set_snapshot, transaction_->NewSetSnapshot());
  set_snapshot->SetCurrentSnapshot(snapshot_id);
  ICEBERG_BUILDER_RETURN_IF_ERROR(set_snapshot->Commit());
  return *this;
}

SnapshotManager& SnapshotManager::RollbackToTime(int64_t timestamp_ms) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(CommitIfRefUpdatesExist());
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto set_snapshot, transaction_->NewSetSnapshot());
  set_snapshot->RollbackToTime(timestamp_ms);
  ICEBERG_BUILDER_RETURN_IF_ERROR(set_snapshot->Commit());
  return *this;
}

SnapshotManager& SnapshotManager::RollbackTo(int64_t snapshot_id) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(CommitIfRefUpdatesExist());
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto set_snapshot, transaction_->NewSetSnapshot());
  set_snapshot->RollbackTo(snapshot_id);
  ICEBERG_BUILDER_RETURN_IF_ERROR(set_snapshot->Commit());
  return *this;
}

SnapshotManager& SnapshotManager::CreateBranch(const std::string& name) {
  if (base().current_snapshot_id != kInvalidSnapshotId) {
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto current_snapshot, base().Snapshot());
    ICEBERG_DCHECK(current_snapshot != nullptr, "Current snapshot should not be null");
    return CreateBranch(name, current_snapshot->snapshot_id);
  }
  const auto& current_refs = base().refs;
  ICEBERG_BUILDER_CHECK(!base().refs.contains(name), "Ref {} already exists", name);
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto fast_append, transaction_->NewFastAppend());
  ICEBERG_BUILDER_RETURN_IF_ERROR(fast_append->SetTargetBranch(name).Commit());
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

Status SnapshotManager::Commit() {
  AutoCommitGuard auto_commit_guard(transaction_, original_auto_commit_);
  transaction_->EnableAutoCommit();
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_RETURN_UNEXPECTED(CommitIfRefUpdatesExist());
  if (!transaction_->is_committed()) {
    ICEBERG_RETURN_UNEXPECTED(transaction_->Commit());
  }
  return {};
}

Result<std::shared_ptr<UpdateSnapshotReference>>
SnapshotManager::UpdateSnapshotReferencesOperation() {
  if (update_snap_refs_ == nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(update_snap_refs_,
                            transaction_->NewUpdateSnapshotReference());
  }
  return update_snap_refs_;
}

Status SnapshotManager::CommitIfRefUpdatesExist() {
  if (update_snap_refs_ != nullptr) {
    ICEBERG_RETURN_UNEXPECTED(update_snap_refs_->Commit());
    update_snap_refs_ = nullptr;
  }
  return {};
}

}  // namespace iceberg
