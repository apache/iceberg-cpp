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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"

/// \file iceberg/update/snapshot_manager.h
/// \brief API for managing snapshots and snapshot references.

namespace iceberg {

/// \brief API for managing snapshots.
///
/// Allows rolling table data back to a stated at an older table snapshot.
///
/// Rollback: This API does not allow conflicting calls to SetCurrentSnapshot and
/// RollbackToTime. When committing, these changes will be applied to the current table
/// metadata. Commit conflicts will not be resolved and will result in a
///
/// Cherrypick: In an audit workflow, new data is written to an orphan snapshot that is
/// not committed as the table's current state until it is audited. After auditing a
/// change, it may need to be applied or cherry-picked on top of the latest snapshot
/// instead of the one that was current when the audited changes were created. This class
/// adds support for cherry-picking the changes from an orphan snapshot by applying them
/// to the current snapshot. The output of the operation is a new snapshot with the
/// changes from cherry-picked snapshot.
class ICEBERG_EXPORT SnapshotManager : public ErrorCollector {
 public:
  /// \brief Create a SnapshotManager that owns its own transaction.
  static Result<std::shared_ptr<SnapshotManager>> Make(std::shared_ptr<Table> table);

  /// \brief Create a SnapshotManager from an existing transaction.
  ///
  /// \note The caller is responsible for committing the transaction.
  static Result<std::shared_ptr<SnapshotManager>> Make(
      std::shared_ptr<Transaction> transaction);

  ~SnapshotManager() override;

  /// \brief Apply supported changes in given snapshot and create a new snapshot which
  /// will be set as the current snapshot on commit.
  ///
  /// \param snapshot_id a Snapshot ID whose changes to apply
  /// \return Reference to this for method chaining
  SnapshotManager& Cherrypick(int64_t snapshot_id);

  /// \brief Roll this table's data back to a specific snapshot identified by id.
  ///
  /// \param snapshot_id long id of the snapshot to roll back table data to
  /// \return Reference to this for method chaining
  SnapshotManager& SetCurrentSnapshot(int64_t snapshot_id);

  /// \brief Roll this table's data back to the last snapshot before the given timestamp.
  ///
  /// \param timestamp_ms a long timestamp in milliseconds
  /// \return Reference to this for method chaining
  SnapshotManager& RollbackToTime(int64_t timestamp_ms);

  /// \brief Rollback table's state to a specific snapshot identified by id.
  ///
  /// \param snapshot_id long id of snapshot to roll back table to. Must be an ancestor
  /// of the current snapshot
  /// \return Reference to this for method chaining
  SnapshotManager& RollbackTo(int64_t snapshot_id);

  /// \brief Create a new branch. The branch will point to current snapshot if the
  /// current snapshot is not NULL. Otherwise, the branch will point to a newly created
  /// empty snapshot.
  ///
  /// \param name branch name
  /// \return Reference to this for method chaining
  SnapshotManager& CreateBranch(const std::string& name);

  /// \brief Create a new branch pointing to the given snapshot id.
  ///
  /// \param name branch name
  /// \param snapshot_id id of the snapshot which will be the head of the branch
  /// \return Reference to this for method chaining
  SnapshotManager& CreateBranch(const std::string& name, int64_t snapshot_id);

  /// \brief Create a new tag pointing to the given snapshot id.
  ///
  /// \param name tag name
  /// \param snapshot_id snapshot id for the head of the new tag
  /// \return Reference to this for method chaining
  SnapshotManager& CreateTag(const std::string& name, int64_t snapshot_id);

  /// \brief Remove a branch by name.
  ///
  /// \param name branch name
  /// \return Reference to this for method chaining
  SnapshotManager& RemoveBranch(const std::string& name);

  /// \brief Remove the tag with the given name.
  ///
  /// \param name tag name
  /// \return Reference to this for method chaining
  SnapshotManager& RemoveTag(const std::string& name);

  /// \brief Replace the tag with the given name to point to the specified snapshot.
  ///
  /// \param name tag to replace
  /// \param snapshot_id new snapshot id for the given tag
  /// \return Reference to this for method chaining
  SnapshotManager& ReplaceTag(const std::string& name, int64_t snapshot_id);

  /// \brief Replace the branch with the given name to point to the specified snapshot.
  ///
  /// \param name branch to replace
  /// \param snapshot_id new snapshot id for the given branch
  /// \return Reference to this for method chaining
  SnapshotManager& ReplaceBranch(const std::string& name, int64_t snapshot_id);

  /// \brief Replace the 'from' branch to point to the 'to' snapshot. The 'to' will
  /// remain unchanged, and 'from' branch will retain its retention properties. If the
  /// 'from' branch does not exist, it will be created with default retention properties.
  ///
  /// \param from branch to replace
  /// \param to the branch 'from' should be replaced with
  /// \return Reference to this for method chaining
  SnapshotManager& ReplaceBranch(const std::string& from, const std::string& to);

  /// \brief Perform a fast-forward of 'from' up to the 'to' snapshot if 'from' is an
  /// ancestor of 'to'. The 'to' will remain unchanged, and 'from' will retain its
  /// retention properties. If the 'from' branch does not exist, it will be created with
  /// default retention properties.
  ///
  /// \param from branch to fast-forward
  /// \param to ref for the 'from' branch to be fast forwarded to
  /// \return Reference to this for method chaining
  SnapshotManager& FastForwardBranch(const std::string& from, const std::string& to);

  /// \brief Rename a branch.
  ///
  /// \param name name of branch to rename
  /// \param new_name the desired new name of the branch
  /// \return Reference to this for method chaining
  SnapshotManager& RenameBranch(const std::string& name, const std::string& new_name);

  /// \brief Update the minimum number of snapshots to keep for a branch.
  ///
  /// \param branch_name branch name
  /// \param min_snapshots_to_keep minimum number of snapshots to retain on the branch
  /// \return Reference to this for method chaining
  SnapshotManager& SetMinSnapshotsToKeep(const std::string& branch_name,
                                         int32_t min_snapshots_to_keep);

  /// \brief Update the max snapshot age for a branch.
  ///
  /// \param branch_name branch name
  /// \param max_snapshot_age_ms maximum snapshot age in milliseconds to retain on branch
  /// \return Reference to this for method chaining
  SnapshotManager& SetMaxSnapshotAgeMs(const std::string& branch_name,
                                       int64_t max_snapshot_age_ms);

  /// \brief Update the retention policy for a reference.
  ///
  /// \param name branch name
  /// \param max_ref_age_ms retention age in milliseconds of the tag reference itself
  /// \return Reference to this for method chaining
  SnapshotManager& SetMaxRefAgeMs(const std::string& name, int64_t max_ref_age_ms);

  /// \brief Commit all pending changes.
  Status Commit();

 private:
  SnapshotManager(std::shared_ptr<Transaction> transaction, bool is_external_transaction);

  /// \brief Get or create the UpdateSnapshotReference operation.
  Result<std::shared_ptr<UpdateSnapshotReference>> UpdateSnapshotReferencesOperation();

  /// \brief Commit any pending reference updates if they exist.
  Status CommitIfRefUpdatesExist();

  std::shared_ptr<Transaction> transaction_;
  const bool is_external_transaction_;
  std::shared_ptr<UpdateSnapshotReference> update_snap_refs_;
  std::vector<std::shared_ptr<UpdateSnapshotReference>> committed_snap_refs_;
};

}  // namespace iceberg
