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

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

/// \brief API for managing snapshots and snapshot references.
///
/// Allows rolling table data back to a state at an older snapshot, cherry-picking
/// snapshots, and managing branches and tags.
class ICEBERG_EXPORT SnapshotManager : public PendingUpdate {
 public:
  /// \brief Create a SnapshotManager for a table.
  ///
  /// \param table_name The name of the table
  /// \param table The table to manage snapshots for
  /// \return A new SnapshotManager instance, or an error if the table doesn't exist
  static Result<std::shared_ptr<SnapshotManager>> Make(const std::string& table_name,
                                                       std::shared_ptr<Table> table);

  /// \brief Create a SnapshotManager from an existing transaction.
  ///
  /// \param transaction The transaction to use
  /// \return A new SnapshotManager instance
  static Result<std::shared_ptr<SnapshotManager>> Make(
      std::shared_ptr<Transaction> transaction);

  ~SnapshotManager() override;

  // TODO(xxx): is this correct?
  Kind kind() const final { return Kind::kUpdateSnapshotReference; }

  /// \brief Apply supported changes in given snapshot and create a new snapshot which
  /// will be set as the current snapshot on commit.
  ///
  /// \param snapshot_id A snapshot ID whose changes to apply
  /// \return Reference to this for method chaining
  SnapshotManager& Cherrypick(int64_t snapshot_id);

  /// \brief Roll this table's data back to a specific Snapshot identified by id.
  ///
  /// \param snapshot_id Long id of the snapshot to roll back table data to
  /// \return Reference to this for method chaining
  SnapshotManager& SetCurrentSnapshot(int64_t snapshot_id);

  /// \brief Roll this table's data back to the last Snapshot before the given timestamp.
  ///
  /// \param timestamp_ms A timestamp in milliseconds
  /// \return Reference to this for method chaining
  SnapshotManager& RollbackToTime(TimePointMs timestamp_ms);

  /// \brief Rollback table's state to a specific Snapshot identified by id.
  ///
  /// \param snapshot_id Long id of snapshot id to roll back table to. Must be an ancestor
  /// of the current snapshot
  /// \return Reference to this for method chaining
  SnapshotManager& RollbackTo(int64_t snapshot_id);

  /// \brief Create a new branch. The branch will point to current snapshot if the current
  /// snapshot is not NULL. Otherwise, the branch will point to a newly created empty
  /// snapshot.
  ///
  /// \param name Branch name
  /// \return Reference to this for method chaining
  SnapshotManager& CreateBranch(const std::string& name);

  /// \brief Create a new branch pointing to the given snapshot id.
  ///
  /// \param name Branch name
  /// \param snapshot_id ID of the snapshot which will be the head of the branch
  /// \return Reference to this for method chaining
  SnapshotManager& CreateBranch(const std::string& name, int64_t snapshot_id);

  /// \brief Create a new tag pointing to the given snapshot id.
  ///
  /// \param name Tag name
  /// \param snapshot_id Snapshot ID for the head of the new tag
  /// \return Reference to this for method chaining
  SnapshotManager& CreateTag(const std::string& name, int64_t snapshot_id);

  /// \brief Remove a branch by name.
  ///
  /// \param name Branch name
  /// \return Reference to this for method chaining
  SnapshotManager& RemoveBranch(const std::string& name);

  /// \brief Remove the tag with the given name.
  ///
  /// \param name Tag name
  /// \return Reference to this for method chaining
  SnapshotManager& RemoveTag(const std::string& name);

  /// \brief Replaces the tag with the given name to point to the specified snapshot.
  ///
  /// \param name Tag to replace
  /// \param snapshot_id New snapshot id for the given tag
  /// \return Reference to this for method chaining
  SnapshotManager& ReplaceTag(const std::string& name, int64_t snapshot_id);

  /// \brief Replaces the branch with the given name to point to the specified snapshot.
  ///
  /// \param name Branch to replace
  /// \param snapshot_id New snapshot id for the given branch
  /// \return Reference to this for method chaining
  SnapshotManager& ReplaceBranch(const std::string& name, int64_t snapshot_id);

  /// \brief Replaces the from branch to point to the to snapshot. The to will remain
  /// unchanged, and from branch will retain its retention properties. If the from branch
  /// does not exist, it will be created with default retention properties.
  ///
  /// \param from Branch to replace
  /// \param to The branch from should be replaced with
  /// \return Reference to this for method chaining
  SnapshotManager& ReplaceBranch(const std::string& from, const std::string& to);

  /// \brief Performs a fast-forward of from up to the to snapshot if from is an ancestor
  /// of to. The to will remain unchanged, and from will retain its retention properties.
  /// If the from branch does not exist, it will be created with default retention
  /// properties.
  ///
  /// \param from Branch to fast-forward
  /// \param to Ref for the from branch to be fast forwarded to
  /// \return Reference to this for method chaining
  SnapshotManager& FastForwardBranch(const std::string& from, const std::string& to);

  /// \brief Rename a branch.
  ///
  /// \param name Name of branch to rename
  /// \param new_name The desired new name of the branch
  /// \return Reference to this for method chaining
  SnapshotManager& RenameBranch(const std::string& name, const std::string& new_name);

  /// \brief Updates the minimum number of snapshots to keep for a branch.
  ///
  /// \param branch_name Branch name
  /// \param min_snapshots_to_keep Minimum number of snapshots to retain on the branch
  /// \return Reference to this for method chaining
  SnapshotManager& SetMinSnapshotsToKeep(const std::string& branch_name,
                                         int32_t min_snapshots_to_keep);

  /// \brief Updates the max snapshot age for a branch.
  ///
  /// \param branch_name Branch name
  /// \param max_snapshot_age_ms Maximum snapshot age in milliseconds to retain on branch
  /// \return Reference to this for method chaining
  SnapshotManager& SetMaxSnapshotAgeMs(const std::string& branch_name,
                                       int64_t max_snapshot_age_ms);

  /// \brief Updates the retention policy for a reference.
  ///
  /// \param name Reference name
  /// \param max_ref_age_ms Retention age in milliseconds of the tag reference itself
  /// \return Reference to this for method chaining
  SnapshotManager& SetMaxRefAgeMs(const std::string& name, int64_t max_ref_age_ms);

  /// \brief Apply the pending changes and return the current snapshot.
  ///
  /// \return The current snapshot after applying changes, or an error
  Result<std::shared_ptr<Snapshot>> Apply();

  /// \brief Commit all pending changes.
  ///
  /// \return Status indicating success or failure
  Status Commit() override;

 private:
  /// \brief Constructor for creating a SnapshotManager with a transaction.
  ///
  /// \param transaction The transaction to use
  /// \param is_external Whether this is an external transaction (true) or created
  /// internally (false)
  SnapshotManager(std::shared_ptr<Transaction> transaction, bool is_external);

  /// \brief Get or create the UpdateSnapshotReference operation.
  Result<std::shared_ptr<UpdateSnapshotReference>> UpdateSnapshotReferencesOperation();

  /// \brief Commit any pending reference updates if they exist.
  Status CommitIfRefUpdatesExist();

  bool is_external_transaction_;
  std::shared_ptr<UpdateSnapshotReference> update_snapshot_references_operation_;
};

}  // namespace iceberg
