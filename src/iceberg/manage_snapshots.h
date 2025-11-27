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

/// \file iceberg/manage_snapshots.h
/// API for managing snapshots, branches, and tags in a table

#include <cstdint>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/pending_update.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief API for managing snapshots, branches, and tags in a table
///
/// ManageSnapshots provides operations for:
/// - Rolling table data back to a state at an older snapshot
/// - Cherry-picking orphan snapshots in audit workflows
/// - Creating, updating, and managing branches and tags
/// - Setting retention policies for snapshots and references
///
/// Behavioral notes:
/// - Does NOT allow conflicting calls to SetCurrentSnapshot() and
///   RollbackToTime() in the same operation
/// - Commit conflicts will NOT be resolved automatically and will result in
///   CommitFailed error
/// - Changes are applied to current table metadata when committed
///
/// Example usage:
/// \code
///   table.ManageSnapshots()
///       .CreateBranch("experiment", snapshot_id)
///       .SetMinSnapshotsToKeep("experiment", 5)
///       .Commit();
/// \endcode
class ICEBERG_EXPORT ManageSnapshots : public PendingUpdateTyped<Snapshot> {
 public:
  ~ManageSnapshots() override = default;

  // ========================================================================
  // SNAPSHOT ROLLBACK OPERATIONS
  // ========================================================================

  /// \brief Roll table's data back to a specific snapshot
  ///
  /// This will be set as the current snapshot. Unlike RollbackTo(), the
  /// snapshot is NOT required to be an ancestor of the current table state.
  ///
  /// \param snapshot_id ID of the snapshot to roll back table data to
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& SetCurrentSnapshot(int64_t snapshot_id) = 0;

  /// \brief Roll table's data back to the last snapshot before given timestamp
  ///
  /// Cannot be used in the same operation as SetCurrentSnapshot().
  ///
  /// \param timestamp_millis Timestamp in milliseconds since epoch
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& RollbackToTime(int64_t timestamp_millis) = 0;

  /// \brief Rollback table's state to a specific snapshot (must be ancestor)
  ///
  /// The snapshot MUST be an ancestor of the current snapshot.
  ///
  /// \param snapshot_id ID of snapshot to roll back to (must be ancestor)
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& RollbackTo(int64_t snapshot_id) = 0;

  /// \brief Apply changes from a snapshot and create new current snapshot
  ///
  /// Used in audit workflows where data is written to an orphan snapshot,
  /// audited, and then applied to the current state.
  ///
  /// \param snapshot_id Snapshot ID whose changes to apply
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& Cherrypick(int64_t snapshot_id) = 0;

  // ========================================================================
  // BRANCH OPERATIONS
  // ========================================================================

  /// \brief Create a new branch pointing to the current snapshot
  ///
  /// The branch will point to the current snapshot if it exists.
  ///
  /// \param name Branch name
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& CreateBranch(std::string_view name) = 0;

  /// \brief Create a new branch pointing to a specific snapshot
  ///
  /// \param name Branch name
  /// \param snapshot_id Snapshot ID to be the branch head
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& CreateBranch(std::string_view name, int64_t snapshot_id) = 0;

  /// \brief Remove a branch by name
  ///
  /// \param name Branch name to remove
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& RemoveBranch(std::string_view name) = 0;

  /// \brief Rename an existing branch
  ///
  /// \param name Current branch name
  /// \param new_name Desired new branch name
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& RenameBranch(std::string_view name,
                                        std::string_view new_name) = 0;

  /// \brief Replace a branch to point to a specified snapshot
  ///
  /// \param name Branch to replace
  /// \param snapshot_id New snapshot ID for the branch
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& ReplaceBranch(std::string_view name, int64_t snapshot_id) = 0;

  /// \brief Replace one branch with another branch's snapshot
  ///
  /// The `to` branch remains unchanged. The `from` branch retains its
  /// retention properties. If `from` doesn't exist, it's created with
  /// default properties.
  ///
  /// \param from Branch to replace
  /// \param to Branch to replace with
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& ReplaceBranch(std::string_view from, std::string_view to) = 0;

  /// \brief Fast-forward one branch to another if from is ancestor of to
  ///
  /// Moves the `from` branch to the `to` branch's snapshot. The `to` branch
  /// remains unchanged. The `from` branch retains its retention properties.
  /// Creates `from` with default properties if it doesn't exist. Only works
  /// if `from` is an ancestor of `to`.
  ///
  /// \param from Branch to fast-forward
  /// \param to Reference branch to fast-forward to
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& FastForwardBranch(std::string_view from,
                                             std::string_view to) = 0;

  // ========================================================================
  // TAG OPERATIONS
  // ========================================================================

  /// \brief Create a new tag pointing to a snapshot
  ///
  /// \param name Tag name
  /// \param snapshot_id Snapshot ID for the tag head
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& CreateTag(std::string_view name, int64_t snapshot_id) = 0;

  /// \brief Remove a tag by name
  ///
  /// \param name Tag name to remove
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& RemoveTag(std::string_view name) = 0;

  /// \brief Replace an existing tag to point to a new snapshot
  ///
  /// \param name Tag to replace
  /// \param snapshot_id New snapshot ID for the tag
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& ReplaceTag(std::string_view name, int64_t snapshot_id) = 0;

  // ========================================================================
  // RETENTION POLICY OPERATIONS
  // ========================================================================

  /// \brief Update minimum number of snapshots to keep for a branch
  ///
  /// \param branch_name Name of the branch
  /// \param min_snapshots_to_keep Minimum number of snapshots to retain
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& SetMinSnapshotsToKeep(std::string_view branch_name,
                                                 int min_snapshots_to_keep) = 0;

  /// \brief Update maximum snapshot age for a branch
  ///
  /// \param branch_name Name of the branch
  /// \param max_snapshot_age_ms Maximum snapshot age in milliseconds
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& SetMaxSnapshotAgeMs(std::string_view branch_name,
                                               int64_t max_snapshot_age_ms) = 0;

  /// \brief Update retention policy for a reference (branch or tag)
  ///
  /// \param name Branch or tag name
  /// \param max_ref_age_ms Retention age in milliseconds of the reference
  /// \return Reference to this for method chaining
  virtual ManageSnapshots& SetMaxRefAgeMs(std::string_view name,
                                          int64_t max_ref_age_ms) = 0;

  // Non-copyable, movable (inherited from PendingUpdate)
  ManageSnapshots(const ManageSnapshots&) = delete;
  ManageSnapshots& operator=(const ManageSnapshots&) = delete;

 protected:
  ManageSnapshots() = default;
};

}  // namespace iceberg
