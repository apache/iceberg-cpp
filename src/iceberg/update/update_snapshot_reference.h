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
#include <unordered_map>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"

/// \file iceberg/update/update_snapshot_reference.h

namespace iceberg {

/// \brief Updates snapshot references.
///
/// TODO(xxx): Add SetSnapshot operations such as SetCurrentSnapshot, RollBackTime,
/// RollbackTo to this class so that we can support those operations for refs.
class ICEBERG_EXPORT UpdateSnapshotReference : public PendingUpdate {
 public:
  static Result<std::shared_ptr<UpdateSnapshotReference>> Make(
      std::shared_ptr<Transaction> transaction);

  ~UpdateSnapshotReference() override;

  /// \brief Create a branch reference.
  ///
  /// \param name The branch name
  /// \param snapshot_id The snapshot ID for the branch
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& CreateBranch(const std::string& name, int64_t snapshot_id);

  /// \brief Create a tag reference.
  ///
  /// \param name The tag name
  /// \param snapshot_id The snapshot ID for the tag
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& CreateTag(const std::string& name, int64_t snapshot_id);

  /// \brief Remove a branch reference.
  ///
  /// \param name The branch name to remove
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& RemoveBranch(const std::string& name);

  /// \brief Remove a tag reference.
  ///
  /// \param name The tag name to remove
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& RemoveTag(const std::string& name);

  /// \brief Rename a branch reference.
  ///
  /// \param name The current branch name
  /// \param new_name The new branch name
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& RenameBranch(const std::string& name,
                                        const std::string& new_name);

  /// \brief Replace a branch reference with a new snapshot ID.
  ///
  /// \param name The branch name
  /// \param snapshot_id The new snapshot ID
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& ReplaceBranch(const std::string& name, int64_t snapshot_id);

  /// \brief Replace a branch reference with another reference's snapshot ID.
  ///
  /// \param from The branch name to update
  /// \param to The reference name to copy the snapshot ID from
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& ReplaceBranch(const std::string& from, const std::string& to);

  /// \brief Fast-forward a branch to another reference's snapshot ID.
  ///
  /// This is similar to ReplaceBranch but validates that the target snapshot is an
  /// ancestor of the current branch snapshot.
  ///
  /// \param from The branch name to update
  /// \param to The reference name to copy the snapshot ID from
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& FastForward(const std::string& from, const std::string& to);

  /// \brief Replace a tag reference with a new snapshot ID.
  ///
  /// \param name The tag name
  /// \param snapshot_id The new snapshot ID
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& ReplaceTag(const std::string& name, int64_t snapshot_id);

  /// \brief Set the minimum number of snapshots to keep for a branch.
  ///
  /// \param name The branch name
  /// \param min_snapshots_to_keep The minimum number of snapshots to keep
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& SetMinSnapshotsToKeep(const std::string& name,
                                                 int32_t min_snapshots_to_keep);

  /// \brief Set the maximum snapshot age in milliseconds for a branch.
  ///
  /// \param name The branch name
  /// \param max_snapshot_age_ms The maximum snapshot age in milliseconds
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& SetMaxSnapshotAgeMs(const std::string& name,
                                               int64_t max_snapshot_age_ms);

  /// \brief Set the maximum reference age in milliseconds.
  ///
  /// \param name The reference name
  /// \param max_ref_age_ms The maximum reference age in milliseconds
  /// \return Reference to this for method chaining
  UpdateSnapshotReference& SetMaxRefAgeMs(const std::string& name,
                                          int64_t max_ref_age_ms);

  Kind kind() const final { return Kind::kUpdateSnapshotReference; }

  struct ApplyResult {
    /// References to set or update (name, ref pairs)
    std::vector<std::pair<std::string, std::shared_ptr<SnapshotRef>>> to_set;
    /// Reference names to remove
    std::vector<std::string> to_remove;
  };

  /// \brief Apply the pending changes and return the updated and removed references.
  Result<ApplyResult> Apply();

 private:
  explicit UpdateSnapshotReference(std::shared_ptr<Transaction> transaction);

  UpdateSnapshotReference& ReplaceBranchInternal(const std::string& from,
                                                 const std::string& to,
                                                 bool fast_forward);

  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> updated_refs_;
};

}  // namespace iceberg
