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

/// \file iceberg/snapshot_update.h
/// API for table updates that produce snapshots

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "iceberg/iceberg_export.h"
#include "iceberg/pending_update.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Interface for updates that produce a new table snapshot
///
/// SnapshotUpdate extends PendingUpdate to provide common methods for all
/// updates that create a new table Snapshot. Implementations include operations
/// like AppendFiles, DeleteFiles, OverwriteFiles, and RewriteFiles.
///
/// This interface uses CRTP (Curiously Recurring Template Pattern) to enable
/// fluent API method chaining in derived classes, matching the Java pattern
/// where SnapshotUpdate<ThisT> allows methods to return the actual derived type.
///
/// Methods included from Java API:
/// - Set(): Set summary properties
/// - StageOnly(): Stage without updating current snapshot
/// - DeleteWith(): Custom file deletion callback
/// - ToBranch(): Commit to a specific branch
///
/// Methods deferred (will be added when infrastructure is available):
/// - ScanManifestsWith(): Custom executor for parallel manifest scanning
///   (requires executor/thread pool infrastructure)
///
/// \tparam Derived The actual implementation class (e.g., AppendFiles)
template <typename Derived>
class ICEBERG_EXPORT SnapshotUpdate : public PendingUpdateTyped<Snapshot> {
 public:
  ~SnapshotUpdate() override = default;

  /// \brief Set a summary property on the snapshot
  ///
  /// Summary properties provide metadata about the changes in the snapshot,
  /// such as the operation type, number of files added/deleted, etc.
  ///
  /// \param property The property name
  /// \param value The property value
  /// \return Reference to derived class for method chaining
  Derived& Set(std::string_view property, std::string_view value) {
    summary_[std::string(property)] = std::string(value);
    return static_cast<Derived&>(*this);
  }

  /// \brief Stage the snapshot without updating the table's current snapshot
  ///
  /// When StageOnly() is called, the snapshot will be committed to table metadata
  /// but will not update the current snapshot ID. The snapshot will not be added
  /// to the table's snapshot log. This is useful for creating wap branches or
  /// validating changes before making them current.
  ///
  /// \return Reference to derived class for method chaining
  Derived& StageOnly() {
    stage_only_ = true;
    return static_cast<Derived&>(*this);
  }

  /// \brief Set a custom file deletion callback
  ///
  /// By default, files are deleted using the table's FileIO implementation.
  /// This method allows providing a custom deletion callback for use cases like:
  /// - Tracking deleted files for auditing
  /// - Implementing custom retention policies
  /// - Delegating deletion to external systems
  ///
  /// \param delete_func Callback function that will be called for each file to delete
  /// \return Reference to derived class for method chaining
  Derived& DeleteWith(std::function<void(std::string_view)> delete_func) {
    delete_func_ = std::move(delete_func);
    return static_cast<Derived&>(*this);
  }

  /// \brief Commit the snapshot to a specific branch
  ///
  /// By default, snapshots are committed to the table's main branch.
  /// This method allows committing to a named branch instead, which is useful for:
  /// - Write-Audit-Publish (WAP) workflows
  /// - Feature branch development
  /// - Testing changes before merging to main
  ///
  /// \param branch The name of the branch to commit to
  /// \return Reference to derived class for method chaining
  Derived& ToBranch(std::string_view branch) {
    target_branch_ = std::string(branch);
    return static_cast<Derived&>(*this);
  }

 protected:
  SnapshotUpdate() = default;

  /// \brief Summary properties to set on the snapshot
  std::unordered_map<std::string, std::string> summary_;

  /// \brief Whether to stage only without updating current snapshot
  bool stage_only_ = false;

  /// \brief Custom file deletion callback
  std::optional<std::function<void(std::string_view)>> delete_func_;

  /// \brief Target branch name for commit (nullopt means main branch)
  std::optional<std::string> target_branch_;
};

}  // namespace iceberg
