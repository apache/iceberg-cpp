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

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/catalog.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"

namespace iceberg {

/// \brief Base class for operations that produce snapshots.
///
/// This class provides common functionality for creating new snapshots,
/// including manifest list writing, commit retries, and cleanup.
///
/// \tparam T The concrete subclass type
template <typename T>
class ICEBERG_EXPORT SnapshotUpdate : public PendingUpdate {
 public:
  ~SnapshotUpdate() override = default;

  /// \brief Set a summary property in the snapshot produced by this update.
  ///
  /// \param property A string property name
  /// \param value A string property value
  /// \return Reference to this for method chaining
  T& Set(const std::string& property, const std::string& value) {
    summary_properties_[property] = value;
    return static_cast<T&>(*this);
  }

  /// \brief Set a callback to delete files instead of the table's default.
  ///
  /// \param delete_func A function used to delete file locations
  /// \return Reference to this for method chaining
  T& DeleteWith(std::function<Status(const std::string&)> delete_func) {
    delete_func_ = std::move(delete_func);
    return static_cast<T&>(*this);
  }

  /// \brief Stage a snapshot in table metadata, but not update the current snapshot id.
  ///
  /// \return Reference to this for method chaining
  T& StageOnly() {
    stage_only_ = true;
    return static_cast<T&>(*this);
  }

  /// \brief Apply the update's changes to create a new snapshot.
  ///
  /// This method validates the changes, applies them to the metadata,
  /// and creates a new snapshot without committing it. The snapshot
  /// is stored internally and can be accessed after Apply() succeeds.
  ///
  /// \return A result containing the apply result, or an error
  Result<ApplyResult> Apply() override;

  /// \brief Commits the snapshot changes to the table.
  ///
  /// This method applies the changes and commits them through the catalog.
  /// It handles retries and cleanup of uncommitted files.
  ///
  /// \return Status::OK if the commit was successful, or an error
  Status Commit() override;

 protected:
  explicit SnapshotUpdate(std::shared_ptr<Transaction> transaction);

  /// \brief Write data manifests for the given data files
  ///
  /// \param data_files The data files to write
  /// \return A vector of manifest files
  std::vector<ManifestFile> WriteDataManifests(const std::vector<DataFile>& data_files);

  /// \brief Write delete manifests for the given delete files
  ///
  /// \param delete_files The delete files to write
  /// \return A vector of manifest files
  std::vector<ManifestFile> WriteDeleteManifests(
      const std::vector<DataFile>& delete_files);

  /// \brief Get the target branch name
  const std::string& target_branch() const { return target_branch_; }

  /// \brief Set the target branch name
  void SetTargetBranch(const std::string& branch);

  /// \brief Check if snapshot ID inheritance is enabled
  bool can_inherit_snapshot_id() const { return can_inherit_snapshot_id_; }

  /// \brief Get the target manifest size in bytes
  int64_t target_manifest_size_bytes() const { return target_manifest_size_bytes_; }

  /// \brief Get the commit UUID
  const std::string& commit_uuid() const { return commit_uuid_; }

  /// \brief Get the attempt number
  int32_t attempt() const { return attempt_.load(); }

  /// \brief Get the manifest count
  int32_t manifest_count() const { return manifest_count_.load(); }

  /// \brief Clean up any uncommitted manifests that were created.
  ///
  /// Manifests may not be committed if apply is called multiple times
  /// because a commit conflict has occurred. Implementations may keep
  /// around manifests because the same changes will be made by both
  /// apply calls. This method instructs the implementation to clean up
  /// those manifests and passes the paths of the manifests that were
  /// actually committed.
  ///
  /// \param committed A set of manifest paths that were actually committed
  virtual void CleanUncommitted(const std::unordered_set<std::string>& committed) = 0;

  /// \brief A string that describes the action that produced the new snapshot.
  ///
  /// \return A string operation name
  virtual std::string operation() = 0;

  /// \brief Validate the current metadata.
  ///
  /// Child operations can override this to add custom validation.
  ///
  /// \param current_metadata Current table metadata to validate
  /// \param snapshot Ending snapshot on the lineage which is being validated
  virtual Status Validate(const TableMetadata& current_metadata,
                          const std::shared_ptr<Snapshot>& snapshot) = 0;

  /// \brief Apply the update's changes to the given metadata and snapshot.
  /// Return the new manifest list.
  ///
  /// \param metadata_to_update The base table metadata to apply changes to
  /// \param snapshot Snapshot to apply the changes to
  /// \return A vector of manifest files for the new snapshot
  virtual std::vector<ManifestFile> Apply(
      const std::shared_ptr<TableMetadata>& metadata_to_update,
      const std::shared_ptr<Snapshot>& snapshot) = 0;

  /// \brief Get the summary map for this operation.
  ///
  /// \return A map of summary properties
  virtual std::unordered_map<std::string, std::string> Summary() = 0;

  /// \brief Check if cleanup should happen after commit
  ///
  /// \return True if cleanup should happen after commit
  virtual bool cleanup_after_commit() const { return true; }

 private:
  /// \brief Compute the final summary including totals
  std::unordered_map<std::string, std::string> ComputeSummary(
      const std::shared_ptr<TableMetadata>& previous);

  /// \brief Clean up all uncommitted files
  void CleanAll();

  /// \brief Delete a file using the configured delete function
  Status DeleteFile(const std::string& path);

  /// \brief Get the path for a manifest list file
  std::string ManifestListPath();

  /// \brief Update a total property in the summary
  void UpdateTotal(std::unordered_map<std::string, std::string>& summary,
                   const std::unordered_map<std::string, std::string>& previous_summary,
                   const std::string& total_property, const std::string& added_property,
                   const std::string& deleted_property);

  std::unordered_map<std::string, std::string> summary_properties_;
  std::function<Status(const std::string&)> delete_func_;
  bool stage_only_ = false;
  std::string target_branch_ = std::string(SnapshotRef::kMainBranch);

  std::optional<int64_t> snapshot_id_{std::nullopt};
  std::atomic<int32_t> attempt_{0};
  std::atomic<int32_t> manifest_count_{0};
  std::unordered_set<std::string> committed_manifest_paths_;
  std::vector<std::string> manifest_lists_;
  std::string commit_uuid_;
  std::shared_ptr<Snapshot> staged_snapshot_;

  int64_t target_manifest_size_bytes_;
  // For format version > 1, inheritance is enabled by default
  bool can_inherit_snapshot_id_{true};
};

}  // namespace iceberg
