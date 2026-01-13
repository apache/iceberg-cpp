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

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"

namespace iceberg {

/// \brief Base class for operations that produce snapshots.
///
/// This class provides common functionality for creating new snapshots,
/// including manifest list writing and cleanup.
class ICEBERG_EXPORT SnapshotUpdate : public PendingUpdate {
 public:
  /// \brief Result of applying a snapshot update
  struct ApplyResult {
    std::shared_ptr<Snapshot> snapshot;
    std::string target_branch;
    bool stage_only = false;
  };

  ~SnapshotUpdate() override;

  /// \brief Set a callback to delete files instead of the table's default.
  ///
  /// \param delete_func A function used to delete file locations
  /// \return Reference to this for method chaining
  /// \note Cannot be called more than once
  auto& DeleteWith(this auto& self,
                   std::function<Status(const std::string&)> delete_func) {
    if (self.delete_func_) {
      return self.AddError(ErrorKind::kInvalidArgument,
                           "Cannot set delete callback more than once");
    }
    self.delete_func_ = std::move(delete_func);
    return self;
  }

  /// \brief Stage a snapshot in table metadata, but not update the current snapshot id.
  ///
  /// \return Reference to this for method chaining
  auto& StageOnly(this auto& self) {
    self.stage_only_ = true;
    return self;
  }

  /// \brief Apply the update's changes to create a new snapshot.
  ///
  /// This method validates the changes, applies them to the metadata,
  /// and creates a new snapshot without committing it. The snapshot
  /// is stored internally and can be accessed after Apply() succeeds.
  ///
  /// \return A result containing the new snapshot, or an error
  Result<ApplyResult> Apply();

  /// \brief Finalize the snapshot update, cleaning up any uncommitted files.
  Status Finalize(std::optional<Error> commit_error) override;

 protected:
  explicit SnapshotUpdate(std::shared_ptr<Transaction> transaction);

  /// \brief Write data manifests for the given data files
  ///
  /// \param data_files The data files to write
  /// \param spec The partition spec to use
  /// \param data_sequence_number Optional data sequence number for the files
  /// \return A vector of manifest files
  Result<std::vector<ManifestFile>> WriteDataManifests(
      const std::vector<std::shared_ptr<DataFile>>& data_files,
      const std::shared_ptr<PartitionSpec>& spec,
      std::optional<int64_t> data_sequence_number = std::nullopt);

  /// \brief Write delete manifests for the given delete files
  ///
  /// \param delete_files The delete files to write
  /// \param spec The partition spec to use
  /// \return A vector of manifest files
  Result<std::vector<ManifestFile>> WriteDeleteManifests(
      const std::vector<std::shared_ptr<DataFile>>& delete_files,
      const std::shared_ptr<PartitionSpec>& spec);

  Status SetTargetBranch(const std::string& branch);
  const std::string& target_branch() const { return target_branch_; }
  bool can_inherit_snapshot_id() const { return can_inherit_snapshot_id_; }
  const std::string& commit_uuid() const { return commit_uuid_; }
  int32_t manifest_count() const { return manifest_count_; }
  int32_t attempt() const { return attempt_; }
  int64_t target_manifest_size_bytes() const { return target_manifest_size_bytes_; }

  /// \brief Clean up any uncommitted manifests that were created.
  ///
  /// Manifests may not be committed if Apply is called multiple times
  /// because a commit conflict has occurred. Implementations may keep
  /// around manifests because the same changes will be made by both
  /// Apply calls. This method instructs the implementation to clean up
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
                          const std::shared_ptr<Snapshot>& snapshot) {
    return {};
  };

  /// \brief Apply the update's changes to the given metadata and snapshot.
  ///
  /// \param metadata_to_update The base table metadata to apply changes to
  /// \param snapshot Snapshot to apply the changes to
  /// \return A vector of manifest files for the new snapshot
  virtual Result<std::vector<ManifestFile>> Apply(
      const TableMetadata& metadata_to_update,
      const std::shared_ptr<Snapshot>& snapshot) = 0;

  /// \brief Get the summary map for this operation.
  ///
  /// \return A map of summary properties
  virtual std::unordered_map<std::string, std::string> Summary() = 0;

  /// \brief Check if cleanup should happen after commit
  ///
  /// \return True if cleanup should happen after commit
  virtual bool CleanupAfterCommit() const { return true; }

  /// \brief Get or generate the snapshot ID for the new snapshot.
  int64_t SnapshotId();

 private:
  /// \brief Returns the snapshot summary from the implementation and updates totals.
  Result<std::unordered_map<std::string, std::string>> ComputeSummary(
      const TableMetadata& previous);

  /// \brief Clean up all uncommitted files
  void CleanAll();

  Status DeleteFile(const std::string& path);
  std::string ManifestListPath();
  std::string ManifestPath();

 private:
  const bool can_inherit_snapshot_id_{true};
  const std::string commit_uuid_;
  int32_t manifest_count_{0};
  int32_t attempt_{0};
  std::vector<std::string> manifest_lists_;
  const int64_t target_manifest_size_bytes_;
  std::optional<int64_t> snapshot_id_;
  bool stage_only_{false};
  std::function<Status(const std::string&)> delete_func_;
  std::string target_branch_{SnapshotRef::kMainBranch};
  std::shared_ptr<Snapshot> staged_snapshot_;
};

}  // namespace iceberg
