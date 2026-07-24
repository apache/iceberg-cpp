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

/// \file iceberg/update/snapshot_update.h
/// \brief Define base APIs for snapshot updates.

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"
#include "iceberg/util/executor.h"

namespace iceberg {

/// \brief API for table changes that produce snapshots.
///
/// This class contains common methods for all updates that create a new table
/// Snapshot. It also provides the shared implementation for snapshot ID
/// assignment, manifest list writing, retry-safe apply, and cleanup.
class ICEBERG_EXPORT SnapshotUpdate : public PendingUpdate {
 public:
  /// \brief Result of applying a snapshot update
  struct ApplyResult {
    std::shared_ptr<Snapshot> snapshot;
    std::string target_branch;
    bool stage_only = false;
  };

  ~SnapshotUpdate() override;

  Kind kind() const override { return Kind::kUpdateSnapshot; }
  bool IsRetryable() const override { return true; }

  /// \brief Set a callback to delete files instead of the table's default.
  ///
  /// \param delete_func A function used to delete file locations.
  /// \return This update for method chaining.
  /// \note Cannot be called more than once.
  auto& DeleteWith(this auto& self,
                   std::function<Status(const std::string&)> delete_func) {
    if (self.delete_func_) {
      return self.AddError(ErrorKind::kInvalidArgument,
                           "Cannot set delete callback more than once");
    }
    self.delete_func_ = std::move(delete_func);
    return self;
  }

  /// \brief Stage a snapshot in table metadata, but do not make it current.
  ///
  /// The snapshot is assigned an ID and added to table metadata. The table's
  /// current snapshot ID is not updated.
  ///
  /// \return This update for method chaining.
  auto& StageOnly(this auto& self) {
    self.stage_only_ = true;
    return self;
  }

  /// \brief Configure an executor for manifest planning work.
  ///
  /// \param executor Executor to use while planning manifests.
  /// \return Reference to this for method chaining.
  auto& ScanManifestsWith(this auto& self, Executor& executor) {
    self.plan_executor_ = std::ref(executor);
    return self;
  }

  /// \brief Perform operations on a particular branch.
  ///
  /// \param branch The name of a SnapshotRef of type branch.
  /// \return This update for method chaining.
  auto& ToBranch(this auto& self, const std::string& branch) {
    if (branch.empty()) [[unlikely]] {
      return self.AddError(ErrorKind::kInvalidArgument, "Branch name cannot be empty");
    }

    if (auto status = static_cast<SnapshotUpdate&>(self).ValidateTargetBranch(branch);
        !status) [[unlikely]] {
      return self.AddError(status.error());
    }

    if (auto ref_it = self.base().refs.find(branch); ref_it != self.base().refs.end()) {
      if (ref_it->second->type() != SnapshotRefType::kBranch) {
        return self.AddError(ErrorKind::kInvalidArgument,
                             "{} is a tag, not a branch. Tags cannot be targets for "
                             "producing snapshots",
                             branch);
      }
    }

    self.target_branch_ = branch;
    return self;
  }

  /// \brief Set a summary property in the snapshot produced by this update.
  ///
  /// \param property A String property name.
  /// \param value A String property value.
  /// \return This update for method chaining.
  auto& Set(this auto& self, const std::string& property, const std::string& value) {
    static_cast<SnapshotUpdate&>(self).SetSummaryProperty(property, value);
    return self;
  }

  /// \brief Configure an executor and max writer count for writing new manifests.
  ///
  /// If this method is not called, manifest writes remain serial. When configured,
  /// files may be split into independent rolling-writer groups.
  ///
  /// \note Custom FileIO implementations and registered writer factories used for
  /// manifest writes must support concurrent calls when an executor is configured.
  auto& WriteManifestsWith(this auto& self, Executor& executor, int32_t parallelism) {
    if (parallelism <= 0) [[unlikely]] {
      return self.AddError(
          ErrorKind::kInvalidArgument,
          "Manifest write parallelism must be greater than 0, but was: {}", parallelism);
    }

    self.write_manifest_executor_ = std::ref(executor);
    self.write_manifest_parallelism_ = parallelism;
    return self;
  }

  /// \brief Apply the update's changes to create a new snapshot.
  ///
  /// This method validates the changes, applies them to the current base
  /// metadata, and creates a new snapshot without committing it. Commit retries
  /// call Apply() again with refreshed metadata so the same changes can be
  /// applied to the new latest snapshot.
  ///
  /// \return A result containing the new snapshot, or an error.
  Result<ApplyResult> Apply();

  /// \brief Finalize the snapshot update, cleaning up any uncommitted files.
  Status Finalize(Result<const TableMetadata*> commit_result) override;

 protected:
  struct ContentFileWithSequenceNumber {
    std::shared_ptr<DataFile> file;
    std::optional<int64_t> data_sequence_number;
  };

  explicit SnapshotUpdate(std::shared_ptr<TransactionContext> ctx);

  /// \brief Write data manifests for the given data files
  ///
  /// \param files Data files to write
  /// \param spec The partition spec to use
  /// \param data_sequence_number Optional data sequence number for the files
  /// \return A vector of manifest files
  Result<std::vector<ManifestFile>> WriteDataManifests(
      std::span<const std::shared_ptr<DataFile>> files,
      const std::shared_ptr<PartitionSpec>& spec,
      std::optional<int64_t> data_sequence_number = std::nullopt);

  Result<std::vector<ManifestFile>> WriteDeleteManifests(
      std::span<const ContentFileWithSequenceNumber> files,
      const std::shared_ptr<PartitionSpec>& spec);

  const std::string& target_branch() const { return target_branch_; }
  bool can_inherit_snapshot_id() const { return can_inherit_snapshot_id_; }
  const std::string& commit_uuid() const { return commit_uuid_; }
  int32_t manifest_count() const {
    return manifest_count_.load(std::memory_order_relaxed);
  }
  int32_t attempt() const { return attempt_.load(std::memory_order_relaxed); }
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
  virtual Status CleanUncommitted(const std::unordered_set<std::string>& committed) = 0;

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

  /// \brief Validate whether this update can target the given branch.
  virtual Status ValidateTargetBranch(const std::string& branch) const { return {}; }

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

  /// \brief Set a summary property.
  ///
  /// Implementations may override this to retain custom properties across
  /// retry-safe summary rebuilds.
  virtual void SetSummaryProperty(const std::string& property, const std::string& value);

  /// \brief Check if cleanup should happen after commit
  ///
  /// \return True if cleanup should happen after commit
  virtual bool CleanupAfterCommit() const { return true; }

  /// \brief Get or generate the snapshot ID for the new snapshot.
  int64_t SnapshotId();

  /// \brief Delete a file at the given path.
  ///
  /// \param path The path of the file to delete
  /// \return A status indicating the result of the deletion
  Status DeleteFile(const std::string& path);

  std::string ManifestPath();
  std::string ManifestListPath();
  OptionalExecutor plan_executor() const { return plan_executor_; }
  SnapshotSummaryBuilder& summary_builder() { return summary_; }
  SnapshotSummaryBuilder BuildManifestCountSummary(
      std::span<const ManifestFile> manifests, int32_t replaced_manifests_count);

 private:
  /// \brief Returns the snapshot summary from the implementation and updates totals.
  Result<std::unordered_map<std::string, std::string>> ComputeSummary(
      const TableMetadata& previous);

  /// \brief Clean up all uncommitted files
  Status CleanAll();

 protected:
  SnapshotSummaryBuilder summary_;

 private:
  const bool can_inherit_snapshot_id_{true};
  const std::string commit_uuid_;
  OptionalExecutor write_manifest_executor_;
  int32_t write_manifest_parallelism_{1};
  std::atomic<int32_t> manifest_count_{0};
  std::atomic<int32_t> attempt_{0};
  std::vector<std::string> manifest_lists_;
  const int64_t target_manifest_size_bytes_;
  std::optional<int64_t> snapshot_id_;
  OptionalExecutor plan_executor_;
  bool stage_only_{false};
  std::function<Status(const std::string&)> delete_func_;
  std::string target_branch_{SnapshotRef::kMainBranch};
  std::shared_ptr<Snapshot> staged_snapshot_;
};

}  // namespace iceberg
