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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"
#include "iceberg/util/timepoint.h"

/// \file iceberg/update/expire_snapshots.h
/// \brief API for removing old snapshots from a table.

namespace iceberg {

/// \brief An enum representing possible clean up levels used in snapshot expiration.
enum class CleanupLevel : uint8_t {
  /// Skip all file cleanup, only remove snapshot metadata.
  kNone,
  /// Clean up only metadata files (manifests, manifest lists, statistics), retain data
  /// files.
  kMetadataOnly,
  /// Clean up both metadata and data files (default).
  kAll
};

/// \brief API for removing old snapshots from a table.
///
/// This API accumulates snapshot deletions and commits the new list to the table. This
/// API does not allow deleting the current snapshot.
///
/// When committing, these changes will be applied to the latest table metadata. Commit
/// conflicts will be resolved by applying the changes to the new latest metadata and
/// reattempting the commit.
///
/// Manifest files that are no longer used by valid snapshots will be deleted. Data files
/// that were deleted by snapshots that are expired will be deleted. DeleteWith() can be
/// used to pass an alternative deletion method.
///
/// Apply() returns a list of the snapshots that will be removed.
class ICEBERG_EXPORT ExpireSnapshots : public PendingUpdate {
 public:
  static Result<std::shared_ptr<ExpireSnapshots>> Make(
      std::shared_ptr<TransactionContext> ctx);

  ~ExpireSnapshots() override;

  struct ApplyResult {
    std::vector<std::string> refs_to_remove;
    std::vector<int64_t> snapshot_ids_to_remove;
    std::vector<int32_t> partition_spec_ids_to_remove;
    std::unordered_set<int32_t> schema_ids_to_remove;
  };

  /// \brief Expires a specific Snapshot identified by id.
  ///
  /// \param snapshot_id Long id of the snapshot to expire.
  /// \return Reference to this for method chaining.
  ExpireSnapshots& ExpireSnapshotId(int64_t snapshot_id);

  /// \brief Expires all snapshots older than the given timestamp.
  ///
  /// \param timestamp_millis A long timestamp in milliseconds.
  /// \return Reference to this for method chaining.
  ExpireSnapshots& ExpireOlderThan(int64_t timestamp_millis);

  /// \brief Retains the most recent ancestors of the current snapshot.
  ///
  /// If a snapshot would be expired because it is older than the expiration timestamp,
  /// but is one of the num_snapshots most recent ancestors of the current state, it will
  /// be retained. This will not cause snapshots explicitly identified by id from
  /// expiring.
  ///
  /// This may keep more than num_snapshots ancestors if snapshots are added concurrently.
  /// This may keep less than num_snapshots ancestors if the current table state does not
  /// have that many.
  ///
  /// \param num_snapshots The number of snapshots to retain.
  /// \return Reference to this for method chaining.
  ExpireSnapshots& RetainLast(int num_snapshots);

  /// \brief Passes an alternative delete implementation that will be used for manifests
  /// and data files.
  ///
  /// Manifest files that are no longer used by valid snapshots will be deleted. Data
  /// files that were deleted by snapshots that are expired will be deleted.
  ///
  /// If this method is not called, unnecessary manifests and data files will still be
  /// deleted.
  ///
  /// \param delete_func A function that will be called to delete manifests and data files
  /// \return Reference to this for method chaining.
  ExpireSnapshots& DeleteWith(std::function<void(const std::string&)> delete_func);

  /// \brief Configures the cleanup level for expired files.
  ///
  /// This method provides fine-grained control over which files are cleaned up during
  /// snapshot expiration.
  ///
  /// Consider CleanupLevel::kMetadataOnly when data files are shared across tables or
  /// when using procedures like add-files that may reference the same data files.
  ///
  /// Consider CleanupLevel::kNone when data and metadata files may be more efficiently
  /// removed using a distributed framework through the actions API.
  ///
  /// \param level The cleanup level to use for expired snapshots.
  /// \return Reference to this for method chaining.
  ExpireSnapshots& CleanupLevel(enum CleanupLevel level);

  /// \brief Enable cleaning up unused metadata, such as partition specs, schemas, etc.
  ///
  /// \param clean Remove unused partition specs, schemas, or other metadata when true.
  /// \return Reference to this for method chaining.
  ExpireSnapshots& CleanExpiredMetadata(bool clean);

  Kind kind() const final { return Kind::kExpireSnapshots; }

  /// \brief Apply the pending changes and return the results
  /// \return The results of changes
  Result<ApplyResult> Apply();

  /// \brief Finalize the expire snapshots update, cleaning up expired files.
  ///
  /// After a successful commit, this method deletes manifest files, manifest lists,
  /// data files, and statistics files that are no longer referenced by any valid
  /// snapshot. The cleanup behavior is controlled by the CleanupLevel setting.
  ///
  /// \param commit_error An optional error indicating whether the commit was successful
  /// \return Status indicating success or failure
  Status Finalize(std::optional<Error> commit_error) override;

 private:
  explicit ExpireSnapshots(std::shared_ptr<TransactionContext> ctx);

  using SnapshotToRef = std::unordered_map<std::string, std::shared_ptr<SnapshotRef>>;

  Result<SnapshotToRef> ComputeRetainedRefs(const SnapshotToRef& refs) const;

  Result<std::unordered_set<int64_t>> ComputeBranchSnapshotsToRetain(
      int64_t snapshot_id, TimePointMs expire_snapshot_older_than,
      int32_t min_snapshots_to_keep) const;

  Result<std::unordered_set<int64_t>> ComputeAllBranchSnapshotIdsToRetain(
      const SnapshotToRef& refs) const;

  Result<std::unordered_set<int64_t>> UnreferencedSnapshotIdsToRetain(
      const SnapshotToRef& refs) const;

  /// \brief Clean up files no longer referenced after snapshot expiration.
  ///
  /// Implements the "reachable file cleanup" strategy from Java's ReachableFileCleanup:
  /// 1. Collect manifests from expired and retained snapshots
  /// 2. Prune manifests still referenced by retained snapshots
  /// 3. Find data files only in manifests being deleted (if kAll)
  /// 4. Remove data files still reachable from retained manifests
  /// 5. Delete orphaned manifests, manifest lists, and statistics files
  ///
  /// All deletions are best-effort: failures are suppressed to avoid blocking
  /// metadata updates (matching Java's suppressFailureWhenFinished behavior).
  ///
  /// Branch/tag awareness: retained_snapshot_ids includes all snapshots referenced
  /// by any branch or tag, as computed by Apply(). This prevents deleting files
  /// that are still reachable from any ref.
  ///
  /// TODO(shangxinli): Add multi-threaded file deletion support.
  /// TODO(shangxinli): Add IncrementalFileCleanup strategy for linear ancestry.
  Status CleanExpiredFiles(const std::vector<int64_t>& expired_snapshot_ids);

  /// \brief Read manifest paths from a single snapshot.
  /// Best-effort: returns OK even if the snapshot or its manifests can't be read.
  Status ReadManifestsForSnapshot(int64_t snapshot_id,
                                  std::unordered_set<std::string>& manifest_paths);

  /// \brief Find data files to delete by reading live entries from manifests being
  /// deleted, then subtracting files still reachable from retained manifests.
  /// If a retained manifest cannot be read, returns an empty set to prevent
  /// accidental data loss.
  Result<std::unordered_set<std::string>> FindDataFilesToDelete(
      const std::unordered_set<std::string>& manifests_to_delete,
      const std::unordered_set<std::string>& retained_manifests);

  /// \brief Create a ManifestReader for the given ManifestFile.
  Result<std::shared_ptr<ManifestReader>> MakeManifestReader(
      const ManifestFile& manifest, const std::shared_ptr<FileIO>& file_io);

  /// \brief Delete a file, suppressing errors (best-effort).
  /// Uses the custom delete function if set, otherwise FileIO::DeleteFile.
  void DeleteFilePath(const std::string& path);

 private:
  const TimePointMs current_time_ms_;
  const int64_t default_max_ref_age_ms_;
  int32_t default_min_num_snapshots_;
  TimePointMs default_expire_older_than_;
  std::function<void(const std::string&)> delete_func_;
  std::vector<int64_t> snapshot_ids_to_expire_;
  enum CleanupLevel cleanup_level_ { CleanupLevel::kAll };
  bool clean_expired_metadata_{false};
  bool specified_snapshot_id_{false};

  /// Cached result from Apply(), used during Finalize() for file cleanup
  std::optional<ApplyResult> apply_result_;

  /// Cache of manifest path -> ManifestFile, built during ReadManifestsForSnapshot
  /// to avoid O(M*S) repeated I/O from re-reading manifest lists in
  /// FindDataFilesToDelete.
  std::unordered_map<std::string, ManifestFile> manifest_cache_;
};

}  // namespace iceberg
