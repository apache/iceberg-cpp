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
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
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
      std::shared_ptr<Transaction> transaction);

  ~ExpireSnapshots() override;

  using SnapshotToRef = std::unordered_map<std::string, std::shared_ptr<SnapshotRef>>;

  struct ExpireSnapshotsResult {
    SnapshotToRef ref_to_remove;
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
  Result<ExpireSnapshotsResult> Apply();

  Status Commit() override;

 private:
  explicit ExpireSnapshots([[maybe_unused]] std::shared_ptr<Transaction> transaction);

  Result<std::vector<int64_t>> ComputeBranchSnapshotsToRetain(
      const Table& table, int64_t snapshot, int64_t expire_snapshot_older_than,
      int32_t min_snapshots_to_keep);

  Result<std::vector<int64_t>> ComputeAllBranchSnapshotIds(
      const Table& table, const SnapshotToRef& retained_refs);

  Result<std::vector<int64_t>> UnreferencedSnapshotIds(
      const Table& table, const TableMetadata& current_metadata,
      const SnapshotToRef& retained_refs);

 private:
  // Internal state
  int32_t default_min_num_snapshots_;
  int64_t default_max_ref_age_ms_;
  int64_t default_expire_older_than_;
  TimePointMs current_time_ms_;
  std::vector<int64_t> snapshot_ids_to_expire_;
  std::function<void(const std::string&)> delete_func_;
  enum CleanupLevel cleanup_level_ { CleanupLevel::kAll };
  bool clean_expired_metadata_{false};
};

}  // namespace iceberg
