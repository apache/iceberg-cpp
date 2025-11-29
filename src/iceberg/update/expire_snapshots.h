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

/// \file iceberg/update/expire_snapshots.h
/// API for removing old snapshots from a table

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/pending_update.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Cleanup level for snapshot expiration
///
/// Controls which files are deleted during snapshot expiration.
enum class CleanupLevel {
  /// Skip all file cleanup, only remove snapshot metadata
  kNone,
  /// Clean up only metadata files (manifests, manifest lists, statistics),
  /// retain data files
  kMetadataOnly,
  /// Clean up both metadata and data files (default)
  kAll,
};

/// \brief API for removing old snapshots from a table
///
/// ExpireSnapshots accumulates snapshot deletions and commits the new snapshot
/// list to the table. This API does not allow deleting the current snapshot.
///
/// When committing, changes are applied to the latest table metadata. Commit
/// conflicts are resolved by applying the changes to the new latest metadata
/// and reattempting the commit.
///
/// Manifest files that are no longer used by valid snapshots will be deleted.
/// Data files that were deleted by snapshots that are expired will be deleted.
/// DeleteWith() can be used to pass an alternative deletion method.
///
/// Apply() returns a list of the snapshots that will be removed (preview mode).
///
/// Example usage:
/// \code
///   table.ExpireSnapshots()
///       .ExpireOlderThan(timestampMillis)
///       .RetainLast(5)
///       .Commit();
/// \endcode
class ICEBERG_EXPORT ExpireSnapshots
    : public PendingUpdateTyped<std::vector<std::shared_ptr<Snapshot>>> {
 public:
  /// \brief Constructor for ExpireSnapshots operation
  ///
  /// \param table The table to expire snapshots from
  explicit ExpireSnapshots(Table* table);
  ~ExpireSnapshots() override = default;

  /// \brief Expire a specific snapshot identified by id
  ///
  /// Marks a specific snapshot for removal. This method can be called multiple
  /// times to expire multiple snapshots. Snapshots marked by this method will
  /// be expired even if they would be retained by RetainLast().
  ///
  /// \param snapshot_id ID of the snapshot to expire
  /// \return Reference to this for method chaining
  ExpireSnapshots& ExpireSnapshotId(int64_t snapshot_id);

  /// \brief Expire all snapshots older than the given timestamp
  ///
  /// Sets a timestamp threshold - all snapshots created before this time will
  /// be expired (unless retained by RetainLast()).
  ///
  /// \param timestamp_millis Timestamp in milliseconds since epoch
  /// \return Reference to this for method chaining
  ExpireSnapshots& ExpireOlderThan(int64_t timestamp_millis);

  /// \brief Retain the most recent ancestors of the current snapshot
  ///
  /// If a snapshot would be expired because it is older than the expiration
  /// timestamp, but is one of the num_snapshots most recent ancestors of the
  /// current state, it will be retained. This will not prevent snapshots
  /// explicitly identified by ExpireSnapshotId() from expiring.
  ///
  /// This may keep more than num_snapshots ancestors if snapshots are added
  /// concurrently. This may keep less than num_snapshots ancestors if the
  /// current table state does not have that many.
  ///
  /// \param num_snapshots The number of snapshots to retain
  /// \return Reference to this for method chaining
  ExpireSnapshots& RetainLast(int num_snapshots);

  /// \brief Set a custom file deletion callback
  ///
  /// Passes an alternative delete implementation that will be used for
  /// manifests and data files. If this method is not called, unnecessary
  /// manifests and data files will still be deleted using the default method.
  ///
  /// Manifest files that are no longer used by valid snapshots will be deleted.
  /// Data files that were deleted by snapshots that are expired will be deleted.
  ///
  /// \param delete_func Callback function that will be called for each file to delete
  /// \return Reference to this for method chaining
  ExpireSnapshots& DeleteWith(std::function<void(std::string_view)> delete_func);

  /// \brief Configure the cleanup level for expired files
  ///
  /// This method provides fine-grained control over which files are cleaned up
  /// during snapshot expiration.
  ///
  /// Use CleanupLevel::kMetadataOnly when data files are shared across tables or
  /// when using procedures like add-files that may reference the same data files.
  ///
  /// Use CleanupLevel::kNone when data and metadata files may be more efficiently
  /// removed using a distributed framework through the actions API.
  ///
  /// \param level The cleanup level to use for expired snapshots
  /// \return Reference to this for method chaining
  ExpireSnapshots& SetCleanupLevel(CleanupLevel level);

  /// \brief Apply the pending changes and return the uncommitted result
  ///
  /// This does not result in a permanent update.
  ///
  /// \return the list of snapshots that would be expired, or an error:
  ///         - ValidationFailed: if pending changes cannot be applied
  Result<std::vector<std::shared_ptr<Snapshot>>> Apply() override;

  /// \brief Apply and commit the pending changes to the table
  ///
  /// Changes are committed by calling the underlying table's commit operation.
  ///
  /// Once the commit is successful, the updated table will be refreshed.
  ///
  /// \return Status::OK if the commit was successful, or an error:
  ///         - ValidationFailed: if update cannot be applied to current metadata
  ///         - CommitFailed: if update cannot be committed due to conflicts
  Status Commit() override;

  // Non-copyable, movable (inherited from PendingUpdate)
  ExpireSnapshots(const ExpireSnapshots&) = delete;
  ExpireSnapshots& operator=(const ExpireSnapshots&) = delete;

 private:
  Table* table_;
  std::vector<int64_t> snapshot_ids_to_expire_;
  std::optional<int64_t> expire_older_than_ms_;
  std::optional<int> retain_last_;
  std::optional<std::function<void(std::string_view)>> delete_func_;
  CleanupLevel cleanup_level_ = CleanupLevel::kAll;
};

}  // namespace iceberg
