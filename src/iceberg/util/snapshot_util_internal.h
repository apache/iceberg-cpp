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
#include <optional>
#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

/// \brief Utility functions for working with snapshots
/// \note All the returned std::shared_ptr<Snapshot> are guaranteed to be not null.
class ICEBERG_EXPORT SnapshotUtil {
 public:
  /// \brief Returns a vector of ancestors of the given snapshot.
  ///
  /// \param table The table
  /// \param snapshot_id The snapshot ID to start from
  /// \return A vector of ancestor snapshots
  static Result<std::vector<std::shared_ptr<Snapshot>>> AncestorsOf(const Table& table,
                                                                    int64_t snapshot_id);

  /// \brief Returns whether ancestor_snapshot_id is an ancestor of snapshot_id.
  ///
  /// \param table The table to check
  /// \param snapshot_id The snapshot ID to check
  /// \param ancestor_snapshot_id The ancestor snapshot ID to check for
  /// \return true if ancestor_snapshot_id is an ancestor of snapshot_id
  static Result<bool> IsAncestorOf(const Table& table, int64_t snapshot_id,
                                   int64_t ancestor_snapshot_id);

  /// \brief Returns whether ancestor_snapshot_id is an ancestor of the table's current
  /// state.
  ///
  /// \param table The table to check
  /// \param ancestor_snapshot_id The ancestor snapshot ID to check for
  /// \return true if ancestor_snapshot_id is an ancestor of the current snapshot
  static Result<bool> IsAncestorOf(const Table& table, int64_t ancestor_snapshot_id);

  /// \brief Returns whether some ancestor of snapshot_id has parentId matches
  /// ancestor_parent_snapshot_id.
  ///
  /// \param table The table to check
  /// \param snapshot_id The snapshot ID to check
  /// \param ancestor_parent_snapshot_id The ancestor parent snapshot ID to check for
  /// \return true if any ancestor has the given parent ID
  static Result<bool> IsParentAncestorOf(const Table& table, int64_t snapshot_id,
                                         int64_t ancestor_parent_snapshot_id);

  /// \brief Returns a vector that traverses the table's snapshots from the current to the
  /// last known ancestor.
  ///
  /// \param table The table
  /// \return A vector from the table's current snapshot to its last known ancestor
  static Result<std::vector<std::shared_ptr<Snapshot>>> CurrentAncestors(
      const Table& table);

  /// \brief Returns the snapshot IDs for the ancestors of the current table state.
  ///
  /// Ancestor IDs are ordered by commit time, descending. The first ID is the current
  /// snapshot, followed by its parent, and so on.
  ///
  /// \param table The table
  /// \return A vector of snapshot IDs of the known ancestor snapshots, including the
  /// current ID
  static Result<std::vector<int64_t>> CurrentAncestorIds(const Table& table);

  /// \brief Traverses the history of the table's current snapshot and finds the oldest
  /// Snapshot.
  ///
  /// \param table The table
  /// \return The oldest snapshot, or nullopt if there is no current snapshot
  static Result<std::optional<std::shared_ptr<Snapshot>>> OldestAncestor(
      const Table& table);

  /// \brief Traverses the history and finds the oldest ancestor of the specified
  /// snapshot.
  ///
  /// Oldest ancestor is defined as the ancestor snapshot whose parent is null or has been
  /// expired. If the specified snapshot has no parent or parent has been expired, the
  /// specified snapshot itself is returned.
  ///
  /// \param table The table
  /// \param snapshot_id The ID of the snapshot to find the oldest ancestor
  /// \return The oldest snapshot, or nullopt if not found
  static Result<std::optional<std::shared_ptr<Snapshot>>> OldestAncestorOf(
      const Table& table, int64_t snapshot_id);

  /// \brief Traverses the history of the table's current snapshot, finds the oldest
  /// snapshot that was committed either at or after a given time.
  ///
  /// \param table The table
  /// \param timestamp_ms A timestamp in milliseconds
  /// \return The first snapshot after the given timestamp, or nullopt if the current
  /// snapshot is older than the timestamp. If the first ancestor after the given time
  /// can't be determined, returns a NotFound error.
  static Result<std::optional<std::shared_ptr<Snapshot>>> OldestAncestorAfter(
      const Table& table, TimePointMs timestamp_ms);

  /// \brief Returns list of snapshot ids in the range (from_snapshot_id,to_snapshot_id]
  ///
  /// This method assumes that from_snapshot_id is an ancestor of to_snapshot_id.
  ///
  /// \param table The table
  /// \param from_snapshot_id The starting snapshot ID (exclusive)
  /// \param to_snapshot_id The ending snapshot ID (inclusive)
  /// \return A vector of snapshot IDs in the range
  static Result<std::vector<int64_t>> SnapshotIdsBetween(const Table& table,
                                                         int64_t from_snapshot_id,
                                                         int64_t to_snapshot_id);

  /// \brief Returns a vector of ancestor IDs between two snapshots.
  ///
  /// \param table The table
  /// \param latest_snapshot_id The latest snapshot ID
  /// \param oldest_snapshot_id The oldest snapshot ID (optional, nullopt means all
  /// ancestors)
  /// \return A vector of snapshot IDs between the two snapshots
  static Result<std::vector<int64_t>> AncestorIdsBetween(
      const Table& table, int64_t latest_snapshot_id,
      const std::optional<int64_t>& oldest_snapshot_id);

  /// \brief Returns a vector of ancestors between two snapshots.
  ///
  /// \param table The table
  /// \param latest_snapshot_id The latest snapshot ID
  /// \param oldest_snapshot_id The oldest snapshot ID (optional, nullopt means all
  /// ancestors)
  /// \return A vector of ancestor snapshots between the two snapshots
  static Result<std::vector<std::shared_ptr<Snapshot>>> AncestorsBetween(
      const Table& table, int64_t latest_snapshot_id,
      std::optional<int64_t> oldest_snapshot_id);

  /// \brief Traverses the history of the table's current snapshot and finds the snapshot
  /// with the given snapshot id as its parent.
  ///
  /// \param table The table
  /// \param snapshot_id The parent snapshot ID
  /// \return The snapshot for which the given snapshot is the parent
  static Result<std::shared_ptr<Snapshot>> SnapshotAfter(const Table& table,
                                                         int64_t snapshot_id);

  /// \brief Returns the ID of the most recent snapshot for the table as of the timestamp.
  ///
  /// \param table The table
  /// \param timestamp_ms The timestamp in millis since the Unix epoch
  /// \return The snapshot ID
  static Result<int64_t> SnapshotIdAsOfTime(const Table& table, TimePointMs timestamp_ms);

  /// \brief Returns the ID of the most recent snapshot for the table as of the timestamp,
  /// or nullopt if not found.
  ///
  /// \param table The table
  /// \param timestamp_ms The timestamp in millis since the Unix epoch
  /// \return The snapshot ID, or nullopt if not found
  static std::optional<int64_t> OptionalSnapshotIdAsOfTime(const Table& table,
                                                           TimePointMs timestamp_ms);

  /// \brief Returns the schema of the table for the specified snapshot.
  ///
  /// \param table The table
  /// \param snapshot_id The ID of the snapshot
  /// \return The schema
  static Result<std::shared_ptr<Schema>> SchemaFor(const Table& table,
                                                   int64_t snapshot_id);

  /// \brief Returns the schema of the table for the specified timestamp.
  ///
  /// \param table The table
  /// \param timestamp_ms The timestamp in millis since the Unix epoch
  /// \return The schema
  static Result<std::shared_ptr<Schema>> SchemaFor(const Table& table,
                                                   TimePointMs timestamp_ms);

  /// \brief Return the schema of the snapshot at a given ref.
  ///
  /// If the ref does not exist or the ref is a branch, the table schema is returned
  /// because it will be the schema when the new branch is created. If the ref is a tag,
  /// then the snapshot schema is returned.
  ///
  /// \param table The table
  /// \param ref Ref name of the table (empty string means main branch)
  /// \return Schema of the specific snapshot at the given ref
  static Result<std::shared_ptr<Schema>> SchemaFor(const Table& table,
                                                   const std::string& ref);

  /// \brief Return the schema of the snapshot at a given ref.
  ///
  /// If the ref does not exist or the ref is a branch, the table schema is returned
  /// because it will be the schema when the new branch is created. If the ref is a tag,
  /// then the snapshot schema is returned.
  ///
  /// \param metadata The table metadata
  /// \param ref Ref name of the table (empty string means main branch)
  /// \return Schema of the specific snapshot at the given branch
  static Result<std::shared_ptr<Schema>> SchemaFor(const TableMetadata& metadata,
                                                   const std::string& ref);

  /// \brief Fetch the snapshot at the head of the given branch in the given table.
  ///
  /// This method calls Table::current_snapshot() instead of using branch API for the main
  /// branch so that existing code still goes through the old code path to ensure
  /// backwards compatibility.
  ///
  /// \param table The table
  /// \param branch Branch name of the table (empty string means main branch)
  /// \return The latest snapshot for the given branch
  static Result<std::shared_ptr<Snapshot>> LatestSnapshot(const Table& table,
                                                          const std::string& branch);

  /// \brief Fetch the snapshot at the head of the given branch in the given table.
  ///
  /// This method calls TableMetadata::Snapshot() instead of using branch API for the main
  /// branch so that existing code still goes through the old code path to ensure
  /// backwards compatibility.
  ///
  /// If branch does not exist, the table's latest snapshot is returned it will be the
  /// schema when the new branch is created.
  ///
  /// \param metadata The table metadata
  /// \param branch Branch name of the table metadata (empty string means main
  /// branch)
  /// \return The latest snapshot for the given branch
  static Result<std::shared_ptr<Snapshot>> LatestSnapshot(const TableMetadata& metadata,
                                                          const std::string& branch);

 private:
  /// \brief Helper function to traverse ancestors of a snapshot.
  ///
  /// \param table The table
  /// \param snapshot The snapshot to start from
  /// \return A vector of ancestor snapshots
  static Result<std::vector<std::shared_ptr<Snapshot>>> AncestorsOf(
      const Table& table, const std::shared_ptr<Snapshot>& snapshot);

  /// \brief Helper function to traverse ancestors of a snapshot using a lookup function.
  ///
  /// \param snapshot The snapshot to start from
  /// \param lookup Function to lookup snapshots by ID
  /// \return A vector of ancestor snapshots
  static Result<std::vector<std::shared_ptr<Snapshot>>> AncestorsOf(
      const std::shared_ptr<Snapshot>& snapshot,
      const std::function<Result<std::shared_ptr<Snapshot>>(int64_t)>& lookup);

  /// \brief Helper function to convert snapshots to IDs.
  ///
  /// \param snapshots The snapshots
  /// \return A vector of snapshot IDs
  static Result<std::vector<int64_t>> ToIds(
      const std::vector<std::shared_ptr<Snapshot>>& snapshots);
};

}  // namespace iceberg
