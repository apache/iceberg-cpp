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

#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

/// \brief The type of snapshot reference
enum class SnapshotRefType {
  /// Branches are mutable named references that can be updated by committing a new
  /// snapshot as the branch’s referenced snapshot using the Commit Conflict Resolution
  /// and Retry procedures.
  kBranch,
  /// Tags are labels for individual snapshots
  kTag,
};

/// \brief A reference to a snapshot, either a branch or a tag.
struct ICEBERG_EXPORT SnapshotRef {
  /// A reference's snapshot ID. The tagged snapshot or latest snapshot of a branch.
  int64_t snapshot_id;
  /// Type of the reference, tag or branch
  SnapshotRefType type;
  /// For branch type only, a positive number for the minimum number of snapshots to keep
  /// in a branch while expiring snapshots. Defaults to table property
  /// history.expire.min-snapshots-to-keep.
  std::optional<int32_t> min_snapshots_to_keep;
  /// For branch type only, a positive number for the max age of snapshots to keep when
  /// expiring, including the latest snapshot. Defaults to table property
  /// history.expire.max-snapshot-age-ms.
  std::optional<int64_t> max_snapshot_age_ms;
  /// For snapshot references except the main branch, a positive number for the max age of
  /// the snapshot reference to keep while expiring snapshots. Defaults to table property
  /// history.expire.max-ref-age-ms. The main branch never expires.
  std::optional<int64_t> max_ref_age_ms;
};

/// \brief Optional Snapshot Summary Fields
struct SnapshotSummaryFields {
  /// \brief The operation field key
  constexpr static std::string_view kOperation = "operation";

  /// Metrics, see https://iceberg.apache.org/spec/#metrics

  /// \brief Number of data files added in the snapshot
  constexpr static std::string_view kAddedDataFiles = "added-data-files";
  /// \brief Number of data files deleted in the snapshot
  constexpr static std::string_view kDeletedDataFiles = "deleted-data-files";
  /// \brief Total number of live data files in the snapshot
  constexpr static std::string_view kTotalDataFiles = "total-data-files";
  /// \brief Number of positional/equality delete files and deletion vectors added in the
  /// snapshot
  constexpr static std::string_view kAddedDeleteFiles = "added-delete-files";
  /// \brief Number of equality delete files added in the snapshot
  constexpr static std::string_view kAddedEqDeleteFiles = "added-equality-delete-files";
  /// \brief Number of equality delete files removed in the snapshot
  constexpr static std::string_view kRemovedEqDeleteFiles =
      "removed-equality-delete-files";
  /// \brief Number of position delete files added in the snapshot
  constexpr static std::string_view kAddedPosDeleteFiles = "added-position-delete-files";
  /// \brief Number of position delete files removed in the snapshot
  constexpr static std::string_view kRemovedPosDeleteFiles =
      "removed-position-delete-files";
  /// \brief Number of deletion vectors added in the snapshot
  constexpr static std::string_view kAddedDVS = "added-dvs";
  /// \brief Number of deletion vectors removed in the snapshot
  constexpr static std::string_view kRemovedDVS = "removed-dvs";
  /// \brief Number of positional/equality delete files and deletion vectors removed in
  /// the snapshot
  constexpr static std::string_view kRemovedDeleteFiles = "removed-delete-files";
  /// \brief Total number of live positional/equality delete files and deletion vectors in
  /// the snapshot
  constexpr static std::string_view kTotalDeleteFiles = "total-delete-files";
  /// \brief Number of records added in the snapshot
  constexpr static std::string_view kAddedRecords = "added-records";
  /// \brief Number of records deleted in the snapshot
  constexpr static std::string_view kDeletedRecords = "deleted-records";
  /// \brief Total number of records in the snapshot
  constexpr static std::string_view kTotalRecords = "total-records";
  /// \brief The size of files added in the snapshot
  constexpr static std::string_view kAddedFileSize = "added-files-size";
  /// \brief The size of files removed in the snapshot
  constexpr static std::string_view kRemovedFileSize = "removed-files-size";
  /// \brief Total size of live files in the snapshot
  constexpr static std::string_view kTotalFileSize = "total-files-size";
  /// \brief Number of position delete records added in the snapshot
  constexpr static std::string_view kAddedPosDeletes = "added-position-deletes";
  /// \brief Number of position delete records removed in the snapshot
  constexpr static std::string_view kRemovedPosDeletes = "removed-position-deletes";
  /// \brief Total number of position delete records in the snapshot
  constexpr static std::string_view kTotalPosDeletes = "total-position-deletes";
  /// \brief Number of equality delete records added in the snapshot
  constexpr static std::string_view kAddedEqDeletes = "added-equality-deletes";
  /// \brief Number of equality delete records removed in the snapshot
  constexpr static std::string_view kRemovedEqDeletes = "removed-equality-deletes";
  /// \brief Total number of equality delete records in the snapshot
  constexpr static std::string_view kTotalEqDeletes = "total-equality-deletes";
  /// \brief Number of duplicate files deleted (duplicates are files recorded more than
  /// once in the manifest)
  constexpr static std::string_view kDeletedDuplicatedFiles = "deleted-duplicate-files";
  /// \brief Number of partitions with files added or removed in the snapshot
  constexpr static std::string_view kChangedPartitionCountProp =
      "changed-partition-count";

  /// Other Fields, see https://iceberg.apache.org/spec/#other-fields

  /// \brief The Write-Audit-Publish id of a staged snapshot
  constexpr static std::string_view kWAPID = "wap.id";
  /// \brief The Write-Audit-Publish id of a snapshot already been published
  constexpr static std::string_view kPublishedWAPID = "published-wap-id";
  /// \brief The original id of a cherry-picked snapshot
  constexpr static std::string_view kSourceSnapshotID = "source-snapshot-id";
  /// \brief Name of the engine that created the snapshot
  constexpr static std::string_view kEngineName = "engine-name";
  /// \brief Version of the engine that created the snapshot
  constexpr static std::string_view kEngineVersion = "engine-version";
};

/// \brief Data operation that produce snapshots.
///
/// A snapshot can return the operation that created the snapshot to help other components
/// ignore snapshots that are not needed for some tasks. For example, snapshot expiration
/// does not need to clean up deleted files for appends, which have no deleted files.
struct ICEBERG_EXPORT DataOperation {
  /// \brief Only data files were added and no files were removed.
  static constexpr std::string_view kAppend = "append";
  /// \brief Data and delete files were added and removed without changing table data;
  /// i.e. compaction, change the data file format, or relocating data files.
  static constexpr std::string_view kReplace = "replace";
  /// \brief Data and delete files were added and removed in a logical overwrite
  /// operation.
  static constexpr std::string_view kOverwrite = "overwrite";
  /// \brief Data files were removed and their contents logically deleted and/or delete
  /// files were added to delete rows.
  static constexpr std::string_view kDelete = "delete";
};

/// \brief A snapshot of the data in a table at a point in time.
///
/// A snapshot consist of one or more file manifests, and the complete table contents is
/// the union of all the data files in those manifests.
///
/// Snapshots are created by table operations.
struct ICEBERG_EXPORT Snapshot {
  using manifest_list_t = std::string;
  using manifests_t = std::vector<std::string>;

  /// A unqiue long ID.
  int64_t snapshot_id;
  /// The snapshot ID of the snapshot's parent. Omitted for any snapshot with no parent.
  std::optional<int64_t> parent_snapshot_id;
  /// A monotonically increasing long that tracks the order of changes to a table.
  int64_t sequence_number;
  /// A timestamp when the snapshot was created, used for garbage collection and table
  /// inspection.
  int64_t timestamp_ms;
  /// The location of a manifest list for this snapshot that tracks manifest files with
  /// additional metadata.
  std::variant<manifest_list_t, manifests_t> manifest_list;
  /// A string map that summaries the snapshot changes, including operation.
  std::unordered_map<std::string, std::string> summary;
  /// ID of the table's current schema when the snapshot was created.
  std::optional<int32_t> schema_id;

  /// \brief Return the name of the DataOperations data operation that produced this
  /// snapshot.
  ///
  /// \return the operation that produced this snapshot, or nullopt if the operation is
  /// unknown.
  std::optional<std::string> operation() const;

  /// \brief Get the manifest list for this snapshot.
  ///
  /// \return the manifest list for this snapshot, or nullopt if the snapshot has no
  /// manifest list.
  std::optional<std::reference_wrapper<const manifest_list_t>> ManifestList() const;

  /// \brief Get the manifests for this snapshot.
  ///
  /// \return the manifests for this snapshot, or nullopt if the snapshot has no
  /// manifests.
  std::optional<std::reference_wrapper<const manifests_t>> Manifests() const;

  /// \brief Compare two snapshots for equality.
  friend bool operator==(const Snapshot& lhs, const Snapshot& rhs) {
    return lhs.Equals(rhs);
  }

  /// \brief Compare two snapshots for inequality.
  friend bool operator!=(const Snapshot& lhs, const Snapshot& rhs) {
    return !(lhs == rhs);
  }

 private:
  /// \brief Compare two snapshots for equality.
  bool Equals(const Snapshot& other) const;
};

}  // namespace iceberg
