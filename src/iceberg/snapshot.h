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
#include <string_view>
#include <unordered_map>
#include <variant>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/util/timepoint.h"
#include "iceberg/util/unreachable.h"

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

/// \brief Get the relative snapshot reference type name
ICEBERG_EXPORT constexpr std::string_view SnapshotRefTypeToString(
    SnapshotRefType type) noexcept {
  switch (type) {
    case SnapshotRefType::kBranch:
      return "branch";
    case SnapshotRefType::kTag:
      return "tag";
  }
  internal::Unreachable(
      std::format("Invalid snapshot reference type: {}", static_cast<int>(type)));
}
/// \brief Get the relative snapshot reference type from name
ICEBERG_EXPORT constexpr Result<SnapshotRefType> SnapshotRefTypeFromString(
    std::string_view str) noexcept {
  if (str == "branch") return SnapshotRefType::kBranch;
  if (str == "tag") return SnapshotRefType::kTag;
  return InvalidArgument("Invalid snapshot reference type: {}", str);
}

/// \brief A reference to a snapshot, either a branch or a tag.
struct ICEBERG_EXPORT SnapshotRef {
  struct ICEBERG_EXPORT Branch {
    /// A positive number for the minimum number of snapshots to keep in a branch while
    /// expiring snapshots. Defaults to table property
    /// history.expire.min-snapshots-to-keep.
    std::optional<int32_t> min_snapshots_to_keep;
    /// A positive number for the max age of snapshots to keep when
    /// expiring, including the latest snapshot. Defaults to table property
    /// history.expire.max-snapshot-age-ms.
    std::optional<int64_t> max_snapshot_age_ms;
    /// For snapshot references except the main branch, a positive number for the max age
    /// of the snapshot reference to keep while expiring snapshots. Defaults to table
    /// property history.expire.max-ref-age-ms. The main branch never expires.
    std::optional<int64_t> max_ref_age_ms;

    /// \brief Compare two branches for equality.
    friend bool operator==(const Branch& lhs, const Branch& rhs) {
      return lhs.Equals(rhs);
    }

   private:
    /// \brief Compare two branches for equality.
    bool Equals(const Branch& other) const;
  };

  struct ICEBERG_EXPORT Tag {
    /// For snapshot references except the main branch, a positive number for the max age
    /// of the snapshot reference to keep while expiring snapshots. Defaults to table
    /// property history.expire.max-ref-age-ms. The main branch never expires.
    std::optional<int64_t> max_ref_age_ms;

    /// \brief Compare two tags for equality.
    friend bool operator==(const Tag& lhs, const Tag& rhs) { return lhs.Equals(rhs); }

   private:
    /// \brief Compare two tags for equality.
    bool Equals(const Tag& other) const;
  };

  /// A reference's snapshot ID. The tagged snapshot or latest snapshot of a branch.
  int64_t snapshot_id;
  /// Snapshot retention policy
  std::variant<Branch, Tag> retention;

  SnapshotRefType type() const noexcept;

  /// \brief Compare two snapshot refs for equality
  friend bool operator==(const SnapshotRef& lhs, const SnapshotRef& rhs) {
    return lhs.Equals(rhs);
  }

 private:
  /// \brief Compare two snapshot refs for equality.
  bool Equals(const SnapshotRef& other) const;
};

/// \brief Optional Snapshot Summary Fields
struct SnapshotSummaryFields {
  /// \brief The operation field key
  inline static const std::string kOperation = "operation";

  /// Metrics, see https://iceberg.apache.org/spec/#metrics

  /// \brief Number of data files added in the snapshot
  inline static const std::string kAddedDataFiles = "added-data-files";
  /// \brief Number of data files deleted in the snapshot
  inline static const std::string kDeletedDataFiles = "deleted-data-files";
  /// \brief Total number of live data files in the snapshot
  inline static const std::string kTotalDataFiles = "total-data-files";
  /// \brief Number of positional/equality delete files and deletion vectors added in the
  /// snapshot
  inline static const std::string kAddedDeleteFiles = "added-delete-files";
  /// \brief Number of equality delete files added in the snapshot
  inline static const std::string kAddedEqDeleteFiles = "added-equality-delete-files";
  /// \brief Number of equality delete files removed in the snapshot
  inline static const std::string kRemovedEqDeleteFiles = "removed-equality-delete-files";
  /// \brief Number of position delete files added in the snapshot
  inline static const std::string kAddedPosDeleteFiles = "added-position-delete-files";
  /// \brief Number of position delete files removed in the snapshot
  inline static const std::string kRemovedPosDeleteFiles =
      "removed-position-delete-files";
  /// \brief Number of deletion vectors added in the snapshot
  inline static const std::string kAddedDVs = "added-dvs";
  /// \brief Number of deletion vectors removed in the snapshot
  inline static const std::string kRemovedDVs = "removed-dvs";
  /// \brief Number of positional/equality delete files and deletion vectors removed in
  /// the snapshot
  inline static const std::string kRemovedDeleteFiles = "removed-delete-files";
  /// \brief Total number of live positional/equality delete files and deletion vectors in
  /// the snapshot
  inline static const std::string kTotalDeleteFiles = "total-delete-files";
  /// \brief Number of records added in the snapshot
  inline static const std::string kAddedRecords = "added-records";
  /// \brief Number of records deleted in the snapshot
  inline static const std::string kDeletedRecords = "deleted-records";
  /// \brief Total number of records in the snapshot
  inline static const std::string kTotalRecords = "total-records";
  /// \brief The size of files added in the snapshot
  inline static const std::string kAddedFileSize = "added-files-size";
  /// \brief The size of files removed in the snapshot
  inline static const std::string kRemovedFileSize = "removed-files-size";
  /// \brief Total size of live files in the snapshot
  inline static const std::string kTotalFileSize = "total-files-size";
  /// \brief Number of position delete records added in the snapshot
  inline static const std::string kAddedPosDeletes = "added-position-deletes";
  /// \brief Number of position delete records removed in the snapshot
  inline static const std::string kRemovedPosDeletes = "removed-position-deletes";
  /// \brief Total number of position delete records in the snapshot
  inline static const std::string kTotalPosDeletes = "total-position-deletes";
  /// \brief Number of equality delete records added in the snapshot
  inline static const std::string kAddedEqDeletes = "added-equality-deletes";
  /// \brief Number of equality delete records removed in the snapshot
  inline static const std::string kRemovedEqDeletes = "removed-equality-deletes";
  /// \brief Total number of equality delete records in the snapshot
  inline static const std::string kTotalEqDeletes = "total-equality-deletes";
  /// \brief Number of duplicate files deleted (duplicates are files recorded more than
  /// once in the manifest)
  inline static const std::string kDeletedDuplicatedFiles = "deleted-duplicate-files";
  /// \brief Number of partitions with files added or removed in the snapshot
  inline static const std::string kChangedPartitionCountProp = "changed-partition-count";

  /// Other Fields, see https://iceberg.apache.org/spec/#other-fields

  /// \brief The Write-Audit-Publish id of a staged snapshot
  inline static const std::string kWAPId = "wap.id";
  /// \brief The Write-Audit-Publish id of a snapshot already been published
  inline static const std::string kPublishedWAPId = "published-wap-id";
  /// \brief The original id of a cherry-picked snapshot
  inline static const std::string kSourceSnapshotId = "source-snapshot-id";
  /// \brief Name of the engine that created the snapshot
  inline static const std::string kEngineName = "engine-name";
  /// \brief Version of the engine that created the snapshot
  inline static const std::string kEngineVersion = "engine-version";
};

/// \brief Data operation that produce snapshots.
///
/// A snapshot can return the operation that created the snapshot to help other components
/// ignore snapshots that are not needed for some tasks. For example, snapshot expiration
/// does not need to clean up deleted files for appends, which have no deleted files.
struct ICEBERG_EXPORT DataOperation {
  /// \brief Only data files were added and no files were removed.
  inline static const std::string kAppend = "append";
  /// \brief Data and delete files were added and removed without changing table data;
  /// i.e. compaction, change the data file format, or relocating data files.
  inline static const std::string kReplace = "replace";
  /// \brief Data and delete files were added and removed in a logical overwrite
  /// operation.
  inline static const std::string kOverwrite = "overwrite";
  /// \brief Data files were removed and their contents logically deleted and/or delete
  /// files were added to delete rows.
  inline static const std::string kDelete = "delete";
};

/// \brief A snapshot of the data in a table at a point in time.
///
/// A snapshot consist of one or more file manifests, and the complete table contents is
/// the union of all the data files in those manifests.
///
/// Snapshots are created by table operations.
struct ICEBERG_EXPORT Snapshot {
  static constexpr int64_t kInvalidSnapshotId = -1;

  /// A unique long ID.
  int64_t snapshot_id;
  /// The snapshot ID of the snapshot's parent. Omitted for any snapshot with no parent.
  std::optional<int64_t> parent_snapshot_id;
  /// A monotonically increasing long that tracks the order of changes to a table.
  int64_t sequence_number;
  /// A timestamp when the snapshot was created, used for garbage collection and table
  /// inspection.
  TimePointMs timestamp_ms;
  /// The location of a manifest list for this snapshot that tracks manifest files with
  /// additional metadata.
  std::string manifest_list;
  /// A string map that summaries the snapshot changes, including operation.
  std::unordered_map<std::string, std::string> summary;
  /// ID of the table's current schema when the snapshot was created.
  std::optional<int32_t> schema_id;

  /// \brief Return the name of the DataOperations data operation that produced this
  /// snapshot.
  ///
  /// \return the operation that produced this snapshot, or nullopt if the operation is
  /// unknown.
  std::optional<std::string_view> operation() const;

  /// \brief Compare two snapshots for equality.
  friend bool operator==(const Snapshot& lhs, const Snapshot& rhs) {
    return lhs.Equals(rhs);
  }

 private:
  /// \brief Compare two snapshots for equality.
  bool Equals(const Snapshot& other) const;
};

}  // namespace iceberg
