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

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>

#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/lazy.h"
#include "iceberg/util/timepoint.h"

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
ICEBERG_EXPORT constexpr std::string_view ToString(SnapshotRefType type) noexcept {
  switch (type) {
    case SnapshotRefType::kBranch:
      return "branch";
    case SnapshotRefType::kTag:
      return "tag";
  }
  std::unreachable();
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
  static constexpr std::string_view kMainBranch = "main";

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

  std::optional<int64_t> max_ref_age_ms() const noexcept;

  /// \brief Create a branch reference
  ///
  /// \param snapshot_id The snapshot ID for the branch
  /// \param min_snapshots_to_keep Optional minimum number of snapshots to keep
  /// \param max_snapshot_age_ms Optional maximum snapshot age in milliseconds
  /// \param max_ref_age_ms Optional maximum reference age in milliseconds
  /// \return A Result containing a unique_ptr to the SnapshotRef, or an error if
  /// validation failed
  static Result<std::unique_ptr<SnapshotRef>> MakeBranch(
      int64_t snapshot_id, std::optional<int32_t> min_snapshots_to_keep = std::nullopt,
      std::optional<int64_t> max_snapshot_age_ms = std::nullopt,
      std::optional<int64_t> max_ref_age_ms = std::nullopt);

  /// \brief Create a tag reference
  ///
  /// \param snapshot_id The snapshot ID for the tag
  /// \param max_ref_age_ms Optional maximum reference age in milliseconds
  /// \return A Result containing a unique_ptr to the SnapshotRef, or an error if
  /// validation failed
  static Result<std::unique_ptr<SnapshotRef>> MakeTag(
      int64_t snapshot_id, std::optional<int64_t> max_ref_age_ms = std::nullopt);

  /// \brief Clone this SnapshotRef with an optional new snapshot ID
  ///
  /// \param new_snapshot_id Optional new snapshot ID. If not provided, uses the current
  /// snapshot_id
  /// \return A unique_ptr to the cloned SnapshotRef
  std::unique_ptr<SnapshotRef> Clone(
      std::optional<int64_t> new_snapshot_id = std::nullopt) const;

  /// \brief Validate the SnapshotRef
  Status Validate() const;

  /// \brief Compare two snapshot refs for equality
  friend bool operator==(const SnapshotRef& lhs, const SnapshotRef& rhs) {
    return lhs.Equals(rhs);
  }

 private:
  /// \brief Compare two snapshot refs for equality.
  bool Equals(const SnapshotRef& other) const;
};

/// \brief Optional Snapshot Summary Fields
struct ICEBERG_EXPORT SnapshotSummaryFields {
  /// \brief The operation field key
  inline static const std::string kOperation = "operation";
  /// \brief The first row id field key
  inline static const std::string kFirstRowId = "first-row-id";
  /// \brief The added rows field key
  inline static const std::string kAddedRows = "added-rows";

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
  /// \brief Partition summaries prefix
  inline static const std::string kChangedPartitionPrefix = "partitions.";
  /// \brief Whether partition summaries are included
  inline static const std::string kPartitionSummaryProp = "partition-summaries-included";

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

/// \brief Helper class for building snapshot summaries.
///
/// This class provides methods to track changes to data and delete files,
/// and produces a map of summary properties for snapshot metadata.
class ICEBERG_EXPORT SnapshotSummaryBuilder {
 private:
  /// \brief Metrics tracking for added and removed files
  class UpdateMetrics {
   public:
    void Clear();
    void AddTo(std::unordered_map<std::string, std::string>& builder) const;
    void AddedFile(const DataFile& file);
    void RemovedFile(const DataFile& file);
    void AddedManifest(const ManifestFile& manifest);
    void Merge(const UpdateMetrics& other);

   private:
    int64_t added_size_{0};
    int64_t removed_size_{0};
    int32_t added_files_{0};
    int32_t removed_files_{0};
    int32_t added_eq_delete_files_{0};
    int32_t removed_eq_delete_files_{0};
    int32_t added_pos_delete_files_{0};
    int32_t removed_pos_delete_files_{0};
    int32_t added_dvs_{0};
    int32_t removed_dvs_{0};
    int32_t added_delete_files_{0};
    int32_t removed_delete_files_{0};
    int64_t added_records_{0};
    int64_t deleted_records_{0};
    int64_t added_pos_deletes_{0};
    int64_t removed_pos_deletes_{0};
    int64_t added_eq_deletes_{0};
    int64_t removed_eq_deletes_{0};
    bool trust_size_and_delete_counts_{true};
  };

 public:
  SnapshotSummaryBuilder() = default;

  /// \brief Clear all tracked metrics and properties
  void Clear();

  /// \brief Set the maximum number of changed partitions before partition summaries will
  /// be excluded.
  ///
  /// If the number of changed partitions is over this max, summaries will not be
  /// included. If the number of changed partitions is <= this limit, then partition-level
  /// summaries will be included in the summary if they are available, and
  /// "partition-summaries-included" will be set to "true".
  ///
  /// \param max Maximum number of changed partitions
  void SetPartitionSummaryLimit(int32_t max);

  /// \brief Increment the count of duplicate files deleted by a specific amount
  ///
  /// \param increment Amount to increment by. Defaults to 1.
  void IncrementDuplicateDeletes(int32_t increment = 1);

  /// \brief Track a data file being added to the snapshot
  ///
  /// \param spec The partition spec
  /// \param file The data file being added
  /// \return Status indicating success or error
  Status AddedFile(const PartitionSpec& spec, const DataFile& file);

  /// \brief Track a data file being deleted from the snapshot
  ///
  /// \param spec The partition spec
  /// \param file The data file being deleted
  /// \return Status indicating success or error
  Status DeletedFile(const PartitionSpec& spec, const DataFile& file);

  /// \brief Track a manifest being added
  ///
  /// \param manifest The manifest file being added
  void AddedManifest(const ManifestFile& manifest);

  /// \brief Set a custom summary property
  ///
  /// \param property Property name
  /// \param value Property value
  void Set(const std::string& property, const std::string& value);

  /// \brief Merge another builder's metrics into this one
  ///
  /// \param other The builder to merge from
  void Merge(const SnapshotSummaryBuilder& other);

  /// \brief Build the final summary map
  ///
  /// \return Map of summary properties
  std::unordered_map<std::string, std::string> Build() const;

 private:
  Status UpdatePartitions(const PartitionSpec& spec, const DataFile& file,
                          bool is_addition);
  std::string PartitionSummary(const UpdateMetrics& metrics) const;

  std::unordered_map<std::string, std::string> properties_;
  std::unordered_map<std::string, UpdateMetrics> partition_metrics_;
  UpdateMetrics metrics_;
  int32_t max_changed_partitions_for_summaries_{0};
  int64_t deleted_duplicate_files_{0};
  bool trust_partition_metrics_{true};
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

  /// \brief Create a new Snapshot instance with validation on the inputs.
  static Result<std::unique_ptr<Snapshot>> Make(
      int64_t sequence_number, int64_t snapshot_id,
      std::optional<int64_t> parent_snapshot_id, TimePointMs timestamp_ms,
      std::string operation, std::unordered_map<std::string, std::string> summary,
      std::optional<int32_t> schema_id, std::string manifest_list,
      std::optional<int64_t> first_row_id = std::nullopt,
      std::optional<int64_t> added_rows = std::nullopt);

  /// \brief Return the name of the DataOperations data operation that produced this
  /// snapshot.
  ///
  /// \return the operation that produced this snapshot, or nullopt if the operation is
  /// unknown.
  std::optional<std::string_view> Operation() const;

  /// \brief The row-id of the first newly added row in this snapshot.
  ///
  /// All rows added in this snapshot will have a row-id assigned to them greater than
  /// this value. All rows with a row-id less than this value were created in a snapshot
  /// that was added to the table (but not necessarily committed to this branch) in the
  /// past.
  ///
  /// \return the first row-id to be used in this snapshot or nullopt when row lineage
  /// is not supported
  Result<std::optional<int64_t>> FirstRowId() const;

  /// \brief The upper bound of number of rows with assigned row IDs in this snapshot.
  ///
  /// It can be used safely to increment the table's `next-row-id` during a commit. It
  /// can be more than the number of rows added in this snapshot and include some
  /// existing rows.
  ///
  /// This field is optional but is required when the table version supports row lineage.
  ///
  /// \return the upper bound of number of rows with assigned row IDs in this snapshot
  /// or nullopt if the value was not stored.
  Result<std::optional<int64_t>> AddedRows() const;

  /// \brief Compare two snapshots for equality.
  friend bool operator==(const Snapshot& lhs, const Snapshot& rhs) {
    return lhs.Equals(rhs);
  }

 private:
  /// \brief Compare two snapshots for equality.
  bool Equals(const Snapshot& other) const;
};

/// \brief A snapshot with cached manifest loading capabilities.
///
/// This class wraps a Snapshot pointer and provides lazy-loading of manifests.
class ICEBERG_EXPORT SnapshotCache {
 public:
  explicit SnapshotCache(const Snapshot* snapshot) : snapshot_(snapshot) {}

  /// \brief Get the underlying Snapshot reference
  const Snapshot& snapshot() const { return *snapshot_; }

  /// \brief Returns all ManifestFile instances for either data or delete manifests
  /// in this snapshot.
  ///
  /// \param file_io The FileIO instance to use for reading the manifest list
  /// \return A span of ManifestFile instances, or an error
  Result<std::span<ManifestFile>> Manifests(std::shared_ptr<FileIO> file_io) const;

  /// \brief Returns a ManifestFile for each data manifest in this snapshot.
  ///
  /// \param file_io The FileIO instance to use for reading the manifest list
  /// \return A span of ManifestFile instances, or an error
  Result<std::span<ManifestFile>> DataManifests(std::shared_ptr<FileIO> file_io) const;

  /// \brief Returns a ManifestFile for each delete manifest in this snapshot.
  ///
  /// \param file_io The FileIO instance to use for reading the manifest list
  /// \return A span of ManifestFile instances, or an error
  Result<std::span<ManifestFile>> DeleteManifests(std::shared_ptr<FileIO> file_io) const;

 private:
  /// \brief Cache structure for storing loaded manifests
  ///
  /// \note Manifests are stored in a single vector with data manifests at the head
  /// and delete manifests at the tail, separated by the number of data manifests.
  using ManifestsCache = std::pair<std::vector<ManifestFile>, size_t>;

  /// \brief Initialize manifests cache by loading them from the manifest list file.
  /// \param snapshot The snapshot to initialize the manifests cache for
  /// \param file_io The FileIO instance to use for reading the manifest list
  /// \return A result containing the manifests cache
  static Result<ManifestsCache> InitManifestsCache(const Snapshot* snapshot,
                                                   std::shared_ptr<FileIO> file_io);

  /// The underlying snapshot data
  const Snapshot* snapshot_;

  /// Lazy-loaded manifests cache
  Lazy<InitManifestsCache> manifests_cache_;
};

}  // namespace iceberg
