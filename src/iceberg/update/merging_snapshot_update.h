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

/// \file iceberg/update/merging_snapshot_update.h

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/delete_file_index.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_filter_manager.h"
#include "iceberg/manifest/manifest_merge_manager.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/snapshot_update.h"
#include "iceberg/util/data_file_set.h"

namespace iceberg {

/// \brief Abstract base class for merge-based snapshot write operations.
///
/// This class is the C++ counterpart of Java's MergingSnapshotProducer. It
/// provides the shared filter, write, and merge pipeline used by operations
/// that accumulate file additions and deletions, produce a new Snapshot, and
/// commit that snapshot as current. Commit conflicts are resolved by applying
/// the same staged changes to the new latest snapshot and reattempting the
/// commit.
///
/// Subclasses describe their changes by calling the protected primitive API and
/// implement `operation()` to set the snapshot operation summary.
///
/// The Apply() pipeline:
///   1. Filter data manifests (via data_filter_manager_)
///   2. Compute min data sequence number and set up delete filter cleanup
///   3. Filter delete manifests (via delete_filter_manager_)
///   4. Write new data manifests (cached for commit retry)
///   5. Write new delete manifests (cached for commit retry)
///   6. Merge data manifests (via data_merge_manager_)
///   7. Merge delete manifests (via delete_merge_manager_)
///
/// TODO(Guotao): Java MergingSnapshotProducer overrides updateEvent() to return a
/// CreateSnapshotEvent(tableName, operation, snapshotId, sequenceNumber, summary)
/// for commit listeners. The C++ update framework does not yet have an event
/// notification mechanism, so this is intentionally not implemented here. Add it
/// once an equivalent CreateSnapshotEvent / listener facility exists.
class ICEBERG_EXPORT MergingSnapshotUpdate : public SnapshotUpdate {
 public:
  ~MergingSnapshotUpdate() override = default;

  // SnapshotUpdate overrides
  Result<std::vector<ManifestFile>> Apply(
      const TableMetadata& metadata_to_update,
      const std::shared_ptr<Snapshot>& snapshot) override;

  Status CleanUncommitted(const std::unordered_set<std::string>& committed) override;

  std::unordered_map<std::string, std::string> Summary() override;

 protected:
  /// \brief Constructor; reads merge configuration from table properties.
  explicit MergingSnapshotUpdate(std::string table_name,
                                 std::shared_ptr<TransactionContext> ctx);

  /// \brief Stage a data file to be added to the table.
  Status AddDataFile(std::shared_ptr<DataFile> file);

  /// \brief Stage a delete file to be added to the table.
  Status AddDeleteFile(std::shared_ptr<DataFile> file);

  /// \brief Validate a delete file against the table format version rules.
  ///
  /// - Format v1: deletes are not supported.
  /// - Format v2: position deletes must NOT be deletion vectors (DVs).
  /// - Format v3+: position deletes MUST be deletion vectors (DVs).
  Status ValidateNewDeleteFile(const TableMetadata& metadata, const DataFile& file);

  /// \brief Stage a delete file with an explicit data sequence number.
  ///
  Status AddDeleteFile(std::shared_ptr<DataFile> file, int64_t data_sequence_number);

  /// \brief Add all files in a pre-existing data manifest to the new snapshot.
  ///
  /// The manifest must contain only appended data files. If snapshot ID
  /// inheritance is enabled and the manifest has no snapshot ID assigned, it is
  /// used directly; otherwise it is copied with the current snapshot ID.
  Status AddManifest(ManifestFile manifest);

  /// \brief Register a data file (by object) to be deleted from the table.
  Status DeleteDataFile(std::shared_ptr<DataFile> file);

  /// \brief Register a delete file (by object) to be removed from the table.
  Status DeleteDeleteFile(std::shared_ptr<DataFile> file);

  /// \brief Register a data file path to be deleted from the table.
  ///
  /// \note Only applies to data files. To remove delete files, use DeleteDeleteFile().
  Status DeleteByPath(std::string_view path);

  /// \brief Register an expression to delete matching rows.
  ///
  /// Both data and delete filter managers receive the expression: delete files
  /// that match the row filter can also be removed because those rows will be
  /// deleted.
  Status DeleteByRowFilter(std::shared_ptr<Expression> expr);

  /// \brief Register a partition to be dropped.
  ///
  /// Both data and delete filter managers receive the partition drop, since
  /// dropping data in a partition also drops all delete files in that partition.
  Status DropPartition(int32_t spec_id, PartitionValues partition);

  /// \brief Fail if any registered delete path is not found in any manifest.
  void FailMissingDeletePaths();

  /// \brief Fail if any manifest entry matches a delete condition.
  void FailAnyDelete();

  /// \brief Override the data sequence number assigned to all newly-added data files.
  void SetNewDataFilesDataSequenceNumber(int64_t sequence_number);

  /// \brief Returns true if SetNewDataFilesDataSequenceNumber was called.
  bool HasDataSequenceNumber() const {
    return new_data_files_data_seq_number_.has_value();
  }

  /// \brief Set case sensitivity for row filter and expression evaluation.
  void CaseSensitive(bool case_sensitive);

  /// \brief Returns true if case-sensitive matching is enabled (default: true).
  bool IsCaseSensitive() const { return case_sensitive_; }

  /// \brief Returns true if any data files have been staged for addition.
  bool AddsDataFiles() const;

  /// \brief Returns true if any delete files have been staged for addition.
  bool AddsDeleteFiles() const;

  /// \brief Returns true if any data files have been registered for deletion.
  bool DeletesDataFiles() const;

  /// \brief Returns true if any delete files have been registered for removal.
  bool DeletesDeleteFiles() const;

  /// \brief Returns the row-filter expression set via DeleteByRowFilter, or nullptr.
  const std::shared_ptr<Expression>& RowFilter() const { return delete_expression_; }

  /// \brief Returns the single partition spec for all staged data files.
  ///
  /// Precondition: exactly one partition spec ID must be represented among staged
  /// data files.
  Result<std::shared_ptr<PartitionSpec>> DataSpec() const;

  /// \brief Returns all data files staged for addition.
  std::vector<std::shared_ptr<DataFile>> AddedDataFiles() const;

  /// \brief Return an error if any snapshot after starting_snapshot_id, or from
  /// the beginning if unset, added a data file matching the given filter expression.
  static Status ValidateAddedDataFiles(const TableMetadata& metadata,
                                       std::optional<int64_t> starting_snapshot_id,
                                       std::shared_ptr<Expression> filter,
                                       const std::shared_ptr<Snapshot>& parent,
                                       std::shared_ptr<FileIO> io,
                                       bool case_sensitive = true);

  /// \brief Return an error if any snapshot after starting_snapshot_id, or from
  /// the beginning if unset, added a data file in any partition of the given partition
  /// set.
  static Status ValidateAddedDataFiles(const TableMetadata& metadata,
                                       std::optional<int64_t> starting_snapshot_id,
                                       const PartitionSet& partition_set,
                                       const std::shared_ptr<Snapshot>& parent,
                                       std::shared_ptr<FileIO> io);

  /// \brief Return an error if any snapshot after starting_snapshot_id, or from
  /// the beginning if unset, removed a file whose path is in file_paths (and
  /// skip_deletes is false).
  static Status ValidateDataFilesExist(
      const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
      const std::unordered_set<std::string>& file_paths, bool skip_deletes,
      std::shared_ptr<Expression> filter, const std::shared_ptr<Snapshot>& parent,
      std::shared_ptr<FileIO> io, bool case_sensitive = true);

  /// \brief Return an error if any snapshot after starting_snapshot_id, or from
  /// the beginning if unset, added a delete file that covers a file in replaced_files.
  ///
  /// Whether equality deletes are checked is derived automatically from whether
  /// a custom data sequence number was set via SetNewDataFilesDataSequenceNumber():
  /// if set, equality deletes are ignored because they still apply to the rewritten
  /// files and are not a conflict.
  ///
  /// Subclasses should prefer this overload over the static one.
  Status ValidateNoNewDeletesForDataFiles(const TableMetadata& metadata,
                                          std::optional<int64_t> starting_snapshot_id,
                                          const DataFileSet& replaced_files,
                                          const std::shared_ptr<Snapshot>& parent,
                                          std::shared_ptr<FileIO> io) const {
    const bool ignore_equality_deletes = new_data_files_data_seq_number_.has_value();
    return ValidateNoNewDeletesForDataFiles(metadata, starting_snapshot_id,
                                            replaced_files, parent, io,
                                            ignore_equality_deletes);
  }

  /// \brief Return an error if any snapshot after starting_snapshot_id, or from
  /// the beginning if unset, added a delete file that covers a file in replaced_files.
  ///
  /// \param ignore_equality_deletes If true, only position deletes are checked.
  ///   Set to true when replaced data files have the same sequence number as the
  ///   new files (e.g. RewriteFiles), so equality deletes at higher sequence numbers
  ///   still apply and are not a conflict.
  static Status ValidateNoNewDeletesForDataFiles(
      const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
      const DataFileSet& replaced_files, const std::shared_ptr<Snapshot>& parent,
      std::shared_ptr<FileIO> io, bool ignore_equality_deletes = false);

  /// \brief Return an error if any snapshot after starting_snapshot_id, or from
  /// the beginning if unset, added a delete file matching the data filter that covers a
  /// file in replaced_files.
  static Status ValidateNoNewDeletesForDataFiles(
      const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
      std::shared_ptr<Expression> data_filter, const DataFileSet& replaced_files,
      const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
      bool case_sensitive = true);

  /// \brief Return an error if any snapshot after starting_snapshot_id, or from
  /// the beginning if unset, added a delete file matching the given row filter.
  ///
  static Status ValidateNoNewDeleteFiles(const TableMetadata& metadata,
                                         std::optional<int64_t> starting_snapshot_id,
                                         std::shared_ptr<Expression> data_filter,
                                         const std::shared_ptr<Snapshot>& parent,
                                         std::shared_ptr<FileIO> io,
                                         bool case_sensitive = true);

  /// \brief Return an error if any snapshot after starting_snapshot_id, or from
  /// the beginning if unset, added a delete file matching any partition in the given
  /// partition set.
  static Status ValidateNoNewDeleteFiles(const TableMetadata& metadata,
                                         std::optional<int64_t> starting_snapshot_id,
                                         const PartitionSet& partition_set,
                                         const std::shared_ptr<Snapshot>& parent,
                                         std::shared_ptr<FileIO> io);

  /// \brief Return an error if any snapshot after starting_snapshot_id, or from
  /// the beginning if unset, deleted a data file matching the given row filter.
  static Status ValidateDeletedDataFiles(const TableMetadata& metadata,
                                         std::optional<int64_t> starting_snapshot_id,
                                         std::shared_ptr<Expression> data_filter,
                                         const std::shared_ptr<Snapshot>& parent,
                                         std::shared_ptr<FileIO> io,
                                         bool case_sensitive = true);

  /// \brief Return an error if any snapshot after starting_snapshot_id, or from
  /// the beginning if unset, deleted a data file in any partition of the given partition
  /// set.
  static Status ValidateDeletedDataFiles(const TableMetadata& metadata,
                                         std::optional<int64_t> starting_snapshot_id,
                                         const PartitionSet& partition_set,
                                         const std::shared_ptr<Snapshot>& parent,
                                         std::shared_ptr<FileIO> io);

  /// \brief Build a DeleteFileIndex of delete files added since starting_snapshot_id.
  static Result<std::unique_ptr<DeleteFileIndex>> AddedDeleteFiles(
      const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
      std::shared_ptr<Expression> data_filter,
      std::shared_ptr<PartitionSet> partition_set,
      const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
      bool case_sensitive = true);

  /// \brief Return an error if any snapshot after starting_snapshot_id, or from
  /// the beginning if unset, added a deletion vector that conflicts with DVs being
  /// written.
  static Status ValidateAddedDVs(
      const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
      std::shared_ptr<Expression> conflict_filter,
      const std::unordered_set<std::string>& referenced_data_files,
      const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
      bool case_sensitive = true);

  /// \brief Return an error if a staged deletion vector conflicts with a deletion
  /// vector added since starting_snapshot_id.
  Status ValidateAddedDVs(const TableMetadata& metadata,
                          std::optional<int64_t> starting_snapshot_id,
                          std::shared_ptr<Expression> conflict_filter,
                          const std::shared_ptr<Snapshot>& parent,
                          std::shared_ptr<FileIO> io) const;

 private:
  struct PendingDeleteFile {
    std::shared_ptr<DataFile> file;
    std::optional<int64_t> data_sequence_number;
  };

  /// \brief Ordered map from referenced data file path to pending DVs.
  ///
  /// Mirrors Java's LinkedHashMap-backed dvsByReferencedFile: lookup is by
  /// referenced data file, and iteration preserves the first-seen key order.
  struct PendingDeleteFilesByReferencedFile {
    struct Entry {
      std::string referenced_file;
      std::vector<PendingDeleteFile> files;
    };

    void Add(std::string referenced_file, PendingDeleteFile file);
    bool empty() const { return entries_.empty(); }
    size_t size() const { return entries_.size(); }
    const std::vector<Entry>& entries() const { return entries_; }

   private:
    std::vector<Entry> entries_;
    std::unordered_map<std::string, size_t> index_by_referenced_file_;
  };

  ManifestWriterFactory MakeWriterFactory(const std::shared_ptr<Schema>& schema);

  /// \brief Copy a manifest with the current snapshot ID, for use when snapshot
  /// ID inheritance is not possible.
  /// \param update_summary Whether to add copied entries to the append summary
  Result<ManifestFile> CopyManifest(const ManifestFile& manifest, bool update_summary);

  Status AddDeleteFile(std::shared_ptr<DataFile> file,
                       std::optional<int64_t> data_sequence_number);

  Status ManagersReady() const;

  void SetSummaryProperty(const std::string& property, const std::string& value) override;

  Result<std::vector<PendingDeleteFile>> MergeDVs();

  /// \brief Write new data manifests for staged data files; caches the result.
  Result<std::vector<ManifestFile>> WriteNewDataManifests();

  /// \brief Write new delete manifests for staged delete files; caches the result.
  Result<std::vector<ManifestFile>> WriteNewDeleteManifests();

  Status CleanUncommittedAppends(const std::unordered_set<std::string>& committed);

  Status DeleteUncommitted(std::vector<ManifestFile>& manifests,
                           const std::unordered_set<std::string>& committed, bool clear);

  // Used for commit event notifications and diagnostic log messages.
  std::string table_name_;
  std::shared_ptr<Expression> delete_expression_;
  bool case_sensitive_ = true;

  // Stable sub-builders for added files — accumulated across retries and merged
  // into summary_builder_ at the start of each Apply() call.
  SnapshotSummaryBuilder added_data_files_summary_;
  SnapshotSummaryBuilder added_delete_files_summary_;
  SnapshotSummaryBuilder appended_manifests_summary_;
  std::unordered_map<std::string, std::string> custom_summary_properties_;

  std::unique_ptr<ManifestFilterManager> data_filter_manager_;
  std::unique_ptr<ManifestFilterManager> delete_filter_manager_;
  std::unique_ptr<ManifestMergeManager> data_merge_manager_;
  std::unique_ptr<ManifestMergeManager> delete_merge_manager_;

  std::unordered_map<int32_t, DataFileSet> new_data_files_by_spec_;
  std::vector<PendingDeleteFile> v2_deletes_;
  PendingDeleteFilesByReferencedFile dvs_by_referenced_file_;
  std::optional<int64_t> new_data_files_data_seq_number_;

  // Manifests passed via AddManifest(): inherit path (no copy needed) and
  // rewrite path (must be copied with the current snapshot ID).
  std::vector<ManifestFile> append_manifests_;
  // Original manifests kept to recreate copied manifests after retry cleanup.
  std::vector<ManifestFile> append_manifests_to_copy_;
  std::vector<ManifestFile> rewritten_append_manifests_;

  // Set to true when new files are staged after the cache was populated, so the
  // cache is invalidated and re-written on the next Apply() call (commit retry).
  bool has_new_data_files_ = false;
  bool has_new_delete_files_ = false;

  std::vector<ManifestFile> cached_new_data_manifests_;
  std::vector<ManifestFile> cached_new_delete_manifests_;
  std::vector<PendingDeleteFile> merged_dvs_;
  int32_t dv_merge_attempt_ = 0;
};

}  // namespace iceberg
