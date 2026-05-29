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

/// \brief Abstract base class for all merge-based snapshot write operations.
///
/// Provides the complete filter → write → merge pipeline that all merge-based
/// operations (MergeAppend, OverwriteFiles, RowDelta, ReplacePartitions,
/// RewriteFiles) share. Subclasses only need to implement `operation()` and
/// call the protected primitive API to describe what changes to make.
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
class ICEBERG_EXPORT MergingSnapshotUpdate : public SnapshotUpdate {
 public:
  ~MergingSnapshotUpdate() override = default;

  // SnapshotUpdate overrides
  Result<std::vector<ManifestFile>> Apply(
      const TableMetadata& metadata_to_update,
      const std::shared_ptr<Snapshot>& snapshot) override;

  void CleanUncommitted(const std::unordered_set<std::string>& committed) override;

  std::unordered_map<std::string, std::string> Summary() override;

  /// \brief Set a custom property in the snapshot summary.
  void Set(const std::string& property, const std::string& value);

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
  Status ValidateNewDeleteFile(const DataFile& file);

  /// \brief Stage a delete file with an explicit data sequence number.
  ///
  Status AddDeleteFile(std::shared_ptr<DataFile> file, int64_t data_sequence_number);

  /// \brief Add all files in a pre-existing data manifest to the new snapshot.
  ///
  /// The manifest must contain DATA content. If snapshot ID inheritance is
  /// enabled and the manifest has no snapshot ID assigned, it is used directly;
  /// otherwise it is copied with the current snapshot ID.
  Status AddManifest(ManifestFile manifest);

  /// \brief Register a data file (by object) to be deleted from the table.
  Status DeleteDataFile(std::shared_ptr<DataFile> file);

  /// \brief Register a delete file (by object) to be removed from the table.
  Status DeleteDeleteFile(std::shared_ptr<DataFile> file);

  /// \brief Register a data file path to be deleted from the table.
  ///
  /// \note Only applies to data files. To remove delete files, use DeleteDeleteFile().
  void DeleteByPath(std::string_view path);

  /// \brief Register an expression to delete matching rows.
  ///
  /// Both data and delete filter managers receive the expression: delete files that
  /// match the row filter can also be removed because those rows will be deleted.
  Status DeleteByRowFilter(std::shared_ptr<Expression> expr);

  /// \brief Register a partition to be dropped.
  ///
  /// Both data and delete filter managers receive the partition drop, since dropping
  /// data in a partition also drops all delete files in that partition.
  void DropPartition(int32_t spec_id, PartitionValues partition);

  /// \brief Fail if any registered delete path is not found in any manifest.
  void FailMissingDeletePaths();

  /// \brief Fail if any manifest entry matches a delete condition.
  void FailAnyDelete();

  /// \brief Override the data sequence number assigned to all newly-added data files.
  void SetNewDataFilesDataSequenceNumber(int64_t sequence_number);

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

  /// \brief Return an error if any snapshot in [starting_snapshot_id+1, parent]
  /// added a data file matching the given filter expression.
  static Status ValidateAddedDataFiles(const TableMetadata& metadata,
                                       int64_t starting_snapshot_id,
                                       std::shared_ptr<Expression> filter,
                                       const std::shared_ptr<Snapshot>& parent,
                                       std::shared_ptr<FileIO> io,
                                       bool case_sensitive = true);

  /// \brief Return an error if any snapshot in [starting_snapshot_id+1, parent]
  /// added a data file in any partition of the given partition set.
  ///
  static Status ValidateAddedDataFiles(const TableMetadata& metadata,
                                       int64_t starting_snapshot_id,
                                       const PartitionSet& partition_set,
                                       const std::shared_ptr<Snapshot>& parent,
                                       std::shared_ptr<FileIO> io);

  /// \brief Return an error if any snapshot in [starting_snapshot_id+1, parent]
  /// removed a file whose path is in file_paths (and allow_deletes is false).
  static Status ValidateDataFilesExist(
      const TableMetadata& metadata, int64_t starting_snapshot_id,
      const std::unordered_set<std::string>& file_paths, bool allow_deletes,
      std::shared_ptr<Expression> filter, const std::shared_ptr<Snapshot>& parent,
      std::shared_ptr<FileIO> io, bool case_sensitive = true);

  /// \brief Return an error if any snapshot in [starting_snapshot_id+1, parent]
  /// added a delete file that covers a file in replaced_files.
  ///
  /// Whether equality deletes are checked is derived automatically from whether
  /// a custom data sequence number was set via SetNewDataFilesDataSequenceNumber():
  /// if set, equality deletes are ignored because they still apply to the rewritten
  /// files and are not a conflict.
  ///
  /// Subclasses should prefer this overload over the static one.
  Status ValidateNoNewDeletesForDataFiles(const TableMetadata& metadata,
                                          int64_t starting_snapshot_id,
                                          const DataFileSet& replaced_files,
                                          const std::shared_ptr<Snapshot>& parent,
                                          std::shared_ptr<FileIO> io) const {
    return ValidateNoNewDeletesForDataFiles(metadata, starting_snapshot_id,
                                            replaced_files, parent, io,
                                            new_data_files_data_seq_number_.has_value());
  }

  /// \brief Return an error if any snapshot in [starting_snapshot_id+1, parent]
  /// added a delete file that covers a file in replaced_files.
  ///
  /// \param ignore_equality_deletes If true, only position deletes are checked.
  ///   Set to true when replaced data files have the same sequence number as the
  ///   new files (e.g. RewriteFiles), so equality deletes at higher sequence numbers
  ///   still apply and are not a conflict.
  static Status ValidateNoNewDeletesForDataFiles(const TableMetadata& metadata,
                                                 int64_t starting_snapshot_id,
                                                 const DataFileSet& replaced_files,
                                                 const std::shared_ptr<Snapshot>& parent,
                                                 std::shared_ptr<FileIO> io,
                                                 bool ignore_equality_deletes = false);

  /// \brief Return an error if any snapshot in [starting_snapshot_id+1, parent]
  /// added a delete file matching the data filter that covers a file in replaced_files.
  ///
  static Status ValidateNoNewDeletesForDataFiles(const TableMetadata& metadata,
                                                 int64_t starting_snapshot_id,
                                                 std::shared_ptr<Expression> data_filter,
                                                 const DataFileSet& replaced_files,
                                                 const std::shared_ptr<Snapshot>& parent,
                                                 std::shared_ptr<FileIO> io);

  /// \brief Return an error if any snapshot in [starting_snapshot_id+1, parent]
  /// added a delete file matching the given row filter.
  ///
  static Status ValidateNoNewDeleteFiles(const TableMetadata& metadata,
                                         int64_t starting_snapshot_id,
                                         std::shared_ptr<Expression> data_filter,
                                         const std::shared_ptr<Snapshot>& parent,
                                         std::shared_ptr<FileIO> io);

  /// \brief Return an error if any snapshot in [starting_snapshot_id+1, parent]
  /// added a delete file matching any partition in the given partition set.
  ///
  static Status ValidateNoNewDeleteFiles(const TableMetadata& metadata,
                                         int64_t starting_snapshot_id,
                                         const PartitionSet& partition_set,
                                         const std::shared_ptr<Snapshot>& parent,
                                         std::shared_ptr<FileIO> io);

  /// \brief Return an error if any snapshot in [starting_snapshot_id+1, parent]
  /// deleted a data file matching the given row filter.
  ///
  static Status ValidateDeletedDataFiles(const TableMetadata& metadata,
                                         int64_t starting_snapshot_id,
                                         std::shared_ptr<Expression> data_filter,
                                         const std::shared_ptr<Snapshot>& parent,
                                         std::shared_ptr<FileIO> io);

  /// \brief Return an error if any snapshot in [starting_snapshot_id+1, parent]
  /// deleted a data file in any partition of the given partition set.
  ///
  static Status ValidateDeletedDataFiles(const TableMetadata& metadata,
                                         int64_t starting_snapshot_id,
                                         const PartitionSet& partition_set,
                                         const std::shared_ptr<Snapshot>& parent,
                                         std::shared_ptr<FileIO> io);

  /// \brief Build a DeleteFileIndex of delete files added since starting_snapshot_id.
  static Result<std::unique_ptr<DeleteFileIndex>> AddedDeleteFiles(
      const TableMetadata& metadata, int64_t starting_snapshot_id,
      std::shared_ptr<Expression> data_filter,
      std::shared_ptr<PartitionSet> partition_set,
      const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
      bool case_sensitive = true);

  /// \brief Return an error if any snapshot in [starting_snapshot_id+1, parent]
  /// added a deletion vector that conflicts with DVs being written.
  ///
  static Status ValidateAddedDVs(
      const TableMetadata& metadata, int64_t starting_snapshot_id,
      std::shared_ptr<Expression> conflict_filter,
      const std::unordered_set<std::string>& referenced_data_files,
      const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io);

 private:
  struct PendingDeleteFile {
    std::shared_ptr<DataFile> file;
    std::optional<int64_t> data_sequence_number;
  };

  /// \brief Create a ManifestWriterFactory that records every path it creates in
  /// all_written_manifests_.
  ManifestWriterFactory MakeTrackedWriterFactory(const std::shared_ptr<Schema>& schema);

  /// \brief Copy a manifest with the current snapshot ID, for use when snapshot
  /// ID inheritance is not possible.
  Result<ManifestFile> CopyManifest(const ManifestFile& manifest);

  Status AddDeleteFile(std::shared_ptr<DataFile> file,
                       std::optional<int64_t> data_sequence_number);

  Status ValidateAddedDVs(const TableMetadata& metadata, int64_t starting_snapshot_id,
                          std::shared_ptr<Expression> conflict_filter,
                          const std::shared_ptr<Snapshot>& parent,
                          std::shared_ptr<FileIO> io) const;

  Result<std::vector<PendingDeleteFile>> NormalizeNewDeleteFiles() const;

  /// \brief Write new data manifests for staged data files; caches the result.
  Result<std::vector<ManifestFile>> WriteNewDataManifests();

  /// \brief Write new delete manifests for staged delete files; caches the result.
  Result<std::vector<ManifestFile>> WriteNewDeleteManifests();

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

  ManifestFilterManager data_filter_manager_;
  ManifestFilterManager delete_filter_manager_;
  ManifestMergeManager data_merge_manager_;
  ManifestMergeManager delete_merge_manager_;

  std::unordered_map<int32_t, DataFileSet> new_data_files_by_spec_;
  std::vector<PendingDeleteFile> new_delete_files_;
  std::optional<int64_t> new_data_files_data_seq_number_;

  // Manifests passed via AddManifest(): inherit path (no copy needed) and
  // rewrite path (must be copied with the current snapshot ID).
  std::vector<ManifestFile> append_manifests_;
  std::vector<ManifestFile> rewritten_append_manifests_;

  // Set to true when new files are staged after the cache was populated, so the
  // cache is invalidated and re-written on the next Apply() call (commit retry).
  bool has_new_data_files_ = false;
  bool has_new_delete_files_ = false;

  std::optional<std::vector<ManifestFile>> cached_new_data_manifests_;
  std::optional<std::vector<ManifestFile>> cached_new_delete_manifests_;

  /// Tracks every manifest path created via MakeTrackedWriterFactory, plus the
  /// paths in cached_new_*_manifests_. Used by CleanUncommitted().
  std::unordered_set<std::string> all_written_manifests_;
};

}  // namespace iceberg
