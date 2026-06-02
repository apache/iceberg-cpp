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

/// \file iceberg/manifest/manifest_filter_manager.h
/// Filters an existing snapshot's manifest list, marking data files as DELETED
/// or EXISTING based on row-filter expressions, exact path deletes, and partition drops.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/data_file_set.h"
#include "iceberg/util/partition_value_util.h"

namespace iceberg {

/// \brief Filters an existing snapshot's manifest list.
///
/// The manager accumulates delete conditions incrementally, then applies them all
/// at once in a single FilterManifests() call.  Manifests that contain no deleted
/// entries are returned unchanged (no I/O).  Manifests that do contain deleted
/// entries are rewritten with those entries marked DELETED.
///
/// TODO(Guotao): For ManifestContent::kDeletes, implement cleanup for orphan delete files
/// and dangling deletion vectors.
///
/// \note This class is non-copyable and non-movable.
class ICEBERG_EXPORT ManifestFilterManager {
 public:
  using PartitionSpecsById = std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>;

  static Result<std::unique_ptr<ManifestFilterManager>> Make(
      ManifestContent content, std::shared_ptr<FileIO> file_io,
      std::function<Status(const std::string&)> delete_file = {});
  ~ManifestFilterManager();

  ManifestFilterManager(const ManifestFilterManager&) = delete;
  ManifestFilterManager& operator=(const ManifestFilterManager&) = delete;

  /// \brief Register a row-filter expression.
  ///
  /// Any manifest entry whose column metrics indicate the file may satisfy the
  /// expression will be marked DELETED.
  ///
  /// \param expr The expression to match files against
  Status DeleteByRowFilter(std::shared_ptr<Expression> expr);

  /// \brief Set whether row-filter field binding is case-sensitive.
  void CaseSensitive(bool case_sensitive);

  /// \brief Register an exact file path for deletion.
  ///
  /// Any manifest entry whose file_path matches this path will be marked DELETED.
  ///
  /// \param path The exact file path to delete
  Status DeleteFile(std::string_view path);

  /// \brief Register a file object for deletion.
  ///
  /// Any manifest entry whose file_path matches file->file_path will be marked
  /// DELETED. Duplicate registrations (same path) are silently ignored.
  ///
  /// \param file The data/delete file to delete (must not be null)
  Status DeleteFile(std::shared_ptr<DataFile> file);

  /// \brief Returns the set of file objects marked for deletion by this manager.
  ///
  /// Includes file objects explicitly registered for deletion plus files deleted while
  /// filtering manifests.
  const DataFileSet& FilesToBeDeleted() const;

  /// \brief Returns content-file objects deleted by the most recent
  /// FilterManifests() call, deduplicated by content-file identity.
  const std::vector<std::shared_ptr<DataFile>>& DeletedFiles() const;

  /// \brief Returns how many duplicate file deletes were found in the most recent
  /// FilterManifests() call.
  int32_t DuplicateDeletesCount() const { return duplicate_deletes_count_; }

  /// \brief Build a snapshot-summary fragment from filtered manifests.
  ///
  Result<SnapshotSummaryBuilder> BuildSummary(
      const std::vector<ManifestFile>& manifests,
      const PartitionSpecsById& specs_by_id) const;

  /// \brief Register a partition for dropping.
  ///
  /// Any manifest entry whose (spec_id, partition) pair matches will be marked DELETED.
  ///
  /// \param spec_id The partition spec ID
  /// \param partition The partition values to drop
  Status DropPartition(int32_t spec_id, PartitionValues partition);

  /// \brief Set a flag that makes FilterManifests() fail if any registered
  /// delete path was not found in any manifest entry.
  void FailMissingDeletePaths();

  /// \brief Set a flag that makes FilterManifests() return an error if any
  /// manifest entry matches a delete condition.
  void FailAnyDelete();

  /// \brief Returns the number of manifests rewritten (replaced) by the last
  /// FilterManifests() call. A manifest is replaced when it contained deleted entries
  /// and was rewritten with those entries marked DELETED.
  int32_t ReplacedManifestsCount() const { return replaced_manifests_count_; }

  /// \brief Returns true if any delete condition has been registered.
  bool ContainsDeletes() const;

  /// \brief Set the minimum data sequence number for delete files to retain.
  ///
  /// Only valid for ManifestContent::kDeletes managers.  Delete entries whose
  /// data_sequence_number is positive and less than sequence_number will be
  /// marked DELETED.  This continuously removes delete files that cannot match
  /// any remaining data rows (i.e. all data written before that sequence number
  /// has itself been deleted).
  ///
  /// \param sequence_number the inclusive lower bound; delete files older than
  ///        this value are dropped
  Status DropDeleteFilesOlderThan(int64_t sequence_number);

  /// \brief Register data files that have been removed so their dangling DVs
  ///        can be cleaned up.
  ///
  /// Only valid for ManifestContent::kDeletes managers.  For each DV whose
  /// referenced_data_file path appears in deleted_files, the DV entry is
  /// marked DELETED because the data file it targets no longer exists.
  ///
  /// \param deleted_files set of data files that have been marked for deletion
  void RemoveDanglingDeletesFor(const DataFileSet& deleted_files);

  /// \brief Apply all accumulated delete conditions to the base snapshot's manifests.
  ///
  /// Manifests that cannot possibly contain deleted files are returned unchanged.
  /// Manifests that do contain deleted files are rewritten using writer_factory.
  ///
  /// \param metadata Table metadata (provides specs and schema for evaluators)
  /// \param base_snapshot The snapshot whose manifests to filter (may be null)
  /// \param writer_factory Factory to create new ManifestWriter instances
  /// \return The filtered manifest list, or an error
  Result<std::vector<ManifestFile>> FilterManifests(
      const TableMetadata& metadata, const std::shared_ptr<Snapshot>& base_snapshot,
      const ManifestWriterFactory& writer_factory);

  /// \brief Apply all accumulated delete conditions using an explicit schema.
  ///
  /// This overload is used when callers need row-filter evaluation bound against a
  /// schema other than metadata.Schema(), such as the schema at a branch head.
  Result<std::vector<ManifestFile>> FilterManifests(
      const std::shared_ptr<Schema>& schema, const TableMetadata& metadata,
      const std::shared_ptr<Snapshot>& base_snapshot,
      const ManifestWriterFactory& writer_factory);

  /// \brief Apply all accumulated delete conditions to the provided manifests.
  ///
  /// This overload accepts only the context needed for filtering.  It is intended for
  /// callers that already have the active schema, partition specs, and manifest list.
  ///
  /// \param schema Active schema to bind row-filter expressions and metrics evaluators
  /// \param specs_by_id All partition specs keyed by spec ID
  /// \param manifests Manifest descriptors to filter
  /// \param writer_factory Factory to create new ManifestWriter instances
  /// \return The filtered manifest list, or an error
  Result<std::vector<ManifestFile>> FilterManifests(
      const std::shared_ptr<Schema>& schema, const PartitionSpecsById& specs_by_id,
      const std::vector<const ManifestFile*>& manifests,
      const ManifestWriterFactory& writer_factory);

  /// \brief Delete cached filtered manifests that were not committed and roll back
  /// replaced-manifest accounting.
  Status CleanUncommitted(const std::unordered_set<std::string>& committed);

 private:
  ManifestFilterManager(ManifestContent content, std::shared_ptr<FileIO> file_io,
                        std::function<Status(const std::string&)> delete_file);

  /// \brief Returns true if the manifest might contain files matching any expression.
  Result<bool> CanContainExpressionDeletes(const ManifestFile& manifest,
                                           const std::shared_ptr<Schema>& schema,
                                           const PartitionSpecsById& specs_by_id);

  /// \brief Returns true if the manifest might contain files in a dropped partition.
  ///
  /// Checks whether the manifest's partition_spec_id matches any spec_id registered
  /// via DropPartition().  Manifests from a different spec cannot contain the dropped
  /// partition values.
  Result<bool> CanContainDroppedPartitions(const ManifestFile& manifest) const;

  /// \brief Returns true if the manifest might contain path-deleted files.
  Result<bool> CanContainDroppedFiles(const ManifestFile& manifest) const;

  /// \brief Returns true if the manifest possibly contains any deleted file.
  Result<bool> CanContainDeletedFiles(const ManifestFile& manifest,
                                      const std::shared_ptr<Schema>& schema,
                                      const PartitionSpecsById& specs_by_id,
                                      bool trust_manifest_references);

  bool CanTrustManifestReferences(
      const std::vector<const ManifestFile*>& manifests) const;

  struct FilteredManifestDeletes {
    std::vector<std::shared_ptr<DataFile>> files;
    int32_t duplicate_deletes_count = 0;
  };

  Result<ManifestFile> FilterManifest(const std::shared_ptr<Schema>& schema,
                                      const PartitionSpecsById& specs_by_id,
                                      const ManifestFile& manifest,
                                      bool trust_manifest_references,
                                      const ManifestWriterFactory& writer_factory);

  Result<bool> ManifestHasDeletedFiles(const std::vector<ManifestEntry>& entries,
                                       const std::shared_ptr<Schema>& schema,
                                       const PartitionSpecsById& specs_by_id,
                                       int32_t manifest_spec_id);

  Result<ManifestFile> FilterManifestWithDeletedFiles(
      const std::vector<ManifestEntry>& entries, int32_t manifest_spec_id,
      const std::shared_ptr<Schema>& schema, const PartitionSpecsById& specs_by_id,
      const ManifestWriterFactory& writer_factory);

  Status ValidateRequiredDeletes() const;

  Status InvalidateFilteredCache();
  void ResetDeletedFiles();

  /// \brief Get or create a ManifestEvaluator for the given spec.
  Result<ManifestEvaluator*> GetManifestEvaluator(const std::shared_ptr<Schema>& schema,
                                                  const PartitionSpecsById& specs_by_id,
                                                  int32_t spec_id);

  /// \brief Get or create a ResidualEvaluator for the given spec.
  Result<ResidualEvaluator*> GetResidualEvaluator(const std::shared_ptr<Schema>& schema,
                                                  const PartitionSpecsById& specs_by_id,
                                                  int32_t spec_id);

  /// \brief Check whether a single entry should be deleted.
  Result<bool> ShouldDelete(const ManifestEntry& entry,
                            const std::shared_ptr<Schema>& schema,
                            const PartitionSpecsById& specs_by_id,
                            int32_t manifest_spec_id);

  const ManifestContent manifest_content_;
  std::shared_ptr<FileIO> file_io_;
  std::function<Status(const std::string&)> delete_file_;

  std::shared_ptr<Expression> delete_expr_;
  std::unordered_set<std::string> delete_paths_;
  // Delete files explicitly registered for deletion by object identity.
  DeleteFileSet delete_files_to_delete_;
  // Data files explicitly registered for deletion by object identity.
  DataFileSet data_files_to_delete_;
  // Data files to remove: explicit object deletes plus files found while filtering.
  DataFileSet data_files_;
  // Delete files to remove: explicit object deletes plus files found while filtering.
  DeleteFileSet delete_files_;
  std::unordered_map<ManifestFile, FilteredManifestDeletes>
      filtered_manifest_to_deleted_files_;
  // Ordered files deleted by the latest filter pass, used for summaries.
  std::vector<std::shared_ptr<DataFile>> deleted_files_;
  // Data-file identity set for latest-pass dedup and required-delete validation.
  DataFileSet deleted_data_file_set_;
  // Delete-file identity set for latest-pass dedup and required-delete validation.
  DeleteFileSet deleted_delete_file_set_;
  PartitionSet drop_partitions_;
  bool fail_missing_delete_paths_{false};
  bool fail_any_delete_{false};
  bool case_sensitive_{true};
  int32_t duplicate_deletes_count_{0};
  int32_t replaced_manifests_count_{0};

  // minimum data sequence number; delete entries older than this are dropped
  int64_t min_sequence_number_{0};
  // paths of data files that were removed; DVs referencing these are dangling
  std::unordered_set<std::string> removed_data_file_paths_;

  std::unordered_map<ManifestFile, ManifestFile> filtered_manifests_;

  std::unordered_map<int32_t, std::unique_ptr<ManifestEvaluator>>
      manifest_evaluator_cache_;
  std::unordered_map<int32_t, std::unique_ptr<ResidualEvaluator>>
      residual_evaluator_cache_;
};

}  // namespace iceberg
