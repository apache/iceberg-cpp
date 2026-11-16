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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
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
#include "iceberg/util/executor.h"
#include "iceberg/util/partition_value_util.h"

namespace iceberg {

/// \brief Filters an existing snapshot's manifest list.
///
/// The manager accumulates delete conditions incrementally, then applies them all
/// at once in a single FilterManifests() call.  Manifests that contain no deleted
/// entries are returned unchanged.  Manifests that do contain deleted
/// entries are rewritten with those entries marked DELETED.
///
/// \note This class is non-copyable and non-movable. Public calls must not overlap.
/// A configured worker pool runs manifest reads and writer callbacks concurrently;
/// those callbacks and their shared state must support concurrent calls.
class ICEBERG_EXPORT ManifestFilterManager {
 public:
  using PartitionSpecsById = std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>;

  using WorkerPoolSupplier = std::function<OptionalExecutor()>;

  static Result<std::unique_ptr<ManifestFilterManager>> Make(
      ManifestContent content, std::shared_ptr<FileIO> file_io,
      std::function<Status(const std::string&)> delete_file = {},
      WorkerPoolSupplier worker_pool_supplier = {});
  ~ManifestFilterManager();

  ManifestFilterManager(const ManifestFilterManager&) = delete;
  ManifestFilterManager& operator=(const ManifestFilterManager&) = delete;

  /// \brief Register a row-filter expression.
  ///
  /// A file is deleted when all of its rows match this or another registered filter.
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
  /// Data files match by path; delete files match by path, content offset and size.
  ///
  /// \param file The data/delete file to delete (must not be null)
  Status DeleteFile(std::shared_ptr<DataFile> file);

  /// \brief Returns cumulative data-file objects marked for deletion.
  ///
  /// Includes file objects explicitly registered for deletion plus files deleted while
  /// filtering manifests. For delete-file managers use DeleteFilesToBeDeleted().
  const DataFileSet& FilesToBeDeleted() const;

  /// \brief Returns cumulative delete-file objects, preserving deletion-vector identity.
  const DeleteFileSet& DeleteFilesToBeDeleted() const;

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

  /// \brief Returns the number of manifests replaced by cached filtered outputs.
  /// Retries reuse these outputs without incrementing the count; cleanup removes
  /// the corresponding replacements from the count.
  int32_t ReplacedManifestsCount() const { return replaced_manifests_count_.load(); }

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
  class PartitionAndMetricsEvaluator;

  ManifestFilterManager(ManifestContent content, std::shared_ptr<FileIO> file_io,
                        std::function<Status(const std::string&)> delete_file,
                        WorkerPoolSupplier worker_pool_supplier);

  bool CanTrustManifestReferences(
      const std::vector<const ManifestFile*>& manifests) const;
  Status ValidateRequiredDeletes() const;
  void CollectDeletedFiles(const std::vector<ManifestFile>& manifests);
  Status InvalidateFilteredCache();

  Result<ManifestFile> FilterManifest(const std::shared_ptr<Schema>& table_schema,
                                      const PartitionSpecsById& specs_by_id,
                                      const ManifestFile& manifest,
                                      bool trust_manifest_references,
                                      const ManifestWriterFactory& writer_factory);
  Result<bool> CanContainDeletedFiles(const ManifestFile& manifest,
                                      const std::shared_ptr<Schema>& table_schema,
                                      const PartitionSpecsById& specs_by_id,
                                      bool trust_manifest_references) const;
  static bool HasNoLiveFiles(const ManifestFile& manifest);
  Result<bool> CanContainExpressionDeletes(const ManifestFile& manifest,
                                           const std::shared_ptr<Schema>& table_schema,
                                           const PartitionSpecsById& specs_by_id) const;
  Result<bool> CanContainDroppedPartitions(const ManifestFile& manifest,
                                           const Schema& table_schema,
                                           const PartitionSpecsById& specs_by_id) const;
  Result<bool> CanContainDroppedFiles(const ManifestFile& manifest,
                                      const Schema& table_schema,
                                      const PartitionSpecsById& specs_by_id) const;
  Result<bool> ManifestHasDeletedFiles(PartitionAndMetricsEvaluator& evaluator,
                                       const ManifestFile& manifest,
                                       const PartitionSpec& spec,
                                       const std::vector<ManifestEntry>& entries) const;
  bool IsDanglingDV(const DataFile& file) const;
  Result<ManifestFile> FilterManifestWithDeletedFiles(
      PartitionAndMetricsEvaluator& evaluator, const ManifestFile& manifest,
      const std::vector<ManifestEntry>& entries,
      const ManifestWriterFactory& writer_factory);

  struct FilteredManifestResult {
    std::vector<std::shared_ptr<DataFile>> files;
    int32_t duplicate_delete_count = 0;
  };

  const ManifestContent manifest_content_;
  std::shared_ptr<FileIO> file_io_;
  std::function<Status(const std::string&)> delete_file_;
  PartitionSet delete_file_partitions_;
  // Java Set<F> uses distinct data-file and delete-file identity semantics.
  DataFileSet data_files_;
  DeleteFileSet delete_files_;
  std::unordered_set<std::string> manifests_with_deletes_;
  PartitionSet drop_partitions_;
  std::unordered_set<std::string> delete_paths_;
  std::atomic<int32_t> replaced_manifests_count_{0};

  std::shared_ptr<Expression> delete_expression_;
  int64_t min_sequence_number_{0};
  bool fail_any_delete_{false};
  bool fail_missing_delete_paths_{false};
  bool case_sensitive_{true};
  bool all_deletes_reference_manifests_{true};
  std::unordered_set<std::string> removed_data_file_paths_;

  // Cache filtered manifests to avoid extra work when commits fail.
  std::unordered_map<ManifestFile, ManifestFile> filtered_manifests_;
  // Track where files were deleted to validate retries quickly.
  std::unordered_map<ManifestFile, FilteredManifestResult> filtered_manifest_results_;
  mutable std::mutex cache_mutex_;
  WorkerPoolSupplier worker_pool_supplier_;

  // Materialized latest-pass views for the C++ accessors and validation.
  std::vector<std::shared_ptr<DataFile>> deleted_files_;
  DataFileSet deleted_data_file_set_;
  DeleteFileSet deleted_delete_file_set_;
  int32_t duplicate_deletes_count_{0};
};

}  // namespace iceberg
