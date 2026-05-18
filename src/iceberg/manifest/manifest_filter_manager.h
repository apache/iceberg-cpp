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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/expression/inclusive_metrics_evaluator.h"
#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/expression/residual_evaluator.h"
#include "iceberg/expression/strict_metrics_evaluator.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/data_file_set.h"
#include "iceberg/util/partition_value_util.h"

namespace iceberg {

/// \brief Factory type for creating ManifestWriter instances during filtering/merging.
///
/// The factory receives the partition spec ID (to look up the spec) and the manifest
/// content type, and returns a new ManifestWriter ready for writing.  The caller
/// (i.e. MergingSnapshotUpdate in PR2) captures metadata, FileIO, and snapshot ID
/// inside the lambda.
using ManifestWriterFactory = std::function<Result<std::unique_ptr<ManifestWriter>>(
    int32_t spec_id, ManifestContent content)>;

/// \brief Filters an existing snapshot's manifest list.
///
/// The manager accumulates delete conditions incrementally, then applies them all
/// at once in a single FilterManifests() call.  Manifests that contain no deleted
/// entries are returned unchanged (no I/O).  Manifests that do contain deleted
/// entries are rewritten with those entries marked DELETED.
///
/// The manager is content-agnostic: pass ManifestContent::kData to process data
/// manifests, or ManifestContent::kDeletes to process delete manifests.
///
/// \note For ManifestContent::kDeletes, this implementation applies
/// path/partition/row-filter/object-level deletes.  Java's additional cleanup
/// semantics (dropDeleteFilesOlderThan, removeDanglingDeletesFor / DV cleanup)
/// are not yet implemented and are planned for a later layer.
///
/// \note This class is non-copyable and non-movable.
class ICEBERG_EXPORT ManifestFilterManager {
 public:
  ManifestFilterManager(ManifestContent content, std::shared_ptr<FileIO> file_io);

  ManifestFilterManager(const ManifestFilterManager&) = delete;
  ManifestFilterManager& operator=(const ManifestFilterManager&) = delete;

  /// \brief Register a row-filter expression.
  ///
  /// Any manifest entry whose column metrics indicate the file may satisfy the
  /// expression will be marked DELETED.
  ///
  /// \param expr The expression to match files against
  void DeleteByRowFilter(std::shared_ptr<Expression> expr);

  /// \brief Set whether row-filter field binding is case-sensitive.
  void CaseSensitive(bool case_sensitive);

  /// \brief Register an exact file path for deletion.
  ///
  /// Any manifest entry whose file_path matches this path will be marked DELETED.
  ///
  /// \param path The exact file path to delete
  void DeleteFile(std::string_view path);

  /// \brief Register a file object for deletion.
  ///
  /// Any manifest entry whose file_path matches file->file_path will be marked
  /// DELETED.  The file object is retained in FilesToBeDeleted(), allowing callers
  /// (e.g. RowDelta in PR5) to enumerate deleted file objects for DV cleanup.
  /// Duplicate registrations (same path) are silently ignored.
  ///
  /// \param file The data/delete file to delete (must not be null)
  void DeleteFile(std::shared_ptr<DataFile> file);

  /// \brief Returns the set of file objects marked for deletion by this manager.
  ///
  /// This includes files registered via DeleteFile(DataFile) and files discovered
  /// during FilterManifests() that were deleted by path, partition, or row-filter
  /// matching.  Used by higher-level operations (e.g. RowDelta) to enumerate the
  /// deleted data files for delete-file cleanup.
  const DataFileSet& FilesToBeDeleted() const;

  /// \brief Register a partition for dropping.
  ///
  /// Any manifest entry whose (spec_id, partition) pair matches will be marked DELETED.
  ///
  /// \param spec_id The partition spec ID
  /// \param partition The partition values to drop
  void DropPartition(int32_t spec_id, PartitionValues partition);

  /// \brief Set a flag that makes FilterManifests() fail if any registered
  /// delete path was not found in any manifest entry.
  void FailMissingDeletePaths();

  /// \brief Set a flag that makes FilterManifests() return an error if any
  /// manifest entry matches a delete condition.
  ///
  /// Used by conflict-detection logic (e.g. OverwriteFiles in PR4) to detect
  /// whether a concurrent delete would invalidate a set of newly written files.
  void FailAnyDelete();

  /// \brief Returns true if any delete condition has been registered.
  bool ContainsDeletes() const;

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

 private:
  struct DeleteExpr {
    std::shared_ptr<Expression> expr;
  };

  /// \brief Returns true if the manifest might contain files matching any expression.
  bool CanContainExpressionDeletes(const ManifestFile& manifest,
                                   const TableMetadata& metadata);

  /// \brief Returns true if the manifest might contain files in a dropped partition.
  ///
  /// Checks whether the manifest's partition_spec_id matches any spec_id registered
  /// via DropPartition().  Manifests from a different spec cannot contain the dropped
  /// partition values.  A more precise implementation could also compare
  /// partition_summaries bounds, but that requires decoding binary bounds against the
  /// PartitionSpec, which is not yet available at this call site.
  bool CanContainDroppedPartitions(const ManifestFile& manifest);

  /// \brief Returns true if the manifest might contain path-deleted files.
  bool CanContainDroppedFiles() const;

  /// \brief Returns true if the manifest possibly contains any deleted file.
  bool CanContainDeletedFiles(const ManifestFile& manifest,
                              const TableMetadata& metadata);

  /// \brief Get or create a ManifestEvaluator for the given spec and expression.
  Result<ManifestEvaluator*> GetManifestEvaluator(const TableMetadata& metadata,
                                                  int32_t spec_id, const DeleteExpr& de);

  /// \brief Get or create a ResidualEvaluator for the given spec and expression.
  Result<ResidualEvaluator*> GetResidualEvaluator(const TableMetadata& metadata,
                                                  int32_t spec_id, const DeleteExpr& de);

  /// \brief Check whether a single entry should be deleted.
  Result<bool> ShouldDelete(const ManifestEntry& entry, const TableMetadata& metadata,
                            int32_t manifest_spec_id);

  const ManifestContent manifest_content_;
  std::shared_ptr<FileIO> file_io_;

  std::vector<DeleteExpr> delete_exprs_;
  std::unordered_set<std::string> delete_paths_;
  DataFileSet delete_files_;
  PartitionSet drop_partitions_;
  bool fail_missing_delete_paths_{false};
  bool fail_any_delete_{false};
  bool case_sensitive_{true};

  // Cache: (spec_id, expr_index) → ManifestEvaluator
  std::unordered_map<int32_t, std::vector<std::unique_ptr<ManifestEvaluator>>>
      manifest_evaluator_cache_;
  // Cache: (spec_id, expr_index) → ResidualEvaluator
  std::unordered_map<int32_t, std::vector<std::unique_ptr<ResidualEvaluator>>>
      residual_evaluator_cache_;
};

}  // namespace iceberg
