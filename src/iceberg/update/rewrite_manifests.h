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

/// \file iceberg/update/rewrite_manifests.h

#include <functional>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/snapshot_update.h"

namespace iceberg {

/// \brief API for rewriting manifests for a table.
///
/// This API accumulates manifest files, produces a new snapshot of the table
/// described only by the manifest files that were added, and commits that snapshot
/// as the current.
///
/// This API can be used to rewrite matching manifests according to a clustering
/// function as well as to replace specific manifests. Manifests that are deleted
/// or added directly are ignored during the rewrite process. The set of active
/// files in replaced manifests must be the same as in new manifests.
///
/// When committing, these changes will be applied to the latest table snapshot.
/// Commit conflicts will be resolved by applying the changes to the new latest
/// snapshot and reattempting the commit.
class ICEBERG_EXPORT RewriteManifests : public SnapshotUpdate {
 public:
  using ClusterByFunc = std::function<std::string(const DataFile&)>;
  using RewritePredicate = std::function<bool(const ManifestFile&)>;

  static Result<std::unique_ptr<RewriteManifests>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  /// \brief Group an existing data file by a cluster key.
  ///
  /// The cluster key determines which data file will be associated with a
  /// particular manifest. All data files with the same cluster key will be written
  /// to the same manifest unless the manifest is large and split into multiple
  /// files. Manifests deleted via DeleteManifest or added via AddManifest are
  /// ignored during the rewrite process.
  RewriteManifests& ClusterBy(ClusterByFunc func);

  /// \brief Determine which existing manifest files should be rewritten.
  ///
  /// Manifests that do not match the predicate are kept as-is. If this is not
  /// called and no predicate is set, all manifests will be rewritten.
  RewriteManifests& RewriteIf(RewritePredicate predicate);

  /// \brief Delete a manifest file from the table.
  RewriteManifests& DeleteManifest(const ManifestFile& manifest);

  /// \brief Add a manifest file to the table.
  ///
  /// The added manifest cannot contain new or deleted files.
  ///
  /// By default, the manifest will be rewritten to ensure all entries have
  /// explicit snapshot IDs. In that case, it is always the responsibility of the
  /// caller to manage the lifecycle of the original manifest.
  ///
  /// If manifest entries are allowed to inherit the snapshot ID assigned on
  /// commit, the manifest should never be deleted manually if the commit succeeds
  /// as it will become part of the table metadata and will be cleaned up on
  /// expiry. If the manifest gets merged with others while preparing a new
  /// snapshot, it will be deleted automatically if this operation is successful.
  /// If the commit fails, the manifest will never be deleted and it is up to the
  /// caller whether to delete or reuse it.
  RewriteManifests& AddManifest(const ManifestFile& manifest);

  std::string operation() override;

  Result<std::vector<ManifestFile>> Apply(
      const TableMetadata& metadata_to_update,
      const std::shared_ptr<Snapshot>& snapshot) override;
  std::unordered_map<std::string, std::string> Summary() override;
  void SetSummaryProperty(const std::string& property, const std::string& value) override;
  Status CleanUncommitted(const std::unordered_set<std::string>& committed) override;
  Status Finalize(Result<const TableMetadata*> commit_result) override;

 private:
  explicit RewriteManifests(std::string table_name,
                            std::shared_ptr<TransactionContext> ctx);

  bool RequiresRewrite(
      const std::unordered_set<std::string>& current_manifest_paths) const;
  bool MatchesPredicate(const ManifestFile& manifest) const;
  Status ValidateDeletedManifests(
      const std::unordered_set<std::string>& current_manifest_paths,
      int64_t current_snapshot_id) const;
  Status ValidateActiveFiles() const;

  Result<ManifestFile> CopyManifest(const ManifestFile& manifest);
  Result<std::vector<ManifestFile>> Rewrite(
      std::span<const ManifestFile> current_manifests);

  Status DeleteUncommitted(std::vector<ManifestFile>& manifests,
                           const std::unordered_set<std::string>& committed, bool clear);
  void ResetRewriteState();
  Status ValidateTargetBranch(const std::string& branch) const override;

 private:
  std::string table_name_;
  ClusterByFunc cluster_by_func_;
  RewritePredicate predicate_;

  std::vector<ManifestFile> deleted_manifests_;
  std::unordered_set<std::string> deleted_manifest_paths_;
  std::vector<ManifestFile> added_manifests_;
  std::vector<ManifestFile> rewritten_added_manifests_;

  std::vector<ManifestFile> kept_manifests_;
  std::vector<ManifestFile> new_manifests_;
  std::vector<ManifestFile> rewritten_manifests_;
  std::unordered_set<std::string> rewritten_manifest_paths_;
  int64_t entry_count_{0};

  std::unordered_map<std::string, std::string> custom_summary_properties_;
  SnapshotSummaryBuilder manifest_count_summary_;
  bool cleanup_all_{false};
};

}  // namespace iceberg
