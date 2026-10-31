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

  static Result<std::shared_ptr<RewriteManifests>> Make(
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
  /// The manifest is copied unless snapshot ID inheritance is enabled and its
  /// snapshot ID is unset. A copy may also be required for format compatibility.
  /// Copies preserve existing entry snapshot IDs. Added manifests are not merged
  /// with other manifests.
  ///
  /// When a copy is made, the caller manages the original manifest and this update
  /// manages the copy. When the original is reused and the commit succeeds, it
  /// becomes part of the table metadata and must not be deleted manually; it will
  /// be cleaned up on expiry. This update does not delete the original on failure.
  RewriteManifests& AddManifest(const ManifestFile& manifest);

 protected:
  std::string operation() override;

  Result<std::vector<ManifestFile>> Apply(
      const TableMetadata& metadata_to_update,
      const std::shared_ptr<Snapshot>& snapshot) override;
  std::unordered_map<std::string, std::string> Summary() override;
  void SetSummaryProperty(const std::string& property, const std::string& value) override;
  Status CleanUncommitted(const std::unordered_set<std::string>& committed) override;

  explicit RewriteManifests(std::string table_name,
                            std::shared_ptr<TransactionContext> ctx);

 private:
  bool RequiresRewrite(const std::unordered_set<ManifestFile>& current_manifests) const;
  bool MatchesPredicate(const ManifestFile& manifest) const;
  Status ValidateDeletedManifests(
      const std::unordered_set<ManifestFile>& current_manifests,
      int64_t current_snapshot_id) const;
  Status ValidateFilesCounts() const;

  Result<ManifestFile> CopyManifest(const ManifestFile& manifest);
  Status Rewrite(std::span<const ManifestFile> current_manifests);

  void DeleteUncommitted(std::vector<ManifestFile>& manifests,
                         const std::unordered_set<std::string>& committed);
  void ResetRewriteState();
  Status ValidateTargetBranch(const std::string& branch) const override;

 private:
  std::string table_name_;
  ClusterByFunc cluster_by_func_;
  RewritePredicate predicate_;
  bool rewrite_dirty_{true};

  std::unordered_set<ManifestFile> deleted_manifests_;
  std::vector<ManifestFile> added_manifests_;
  std::vector<ManifestFile> added_manifests_to_copy_;
  std::vector<ManifestFile> rewritten_added_manifests_;

  std::vector<ManifestFile> kept_manifests_;
  std::vector<ManifestFile> new_manifests_;
  std::unordered_set<ManifestFile> rewritten_manifests_;
  int64_t entry_count_{0};

  std::unordered_map<std::string, std::string> custom_summary_properties_;
};

}  // namespace iceberg
