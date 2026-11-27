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

  ~RewriteManifests() override;

  /// \brief Groups an existing data file by a cluster key produced by a function.
  ///
  /// The cluster key determines which data file will be associated with a
  /// particular manifest. All data files with the same cluster key will be written
  /// to the same manifest unless the file is large and split into multiple files.
  /// Manifests deleted via DeleteManifest or added via AddManifest are ignored
  /// during the rewrite process.
  RewriteManifests& ClusterBy(ClusterByFunc func);

  /// \brief Determines which existing manifest files for the table should be rewritten.
  ///
  /// Manifests that do not match the predicate are kept as-is. If this is not
  /// called and no predicate is set, all manifests are rewritten.
  RewriteManifests& RewriteIf(RewritePredicate predicate);

  /// \brief Deletes a manifest file from the table.
  RewriteManifests& DeleteManifest(const ManifestFile& manifest);

  /// \brief Adds a manifest file to the table.
  ///
  /// The added manifest cannot contain new or deleted files.
  ///
  /// By default, the manifest is rewritten to ensure all entries have explicit
  /// snapshot IDs. In that case, the caller manages the original manifest and
  /// this update manages the copy.
  ///
  /// If manifest entries are allowed to inherit the snapshot ID assigned on
  /// commit, the original manifest becomes part of the table metadata when the
  /// commit succeeds and must not be deleted manually. If the manifest gets
  /// merged while preparing a new snapshot, it is deleted automatically when
  /// this operation succeeds. If the commit fails, the original manifest is
  /// retained and may be deleted or reused by the caller.
  RewriteManifests& AddManifest(const ManifestFile& manifest);

 protected:
  std::string operation() override;

  Result<std::vector<ManifestFile>> Apply(
      const TableMetadata& metadata_to_update,
      const std::shared_ptr<Snapshot>& snapshot) override;
  std::unordered_map<std::string, std::string> Summary() override;
  Status CleanUncommitted(const std::unordered_set<std::string>& committed) override;
  Status PrepareForReplay() override;

  explicit RewriteManifests(std::string table_name,
                            std::shared_ptr<TransactionContext> ctx);

 private:
  struct WriterKey {
    std::string cluster_key;
    int32_t partition_spec_id;

    friend bool operator==(const WriterKey&, const WriterKey&) = default;
  };

  struct WriterKeyHash {
    size_t operator()(const WriterKey& key) const;
  };

  class WriterWrapper;

  bool RequiresRewrite(const std::unordered_set<ManifestFile>& current_manifests) const;
  bool MatchesPredicate(const ManifestFile& manifest) const;
  bool ContainsDeletes(const ManifestFile& manifest) const;
  void KeepActiveManifests(std::span<const ManifestFile> current_manifests);
  Status ValidateDeletedManifests(
      const std::unordered_set<ManifestFile>& current_manifests,
      int64_t current_snapshot_id) const;
  Status ValidateFilesCounts() const;

  Result<ManifestFile> CopyManifest(const ManifestFile& manifest);
  Status PerformRewrite(std::span<const ManifestFile> current_manifests);
  Status AppendEntry(const ManifestEntry& entry, const ManifestFile& manifest,
                     const std::shared_ptr<PartitionSpec>& spec);
  Result<WriterWrapper*> GetWriter(const WriterKey& key,
                                   const std::shared_ptr<PartitionSpec>& spec);
  Result<std::unique_ptr<ManifestWriter>> NewManifestWriter(
      const std::shared_ptr<PartitionSpec>& spec);
  Status CloseWriters();

  void Reset();

 private:
  std::unordered_set<ManifestFile> deleted_manifests_;
  std::vector<ManifestFile> added_manifests_;
  std::vector<ManifestFile> added_manifests_to_copy_;
  std::vector<ManifestFile> rewritten_added_manifests_;

  std::vector<ManifestFile> kept_manifests_;
  std::vector<ManifestFile> new_manifests_;
  std::unordered_set<ManifestFile> rewritten_manifests_;
  std::unordered_map<WriterKey, std::unique_ptr<WriterWrapper>, WriterKeyHash> writers_;

  int64_t entry_count_{0};

  ClusterByFunc cluster_by_func_;
  RewritePredicate predicate_;
  bool rewrite_dirty_{true};
};

}  // namespace iceberg
