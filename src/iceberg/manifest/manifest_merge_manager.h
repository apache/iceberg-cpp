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

/// \file iceberg/manifest/manifest_merge_manager.h
/// Merges small manifests into fewer larger ones according to table properties.

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
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Merges small manifests into larger ones using greedy bin-packing.
///
/// Manifests are grouped by partition_spec_id before merging; manifests with
/// different spec IDs are never merged together.  Within a group, manifests are
/// accumulated into bins until a bin would exceed target_size_bytes, at which
/// point the bin is flushed (written) and a new one started.  Manifests already
/// larger than target_size_bytes pass through unchanged.
///
/// \note This class is non-copyable and non-movable.
class ICEBERG_EXPORT ManifestMergeManager {
 public:
  using SnapshotIdSupplier = std::function<int64_t()>;

  /// \brief Construct a merge manager with the given configuration.
  ///
  /// \param content Manifest content this manager accepts
  /// \param target_size_bytes Target output manifest size in bytes
  /// \param min_count_to_merge Minimum number of manifests before any merging occurs
  /// \param merge_enabled Whether merging is enabled at all
  /// \param file_io File IO used to open manifests for reading
  /// \param snapshot_id_supplier Supplies the snapshot id being committed
  /// \param delete_file Callback that deletes uncommitted merged manifest files
  static Result<std::unique_ptr<ManifestMergeManager>> Make(
      ManifestContent content, int64_t target_size_bytes, int32_t min_count_to_merge,
      bool merge_enabled, std::shared_ptr<FileIO> file_io,
      SnapshotIdSupplier snapshot_id_supplier,
      std::function<Status(const std::string&)> delete_file = {});

  ManifestMergeManager(const ManifestMergeManager&) = delete;
  ManifestMergeManager& operator=(const ManifestMergeManager&) = delete;

  /// \brief Merge existing and new manifests according to configured thresholds.
  ///
  /// Manifests are grouped by partition_spec_id. Within each group, a greedy
  /// bin-packing algorithm combines manifests up to target_size_bytes. The bin that
  /// contains the newest manifest is protected by min_count_to_merge: if it has fewer
  /// than that many items it is passed through unchanged.
  ///
  /// \param existing_manifests Manifests already in the base snapshot
  /// \param new_manifests Newly written manifests to incorporate
  /// \param metadata Table metadata (provides specs and schema for readers)
  /// \param writer_factory Factory to create new ManifestWriter instances
  /// \return The merged manifest list, or an error
  Result<std::vector<ManifestFile>> MergeManifests(
      const std::vector<ManifestFile>& existing_manifests,
      const std::vector<ManifestFile>& new_manifests, const TableMetadata& metadata,
      const ManifestWriterFactory& writer_factory);

  /// \brief Returns the number of manifests replaced by cached merged outputs.
  int32_t ReplacedManifestsCount() const { return replaced_manifests_count_; }

  /// \brief Delete cached merged manifests whose paths were not committed and roll
  /// back replaced-manifest accounting.
  Status CleanUncommitted(const std::unordered_set<std::string>& committed);

 private:
  ManifestMergeManager(ManifestContent content, int64_t target_size_bytes,
                       int32_t min_count_to_merge, bool merge_enabled,
                       std::shared_ptr<FileIO> file_io,
                       SnapshotIdSupplier snapshot_id_supplier,
                       std::function<Status(const std::string&)> delete_file);

  struct MergedManifestCache {
    struct Key {
      std::vector<ManifestFile> bin;
      bool operator==(const Key& other) const = default;
    };

    struct KeyHash {
      size_t operator()(const Key& key) const;
    };

    const ManifestFile* Find(const std::vector<const ManifestFile*>& bin) const;
    void Add(const std::vector<const ManifestFile*>& bin, const ManifestFile& manifest);
    bool empty() const { return entries_.empty(); }
    Result<int32_t> CleanUncommitted(
        const std::unordered_set<std::string>& committed, int64_t snapshot_id,
        const std::function<Status(const std::string&)>& delete_file);

   private:
    static Key MakeKey(const std::vector<const ManifestFile*>& bin);

    std::unordered_map<Key, ManifestFile, KeyHash> entries_;
  };

  /// \brief Merge a group of manifests sharing the same spec_id.
  ///
  /// \param first The overall first (newest) manifest across all groups, used to
  ///   apply the min_count_to_merge threshold on the bin that contains it.
  Result<std::vector<ManifestFile>> MergeGroup(
      const std::vector<const ManifestFile*>& group, const ManifestFile* first,
      const TableMetadata& metadata, const ManifestWriterFactory& writer_factory);

  /// \brief Write a merged manifest from all manifests in a bin.
  ///
  /// Entries are written snapshot-aware:
  /// - ADDED from snapshot_id  → WriteAddedEntry (preserve status)
  /// - DELETED from snapshot_id → WriteDeletedEntry (preserve tombstone)
  /// - DELETED from older snapshots → dropped (stale tombstones are not carried forward)
  /// - All other entries          → WriteExistingEntry
  Result<ManifestFile> FlushBin(const std::vector<const ManifestFile*>& bin,
                                const TableMetadata& metadata,
                                const ManifestWriterFactory& writer_factory);

  const ManifestContent manifest_content_;
  const int64_t target_size_bytes_;
  const int32_t min_count_to_merge_;
  const bool merge_enabled_;
  std::shared_ptr<FileIO> file_io_;
  SnapshotIdSupplier snapshot_id_supplier_;
  std::function<Status(const std::string&)> delete_file_;
  int32_t replaced_manifests_count_{0};
  MergedManifestCache merged_manifests_;
};

}  // namespace iceberg
