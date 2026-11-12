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

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
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
#include "iceberg/type_fwd.h"
#include "iceberg/util/executor.h"

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
  using WorkerPoolSupplier = std::function<OptionalExecutor()>;

  /// \brief Construct a merge manager with the given configuration.
  ///
  /// \param content Manifest content this manager accepts
  /// \param target_size_bytes Target output manifest size in bytes
  /// \param min_count_to_merge Minimum count for the bin containing the first manifest
  /// \param merge_enabled Whether merging is enabled at all
  /// \param file_io File IO used to open manifests for reading
  /// \param snapshot_id_supplier Supplies the snapshot id being committed
  /// \param delete_file Callback that deletes uncommitted merged manifest files
  /// \param worker_pool_supplier Supplies the executor for parallel bin merging;
  ///        an empty supplier runs on the calling thread. Callbacks must support
  ///        concurrent calls when an executor is supplied.
  static Result<std::unique_ptr<ManifestMergeManager>> Make(
      ManifestContent content, int64_t target_size_bytes, int32_t min_count_to_merge,
      bool merge_enabled, std::shared_ptr<FileIO> file_io,
      SnapshotIdSupplier snapshot_id_supplier,
      std::function<Status(const std::string&)> delete_file = {},
      WorkerPoolSupplier worker_pool_supplier = {});

  ManifestMergeManager(const ManifestMergeManager&) = delete;
  ManifestMergeManager& operator=(const ManifestMergeManager&) = delete;

  /// \brief Merge manifests in their supplied order, with new manifests first.
  ///
  /// The first manifest's bin is protected by min_count_to_merge. Groups are
  /// processed in descending spec ID order and bins preserve manifest order.
  Result<std::vector<ManifestFile>> MergeManifests(
      const std::vector<ManifestFile>& manifests, const TableMetadata& metadata,
      const ManifestWriterFactory& writer_factory);

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
  int32_t ReplacedManifestsCount() const { return replaced_manifests_count_.load(); }

  /// \brief Delete cached merged manifests whose paths were not committed and roll
  /// back replaced-manifest accounting.
  Status CleanUncommitted(const std::unordered_set<std::string>& committed);

 private:
  ManifestMergeManager(ManifestContent content, int64_t target_size_bytes,
                       int32_t min_count_to_merge, bool merge_enabled,
                       std::shared_ptr<FileIO> file_io,
                       SnapshotIdSupplier snapshot_id_supplier,
                       std::function<Status(const std::string&)> delete_file,
                       WorkerPoolSupplier worker_pool_supplier);

  using ManifestGroups = std::map<int32_t, std::vector<ManifestFile>, std::greater<>>;

  struct BinHash {
    size_t operator()(const std::vector<ManifestFile>& bin) const;
  };

  static ManifestGroups GroupBySpec(const std::vector<ManifestFile>& manifests);

  Result<std::vector<ManifestFile>> MergeGroup(
      const ManifestFile& first, int32_t spec_id, const std::vector<ManifestFile>& group,
      const TableMetadata& metadata, const ManifestWriterFactory& writer_factory);

  Result<ManifestFile> CreateManifest(int32_t spec_id,
                                      const std::vector<ManifestFile>& bin,
                                      const TableMetadata& metadata,
                                      const ManifestWriterFactory& writer_factory);

  const ManifestContent manifest_content_;
  const int64_t target_size_bytes_;
  const int32_t min_count_to_merge_;
  const bool merge_enabled_;
  std::shared_ptr<FileIO> file_io_;
  SnapshotIdSupplier snapshot_id_supplier_;
  std::function<Status(const std::string&)> delete_file_;
  // Track manifests replaced during bin-packing.
  std::atomic<int32_t> replaced_manifests_count_{0};
  // Cache merge results to reuse when retrying.
  std::unordered_map<std::vector<ManifestFile>, ManifestFile, BinHash> merged_manifests_;
  std::mutex merged_manifests_mutex_;
  WorkerPoolSupplier worker_pool_supplier_;
};

}  // namespace iceberg
