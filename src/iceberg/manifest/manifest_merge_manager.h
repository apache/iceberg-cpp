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
#include <memory>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_filter_manager.h"
#include "iceberg/manifest/manifest_list.h"
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
  /// \brief Construct a merge manager with the given configuration.
  ///
  /// \param target_size_bytes Target output manifest size in bytes
  /// \param min_count_to_merge Minimum number of manifests before any merging occurs
  /// \param merge_enabled Whether merging is enabled at all
  ManifestMergeManager(int64_t target_size_bytes, int32_t min_count_to_merge,
                       bool merge_enabled);

  ManifestMergeManager(const ManifestMergeManager&) = delete;
  ManifestMergeManager& operator=(const ManifestMergeManager&) = delete;

  /// \brief Merge existing and new manifests according to configured thresholds.
  ///
  /// \param existing_manifests Manifests already in the base snapshot
  /// \param new_manifests Newly written manifests to incorporate
  /// \param metadata Table metadata (provides specs and schema for readers)
  /// \param file_io File IO used to open existing manifests for reading
  /// \param writer_factory Factory to create new ManifestWriter instances
  /// \return The merged manifest list, or an error
  Result<std::vector<ManifestFile>> MergeManifests(
      const std::vector<ManifestFile>& existing_manifests,
      const std::vector<ManifestFile>& new_manifests, const TableMetadata& metadata,
      std::shared_ptr<FileIO> file_io, const ManifestWriterFactory& writer_factory);

 private:
  /// \brief Merge a group of manifests sharing the same spec_id.
  ///
  /// Returns the merged manifests for this group (pass-throughs + newly written).
  Result<std::vector<ManifestFile>> MergeGroup(
      const std::vector<ManifestFile>& group, const TableMetadata& metadata,
      std::shared_ptr<FileIO> file_io, const ManifestWriterFactory& writer_factory);

  /// \brief Write a merged manifest from all manifests in a bin.
  Result<ManifestFile> FlushBin(const std::vector<ManifestFile>& bin,
                                const TableMetadata& metadata,
                                std::shared_ptr<FileIO> file_io,
                                const ManifestWriterFactory& writer_factory);

  const int64_t target_size_bytes_;
  const int32_t min_count_to_merge_;
  const bool merge_enabled_;
};

}  // namespace iceberg
