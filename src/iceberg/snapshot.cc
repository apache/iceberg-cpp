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

#include "iceberg/snapshot.h"

#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/util/macros.h"

namespace iceberg {

bool SnapshotRef::Branch::Equals(const SnapshotRef::Branch& other) const {
  return min_snapshots_to_keep == other.min_snapshots_to_keep &&
         max_snapshot_age_ms == other.max_snapshot_age_ms &&
         max_ref_age_ms == other.max_ref_age_ms;
}

bool SnapshotRef::Tag::Equals(const SnapshotRef::Tag& other) const {
  return max_ref_age_ms == other.max_ref_age_ms;
}

SnapshotRefType SnapshotRef::type() const noexcept {
  return std::visit(
      [&](const auto& retention) -> SnapshotRefType {
        using T = std::decay_t<decltype(retention)>;
        if constexpr (std::is_same_v<T, Branch>) {
          return SnapshotRefType::kBranch;
        } else {
          return SnapshotRefType::kTag;
        }
      },
      retention);
}

bool SnapshotRef::Equals(const SnapshotRef& other) const {
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }

  if (type() == SnapshotRefType::kBranch) {
    return snapshot_id == other.snapshot_id &&
           std::get<Branch>(retention) == std::get<Branch>(other.retention);

  } else {
    return snapshot_id == other.snapshot_id &&
           std::get<Tag>(retention) == std::get<Tag>(other.retention);
  }
}

std::optional<std::string_view> Snapshot::operation() const {
  auto it = summary.find(SnapshotSummaryFields::kOperation);
  if (it != summary.end()) {
    return it->second;
  }
  return std::nullopt;
}

bool Snapshot::Equals(const Snapshot& other) const {
  if (this == &other) {
    return true;
  }
  return snapshot_id == other.snapshot_id &&
         parent_snapshot_id == other.parent_snapshot_id &&
         sequence_number == other.sequence_number && timestamp_ms == other.timestamp_ms &&
         schema_id == other.schema_id;
}

Result<SnapshotCache::ManifestsCache> SnapshotCache::InitManifestsCache(
    const Snapshot* snapshot, std::shared_ptr<FileIO> file_io) {
  if (file_io == nullptr) {
    return InvalidArgument("Cannot cache manifests: FileIO is null");
  }

  // Read manifest list
  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ManifestListReader::Make(snapshot->manifest_list, file_io));
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_files, reader->Files());

  std::vector<ManifestFile> manifests;
  manifests.reserve(manifest_files.size());

  // Partition manifests: data manifests first, then delete manifests
  // First pass: collect data manifests
  for (const auto& manifest_file : manifest_files) {
    if (manifest_file.content == ManifestContent::kData) {
      manifests.push_back(manifest_file);
    }
  }
  size_t data_manifests_count = manifests.size();

  // Second pass: append delete manifests
  for (const auto& manifest_file : manifest_files) {
    if (manifest_file.content == ManifestContent::kDeletes) {
      manifests.push_back(manifest_file);
    }
  }

  return std::make_pair(std::move(manifests), data_manifests_count);
}

Result<std::span<ManifestFile>> SnapshotCache::Manifests(
    std::shared_ptr<FileIO> file_io) const {
  ICEBERG_ASSIGN_OR_RAISE(auto cache_ref, manifests_cache_.Get(snapshot_, file_io));
  auto& cache = cache_ref.get();
  return std::span<ManifestFile>(cache.first.data(), cache.first.size());
}

Result<std::span<ManifestFile>> SnapshotCache::DataManifests(
    std::shared_ptr<FileIO> file_io) const {
  ICEBERG_ASSIGN_OR_RAISE(auto cache_ref, manifests_cache_.Get(snapshot_, file_io));
  auto& cache = cache_ref.get();
  return std::span<ManifestFile>(cache.first.data(), cache.second);
}

Result<std::span<ManifestFile>> SnapshotCache::DeleteManifests(
    std::shared_ptr<FileIO> file_io) const {
  ICEBERG_ASSIGN_OR_RAISE(auto cache_ref, manifests_cache_.Get(snapshot_, file_io));
  auto& cache = cache_ref.get();
  const size_t delete_start = cache.second;
  const size_t delete_count = cache.first.size() - delete_start;
  return std::span<ManifestFile>(cache.first.data() + delete_start, delete_count);
}

}  // namespace iceberg
