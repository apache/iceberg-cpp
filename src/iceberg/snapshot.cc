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

#include <charconv>

#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

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
        using T = std::remove_cvref_t<decltype(retention)>;
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

std::optional<int64_t> Snapshot::FirstRowId() const {
  auto it = summary.find("first-row-id");
  if (it == summary.end()) {
    return std::nullopt;
  }

  return StringUtils::ParseInt<int64_t>(it->second);
}

std::optional<int64_t> Snapshot::AddedRows() const {
  auto it = summary.find("added-rows");
  if (it == summary.end()) {
    return std::nullopt;
  }

  return StringUtils::ParseInt<int64_t>(it->second);
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

// SnapshotRef::Builder implementation

SnapshotRef::Builder::Builder(SnapshotRefType type, int64_t snapshot_id)
    : type_(type), snapshot_id_(snapshot_id) {}

SnapshotRef::Builder SnapshotRef::Builder::TagBuilder(int64_t snapshot_id) {
  return Builder(SnapshotRefType::kTag, snapshot_id);
}

SnapshotRef::Builder SnapshotRef::Builder::BranchBuilder(int64_t snapshot_id) {
  return Builder(SnapshotRefType::kBranch, snapshot_id);
}

SnapshotRef::Builder SnapshotRef::Builder::BuilderFor(int64_t snapshot_id,
                                                      SnapshotRefType type) {
  return Builder(type, snapshot_id);
}

SnapshotRef::Builder SnapshotRef::Builder::BuilderFrom(const SnapshotRef& ref) {
  Builder builder(ref.type(), ref.snapshot_id);
  if (ref.type() == SnapshotRefType::kBranch) {
    const auto& branch = std::get<SnapshotRef::Branch>(ref.retention);
    builder.min_snapshots_to_keep_ = branch.min_snapshots_to_keep;
    builder.max_snapshot_age_ms_ = branch.max_snapshot_age_ms;
    builder.max_ref_age_ms_ = branch.max_ref_age_ms;
  } else {
    const auto& tag = std::get<SnapshotRef::Tag>(ref.retention);
    builder.max_ref_age_ms_ = tag.max_ref_age_ms;
  }
  return builder;
}

SnapshotRef::Builder SnapshotRef::Builder::BuilderFrom(const SnapshotRef& ref,
                                                       int64_t snapshot_id) {
  Builder builder(ref.type(), snapshot_id);
  if (ref.type() == SnapshotRefType::kBranch) {
    const auto& branch = std::get<SnapshotRef::Branch>(ref.retention);
    builder.min_snapshots_to_keep_ = branch.min_snapshots_to_keep;
    builder.max_snapshot_age_ms_ = branch.max_snapshot_age_ms;
    builder.max_ref_age_ms_ = branch.max_ref_age_ms;
  } else {
    const auto& tag = std::get<SnapshotRef::Tag>(ref.retention);
    builder.max_ref_age_ms_ = tag.max_ref_age_ms;
  }
  return builder;
}

SnapshotRef::Builder& SnapshotRef::Builder::MinSnapshotsToKeep(
    std::optional<int32_t> value) {
  if (type_ == SnapshotRefType::kTag && value.has_value()) {
    return AddError(ErrorKind::kInvalidArgument,
                    "Tags do not support setting minSnapshotsToKeep");
  }
  if (value.has_value() && value.value() <= 0) {
    return AddError(ErrorKind::kInvalidArgument,
                    "Min snapshots to keep must be greater than 0");
  }
  min_snapshots_to_keep_ = value;
  return *this;
}

SnapshotRef::Builder& SnapshotRef::Builder::MaxSnapshotAgeMs(
    std::optional<int64_t> value) {
  if (type_ == SnapshotRefType::kTag && value.has_value()) {
    return AddError(ErrorKind::kInvalidArgument,
                    "Tags do not support setting maxSnapshotAgeMs");
  }
  if (value.has_value() && value.value() <= 0) {
    return AddError(ErrorKind::kInvalidArgument,
                    "Max snapshot age must be greater than 0 ms");
  }
  max_snapshot_age_ms_ = value;
  return *this;
}

SnapshotRef::Builder& SnapshotRef::Builder::MaxRefAgeMs(std::optional<int64_t> value) {
  if (value.has_value() && value.value() <= 0) {
    return AddError(ErrorKind::kInvalidArgument,
                    "Max reference age must be greater than 0");
  }
  max_ref_age_ms_ = value;
  return *this;
}

Result<SnapshotRef> SnapshotRef::Builder::Build() const {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  if (type_ == SnapshotRefType::kBranch) {
    return SnapshotRef{
        .snapshot_id = snapshot_id_,
        .retention = SnapshotRef::Branch{.min_snapshots_to_keep = min_snapshots_to_keep_,
                                         .max_snapshot_age_ms = max_snapshot_age_ms_,
                                         .max_ref_age_ms = max_ref_age_ms_}};
  } else {
    return SnapshotRef{.snapshot_id = snapshot_id_,
                       .retention = SnapshotRef::Tag{.max_ref_age_ms = max_ref_age_ms_}};
  }
}

}  // namespace iceberg
