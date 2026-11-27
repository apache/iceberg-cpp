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

#include "iceberg/manifest/manifest_merge_manager.h"

#include <algorithm>
#include <functional>
#include <ranges>
#include <utility>
#include <vector>

#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/bin_packing_internal.h"
#include "iceberg/util/error_util_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/task_group_internal.h"

namespace iceberg {

ManifestMergeManager::ManifestMergeManager(
    ManifestContent content, int64_t target_size_bytes, int32_t min_count_to_merge,
    bool merge_enabled, std::shared_ptr<FileIO> file_io,
    SnapshotIdSupplier snapshot_id_supplier,
    std::function<Status(const std::string&)> delete_file)
    : manifest_content_(content),
      target_size_bytes_(target_size_bytes),
      min_count_to_merge_(min_count_to_merge),
      merge_enabled_(merge_enabled),
      file_io_(std::move(file_io)),
      snapshot_id_supplier_(std::move(snapshot_id_supplier)),
      delete_file_(std::move(delete_file)) {
  ICEBERG_DCHECK(file_io_, "FileIO cannot be null");
  ICEBERG_DCHECK(snapshot_id_supplier_, "Snapshot ID supplier cannot be null");
  if (delete_file_ == nullptr) {
    delete_file_ = [this](const std::string& location) {
      return file_io_->DeleteFile(location);
    };
  }
}

Result<std::unique_ptr<ManifestMergeManager>> ManifestMergeManager::Make(
    ManifestContent content, int64_t target_size_bytes, int32_t min_count_to_merge,
    bool merge_enabled, std::shared_ptr<FileIO> file_io,
    SnapshotIdSupplier snapshot_id_supplier,
    std::function<Status(const std::string&)> delete_file) {
  ICEBERG_PRECHECK(file_io != nullptr, "FileIO cannot be null");
  ICEBERG_PRECHECK(snapshot_id_supplier != nullptr,
                   "Snapshot ID supplier cannot be null");
  return std::unique_ptr<ManifestMergeManager>(new ManifestMergeManager(
      content, target_size_bytes, min_count_to_merge, merge_enabled, std::move(file_io),
      std::move(snapshot_id_supplier), std::move(delete_file)));
}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeManifests(
    const std::vector<ManifestFile>& existing_manifests,
    const std::vector<ManifestFile>& new_manifests, const TableMetadata& metadata,
    const ManifestWriterFactory& writer_factory) {
  std::vector<ManifestFile> ordered_manifests;
  ordered_manifests.reserve(new_manifests.size() + existing_manifests.size());
  ordered_manifests.append_range(new_manifests);
  ordered_manifests.append_range(existing_manifests);
  return MergeManifests(std::move(ordered_manifests), metadata, writer_factory,
                        std::nullopt);
}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeManifests(
    std::vector<ManifestFile> ordered_manifests, const TableMetadata& metadata,
    const ManifestWriterFactory& writer_factory, OptionalExecutor executor) {
  for (const auto& manifest : ordered_manifests) {
    ICEBERG_PRECHECK(manifest.content == manifest_content_,
                     "Cannot merge manifest with unexpected content");
  }
  if (!merge_enabled_ || ordered_manifests.empty()) {
    return ordered_manifests;
  }

  const auto& first = ordered_manifests.front();
  std::vector<ManifestFile> merged;
  merged.reserve(ordered_manifests.size());
  for (const auto& [spec_id, group] : GroupBySpec(ordered_manifests)) {
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, MergeGroup(first, spec_id, group, metadata,
                                                       writer_factory, executor));
    merged.append_range(std::views::as_rvalue(manifests));
  }
  return merged;
}

ManifestMergeManager::GroupsBySpec ManifestMergeManager::GroupBySpec(
    const std::vector<ManifestFile>& manifests) {
  GroupsBySpec groups;
  for (const auto& manifest : manifests) {
    groups[manifest.partition_spec_id].push_back(&manifest);
  }
  return groups;
}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeGroup(
    const ManifestFile& first, int32_t spec_id, const ManifestGroup& group,
    const TableMetadata& metadata, const ManifestWriterFactory& writer_factory,
    OptionalExecutor executor) {
  // Pack from the end with lookback 1 to leave the newest bin under-filled.
  internal::bin_packing::ListPacker<const ManifestFile*> packer(
      target_size_bytes_, /*lookback=*/1, /*largest_bin_first=*/false);
  ICEBERG_ASSIGN_OR_RAISE(auto bins,
                          packer.PackEnd(group, &ManifestFile::manifest_length));

  // Publish results in input order even if workers finish out of order.
  std::vector<std::vector<ManifestFile>> bin_results(bins.size());
  ICEBERG_RETURN_UNEXPECTED(internal::RunTasksWithStopOnFailure(
      bins.size(), executor, [&](size_t index) -> Status {
        const auto& bin = bins[index];
        auto& output_manifests = bin_results[index];
        if (bin.size() == 1) {
          output_manifests.push_back(*bin.front());
          return {};
        }

        if (std::ranges::contains(bin, first.manifest_path,
                                  &ManifestFile::manifest_path) &&
            std::cmp_less(bin.size(), min_count_to_merge_)) {
          for (const auto* manifest : bin) {
            output_manifests.push_back(*manifest);
          }
        } else {
          ICEBERG_ASSIGN_OR_RAISE(auto merged,
                                  CreateManifest(spec_id, bin, metadata, writer_factory));
          output_manifests.push_back(std::move(merged));
        }
        return {};
      }));

  std::vector<ManifestFile> merged;
  merged.reserve(group.size());
  for (auto& manifests : bin_results) {
    merged.append_range(std::views::as_rvalue(manifests));
  }
  return merged;
}

Result<ManifestFile> ManifestMergeManager::CreateManifest(
    int32_t spec_id, const ManifestGroup& bin, const TableMetadata& metadata,
    const ManifestWriterFactory& writer_factory) {
  auto paths = bin | std::views::transform(&ManifestFile::manifest_path) |
               std::ranges::to<std::vector<std::string>>();
  {
    std::lock_guard lock(cache_mutex_);
    if (auto it = merged_manifests_.find(paths); it != merged_manifests_.end()) {
      return it->second.manifest;
    }
  }

  const int64_t snapshot_id = snapshot_id_supplier_();
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto writer, writer_factory(spec_id, manifest_content_));

  int32_t replaced_count = 0;
  auto write_entries = [&]() -> Status {
    for (const auto* manifest : bin) {
      const bool is_committed = manifest->added_snapshot_id.has_value() &&
                                manifest->added_snapshot_id != snapshot_id;
      ICEBERG_ASSIGN_OR_RAISE(
          auto reader,
          ManifestReader::Make(*manifest, file_io_, schema, spec, is_committed));
      ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->EntriesStream());
      while (true) {
        ICEBERG_ASSIGN_OR_RAISE(auto entry, entries->Next());
        if (!entry) {
          break;
        }

        if (entry->status == ManifestStatus::kDeleted) {
          // Only the current snapshot's deletes are carried forward.
          if (entry->snapshot_id == snapshot_id) {
            ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(*entry));
          }
        } else if (entry->status == ManifestStatus::kAdded &&
                   entry->snapshot_id == snapshot_id) {
          ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(*entry));
        } else {
          ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(*entry));
        }
      }
      if (is_committed) {
        ++replaced_count;
      }
    }
    return {};
  };
  ICEBERG_ASSIGN_OR_RAISE(auto manifest,
                          writer->WriteAndClose(write_entries, delete_file_));

  ManifestFile cached;
  {
    std::lock_guard lock(cache_mutex_);
    auto [it, inserted] = merged_manifests_.try_emplace(
        std::move(paths), CachedManifest{manifest, replaced_count});
    if (inserted) {
      replaced_manifests_count_ += replaced_count;
      return manifest;
    }
    cached = it->second.manifest;
  }

  // Duplicate input bins may complete concurrently. Keep one output and count it once.
  if (manifest.manifest_path != cached.manifest_path) {
    internal::LogAndIgnoreFailure("Duplicate merged manifest cleanup",
                                  [&] { return delete_file_(manifest.manifest_path); });
  }
  return cached;
}

size_t ManifestMergeManager::ManifestPathsHash::operator()(
    const std::vector<std::string>& paths) const {
  size_t hash = 0;
  for (const auto& path : paths) {
    hash ^= std::hash<std::string>{}(path) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
  }
  return hash;
}

Status ManifestMergeManager::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  std::vector<std::string> uncommitted;
  {
    std::lock_guard lock(cache_mutex_);
    std::erase_if(merged_manifests_, [&](const auto& entry) {
      const auto& cached = entry.second;
      if (committed.contains(cached.manifest.manifest_path)) {
        return false;
      }
      uncommitted.push_back(cached.manifest.manifest_path);
      replaced_manifests_count_ -= cached.replaced_count;
      return true;
    });
  }
  for (const auto& path : uncommitted) {
    internal::LogAndIgnoreFailure("Uncommitted merged manifest cleanup",
                                  [&] { return delete_file_(path); });
  }
  return {};
}

}  // namespace iceberg
