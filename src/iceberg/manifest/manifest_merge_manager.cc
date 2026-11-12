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
#include <iterator>
#include <ranges>
#include <tuple>
#include <utility>
#include <vector>

#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/task_group.h"

namespace iceberg {

ManifestMergeManager::ManifestMergeManager(
    ManifestContent content, int64_t target_size_bytes, int32_t min_count_to_merge,
    bool merge_enabled, std::shared_ptr<FileIO> file_io,
    SnapshotIdSupplier snapshot_id_supplier,
    std::function<Status(const std::string&)> delete_file,
    WorkerPoolSupplier worker_pool_supplier)
    : manifest_content_(content),
      target_size_bytes_(target_size_bytes),
      min_count_to_merge_(min_count_to_merge),
      merge_enabled_(merge_enabled),
      file_io_(std::move(file_io)),
      snapshot_id_supplier_(std::move(snapshot_id_supplier)),
      delete_file_(std::move(delete_file)),
      worker_pool_supplier_(std::move(worker_pool_supplier)) {
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
    std::function<Status(const std::string&)> delete_file,
    WorkerPoolSupplier worker_pool_supplier) {
  ICEBERG_PRECHECK(file_io != nullptr, "FileIO cannot be null");
  ICEBERG_PRECHECK(snapshot_id_supplier != nullptr,
                   "Snapshot ID supplier cannot be null");
  return std::unique_ptr<ManifestMergeManager>(new ManifestMergeManager(
      content, target_size_bytes, min_count_to_merge, merge_enabled, std::move(file_io),
      std::move(snapshot_id_supplier), std::move(delete_file),
      std::move(worker_pool_supplier)));
}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeManifests(
    const std::vector<ManifestFile>& existing_manifests,
    const std::vector<ManifestFile>& new_manifests, const TableMetadata& metadata,
    const ManifestWriterFactory& writer_factory) {
  std::vector<ManifestFile> manifests = new_manifests;
  manifests.insert(manifests.end(), existing_manifests.begin(), existing_manifests.end());
  return MergeManifests(manifests, metadata, writer_factory);
}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeManifests(
    const std::vector<ManifestFile>& manifests, const TableMetadata& metadata,
    const ManifestWriterFactory& writer_factory) {
  if (!merge_enabled_ || manifests.empty()) {
    return manifests;
  }

  // C++ uses a runtime content tag instead of Java's ContentFile type parameter.
  for (const auto& manifest : manifests) {
    ICEBERG_PRECHECK(manifest.content == manifest_content_,
                     "Cannot merge manifest with unexpected content");
  }

  const ManifestFile& first = manifests.front();
  std::vector<ManifestFile> merged;
  auto groups = GroupBySpec(manifests);
  for (const auto& [spec_id, group] : groups) {
    ICEBERG_ASSIGN_OR_RAISE(auto result,
                            MergeGroup(first, spec_id, group, metadata, writer_factory));
    std::ranges::move(result, std::back_inserter(merged));
  }
  return merged;
}

Status ManifestMergeManager::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  // Iterate over a copy of entries to avoid concurrent modification.
  auto entries = [this] {
    std::lock_guard lock(merged_manifests_mutex_);
    return merged_manifests_;
  }();
  for (const auto& [bin, merged] : entries) {
    // Delete any new merged manifests that aren't in the committed list.
    if (!committed.contains(merged.manifest_path)) {
      std::ignore = delete_file_(merged.manifest_path);
      {
        std::lock_guard lock(merged_manifests_mutex_);
        merged_manifests_.erase(bin);
      }
      for (const auto& manifest : bin) {
        if (snapshot_id_supplier_() != manifest.added_snapshot_id) {
          --replaced_manifests_count_;
        }
      }
    }
  }
  return {};
}

ManifestMergeManager::ManifestGroups ManifestMergeManager::GroupBySpec(
    const std::vector<ManifestFile>& manifests) {
  ManifestGroups groups;
  for (const auto& manifest : manifests) {
    groups[manifest.partition_spec_id].push_back(manifest);
  }
  return groups;
}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeGroup(
    const ManifestFile& first, int32_t spec_id, const std::vector<ManifestFile>& group,
    const TableMetadata& metadata, const ManifestWriterFactory& writer_factory) {
  // Use a lookback of 1 to avoid reordering the manifests. Using 1 also means this
  // should pack from the end so that the manifest that gets under-filled is the
  // first one, which will be merged the next time.
  // This is ListPacker(targetSizeBytes, 1, false).packEnd(group, ManifestFile::length).
  std::vector<std::vector<ManifestFile>> bins;
  std::vector<ManifestFile> current_bin;
  int64_t bin_weight = 0;
  for (const auto& manifest : std::views::reverse(group)) {
    if (!current_bin.empty() &&
        bin_weight + manifest.manifest_length > target_size_bytes_) {
      std::ranges::reverse(current_bin);
      bins.push_back(std::move(current_bin));
      current_bin.clear();
      bin_weight = 0;
    }
    current_bin.push_back(manifest);
    bin_weight += manifest.manifest_length;
  }
  if (!current_bin.empty()) {
    std::ranges::reverse(current_bin);
    bins.push_back(std::move(current_bin));
  }
  std::ranges::reverse(bins);

  // Process bins in parallel, but put results in the order of the bins into an
  // array to preserve the order of manifests and contents. Preserving the order
  // helps avoid random deletes when data files are eventually aged off.
  std::vector<std::vector<ManifestFile>> bin_results(bins.size());
  std::atomic<bool> failed{false};
  auto tasks = TaskGroup().SetExecutor(worker_pool_supplier_ ? worker_pool_supplier_()
                                                             : OptionalExecutor{});
  for (size_t index = 0; index < bins.size(); ++index) {
    tasks.Submit([&, index]() -> Status {
      if (failed.load()) {
        return {};
      }
      const auto& bin = bins[index];
      auto& output_manifests = bin_results[index];
      if (bin.size() == 1) {
        // No need to rewrite.
        output_manifests.push_back(bin.front());
        return {};
      }

      // If the bin has the first manifest (the new data files or an appended
      // manifest file), then only merge it if the number of manifests is above
      // the minimum count. This is applied only to bins with an in-memory
      // manifest so that large manifests don't prevent merging older groups.
      if (std::ranges::find(bin, first) != bin.end() &&
          std::cmp_less(bin.size(), min_count_to_merge_)) {
        // Not enough to merge, add all manifest files to the output list.
        output_manifests = bin;
      } else {
        // Merge the bin into a single manifest.
        auto manifest = CreateManifest(spec_id, bin, metadata, writer_factory);
        if (!manifest) {
          failed.store(true);
          return ::iceberg::unexpected(std::move(manifest.error()));
        }
        output_manifests.push_back(std::move(manifest.value()));
      }
      return {};
    });
  }
  ICEBERG_RETURN_UNEXPECTED(std::move(tasks).Run());
  return bin_results | std::views::join | std::views::as_rvalue |
         std::ranges::to<std::vector<ManifestFile>>();
}

Result<ManifestFile> ManifestMergeManager::CreateManifest(
    int32_t spec_id, const std::vector<ManifestFile>& bin, const TableMetadata& metadata,
    const ManifestWriterFactory& writer_factory) {
  // If this merge was already rewritten, use the existing file. If the new files
  // are in this merge, then the ManifestFile for the new files has changed and
  // will be a cache miss.
  {
    std::lock_guard lock(merged_manifests_mutex_);
    auto cached = merged_manifests_.find(bin);
    if (cached != merged_manifests_.end()) {
      return cached->second;
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto writer, writer_factory(spec_id, manifest_content_));
  auto write_entries = [&]() -> Status {
    for (const auto& manifest : bin) {
      bool is_committed = manifest.added_snapshot_id.has_value() &&
                          snapshot_id_supplier_() != manifest.added_snapshot_id;
      ICEBERG_ASSIGN_OR_RAISE(
          auto reader,
          ManifestReader::Make(manifest, file_io_, schema, spec, is_committed));
      ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
      for (const auto& entry : entries) {
        if (entry.status == ManifestStatus::kDeleted) {
          // Suppress deletes from previous snapshots. Only files deleted by this
          // snapshot should be added to the new manifest.
          if (entry.snapshot_id == snapshot_id_supplier_()) {
            ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(entry));
          }
        } else if (entry.status == ManifestStatus::kAdded &&
                   entry.snapshot_id == snapshot_id_supplier_()) {
          // Adds from this snapshot are still adds, otherwise they should be existing.
          ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(entry));
        } else {
          // Add all files from the old manifest as existing files.
          ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
        }
      }
    }
    return {};
  };
  auto status = write_entries();
  // Java Exceptions.close(writer, threw): always close, preserving the original
  // read/write failure if closing fails as well.
  auto close_status = writer->Close();
  ICEBERG_RETURN_UNEXPECTED(status);
  ICEBERG_RETURN_UNEXPECTED(close_status);
  ICEBERG_ASSIGN_OR_RAISE(auto manifest, writer->ToManifestFile());

  // Cache the merged manifest to reuse when retrying and track replaced manifests.
  {
    std::lock_guard lock(merged_manifests_mutex_);
    merged_manifests_.insert_or_assign(bin, manifest);
  }
  for (const auto& input : bin) {
    // Only count manifests from previous snapshots; in-memory manifests are not replaced.
    if (snapshot_id_supplier_() != input.added_snapshot_id) {
      ++replaced_manifests_count_;
    }
  }
  return manifest;
}

size_t ManifestMergeManager::BinHash::operator()(
    const std::vector<ManifestFile>& bin) const {
  size_t hash = 0;
  for (const auto& manifest : bin) {
    hash ^= std::hash<ManifestFile>{}(manifest) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
  }
  return hash;
}

}  // namespace iceberg
