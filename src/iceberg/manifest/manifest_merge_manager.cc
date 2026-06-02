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
#include <array>
#include <functional>
#include <iterator>
#include <map>
#include <ranges>
#include <tuple>
#include <utility>
#include <vector>

#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

size_t CombineHash(size_t seed, size_t value) {
  return seed ^ (value + 0x9e3779b9 + (seed << 6) + (seed >> 2));
}

}  // namespace

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
  auto append_manifest = [this](const ManifestFile& manifest,
                                std::vector<const ManifestFile*>& manifests) -> Status {
    ICEBERG_PRECHECK(manifest.content == manifest_content_,
                     "Cannot merge manifest with unexpected content");
    manifests.push_back(&manifest);
    return {};
  };

  std::vector<const ManifestFile*> all;
  all.reserve(new_manifests.size() + existing_manifests.size());
  for (const auto& manifest : new_manifests) {
    ICEBERG_RETURN_UNEXPECTED(append_manifest(manifest, all));
  }
  for (const auto& manifest : existing_manifests) {
    ICEBERG_RETURN_UNEXPECTED(append_manifest(manifest, all));
  }

  if (all.empty() || !merge_enabled_) {
    return all |
           std::views::transform([](const ManifestFile* manifest) { return *manifest; }) |
           std::ranges::to<std::vector<ManifestFile>>();
  }

  const auto* first = all.front();
  std::map<int32_t, std::vector<const ManifestFile*>, std::greater<>> by_spec;
  std::ranges::for_each(all, [&by_spec](const ManifestFile* manifest) {
    by_spec[manifest->partition_spec_id].push_back(manifest);
  });

  std::vector<ManifestFile> result;
  result.reserve(all.size());
  for (auto& [spec_id, group] : by_spec) {
    std::ignore = spec_id;
    ICEBERG_ASSIGN_OR_RAISE(auto merged,
                            MergeGroup(group, first, metadata, writer_factory));
    std::ranges::move(merged, std::back_inserter(result));
  }
  return result;
}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeGroup(
    const std::vector<const ManifestFile*>& group, const ManifestFile* first,
    const TableMetadata& metadata, const ManifestWriterFactory& writer_factory) {
  // Match packEnd(group, ManifestFile::length) with lookback 1:
  //   1. Process manifests in reverse order (oldest-first).
  //   2. Greedy forward-pack with lookback=1: emit the current bin when the next item
  //      doesn't fit, then start a new bin.
  //   3. Reverse each bin (restoring original item order within a bin).
  //   4. Reverse the bin list (newest manifest's bin ends up first).
  // Effect: the newest manifest is in the first, possibly under-filled, bin.
  std::vector<std::vector<const ManifestFile*>> bins;
  std::vector<const ManifestFile*> current_bin;
  int64_t bin_size = 0;

  for (const auto* manifest : std::views::reverse(group)) {
    if (!current_bin.empty() &&
        bin_size + manifest->manifest_length > target_size_bytes_) {
      bins.push_back(std::move(current_bin));
      current_bin.clear();
      bin_size = 0;
    }
    current_bin.push_back(manifest);
    bin_size += manifest->manifest_length;
  }
  if (!current_bin.empty()) {
    bins.push_back(std::move(current_bin));
  }

  for (auto& bin : bins) {
    std::ranges::reverse(bin);
  }
  std::ranges::reverse(bins);

  // Process each bin: if the bin contains the newest manifest and is too small,
  // pass its contents through unchanged.
  std::vector<ManifestFile> result;
  result.reserve(group.size());
  for (auto& bin : bins) {
    if (bin.size() == 1) {
      result.push_back(*bin[0]);
    } else if (bool contains_first = std::ranges::find(bin, first) != bin.end();
               contains_first && std::cmp_less(bin.size(), min_count_to_merge_)) {
      for (const auto* manifest : bin) {
        result.push_back(*manifest);
      }
    } else {
      const auto* cached = merged_manifests_.Find(bin);
      if (cached != nullptr) {
        result.push_back(*cached);
      } else {
        const int64_t snapshot_id = snapshot_id_supplier_();
        ICEBERG_ASSIGN_OR_RAISE(auto merged, FlushBin(bin, metadata, writer_factory));
        merged_manifests_.Add(bin, merged);
        for (const auto* manifest : bin) {
          if (manifest->added_snapshot_id != snapshot_id) {
            ++replaced_manifests_count_;
          }
        }
        result.push_back(std::move(merged));
      }
    }
  }

  return result;
}

Result<ManifestFile> ManifestMergeManager::FlushBin(
    const std::vector<const ManifestFile*>& bin, const TableMetadata& metadata,
    const ManifestWriterFactory& writer_factory) {
  const ManifestFile& first = *bin[0];
  int32_t spec_id = first.partition_spec_id;

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_id));

  ICEBERG_ASSIGN_OR_RAISE(auto writer, writer_factory(spec_id, manifest_content_));

  const int64_t snapshot_id = snapshot_id_supplier_();
  for (const auto* manifest : bin) {
    bool is_committed = manifest->added_snapshot_id != kInvalidSnapshotId &&
                        manifest->added_snapshot_id != snapshot_id;
    ICEBERG_ASSIGN_OR_RAISE(auto reader, ManifestReader::Make(*manifest, file_io_, schema,
                                                              spec, is_committed));
    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
    for (const auto& entry : entries) {
      bool is_current =
          entry.snapshot_id.has_value() && entry.snapshot_id.value() == snapshot_id;
      if (entry.status == ManifestStatus::kDeleted) {
        // Carry forward only the current snapshot's deletes; drop older tombstones.
        if (is_current) {
          ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(entry));
        }
      } else if (entry.status == ManifestStatus::kAdded && is_current) {
        // Files added by the current snapshot retain their ADDED status.
        ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(entry));
      } else {
        // Files added by prior snapshots (ADDED or EXISTING) become EXISTING.
        ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
      }
    }
  }

  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  return writer->ToManifestFile();
}

ManifestMergeManager::MergedManifestCache::Key
ManifestMergeManager::MergedManifestCache::MakeKey(
    const std::vector<const ManifestFile*>& bin) {
  Key key;
  key.bin.reserve(bin.size());
  for (const auto* manifest : bin) {
    key.bin.push_back(*manifest);
  }
  return key;
}

const ManifestFile* ManifestMergeManager::MergedManifestCache::Find(
    const std::vector<const ManifestFile*>& bin) const {
  auto iter = entries_.find(MakeKey(bin));
  if (iter == entries_.end()) {
    return nullptr;
  }
  return &iter->second;
}

void ManifestMergeManager::MergedManifestCache::Add(
    const std::vector<const ManifestFile*>& bin, const ManifestFile& manifest) {
  entries_.emplace(MakeKey(bin), manifest);
}

size_t ManifestMergeManager::MergedManifestCache::KeyHash::operator()(
    const Key& key) const {
  size_t hash = 0;
  for (const auto& manifest : key.bin) {
    hash = CombineHash(hash, std::hash<std::string>{}(manifest.manifest_path));
  }
  return hash;
}

Result<int32_t> ManifestMergeManager::MergedManifestCache::CleanUncommitted(
    const std::unordered_set<std::string>& committed, int64_t snapshot_id,
    const std::function<Status(const std::string&)>& delete_file) {
  int32_t removed_replaced_manifests_count = 0;
  auto cached_entries =
      std::vector<std::pair<Key, ManifestFile>>{entries_.begin(), entries_.end()};
  for (const auto& [bin, merged] : cached_entries) {
    if (committed.contains(merged.manifest_path)) {
      continue;
    }

    std::ignore = delete_file(merged.manifest_path);
    for (const auto& manifest : bin.bin) {
      if (manifest.added_snapshot_id != snapshot_id) {
        ++removed_replaced_manifests_count;
      }
    }
    entries_.erase(bin);
  }
  return removed_replaced_manifests_count;
}

Status ManifestMergeManager::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  if (merged_manifests_.empty()) {
    return {};
  }
  const int64_t snapshot_id = snapshot_id_supplier_();
  ICEBERG_ASSIGN_OR_RAISE(
      auto removed_replaced_manifests_count,
      merged_manifests_.CleanUncommitted(committed, snapshot_id, delete_file_));
  replaced_manifests_count_ -= removed_replaced_manifests_count;
  return {};
}

}  // namespace iceberg
