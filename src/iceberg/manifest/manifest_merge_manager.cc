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
#include <iterator>
#include <map>
#include <ranges>
#include <utility>
#include <vector>

#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg {

ManifestMergeManager::ManifestMergeManager(int64_t target_size_bytes,
                                           int32_t min_count_to_merge, bool merge_enabled)
    : target_size_bytes_(target_size_bytes),
      min_count_to_merge_(min_count_to_merge),
      merge_enabled_(merge_enabled) {}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeManifests(
    const std::vector<ManifestFile>& existing_manifests,
    const std::vector<ManifestFile>& new_manifests, int64_t snapshot_id,
    const TableMetadata& metadata, std::shared_ptr<FileIO> file_io,
    const ManifestWriterFactory& writer_factory) {
  // Combine new then existing (new-first ordering is preserved in output).
  auto to_manifest_ptr = [](const ManifestFile& manifest) { return &manifest; };
  auto manifest_ranges = std::array{
      new_manifests | std::views::transform(to_manifest_ptr),
      existing_manifests | std::views::transform(to_manifest_ptr),
  };

  std::vector<const ManifestFile*> all;
  all.reserve(new_manifests.size() + existing_manifests.size());
  std::ranges::copy(manifest_ranges | std::views::join, std::back_inserter(all));

  if (all.empty() || !merge_enabled_) {
    return all |
           std::views::transform([](const ManifestFile* manifest) { return *manifest; }) |
           std::ranges::to<std::vector<ManifestFile>>();
  }

  // Track the first (newest) manifest independently per content type.
  std::map<ManifestContent, const ManifestFile*> first_by_content;
  std::ranges::for_each(all, [&first_by_content](const ManifestFile* manifest) {
    first_by_content.try_emplace(manifest->content, manifest);
  });

  // Group manifests by (partition_spec_id, content), never merging across specs or
  // content types. Reverse spec ordering preserves v3 first-row-id assignment order.
  using GroupKey = std::pair<int32_t, ManifestContent>;
  auto group_key = [](const ManifestFile* manifest) {
    return GroupKey{manifest->partition_spec_id, manifest->content};
  };

  std::map<GroupKey, std::vector<const ManifestFile*>, std::greater<>> by_spec;
  std::ranges::for_each(all, [&by_spec, &group_key](const ManifestFile* manifest) {
    by_spec[group_key(manifest)].push_back(manifest);
  });

  std::vector<ManifestFile> result;
  result.reserve(all.size());
  for (auto& [key, group] : by_spec) {
    const auto* first = first_by_content.at(key.second);
    ICEBERG_ASSIGN_OR_RAISE(auto merged, MergeGroup(group, first, snapshot_id, metadata,
                                                    file_io, writer_factory));
    std::ranges::move(merged, std::back_inserter(result));
  }
  return result;
}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeGroup(
    const std::vector<const ManifestFile*>& group, const ManifestFile* first,
    int64_t snapshot_id, const TableMetadata& metadata, std::shared_ptr<FileIO> file_io,
    const ManifestWriterFactory& writer_factory) {
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
  // TODO(Guotao): Flush independent bins in parallel and cache successful merged bins
  // for commit retries.
  for (auto& bin : bins) {
    bool contains_first = std::ranges::find(bin, first) != bin.end();
    if (contains_first && std::cmp_less(bin.size(), min_count_to_merge_)) {
      for (const auto* manifest : bin) {
        result.push_back(*manifest);
      }
    } else {
      ICEBERG_ASSIGN_OR_RAISE(
          auto merged, FlushBin(bin, snapshot_id, metadata, file_io, writer_factory));
      result.push_back(std::move(merged));
    }
  }

  return result;
}

Result<ManifestFile> ManifestMergeManager::FlushBin(
    const std::vector<const ManifestFile*>& bin, int64_t snapshot_id,
    const TableMetadata& metadata, std::shared_ptr<FileIO> file_io,
    const ManifestWriterFactory& writer_factory) {
  // A single-manifest bin requires no merging.
  if (bin.size() == 1) return *bin[0];

  const ManifestFile& first = *bin[0];
  int32_t spec_id = first.partition_spec_id;

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_id));

  ICEBERG_ASSIGN_OR_RAISE(auto writer, writer_factory(spec_id, first.content));

  for (const auto* manifest : bin) {
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            ManifestReader::Make(*manifest, file_io, schema, spec));
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

}  // namespace iceberg
