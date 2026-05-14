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
  // Combine new then existing (new-first ordering is preserved in output)
  std::vector<ManifestFile> all;
  all.reserve(new_manifests.size() + existing_manifests.size());
  all.insert(all.end(), new_manifests.begin(), new_manifests.end());
  all.insert(all.end(), existing_manifests.begin(), existing_manifests.end());

  if (all.empty() || !merge_enabled_) {
    return all;
  }

  // Match Java's separate DataFileMergeManager/DeleteFileMergeManager behavior by
  // tracking the first (newest) manifest independently per content type.
  std::map<ManifestContent, const ManifestFile*> first_by_content;
  for (const auto& manifest : all) {
    first_by_content.try_emplace(manifest.content, &manifest);
  }

  // Group manifests by (partition_spec_id, content) — never merge across specs or
  // content types.  Use reverse spec ordering to match Java's reverse-TreeMap behaviour,
  // which is observable in v3 tables where first-row IDs are assigned in output order.
  using GroupKey = std::pair<int32_t, ManifestContent>;
  std::map<GroupKey, std::vector<ManifestFile>, std::greater<>> by_spec;
  for (const auto& m : all) {
    by_spec[{m.partition_spec_id, m.content}].push_back(m);
  }

  std::vector<ManifestFile> result;
  result.reserve(all.size());
  for (auto& [key, group] : by_spec) {
    const auto* first = first_by_content.at(key.second);
    ICEBERG_ASSIGN_OR_RAISE(auto merged, MergeGroup(group, *first, snapshot_id, metadata,
                                                    file_io, writer_factory));
    result.insert(result.end(), std::make_move_iterator(merged.begin()),
                  std::make_move_iterator(merged.end()));
  }
  return result;
}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeGroup(
    const std::vector<ManifestFile>& group, const ManifestFile& first,
    int64_t snapshot_id, const TableMetadata& metadata, std::shared_ptr<FileIO> file_io,
    const ManifestWriterFactory& writer_factory) {
  // Mirror Java's ListPacker.packEnd(group, ManifestFile::length) with lookback 1:
  //   1. Process manifests in reverse order (oldest-first).
  //   2. Greedy forward-pack with lookback=1: emit the current bin when the next item
  //      doesn't fit, then start a new bin.
  //   3. Reverse each bin (restoring original item order within a bin).
  //   4. Reverse the bin list (newest manifest's bin ends up first).
  // Effect: the newest manifest is in the first, possibly under-filled, bin — exactly
  // what Java's comment describes ("the manifest that gets under-filled is the first one,
  // which will be merged the next time").
  std::vector<std::vector<ManifestFile>> bins;
  std::vector<ManifestFile> current_bin;
  int64_t bin_size = 0;

  for (const auto& manifest : std::views::reverse(group)) {
    if (!current_bin.empty() &&
        bin_size + manifest.manifest_length > target_size_bytes_) {
      bins.push_back(std::move(current_bin));
      current_bin.clear();
      bin_size = 0;
    }
    current_bin.push_back(manifest);
    bin_size += manifest.manifest_length;
  }
  if (!current_bin.empty()) {
    bins.push_back(std::move(current_bin));
  }

  for (auto& bin : bins) {
    std::ranges::reverse(bin);
  }
  std::ranges::reverse(bins);

  // Process each bin: if the bin contains the newest manifest and is too small,
  // pass its contents through unchanged (mirrors Java's minCountToMerge logic).
  std::vector<ManifestFile> result;
  result.reserve(group.size());
  for (auto& bin : bins) {
    bool contains_first = std::ranges::find(bin, first) != bin.end();
    if (contains_first && std::cmp_less(bin.size(), min_count_to_merge_)) {
      result.insert(result.end(), bin.begin(), bin.end());
    } else {
      ICEBERG_ASSIGN_OR_RAISE(
          auto merged, FlushBin(bin, snapshot_id, metadata, file_io, writer_factory));
      result.push_back(std::move(merged));
    }
  }

  return result;
}

Result<ManifestFile> ManifestMergeManager::FlushBin(
    const std::vector<ManifestFile>& bin, int64_t snapshot_id,
    const TableMetadata& metadata, std::shared_ptr<FileIO> file_io,
    const ManifestWriterFactory& writer_factory) {
  // A single-manifest bin requires no merging.
  if (bin.size() == 1) return bin[0];

  const ManifestFile& first = bin[0];
  int32_t spec_id = first.partition_spec_id;

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_id));

  ICEBERG_ASSIGN_OR_RAISE(auto writer, writer_factory(spec_id, first.content));

  for (const auto& manifest : bin) {
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            ManifestReader::Make(manifest, file_io, schema, spec));
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
