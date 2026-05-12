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

#include <map>
#include <vector>

#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg {

ManifestMergeManager::ManifestMergeManager(int64_t target_size_bytes,
                                           int32_t min_count_to_merge,
                                           bool merge_enabled)
    : target_size_bytes_(target_size_bytes),
      min_count_to_merge_(min_count_to_merge),
      merge_enabled_(merge_enabled) {}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeManifests(
    const std::vector<ManifestFile>& existing_manifests,
    const std::vector<ManifestFile>& new_manifests, const TableMetadata& metadata,
    std::shared_ptr<FileIO> file_io, const ManifestWriterFactory& writer_factory) {
  // Combine new then existing (new-first ordering is preserved in output)
  std::vector<ManifestFile> all;
  all.reserve(new_manifests.size() + existing_manifests.size());
  all.insert(all.end(), new_manifests.begin(), new_manifests.end());
  all.insert(all.end(), existing_manifests.begin(), existing_manifests.end());

  if (!merge_enabled_ || static_cast<int32_t>(all.size()) < min_count_to_merge_) {
    return all;
  }

  // Group manifests by partition_spec_id — never merge across specs
  std::map<int32_t, std::vector<ManifestFile>> by_spec;
  for (const auto& m : all) {
    by_spec[m.partition_spec_id].push_back(m);
  }

  std::vector<ManifestFile> result;
  result.reserve(all.size());
  for (auto& [spec_id, group] : by_spec) {
    ICEBERG_ASSIGN_OR_RAISE(auto merged,
                             MergeGroup(group, metadata, file_io, writer_factory));
    result.insert(result.end(), std::make_move_iterator(merged.begin()),
                  std::make_move_iterator(merged.end()));
  }
  return result;
}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeGroup(
    const std::vector<ManifestFile>& group, const TableMetadata& metadata,
    std::shared_ptr<FileIO> file_io, const ManifestWriterFactory& writer_factory) {
  std::vector<ManifestFile> result;
  std::vector<ManifestFile> current_bin;
  int64_t bin_size = 0;

  for (const auto& manifest : group) {
    if (manifest.manifest_length >= target_size_bytes_) {
      // Oversized manifest passes through unchanged without affecting the current bin
      result.push_back(manifest);
    } else if (bin_size + manifest.manifest_length > target_size_bytes_ &&
               !current_bin.empty()) {
      // Adding this manifest would overflow the bin — flush and start a new bin
      ICEBERG_ASSIGN_OR_RAISE(auto merged,
                               FlushBin(current_bin, metadata, file_io, writer_factory));
      result.push_back(std::move(merged));
      current_bin.clear();
      bin_size = 0;
      current_bin.push_back(manifest);
      bin_size = manifest.manifest_length;
    } else {
      current_bin.push_back(manifest);
      bin_size += manifest.manifest_length;
    }
  }

  if (!current_bin.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto merged,
                             FlushBin(current_bin, metadata, file_io, writer_factory));
    result.push_back(std::move(merged));
  }

  return result;
}

Result<ManifestFile> ManifestMergeManager::FlushBin(
    const std::vector<ManifestFile>& bin, const TableMetadata& metadata,
    std::shared_ptr<FileIO> file_io, const ManifestWriterFactory& writer_factory) {
  // A single-manifest bin requires no merging
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
      // When merging, ADDED entries from prior manifests become EXISTING;
      // DELETED entries are preserved as-is (tombstones).
      if (entry.status == ManifestStatus::kDeleted) {
        ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(entry));
      } else {
        ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
      }
    }
  }

  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  return writer->ToManifestFile();
}

}  // namespace iceberg
