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

#include "iceberg/changelog_dv_planner_internal.h"

#include <algorithm>
#include <utility>

#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/expression/projections.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/partition_spec.h"
#include "iceberg/util/executor_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::unique_ptr<DeletionVectorChangelogPlanner>>
DeletionVectorChangelogPlanner::Make(
    std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs_by_id,
    std::shared_ptr<Expression> filter, bool case_sensitive, OptionalExecutor executor,
    const std::vector<ManifestFile>& delete_manifests,
    const std::unordered_map<int64_t, int64_t>& sequence_numbers) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto entries,
      ParallelCollect(
          executor, delete_manifests,
          [&](const ManifestFile& manifest) -> Result<std::vector<ManifestEntry>> {
            auto spec_it = specs_by_id.find(manifest.partition_spec_id);
            ICEBERG_CHECK(spec_it != specs_by_id.end(),
                          "Partition spec ID {} not found when reading delete manifest",
                          manifest.partition_spec_id);
            std::shared_ptr<Expression> partition_filter;
            if (filter != nullptr) {
              auto projector =
                  Projections::Inclusive(*spec_it->second, *schema, case_sensitive);
              ICEBERG_ASSIGN_OR_RAISE(partition_filter, projector->Project(filter));
              ICEBERG_ASSIGN_OR_RAISE(
                  auto evaluator,
                  ManifestEvaluator::MakePartitionFilter(
                      partition_filter, spec_it->second, *schema, case_sensitive));
              ICEBERG_ASSIGN_OR_RAISE(bool may_match, evaluator->Evaluate(manifest));
              if (!may_match) {
                return std::vector<ManifestEntry>{};
              }
            }

            ICEBERG_ASSIGN_OR_RAISE(
                auto reader, ManifestReader::Make(manifest, io, schema, specs_by_id));
            if (partition_filter != nullptr) {
              reader->FilterPartitions(partition_filter).CaseSensitive(case_sensitive);
            }
            reader->TryDropStats();
            ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
            // A delete file removed before the range no longer applies to any data
            // file. One removed by a snapshot in the range applied until then.
            std::erase_if(entries, [&](const ManifestEntry& entry) {
              return entry.status == ManifestStatus::kDeleted &&
                     !(entry.snapshot_id.has_value() &&
                       sequence_numbers.contains(entry.snapshot_id.value()));
            });
            return entries;
          }));

  auto planner = std::unique_ptr<DeletionVectorChangelogPlanner>(
      new DeletionVectorChangelogPlanner());
  for (auto& entry : entries) {
    ICEBERG_PRECHECK(entry.data_file != nullptr,
                     "Invalid manifest entry with missing delete file");
    const DataFile& file = *entry.data_file;
    // The file sequence number belongs to the snapshot that committed the file, unlike
    // the data sequence number, which a rewrite can carry over from an older file.
    ICEBERG_PRECHECK(
        entry.file_sequence_number.has_value() && entry.sequence_number.has_value(),
        "Missing sequence number from delete file {}", file.file_path);
    ICEBERG_PRECHECK(file.partition_spec_id.has_value(),
                     "Missing partition spec ID from delete file {}", file.file_path);

    if (file.content == DataFile::Content::kEqualityDeletes) {
      auto spec_it = specs_by_id.find(file.partition_spec_id.value());
      ICEBERG_CHECK(spec_it != specs_by_id.end(),
                    "Partition spec ID {} not found for delete file {}",
                    file.partition_spec_id.value(), file.file_path);
      KeepLatest(spec_it->second->IsUnpartitioned()
                     ? planner->global_equality_deletes_
                     : LatestFor(planner->equality_deletes_by_partition_, file),
                 entry.sequence_number.value(), file.file_path);
      continue;
    }

    if (!file.IsDeletionVector()) {
      KeepLatest(
          file.referenced_data_file.has_value()
              ? planner->position_deletes_by_path_[file.referenced_data_file.value()]
              : LatestFor(planner->position_deletes_by_partition_, file),
          entry.sequence_number.value(), file.file_path);
      continue;
    }

    ICEBERG_PRECHECK(file.referenced_data_file.has_value(),
                     "Deletion vector {} does not reference a data file", file.file_path);
    // The same vector appears once per manifest that lists it, for example as added in
    // the manifest of the snapshot that committed it and as deleted in the manifest of
    // the snapshot that removed it.
    auto& versions = planner->dvs_by_path_[file.referenced_data_file.value()];
    auto version_it = std::ranges::find_if(versions, [&file](const auto& version) {
      return version.file->file_path == file.file_path &&
             version.file->content_offset == file.content_offset;
    });
    if (version_it == versions.end()) {
      versions.push_back({.file = entry.data_file,
                          .added_sequence_number = entry.file_sequence_number.value()});
      version_it = std::prev(versions.end());
    }
    if (entry.status == ManifestStatus::kDeleted) {
      version_it->removed_sequence_number =
          sequence_numbers.at(entry.snapshot_id.value());
    }
  }

  for (const auto& [data_file_path, versions] : planner->dvs_by_path_) {
    for (auto it = versions.begin(); it != versions.end(); ++it) {
      ICEBERG_PRECHECK(
          std::ranges::find(std::next(it), versions.end(), it->added_sequence_number,
                            &DeletionVectorVersion::added_sequence_number) ==
              versions.end(),
          "Snapshot with sequence number {} added multiple deletion vectors for {}",
          it->added_sequence_number, data_file_path);
    }
  }

  return planner;
}

std::optional<DeletionVectorChangelogPlanner::LatestDeleteFile>&
DeletionVectorChangelogPlanner::LatestFor(
    PartitionMap<std::optional<LatestDeleteFile>>& deletes_by_partition,
    const DataFile& file) {
  const int32_t spec_id = file.partition_spec_id.value();
  if (!deletes_by_partition.contains(spec_id, file.partition)) {
    deletes_by_partition.put(spec_id, file.partition, std::nullopt);
  }
  return deletes_by_partition.get(spec_id, file.partition)->get();
}

void DeletionVectorChangelogPlanner::KeepLatest(std::optional<LatestDeleteFile>& latest,
                                                int64_t sequence_number,
                                                const std::string& file_path) {
  if (!latest.has_value() || latest->sequence_number < sequence_number) {
    latest = LatestDeleteFile{.sequence_number = sequence_number, .file_path = file_path};
  }
}

std::shared_ptr<DataFile> DeletionVectorChangelogPlanner::AddedDeletionVector(
    int64_t sequence_number, const std::string& data_file_path) const {
  auto it = dvs_by_path_.find(data_file_path);
  if (it == dvs_by_path_.end()) {
    return nullptr;
  }
  auto version_it = std::ranges::find(it->second, sequence_number,
                                      &DeletionVectorVersion::added_sequence_number);
  return version_it == it->second.end() ? nullptr : version_it->file;
}

std::unordered_map<std::string, std::vector<int64_t>>
DeletionVectorChangelogPlanner::AddedDeletionVectors(
    const std::unordered_set<int64_t>& sequence_numbers) const {
  std::unordered_map<std::string, std::vector<int64_t>> added;
  for (const auto& [data_file_path, versions] : dvs_by_path_) {
    for (const auto& version : versions) {
      if (sequence_numbers.contains(version.added_sequence_number)) {
        added[data_file_path].push_back(version.added_sequence_number);
      }
    }
  }
  return added;
}

Result<std::shared_ptr<DataFile>> DeletionVectorChangelogPlanner::ExistingDeletionVector(
    int64_t sequence_number, const std::string& data_file_path) const {
  auto it = dvs_by_path_.find(data_file_path);
  if (it == dvs_by_path_.end()) {
    return nullptr;
  }
  std::shared_ptr<DataFile> existing;
  for (const auto& version : it->second) {
    const bool live = version.added_sequence_number < sequence_number &&
                      (!version.removed_sequence_number.has_value() ||
                       version.removed_sequence_number.value() >= sequence_number);
    if (!live) {
      continue;
    }
    ICEBERG_PRECHECK(existing == nullptr,
                     "Multiple deletion vectors apply to {} before sequence number {}",
                     data_file_path, sequence_number);
    existing = version.file;
  }
  return existing;
}

Status DeletionVectorChangelogPlanner::ValidateNoOtherDeletes(
    int64_t data_sequence_number, const DataFile& data_file) const {
  ICEBERG_PRECHECK(data_file.partition_spec_id.has_value(),
                   "Missing partition spec ID from data file {}", data_file.file_path);
  const int32_t spec_id = data_file.partition_spec_id.value();
  auto in_partition = [&](const PartitionMap<std::optional<LatestDeleteFile>>& deletes) {
    auto latest = deletes.get(spec_id, data_file.partition);
    return latest.has_value() ? latest->get() : std::nullopt;
  };
  auto for_path = position_deletes_by_path_.find(data_file.file_path);

  // An equality delete file applies to data files written before it, and a position
  // delete file also to those written in the same commit.
  for (const auto& latest :
       {global_equality_deletes_, in_partition(equality_deletes_by_partition_)}) {
    if (latest.has_value() && latest->sequence_number > data_sequence_number) {
      return NotSupported(
          "Equality delete files are not supported in changelog scans: {} applies to {}",
          latest->file_path, data_file.file_path);
    }
  }
  for (const auto& latest :
       {in_partition(position_deletes_by_partition_),
        for_path == position_deletes_by_path_.end() ? std::nullopt : for_path->second}) {
    if (latest.has_value() && latest->sequence_number >= data_sequence_number) {
      return NotSupported(
          "Position delete files are not supported in changelog scans: {} applies to {}",
          latest->file_path, data_file.file_path);
    }
  }
  return {};
}

}  // namespace iceberg
