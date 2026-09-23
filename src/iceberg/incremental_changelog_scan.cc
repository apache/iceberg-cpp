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

#include "iceberg/incremental_changelog_scan.h"

#include <iterator>
#include <limits>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "iceberg/changelog_dv_planner_internal.h"
#include "iceberg/expression/residual_evaluator.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_group.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util.h"

namespace iceberg {

Result<std::unique_ptr<IncrementalChangelogScan>> IncrementalChangelogScan::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<IncrementalChangelogScan>(new IncrementalChangelogScan(
      std::move(metadata), std::move(schema), std::move(io), std::move(context)));
}

Result<std::vector<std::shared_ptr<ChangelogScanTask>>>
IncrementalChangelogScan::PlanFiles() const {
  return ResolvePlanFiles<ChangelogScanTask>(*this);
}

Result<std::vector<std::shared_ptr<ChangelogScanTask>>>
IncrementalChangelogScan::PlanFiles(std::optional<int64_t> from_snapshot_id_exclusive,
                                    int64_t to_snapshot_id_inclusive) const {
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors_snapshots,
      SnapshotUtil::AncestorsBetween(*metadata_, to_snapshot_id_inclusive,
                                     from_snapshot_id_exclusive));

  // Row-level deletes are only understood as deletion vectors, which format version 3
  // requires; earlier versions store them as position and equality delete files.
  const bool deletes_supported = metadata_->format_version >= 3;

  std::vector<std::pair<std::shared_ptr<Snapshot>, std::unique_ptr<SnapshotReader>>>
      changelog_snapshots;

  for (const auto& snapshot : std::ranges::reverse_view(ancestors_snapshots)) {
    auto operation = snapshot->Operation();
    if (!operation.has_value() || operation.value() != DataOperation::kReplace) {
      auto snapshot_reader = std::make_unique<SnapshotReader>(snapshot.get());
      if (!deletes_supported) {
        ICEBERG_ASSIGN_OR_RAISE(auto delete_manifests,
                                snapshot_reader->DeleteManifests(io_));
        if (!delete_manifests.empty()) {
          return NotSupported(
              "Delete files are only supported in changelog scans of format version 3 "
              "tables, but the table uses format version {}",
              metadata_->format_version);
        }
      }
      changelog_snapshots.emplace_back(snapshot, std::move(snapshot_reader));
    }
  }
  if (changelog_snapshots.empty()) {
    return std::vector<std::shared_ptr<ChangelogScanTask>>{};
  }

  std::unordered_set<int64_t> snapshot_ids;
  std::unordered_map<int64_t, int32_t> snapshot_ordinals;
  for (const auto& snapshot : changelog_snapshots) {
    ICEBERG_PRECHECK(
        std::cmp_less_equal(snapshot_ids.size(), std::numeric_limits<int32_t>::max()),
        "Number of snapshots in changelog scan exceeds maximum supported");
    snapshot_ids.insert(snapshot.first->snapshot_id);
    snapshot_ordinals.try_emplace(snapshot.first->snapshot_id,
                                  static_cast<int32_t>(snapshot_ordinals.size()));
  }

  std::vector<ManifestFile> data_manifests;
  std::vector<ManifestFile> delete_manifests;
  std::unordered_set<std::string> seen_manifest_paths;
  for (const auto& [snapshot, snapshot_reader] : changelog_snapshots) {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_data_manifests,
                            snapshot_reader->DataManifests(io_));
    for (auto& manifest : snapshot_data_manifests) {
      if (manifest.added_snapshot_id.has_value() &&
          snapshot_ids.contains(manifest.added_snapshot_id.value()) &&
          seen_manifest_paths.insert(manifest.manifest_path).second) {
        data_manifests.push_back(manifest);
      }
    }
    if (deletes_supported) {
      // Every delete manifest is collected, not only those written in the range, so
      // that position delete files and equality delete files carried in older manifests
      // are rejected rather than silently left out of the changelog.
      ICEBERG_ASSIGN_OR_RAISE(auto snapshot_delete_manifests,
                              snapshot_reader->DeleteManifests(io_));
      for (auto& manifest : snapshot_delete_manifests) {
        if (seen_manifest_paths.insert(manifest.manifest_path).second) {
          delete_manifests.push_back(manifest);
        }
      }
    }
  }
  if (data_manifests.empty() && delete_manifests.empty()) {
    return std::vector<std::shared_ptr<ChangelogScanTask>>{};
  }

  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());

  std::unique_ptr<DeletionVectorChangelogPlanner> dv_planner;
  if (!delete_manifests.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(
        dv_planner, DeletionVectorChangelogPlanner::Make(
                        io_, schema_, specs_by_id, context_, ScanColumns(), filter(),
                        delete_manifests, snapshot_ids));
  }

  std::vector<std::shared_ptr<ChangelogScanTask>> tasks;
  // Data files added by each snapshot. A deletion vector committed together with its data
  // file belongs to the AddedRowsScanTask rather than to a DeletedRowsScanTask.
  std::unordered_map<int64_t, std::unordered_set<std::string>> added_paths_by_snapshot;

  if (!data_manifests.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto manifest_group,
        ManifestGroup::Make(io_, schema_, specs_by_id, std::move(data_manifests),
                            /*delete_manifests=*/{}));

    manifest_group->CaseSensitive(context_.case_sensitive)
        .Select(ScanColumns())
        .FilterData(filter())
        .FilterManifestEntries([&snapshot_ids](const ManifestEntry& entry) {
          return entry.snapshot_id.has_value() &&
                 snapshot_ids.contains(entry.snapshot_id.value());
        })
        .IgnoreExisting()
        .ColumnsToKeepStats(context_.columns_to_keep_stats)
        .PlanWith(context_.plan_executor);

    if (context_.ignore_residuals) {
      manifest_group->IgnoreResiduals();
    }

    auto create_tasks_func =
        [&](std::vector<ManifestEntry>&& entries,
            const TaskContext& ctx) -> Result<std::vector<std::shared_ptr<ScanTask>>> {
      std::vector<std::shared_ptr<ScanTask>> tasks;
      tasks.reserve(entries.size());

      for (auto& entry : entries) {
        ICEBERG_PRECHECK(entry.snapshot_id.has_value() && entry.data_file,
                         "Invalid manifest entry with missing snapshot id or data file");

        int64_t commit_snapshot_id = entry.snapshot_id.value();
        auto ordinal_it = snapshot_ordinals.find(commit_snapshot_id);
        ICEBERG_PRECHECK(ordinal_it != snapshot_ordinals.end(),
                         "Invalid manifest entry with missing snapshot ordinal");

        int32_t change_ordinal = ordinal_it->second;

        if (ctx.drop_stats) {
          ContentFileUtil::DropAllStats(*entry.data_file);
        } else if (!ctx.columns_to_keep_stats.empty()) {
          ContentFileUtil::DropUnselectedStats(*entry.data_file,
                                               ctx.columns_to_keep_stats);
        }

        ICEBERG_ASSIGN_OR_RAISE(auto residual,
                                ctx.residuals->ResidualFor(entry.data_file->partition));
        const std::string& data_file_path = entry.data_file->file_path;
        switch (entry.status) {
          case ManifestStatus::kAdded: {
            std::vector<std::shared_ptr<DataFile>> deletes;
            if (dv_planner) {
              deletes = dv_planner->AddedDeletes(commit_snapshot_id, data_file_path);
              added_paths_by_snapshot[commit_snapshot_id].insert(data_file_path);
            }
            tasks.push_back(std::make_shared<AddedRowsScanTask>(
                change_ordinal, commit_snapshot_id, std::move(entry.data_file),
                std::move(deletes), std::move(residual)));
            break;
          }
          case ManifestStatus::kDeleted: {
            std::vector<std::shared_ptr<DataFile>> existing_deletes;
            if (dv_planner) {
              existing_deletes =
                  dv_planner->RemovedDeletes(commit_snapshot_id, data_file_path);
            }
            tasks.push_back(std::make_shared<DeletedDataFileScanTask>(
                change_ordinal, commit_snapshot_id, std::move(entry.data_file),
                std::move(existing_deletes), std::move(residual)));
            break;
          }
          case ManifestStatus::kExisting:
            return InvalidArgument("Unexpected entry status: EXISTING");
        }
      }
      return tasks;
    };

    ICEBERG_ASSIGN_OR_RAISE(auto data_file_tasks,
                            manifest_group->Plan(create_tasks_func));
    tasks = data_file_tasks | std::views::transform([](const auto& task) {
              return std::static_pointer_cast<ChangelogScanTask>(task);
            }) |
            std::ranges::to<std::vector>();
  }

  if (dv_planner) {
    const std::unordered_set<std::string> no_added_paths;
    for (const auto& [snapshot, snapshot_reader] : changelog_snapshots) {
      const int64_t snapshot_id = snapshot->snapshot_id;
      auto added_paths_it = added_paths_by_snapshot.find(snapshot_id);
      const auto& added_paths = added_paths_it == added_paths_by_snapshot.end()
                                    ? no_added_paths
                                    : added_paths_it->second;
      ICEBERG_ASSIGN_OR_RAISE(auto snapshot_data_manifests,
                              snapshot_reader->DataManifests(io_));
      ICEBERG_ASSIGN_OR_RAISE(
          auto deleted_rows_tasks,
          dv_planner->PlanDeletedRows(snapshot_id, snapshot_ordinals.at(snapshot_id),
                                      snapshot_data_manifests, added_paths));
      tasks.insert(tasks.end(), std::make_move_iterator(deleted_rows_tasks.begin()),
                   std::make_move_iterator(deleted_rows_tasks.end()));
    }
  }

  return tasks;
}

}  // namespace iceberg
