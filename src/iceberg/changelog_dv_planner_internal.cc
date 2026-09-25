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

#include <ranges>
#include <utility>

#include "iceberg/expression/residual_evaluator.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_group.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/executor_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

Status ValidateChangelogDeleteFile(const DataFile& file) {
  if (file.content == DataFile::Content::kEqualityDeletes) {
    return NotSupported("Equality delete files are not supported in changelog scans: {}",
                        file.file_path);
  }
  if (!file.IsDeletionVector()) {
    return NotSupported("Position delete files are not supported in changelog scans: {}",
                        file.file_path);
  }
  ICEBERG_PRECHECK(file.referenced_data_file.has_value(),
                   "Deletion vector {} does not reference a data file", file.file_path);
  return {};
}

std::vector<std::shared_ptr<DataFile>> FindDeletionVector(
    const std::unordered_map<std::string, std::shared_ptr<DataFile>>& dvs_by_path,
    const std::string& data_file_path) {
  auto it = dvs_by_path.find(data_file_path);
  if (it == dvs_by_path.end()) {
    return {};
  }
  return {it->second};
}

}  // namespace

DeletionVectorChangelogPlanner::DeletionVectorChangelogPlanner(
    std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
    std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
    internal::TableScanContext context, std::vector<std::string> scan_columns,
    std::shared_ptr<Expression> filter,
    std::unordered_map<int64_t, SnapshotDeletionVectors> dvs_by_snapshot)
    : io_(std::move(io)),
      schema_(std::move(schema)),
      specs_by_id_(std::move(specs_by_id)),
      context_(std::move(context)),
      scan_columns_(std::move(scan_columns)),
      filter_(std::move(filter)),
      dvs_by_snapshot_(std::move(dvs_by_snapshot)) {}

Result<std::unique_ptr<DeletionVectorChangelogPlanner>>
DeletionVectorChangelogPlanner::Make(
    std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
    std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
    const internal::TableScanContext& context, std::vector<std::string> scan_columns,
    std::shared_ptr<Expression> filter, const std::vector<ManifestFile>& delete_manifests,
    const std::unordered_set<int64_t>& snapshot_ids) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto entries,
      ParallelCollect(
          context.plan_executor, delete_manifests,
          [&](const ManifestFile& manifest) -> Result<std::vector<ManifestEntry>> {
            ICEBERG_ASSIGN_OR_RAISE(
                auto reader, ManifestReader::Make(manifest, io, schema, specs_by_id));
            reader->TryDropStats();
            ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
            auto in_range = [&snapshot_ids](const ManifestEntry& entry) {
              return entry.snapshot_id.has_value() &&
                     snapshot_ids.contains(entry.snapshot_id.value());
            };
            for (const auto& entry : entries) {
              ICEBERG_PRECHECK(entry.data_file != nullptr,
                               "Invalid manifest entry with missing delete file");
              // A delete file removed before the range no longer applies to any data
              // file, so it cannot affect the changelog. Every other delete file can: a
              // live one applies to data files whose rows the changelog reports, and one
              // removed by a changelog snapshot was applied to the rows that snapshot
              // reports as deleted. Those must be deletion vectors.
              if (entry.status == ManifestStatus::kDeleted && !in_range(entry)) {
                continue;
              }
              ICEBERG_RETURN_UNEXPECTED(ValidateChangelogDeleteFile(*entry.data_file));
            }
            std::erase_if(entries, [&in_range](const ManifestEntry& entry) {
              return entry.status == ManifestStatus::kExisting || !in_range(entry);
            });
            return entries;
          }));

  std::unordered_map<int64_t, SnapshotDeletionVectors> dvs_by_snapshot;
  for (auto& entry : entries) {
    const int64_t snapshot_id = entry.snapshot_id.value();
    auto& dvs = dvs_by_snapshot[snapshot_id];
    auto& dvs_by_path = entry.status == ManifestStatus::kAdded ? dvs.added : dvs.removed;
    const std::string& data_file_path = entry.data_file->referenced_data_file.value();
    auto [it, inserted] =
        dvs_by_path.try_emplace(data_file_path, std::move(entry.data_file));
    ICEBERG_PRECHECK(
        inserted, "Snapshot {} {} multiple deletion vectors for {}", snapshot_id,
        entry.status == ManifestStatus::kAdded ? "added" : "removed", data_file_path);
  }

  return std::unique_ptr<DeletionVectorChangelogPlanner>(
      new DeletionVectorChangelogPlanner(
          std::move(io), std::move(schema), std::move(specs_by_id), context,
          std::move(scan_columns), std::move(filter), std::move(dvs_by_snapshot)));
}

std::vector<std::shared_ptr<DataFile>> DeletionVectorChangelogPlanner::AddedDeletes(
    int64_t snapshot_id, const std::string& data_file_path) const {
  auto it = dvs_by_snapshot_.find(snapshot_id);
  if (it == dvs_by_snapshot_.end()) {
    return {};
  }
  return FindDeletionVector(it->second.added, data_file_path);
}

std::vector<std::shared_ptr<DataFile>> DeletionVectorChangelogPlanner::RemovedDeletes(
    int64_t snapshot_id, const std::string& data_file_path) const {
  auto it = dvs_by_snapshot_.find(snapshot_id);
  if (it == dvs_by_snapshot_.end()) {
    return {};
  }
  return FindDeletionVector(it->second.removed, data_file_path);
}

Result<std::vector<std::shared_ptr<ChangelogScanTask>>>
DeletionVectorChangelogPlanner::PlanDeletedRows(
    int64_t snapshot_id, int32_t change_ordinal, std::span<ManifestFile> data_manifests,
    const std::unordered_set<std::string>& added_data_file_paths) const {
  auto dvs_it = dvs_by_snapshot_.find(snapshot_id);
  if (dvs_it == dvs_by_snapshot_.end()) {
    return std::vector<std::shared_ptr<ChangelogScanTask>>{};
  }
  const SnapshotDeletionVectors& dvs = dvs_it->second;

  std::unordered_set<std::string> pending_paths;
  for (const auto& [data_file_path, dv] : dvs.added) {
    if (!added_data_file_paths.contains(data_file_path)) {
      pending_paths.insert(data_file_path);
    }
  }
  if (pending_paths.empty()) {
    return std::vector<std::shared_ptr<ChangelogScanTask>>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_group,
      ManifestGroup::Make(
          io_, schema_, specs_by_id_,
          std::vector<ManifestFile>(data_manifests.begin(), data_manifests.end()),
          /*delete_manifests=*/{}));
  manifest_group->CaseSensitive(context_.case_sensitive)
      .Select(scan_columns_)
      .FilterData(filter_)
      .FilterManifestEntries([&pending_paths](const ManifestEntry& entry) {
        return entry.data_file != nullptr &&
               pending_paths.contains(entry.data_file->file_path);
      })
      .IgnoreDeleted()
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
      ICEBERG_PRECHECK(entry.data_file != nullptr,
                       "Invalid manifest entry with missing data file");

      if (ctx.drop_stats) {
        ContentFileUtil::DropAllStats(*entry.data_file);
      } else if (!ctx.columns_to_keep_stats.empty()) {
        ContentFileUtil::DropUnselectedStats(*entry.data_file, ctx.columns_to_keep_stats);
      }

      ICEBERG_ASSIGN_OR_RAISE(auto residual,
                              ctx.residuals->ResidualFor(entry.data_file->partition));
      const std::string& data_file_path = entry.data_file->file_path;
      auto added_deletes = FindDeletionVector(dvs.added, data_file_path);
      auto existing_deletes = FindDeletionVector(dvs.removed, data_file_path);
      tasks.push_back(std::make_shared<DeletedRowsScanTask>(
          change_ordinal, snapshot_id, std::move(entry.data_file),
          std::move(added_deletes), std::move(existing_deletes), std::move(residual)));
    }
    return tasks;
  };

  ICEBERG_ASSIGN_OR_RAISE(auto tasks, manifest_group->Plan(create_tasks_func));
  return tasks | std::views::transform([](const auto& task) {
           return std::static_pointer_cast<ChangelogScanTask>(task);
         }) |
         std::ranges::to<std::vector>();
}

}  // namespace iceberg
