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

#include <functional>
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

namespace {

// Appends the changelog tasks for one data file of a changelog scan.
using ChangelogTaskFactory =
    std::function<Status(ManifestEntry&& entry, std::shared_ptr<Expression> residual,
                         std::vector<std::shared_ptr<ScanTask>>& tasks)>;

// Plans the data files of the manifest group, keeping the stats and computing the
// residual that the scan requests before handing each file to make_tasks.
Result<std::vector<std::shared_ptr<ChangelogScanTask>>> PlanChangelogTasks(
    ManifestGroup& manifest_group, const ChangelogTaskFactory& make_tasks) {
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
      ICEBERG_RETURN_UNEXPECTED(make_tasks(std::move(entry), std::move(residual), tasks));
    }
    return tasks;
  };

  ICEBERG_ASSIGN_OR_RAISE(auto tasks, manifest_group.Plan(create_tasks_func));
  return tasks | std::views::transform([](const auto& task) {
           return std::static_pointer_cast<ChangelogScanTask>(task);
         }) |
         std::ranges::to<std::vector>();
}

}  // namespace

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
  std::unordered_set<int64_t> changelog_sequence_numbers;
  std::unordered_map<int64_t, int64_t> snapshot_ids_by_sequence_number;
  for (const auto& snapshot : changelog_snapshots) {
    ICEBERG_PRECHECK(
        std::cmp_less_equal(snapshot_ids.size(), std::numeric_limits<int32_t>::max()),
        "Number of snapshots in changelog scan exceeds maximum supported");
    snapshot_ids.insert(snapshot.first->snapshot_id);
    snapshot_ordinals.try_emplace(snapshot.first->snapshot_id,
                                  static_cast<int32_t>(snapshot_ordinals.size()));
    changelog_sequence_numbers.insert(snapshot.first->sequence_number);
    snapshot_ids_by_sequence_number.try_emplace(snapshot.first->sequence_number,
                                                snapshot.first->snapshot_id);
  }

  // Data manifests written by the changelog snapshots hold their added and deleted data
  // files; all data manifests of the changelog snapshots hold the data files that a
  // deletion vector can reference.
  std::vector<ManifestFile> added_data_manifests;
  std::vector<ManifestFile> data_manifests;
  std::unordered_set<std::string> seen_manifest_paths;
  for (const auto& [snapshot, snapshot_reader] : changelog_snapshots) {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_data_manifests,
                            snapshot_reader->DataManifests(io_));
    for (auto& manifest : snapshot_data_manifests) {
      if (!seen_manifest_paths.insert(manifest.manifest_path).second) {
        continue;
      }
      if (manifest.added_snapshot_id.has_value() &&
          snapshot_ids.contains(manifest.added_snapshot_id.value())) {
        added_data_manifests.push_back(manifest);
      }
      data_manifests.push_back(manifest);
    }
  }

  // Delete manifests of every snapshot in the range, replace snapshots included, so that
  // each removal of a deletion vector is seen in the manifest of the snapshot that made
  // it.
  std::vector<ManifestFile> delete_manifests;
  std::unordered_map<int64_t, int64_t> sequence_numbers;
  if (deletes_supported) {
    for (const auto& snapshot : ancestors_snapshots) {
      sequence_numbers.emplace(snapshot->snapshot_id, snapshot->sequence_number);
      SnapshotReader snapshot_reader(snapshot.get());
      ICEBERG_ASSIGN_OR_RAISE(auto snapshot_delete_manifests,
                              snapshot_reader.DeleteManifests(io_));
      for (auto& manifest : snapshot_delete_manifests) {
        if (seen_manifest_paths.insert(manifest.manifest_path).second) {
          delete_manifests.push_back(manifest);
        }
      }
    }
  }
  if (added_data_manifests.empty() && delete_manifests.empty()) {
    return std::vector<std::shared_ptr<ChangelogScanTask>>{};
  }

  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());

  std::unique_ptr<DeletionVectorChangelogPlanner> dv_planner;
  if (!delete_manifests.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(
        dv_planner, DeletionVectorChangelogPlanner::Make(
                        io_, schema_, specs_by_id, filter(), context_.case_sensitive,
                        context_.plan_executor, delete_manifests, sequence_numbers));
  }

  auto make_manifest_group =
      [&](std::vector<ManifestFile> manifests) -> Result<std::unique_ptr<ManifestGroup>> {
    ICEBERG_ASSIGN_OR_RAISE(
        auto manifest_group,
        ManifestGroup::Make(io_, schema_, specs_by_id, std::move(manifests),
                            /*delete_manifests=*/{}));
    manifest_group->CaseSensitive(context_.case_sensitive)
        .Select(ScanColumns())
        .FilterData(filter())
        .ColumnsToKeepStats(context_.columns_to_keep_stats)
        .PlanWith(context_.plan_executor);
    if (context_.ignore_residuals) {
      manifest_group->IgnoreResiduals();
    }
    return manifest_group;
  };

  std::vector<std::shared_ptr<ChangelogScanTask>> tasks;

  if (!added_data_manifests.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_group,
                            make_manifest_group(std::move(added_data_manifests)));
    manifest_group
        ->FilterManifestEntries([&snapshot_ids](const ManifestEntry& entry) {
          return entry.snapshot_id.has_value() &&
                 snapshot_ids.contains(entry.snapshot_id.value());
        })
        .IgnoreExisting();

    ICEBERG_ASSIGN_OR_RAISE(
        tasks,
        PlanChangelogTasks(
            *manifest_group,
            [&](ManifestEntry&& entry, std::shared_ptr<Expression> residual,
                std::vector<std::shared_ptr<ScanTask>>& planned) -> Status {
              ICEBERG_PRECHECK(entry.snapshot_id.has_value(),
                               "Invalid manifest entry with missing snapshot id");

              int64_t commit_snapshot_id = entry.snapshot_id.value();
              auto ordinal_it = snapshot_ordinals.find(commit_snapshot_id);
              ICEBERG_PRECHECK(ordinal_it != snapshot_ordinals.end(),
                               "Invalid manifest entry with missing snapshot ordinal");

              int32_t change_ordinal = ordinal_it->second;

              std::vector<std::shared_ptr<DataFile>> deletes;
              if (dv_planner) {
                ICEBERG_PRECHECK(entry.sequence_number.has_value(),
                                 "Missing sequence number from data file {}",
                                 entry.data_file->file_path);
                ICEBERG_RETURN_UNEXPECTED(dv_planner->ValidateNoOtherDeletes(
                    entry.sequence_number.value(), *entry.data_file));
                const int64_t sequence_number = sequence_numbers.at(commit_snapshot_id);
                const std::string& data_file_path = entry.data_file->file_path;
                std::shared_ptr<DataFile> dv;
                if (entry.status == ManifestStatus::kAdded) {
                  dv = dv_planner->AddedDeletionVector(sequence_number, data_file_path);
                } else {
                  ICEBERG_ASSIGN_OR_RAISE(dv, dv_planner->ExistingDeletionVector(
                                                  sequence_number, data_file_path));
                }
                if (dv != nullptr) {
                  deletes.push_back(std::move(dv));
                }
              }

              switch (entry.status) {
                case ManifestStatus::kAdded:
                  planned.push_back(std::make_shared<AddedRowsScanTask>(
                      change_ordinal, commit_snapshot_id, std::move(entry.data_file),
                      std::move(deletes), std::move(residual)));
                  return {};
                case ManifestStatus::kDeleted:
                  planned.push_back(std::make_shared<DeletedDataFileScanTask>(
                      change_ordinal, commit_snapshot_id, std::move(entry.data_file),
                      std::move(deletes), std::move(residual)));
                  return {};
                case ManifestStatus::kExisting:
                  return InvalidArgument("Unexpected entry status: EXISTING");
              }
              return {};
            }));
  }

  if (dv_planner == nullptr) {
    return tasks;
  }
  auto added_dvs = dv_planner->AddedDeletionVectors(changelog_sequence_numbers);
  if (added_dvs.empty()) {
    return tasks;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto manifest_group,
                          make_manifest_group(std::move(data_manifests)));
  manifest_group
      ->FilterManifestEntries([&added_dvs](const ManifestEntry& entry) {
        return entry.data_file != nullptr &&
               added_dvs.contains(entry.data_file->file_path);
      })
      .IgnoreDeleted();

  // A data file is listed in the manifests of every snapshot it is live in.
  std::unordered_set<std::string> planned_paths;
  ICEBERG_ASSIGN_OR_RAISE(
      auto deleted_rows_tasks,
      PlanChangelogTasks(
          *manifest_group,
          [&](ManifestEntry&& entry, std::shared_ptr<Expression> residual,
              std::vector<std::shared_ptr<ScanTask>>& planned) -> Status {
            const std::string& data_file_path = entry.data_file->file_path;
            if (!planned_paths.insert(data_file_path).second) {
              return {};
            }
            ICEBERG_PRECHECK(entry.sequence_number.has_value() &&
                                 entry.file_sequence_number.has_value(),
                             "Missing sequence number from data file {}", data_file_path);
            ICEBERG_RETURN_UNEXPECTED(dv_planner->ValidateNoOtherDeletes(
                entry.sequence_number.value(), *entry.data_file));

            for (int64_t sequence_number : added_dvs.at(data_file_path)) {
              // A deletion vector committed together with its data file belongs to the
              // file's AddedRowsScanTask.
              if (sequence_number == entry.file_sequence_number.value()) {
                continue;
              }
              const int64_t snapshot_id =
                  snapshot_ids_by_sequence_number.at(sequence_number);
              std::vector<std::shared_ptr<DataFile>> existing_deletes;
              ICEBERG_ASSIGN_OR_RAISE(
                  auto existing_dv,
                  dv_planner->ExistingDeletionVector(sequence_number, data_file_path));
              if (existing_dv != nullptr) {
                existing_deletes.push_back(std::move(existing_dv));
              }
              planned.push_back(std::make_shared<DeletedRowsScanTask>(
                  snapshot_ordinals.at(snapshot_id), snapshot_id, entry.data_file,
                  std::vector<std::shared_ptr<DataFile>>{
                      dv_planner->AddedDeletionVector(sequence_number, data_file_path)},
                  std::move(existing_deletes), residual));
            }
            return {};
          }));
  tasks.insert(tasks.end(), std::make_move_iterator(deleted_rows_tasks.begin()),
               std::make_move_iterator(deleted_rows_tasks.end()));
  return tasks;
}

}  // namespace iceberg
