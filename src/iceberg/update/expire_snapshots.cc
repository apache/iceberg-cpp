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

#include "iceberg/update/expire_snapshots.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/statistics_file.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"

namespace iceberg {

namespace {

Result<std::shared_ptr<ManifestReader>> MakeManifestReader(
    const ManifestFile& manifest, const std::shared_ptr<FileIO>& file_io,
    const TableMetadata& metadata) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          metadata.PartitionSpecById(manifest.partition_spec_id));
  return ManifestReader::Make(manifest, file_io, std::move(schema), std::move(spec));
}

/// \brief Abstract strategy for cleaning up files after snapshot expiration.
class FileCleanupStrategy {
 public:
  FileCleanupStrategy(std::shared_ptr<FileIO> file_io,
                      std::function<void(const std::string&)> delete_func)
      : file_io_(std::move(file_io)), delete_func_(std::move(delete_func)) {}

  virtual ~FileCleanupStrategy() = default;

  /// \brief Clean up files that are only reachable by expired snapshots.
  ///
  /// \param metadata_before_expiration Table metadata before expiration.
  /// \param metadata_after_expiration Table metadata after expiration.
  /// \param expired_snapshot_ids Snapshot IDs that were expired during this operation.
  /// \param level Controls which types of files are eligible for deletion.
  virtual Status CleanFiles(const TableMetadata& metadata_before_expiration,
                            const TableMetadata& metadata_after_expiration,
                            const std::unordered_set<int64_t>& expired_snapshot_ids,
                            CleanupLevel level) = 0;

 protected:
  /// \brief Delete a single file
  void DeleteFile(const std::string& path) {
    try {
      if (delete_func_) {
        delete_func_(path);
      } else {
        std::ignore = file_io_->DeleteFile(path);
      }
    } catch (...) {
      /// TODO(shangxinli): add retry
    }
  }

  /// TODO(shangxinli): Add bulk deletion
  void DeleteFiles(const std::unordered_set<std::string>& paths) {
    for (const auto& path : paths) {
      DeleteFile(path);
    }
  }

  bool HasAnyStatisticsFiles(const TableMetadata& metadata) const {
    return !metadata.statistics.empty() || !metadata.partition_statistics.empty();
  }

  std::unordered_set<std::string> StatisticsFilesToDelete(
      const TableMetadata& metadata_before_expiration,
      const TableMetadata& metadata_after_expiration) const {
    std::unordered_set<std::string> stats_files_to_delete;
    std::unordered_set<std::string> live_stats_paths;

    for (const auto& stats_file : metadata_after_expiration.statistics) {
      if (stats_file) {
        live_stats_paths.insert(stats_file->path);
      }
    }

    for (const auto& part_stats_file : metadata_after_expiration.partition_statistics) {
      if (part_stats_file) {
        live_stats_paths.insert(part_stats_file->path);
      }
    }

    for (const auto& stats_file : metadata_before_expiration.statistics) {
      if (stats_file && !live_stats_paths.contains(stats_file->path)) {
        stats_files_to_delete.insert(stats_file->path);
      }
    }

    for (const auto& part_stats_file : metadata_before_expiration.partition_statistics) {
      if (part_stats_file && !live_stats_paths.contains(part_stats_file->path)) {
        stats_files_to_delete.insert(part_stats_file->path);
      }
    }

    return stats_files_to_delete;
  }

  std::shared_ptr<FileIO> file_io_;
  std::function<void(const std::string&)> delete_func_;
};

/// \brief File cleanup strategy that determines safe deletions via full reachability.
///
/// Collects manifests from all expired and retained snapshots, prunes candidates
/// still referenced by retained snapshots, then deletes orphaned manifests, data
/// files, and manifest lists.
///
/// TODO(shangxinli): Add multi-threaded manifest reading and file deletion support.
class ReachableFileCleanup : public FileCleanupStrategy {
 public:
  using FileCleanupStrategy::FileCleanupStrategy;

  Status CleanFiles(const TableMetadata& metadata_before_expiration,
                    const TableMetadata& metadata_after_expiration,
                    const std::unordered_set<int64_t>& expired_snapshot_ids,
                    CleanupLevel level) override {
    std::unordered_set<int64_t> retained_snapshot_ids;
    for (const auto& snapshot : metadata_after_expiration.snapshots) {
      if (snapshot) {
        retained_snapshot_ids.insert(snapshot->snapshot_id);
      }
    }

    std::unordered_set<std::string> manifest_lists_to_delete;
    for (int64_t snapshot_id : expired_snapshot_ids) {
      ICEBERG_ASSIGN_OR_RAISE(auto snapshot,
                              metadata_before_expiration.SnapshotById(snapshot_id));
      if (snapshot && !snapshot->manifest_list.empty()) {
        manifest_lists_to_delete.insert(snapshot->manifest_list);
      }
    }

    ICEBERG_ASSIGN_OR_RAISE(
        auto deletion_candidates,
        ReadManifests(metadata_before_expiration, expired_snapshot_ids));

    if (!deletion_candidates.empty()) {
      std::unordered_set<ManifestFile> current_manifests;
      ICEBERG_ASSIGN_OR_RAISE(
          auto manifests_to_delete,
          PruneReferencedManifests(metadata_after_expiration, retained_snapshot_ids,
                                   std::move(deletion_candidates), current_manifests));

      if (!manifests_to_delete.empty()) {
        if (level == CleanupLevel::kAll) {
          // Deleting data files
          auto data_files_to_delete = FindDataFilesToDelete(
              metadata_after_expiration, manifests_to_delete, current_manifests);
          DeleteFiles(data_files_to_delete);
        }

        // Deleting manifest files
        DeleteFiles(ManifestPaths(manifests_to_delete));
      }
    }

    // Deleting manifest-list files
    DeleteFiles(manifest_lists_to_delete);

    // Deleting statistics files
    if (HasAnyStatisticsFiles(metadata_before_expiration) ||
        HasAnyStatisticsFiles(metadata_after_expiration)) {
      DeleteFiles(
          StatisticsFilesToDelete(metadata_before_expiration, metadata_after_expiration));
    }

    return {};
  }

 private:
  /// \brief Collect manifests for a snapshot into manifests.
  Result<std::unordered_set<ManifestFile>> ReadManifestsForSnapshot(
      const TableMetadata& metadata, int64_t snapshot_id) {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, metadata.SnapshotById(snapshot_id));

    SnapshotCache snapshot_cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_manifests, snapshot_cache.Manifests(file_io_));

    std::unordered_set<ManifestFile> manifests;
    for (const auto& manifest : snapshot_manifests) {
      manifests.insert(manifest);
    }
    return manifests;
  }

  /// \brief Collect manifests for a set of snapshots.
  Result<std::unordered_set<ManifestFile>> ReadManifests(
      const TableMetadata& metadata, const std::unordered_set<int64_t>& snapshot_ids) {
    std::unordered_set<ManifestFile> manifests;
    for (int64_t snapshot_id : snapshot_ids) {
      ICEBERG_ASSIGN_OR_RAISE(auto snapshot_manifests,
                              ReadManifestsForSnapshot(metadata, snapshot_id));
      manifests.insert(snapshot_manifests.begin(), snapshot_manifests.end());
    }
    return manifests;
  }

  /// \brief Remove manifests still referenced by retained snapshots.
  Result<std::unordered_set<ManifestFile>> PruneReferencedManifests(
      const TableMetadata& metadata,
      const std::unordered_set<int64_t>& retained_snapshot_ids,
      std::unordered_set<ManifestFile> manifests_to_delete,
      std::unordered_set<ManifestFile>& current_manifests) {
    for (int64_t snapshot_id : retained_snapshot_ids) {
      ICEBERG_ASSIGN_OR_RAISE(auto snapshot_manifests,
                              ReadManifestsForSnapshot(metadata, snapshot_id));

      for (const auto& manifest : snapshot_manifests) {
        manifests_to_delete.erase(manifest);

        if (manifests_to_delete.empty()) {
          return manifests_to_delete;
        }

        current_manifests.insert(manifest);
      }
    }

    return manifests_to_delete;
  }

  Result<std::unordered_set<std::string>> ReadLiveDataFilePaths(
      const TableMetadata& metadata, const ManifestFile& manifest) {
    ICEBERG_PRECHECK(manifest.content == ManifestContent::kData,
                     "Cannot read data file paths from a delete manifest: {}",
                     manifest.manifest_path);

    /// TODO(shangxinli): optimize by only reading file paths
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            MakeManifestReader(manifest, file_io_, metadata));
    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());

    std::unordered_set<std::string> data_file_paths;
    for (const auto& entry : entries) {
      if (entry.data_file) {
        data_file_paths.insert(entry.data_file->file_path);
      }
    }

    return data_file_paths;
  }

  /// \brief Project manifests to manifest paths for deletion.
  std::unordered_set<std::string> ManifestPaths(
      const std::unordered_set<ManifestFile>& manifests) const {
    std::unordered_set<std::string> manifest_paths;
    manifest_paths.reserve(manifests.size());
    for (const auto& manifest : manifests) {
      manifest_paths.insert(manifest.manifest_path);
    }
    return manifest_paths;
  }

  /// \brief Find data files to delete from manifests being removed.
  std::unordered_set<std::string> FindDataFilesToDelete(
      const TableMetadata& metadata,
      const std::unordered_set<ManifestFile>& manifests_to_delete,
      const std::unordered_set<ManifestFile>& current_manifests) {
    std::unordered_set<std::string> data_files_to_delete;

    // Collect live file paths from manifests being deleted.
    for (const auto& manifest : manifests_to_delete) {
      auto live_data_files = ReadLiveDataFilePaths(metadata, manifest);
      // Ignore expired-manifest read failures and keep scanning candidates.
      if (!live_data_files.has_value()) {
        continue;
      }

      data_files_to_delete.insert(live_data_files->begin(), live_data_files->end());
    }

    if (data_files_to_delete.empty()) {
      return data_files_to_delete;
    }

    // Remove files still referenced by current manifests.
    for (const auto& manifest : current_manifests) {
      if (data_files_to_delete.empty()) {
        return data_files_to_delete;
      }

      auto live_data_files = ReadLiveDataFilePaths(metadata, manifest);
      // Fail closed if any retained manifest cannot be read safely.
      if (!live_data_files.has_value()) {
        return std::unordered_set<std::string>{};
      }

      for (const auto& file_path : live_data_files.value()) {
        data_files_to_delete.erase(file_path);
      }
    }

    return data_files_to_delete;
  }
};

}  // namespace

Result<std::shared_ptr<ExpireSnapshots>> ExpireSnapshots::Make(
    std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create ExpireSnapshots without a context");
  return std::shared_ptr<ExpireSnapshots>(new ExpireSnapshots(std::move(ctx)));
}

ExpireSnapshots::ExpireSnapshots(std::shared_ptr<TransactionContext> ctx)
    : PendingUpdate(std::move(ctx)),
      current_time_ms_(CurrentTimePointMs()),
      default_max_ref_age_ms_(base().properties.Get(TableProperties::kMaxRefAgeMs)),
      default_min_num_snapshots_(
          base().properties.Get(TableProperties::kMinSnapshotsToKeep)),
      default_expire_older_than_(current_time_ms_ -
                                 std::chrono::milliseconds(base().properties.Get(
                                     TableProperties::kMaxSnapshotAgeMs))) {
  if (!base().properties.Get(TableProperties::kGcEnabled)) {
    AddError(
        ValidationFailed("Cannot expire snapshots: GC is disabled (deleting files may "
                         "corrupt other tables)"));
    return;
  }
}

ExpireSnapshots::~ExpireSnapshots() = default;

ExpireSnapshots& ExpireSnapshots::ExpireSnapshotId(int64_t snapshot_id) {
  snapshot_ids_to_expire_.push_back(snapshot_id);
  specified_snapshot_id_ = true;
  return *this;
}

ExpireSnapshots& ExpireSnapshots::ExpireOlderThan(int64_t timestamp_millis) {
  default_expire_older_than_ = TimePointMsFromUnixMs(timestamp_millis);
  return *this;
}

ExpireSnapshots& ExpireSnapshots::RetainLast(int num_snapshots) {
  ICEBERG_BUILDER_CHECK(num_snapshots > 0,
                        "Number of snapshots to retain must be positive: {}",
                        num_snapshots);
  default_min_num_snapshots_ = num_snapshots;
  return *this;
}

ExpireSnapshots& ExpireSnapshots::DeleteWith(
    std::function<void(const std::string&)> delete_func) {
  delete_func_ = std::move(delete_func);
  return *this;
}

ExpireSnapshots& ExpireSnapshots::CleanupLevel(enum CleanupLevel level) {
  cleanup_level_ = level;
  return *this;
}

ExpireSnapshots& ExpireSnapshots::CleanExpiredMetadata(bool clean) {
  clean_expired_metadata_ = clean;
  return *this;
}

Result<std::unordered_set<int64_t>> ExpireSnapshots::ComputeBranchSnapshotsToRetain(
    int64_t snapshot_id, TimePointMs expire_snapshot_older_than,
    int32_t min_snapshots_to_keep) const {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshots,
                          SnapshotUtil::AncestorsOf(snapshot_id, [this](int64_t id) {
                            return base().SnapshotById(id);
                          }));

  std::unordered_set<int64_t> ids_to_retain;
  ids_to_retain.reserve(snapshots.size());

  for (const auto& ancestor : snapshots) {
    ICEBERG_DCHECK(ancestor != nullptr, "Ancestor snapshot is null");
    if (ids_to_retain.size() < min_snapshots_to_keep ||
        ancestor->timestamp_ms >= expire_snapshot_older_than) {
      ids_to_retain.insert(ancestor->snapshot_id);
    } else {
      break;
    }
  }

  return ids_to_retain;
}

Result<std::unordered_set<int64_t>> ExpireSnapshots::ComputeAllBranchSnapshotIdsToRetain(
    const SnapshotToRef& refs) const {
  std::unordered_set<int64_t> snapshot_ids_to_retain;
  for (const auto& [key, ref] : refs) {
    if (ref->type() != SnapshotRefType::kBranch) {
      continue;
    }
    const auto& branch = std::get<SnapshotRef::Branch>(ref->retention);
    TimePointMs expire_snapshot_older_than =
        branch.max_snapshot_age_ms.has_value()
            ? current_time_ms_ -
                  std::chrono::milliseconds(branch.max_snapshot_age_ms.value())
            : default_expire_older_than_;
    int32_t min_snapshots_to_keep =
        branch.min_snapshots_to_keep.value_or(default_min_num_snapshots_);
    ICEBERG_ASSIGN_OR_RAISE(
        auto to_retain,
        ComputeBranchSnapshotsToRetain(ref->snapshot_id, expire_snapshot_older_than,
                                       min_snapshots_to_keep));
    snapshot_ids_to_retain.insert(std::make_move_iterator(to_retain.begin()),
                                  std::make_move_iterator(to_retain.end()));
  }
  return snapshot_ids_to_retain;
}

Result<std::unordered_set<int64_t>> ExpireSnapshots::UnreferencedSnapshotIdsToRetain(
    const SnapshotToRef& refs) const {
  std::unordered_set<int64_t> referenced_ids;
  for (const auto& [key, ref] : refs) {
    if (ref->type() == SnapshotRefType::kBranch) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto snapshots, SnapshotUtil::AncestorsOf(ref->snapshot_id, [this](int64_t id) {
            return base().SnapshotById(id);
          }));
      for (const auto& snapshot : snapshots) {
        ICEBERG_DCHECK(snapshot != nullptr, "Ancestor snapshot is null");
        referenced_ids.insert(snapshot->snapshot_id);
      }
    } else {
      referenced_ids.insert(ref->snapshot_id);
    }
  }

  std::unordered_set<int64_t> ids_to_retain;
  for (const auto& snapshot : base().snapshots) {
    ICEBERG_DCHECK(snapshot != nullptr, "Snapshot is null");
    if (!referenced_ids.contains(snapshot->snapshot_id) &&
        snapshot->timestamp_ms > default_expire_older_than_) {
      // unreferenced and not old enough to be expired
      ids_to_retain.insert(snapshot->snapshot_id);
    }
  }
  return ids_to_retain;
}

Result<ExpireSnapshots::SnapshotToRef> ExpireSnapshots::ComputeRetainedRefs(
    const SnapshotToRef& refs) const {
  const TableMetadata& base = this->base();
  SnapshotToRef retained_refs;

  for (const auto& [key, ref] : refs) {
    if (key == SnapshotRef::kMainBranch) {
      retained_refs[key] = ref;
      continue;
    }

    std::shared_ptr<Snapshot> snapshot;
    if (auto result = base.SnapshotById(ref->snapshot_id); result.has_value()) {
      snapshot = std::move(result.value());
    } else if (result.error().kind != ErrorKind::kNotFound) {
      ICEBERG_RETURN_UNEXPECTED(result);
    }

    auto max_ref_ags_ms = ref->max_ref_age_ms().value_or(default_max_ref_age_ms_);
    if (snapshot != nullptr) {
      if (current_time_ms_ - snapshot->timestamp_ms <=
          std::chrono::milliseconds(max_ref_ags_ms)) {
        retained_refs[key] = ref;
      }
    } else {
      // Removing invalid refs that point to non-existing snapshot
    }
  }

  return retained_refs;
}

Result<ExpireSnapshots::ApplyResult> ExpireSnapshots::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  const TableMetadata& base = this->base();
  // Attempt to clean expired metadata even if there are no snapshots to expire.
  // Table metadata builder takes care of the case when this should actually be a no-op
  if (base.snapshots.empty() && !clean_expired_metadata_) {
    return {};
  }

  std::unordered_set<int64_t> ids_to_retain;
  ICEBERG_ASSIGN_OR_RAISE(auto retained_refs, ComputeRetainedRefs(base.refs));
  std::unordered_map<int64_t, std::vector<std::string>> retained_id_to_refs;
  for (const auto& [key, ref] : retained_refs) {
    int64_t snapshot_id = ref->snapshot_id;
    retained_id_to_refs.try_emplace(snapshot_id, std::vector<std::string>{});
    retained_id_to_refs[snapshot_id].push_back(key);
    ids_to_retain.insert(snapshot_id);
  }

  for (int64_t id : snapshot_ids_to_expire_) {
    ICEBERG_PRECHECK(!retained_id_to_refs.contains(id),
                     "Cannot expire {}. Still referenced by refs", id);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto all_branch_snapshot_ids,
                          ComputeAllBranchSnapshotIdsToRetain(retained_refs));
  ICEBERG_ASSIGN_OR_RAISE(auto unreferenced_snapshot_ids,
                          UnreferencedSnapshotIdsToRetain(retained_refs));
  ids_to_retain.insert(all_branch_snapshot_ids.begin(), all_branch_snapshot_ids.end());
  ids_to_retain.insert(unreferenced_snapshot_ids.begin(),
                       unreferenced_snapshot_ids.end());

  ApplyResult result;
  result.metadata_before_expiration = std::make_shared<TableMetadata>(base);

  std::ranges::for_each(base.refs, [&retained_refs, &result](const auto& key_to_ref) {
    if (!retained_refs.contains(key_to_ref.first)) {
      result.refs_to_remove.push_back(key_to_ref.first);
    }
  });
  std::ranges::for_each(base.snapshots, [&ids_to_retain, &result](const auto& snapshot) {
    if (snapshot && !ids_to_retain.contains(snapshot->snapshot_id)) {
      result.snapshot_ids_to_remove.push_back(snapshot->snapshot_id);
    }
  });

  if (clean_expired_metadata_) {
    std::unordered_set<int32_t> reachable_specs = {base.default_spec_id};
    std::unordered_set<int32_t> reachable_schemas = {base.current_schema_id};

    // TODO(xiao.dong) parallel processing
    for (int64_t snapshot_id : ids_to_retain) {
      ICEBERG_ASSIGN_OR_RAISE(auto snapshot, base.SnapshotById(snapshot_id));
      SnapshotCache snapshot_cache(snapshot.get());
      ICEBERG_ASSIGN_OR_RAISE(auto manifests,
                              snapshot_cache.Manifests(ctx_->table->io()));
      for (const auto& manifest : manifests) {
        reachable_specs.insert(manifest.partition_spec_id);
      }
      if (snapshot->schema_id.has_value()) {
        reachable_schemas.insert(snapshot->schema_id.value());
      }
    }

    std::ranges::for_each(
        base.partition_specs, [&reachable_specs, &result](const auto& spec) {
          if (!reachable_specs.contains(spec->spec_id())) {
            result.partition_spec_ids_to_remove.emplace_back(spec->spec_id());
          }
        });
    std::ranges::for_each(base.schemas,
                          [&reachable_schemas, &result](const auto& schema) {
                            if (!reachable_schemas.contains(schema->schema_id())) {
                              result.schema_ids_to_remove.insert(schema->schema_id());
                            }
                          });
  }

  // Cache the result for use during Finalize()
  apply_result_ = result;

  return result;
}

Status ExpireSnapshots::Finalize(Result<const TableMetadata*> commit_result) {
  if (!commit_result.has_value()) {
    return {};
  }

  if (cleanup_level_ == CleanupLevel::kNone) {
    return {};
  }

  if (!apply_result_.has_value() || apply_result_->snapshot_ids_to_remove.empty()) {
    return {};
  }

  ICEBERG_PRECHECK(apply_result_->metadata_before_expiration != nullptr,
                   "Missing pre-expiration table metadata for cleanup");
  ICEBERG_PRECHECK(commit_result.value() != nullptr,
                   "Missing committed table metadata for cleanup");
  auto metadata_before_expiration_ptr = apply_result_->metadata_before_expiration;
  const TableMetadata& metadata_before_expiration = *metadata_before_expiration_ptr;
  const TableMetadata& metadata_after_expiration = *commit_result.value();
  std::unordered_set<int64_t> expired_ids(apply_result_->snapshot_ids_to_remove.begin(),
                                          apply_result_->snapshot_ids_to_remove.end());
  apply_result_.reset();

  // File cleanup is best-effort: log and continue on individual file deletion failures
  ReachableFileCleanup strategy(ctx_->table->io(), delete_func_);
  return strategy.CleanFiles(metadata_before_expiration, metadata_after_expiration,
                             expired_ids, cleanup_level_);
}

// TODO(shangxinli): add IncrementalFileCleanup strategy for linear ancestry optimization.

}  // namespace iceberg
