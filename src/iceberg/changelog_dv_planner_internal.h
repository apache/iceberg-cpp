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

#pragma once

/// \file iceberg/changelog_dv_planner_internal.h
/// Deletion vector handling for changelog scans of format version 3 tables.

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/manifest/manifest_list.h"
#include "iceberg/result.h"
#include "iceberg/table_scan.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Plans the delete side of a changelog scan from deletion vectors.
///
/// Format version 3 stores row-level deletes as deletion vectors, with at most one live
/// vector per data file, and a new vector replaces the previous one. The rows a snapshot
/// deleted are therefore the difference between the vector it added and the vector it
/// removed, which DeletedRowsScanTask carries as added and existing deletes. Position
/// delete files and equality delete files accumulate instead, so attributing their rows
/// to a snapshot would require reading every earlier delete file; they are rejected
/// wherever they could still apply to a reported row, whether or not a changelog
/// snapshot wrote them.
class DeletionVectorChangelogPlanner {
 public:
  /// \brief Index the deletion vectors added and removed by the changelog snapshots.
  ///
  /// \param context Scan context that supplies filtering and planning options.
  /// \param scan_columns Manifest columns to read for data files.
  /// \param filter Row filter of the scan.
  /// \param delete_manifests All delete manifests of the changelog snapshots. Only the
  /// manifests written by those snapshots can hold the vectors, because removing a
  /// vector rewrites its manifest, but a manifest untouched during the range can still
  /// carry position delete files or equality delete files that apply to the reported
  /// rows. Every manifest is therefore checked, and only entries written by the
  /// changelog snapshots are indexed.
  /// \param snapshot_ids IDs of the changelog snapshots.
  static Result<std::unique_ptr<DeletionVectorChangelogPlanner>> Make(
      std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
      std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
      const internal::TableScanContext& context, std::vector<std::string> scan_columns,
      std::shared_ptr<Expression> filter,
      const std::vector<ManifestFile>& delete_manifests,
      const std::unordered_set<int64_t>& snapshot_ids);

  /// \brief The deletion vector that the snapshot committed for the data file, if any.
  std::vector<std::shared_ptr<DataFile>> AddedDeletes(
      int64_t snapshot_id, const std::string& data_file_path) const;

  /// \brief The deletion vector that the snapshot removed from the data file, if any.
  std::vector<std::shared_ptr<DataFile>> RemovedDeletes(
      int64_t snapshot_id, const std::string& data_file_path) const;

  /// \brief Plan DeletedRowsScanTasks for the deletion vectors a snapshot committed
  /// against data files it did not add.
  ///
  /// \param snapshot_id The changelog snapshot.
  /// \param change_ordinal Position of the snapshot in the changelog order.
  /// \param data_manifests All data manifests of the snapshot. The referenced data files
  /// are located there because an earlier snapshot added them and a later snapshot in the
  /// range may remove them again.
  /// \param added_data_file_paths Data files added by the snapshot, whose deletion
  /// vectors belong to their AddedRowsScanTask instead.
  Result<std::vector<std::shared_ptr<ChangelogScanTask>>> PlanDeletedRows(
      int64_t snapshot_id, int32_t change_ordinal, std::span<ManifestFile> data_manifests,
      const std::unordered_set<std::string>& added_data_file_paths) const;

 private:
  using DeletionVectorsByPath =
      std::unordered_map<std::string, std::shared_ptr<DataFile>>;

  // Deletion vectors committed and removed by one changelog snapshot, keyed by the path
  // of the data file they reference.
  struct SnapshotDeletionVectors {
    DeletionVectorsByPath added;
    DeletionVectorsByPath removed;
  };

  DeletionVectorChangelogPlanner(
      std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
      std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
      internal::TableScanContext context, std::vector<std::string> scan_columns,
      std::shared_ptr<Expression> filter,
      std::unordered_map<int64_t, SnapshotDeletionVectors> dvs_by_snapshot);

  std::shared_ptr<FileIO> io_;
  std::shared_ptr<Schema> schema_;
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id_;
  internal::TableScanContext context_;
  std::vector<std::string> scan_columns_;
  std::shared_ptr<Expression> filter_;
  std::unordered_map<int64_t, SnapshotDeletionVectors> dvs_by_snapshot_;
};

}  // namespace iceberg
