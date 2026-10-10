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
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/manifest/manifest_list.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/executor.h"
#include "iceberg/util/partition_value_util.h"

namespace iceberg {

/// \brief Plans the delete side of a changelog scan from deletion vectors.
///
/// Format version 3 stores row-level deletes as deletion vectors, with at most one live
/// vector per data file, and a new vector replaces the previous one. The rows a snapshot
/// deleted are therefore the difference between the vector it added and the vector that
/// was live just before it, which DeletedRowsScanTask carries as added and existing
/// deletes. Snapshots are identified by sequence number, so the history of each vector is
/// the sequence number of the snapshot that added it and, if it was removed, of the
/// snapshot that removed it.
///
/// Position delete files and equality delete files accumulate instead, so attributing
/// their rows to a snapshot would require reading every earlier delete file. A data file
/// that the changelog reports is rejected when one of them applies to it.
class DeletionVectorChangelogPlanner {
 public:
  /// \brief Index the delete files that can apply to the rows of a changelog.
  ///
  /// \param io FileIO used to read the delete manifests.
  /// \param schema Schema of the scan.
  /// \param specs_by_id Partition specs of the table.
  /// \param filter Row filter of the scan, used to prune delete manifests and entries by
  /// partition. A data file outside the filter is never reported, so neither are the
  /// delete files of its partition.
  /// \param case_sensitive Whether the filter binds case-sensitively.
  /// \param executor Executor used to read the delete manifests.
  /// \param delete_manifests Delete manifests of every snapshot in the range, including
  /// replace snapshots. A snapshot's own manifests are the only ones guaranteed to hold
  /// the entries it removed, because later snapshots drop removed entries when they
  /// rewrite a manifest.
  /// \param sequence_numbers Sequence numbers of every snapshot in the range, keyed by
  /// snapshot ID. A delete file removed by any other snapshot was removed before the
  /// range and can no longer apply to a reported row.
  static Result<std::unique_ptr<DeletionVectorChangelogPlanner>> Make(
      std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
      const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs_by_id,
      std::shared_ptr<Expression> filter, bool case_sensitive, OptionalExecutor executor,
      const std::vector<ManifestFile>& delete_manifests,
      const std::unordered_map<int64_t, int64_t>& sequence_numbers);

  /// \brief The deletion vector that the snapshot with the given sequence number added
  /// for the data file, or nullptr.
  std::shared_ptr<DataFile> AddedDeletionVector(int64_t sequence_number,
                                                const std::string& data_file_path) const;

  /// \brief The deletion vector that applied to the data file just before the snapshot
  /// with the given sequence number, or nullptr.
  ///
  /// This is the vector the snapshot replaced or removed, or one it left behind when it
  /// removed the data file, so every row it deletes is excluded from the changelog.
  Result<std::shared_ptr<DataFile>> ExistingDeletionVector(
      int64_t sequence_number, const std::string& data_file_path) const;

  /// \brief Which of the snapshots with the given sequence numbers added a deletion
  /// vector, keyed by the path of the data file the vector references.
  std::unordered_map<std::string, std::vector<int64_t>> AddedDeletionVectors(
      const std::unordered_set<int64_t>& sequence_numbers) const;

  /// \brief Fail with NotSupported if a position delete file or an equality delete file
  /// applies to the data file.
  ///
  /// \param data_sequence_number Data sequence number of the data file.
  /// \param data_file The data file that the changelog reports.
  Status ValidateNoOtherDeletes(int64_t data_sequence_number,
                                const DataFile& data_file) const;

 private:
  // One deletion vector and the sequence numbers of the snapshots that added and
  // removed it.
  struct DeletionVectorVersion {
    std::shared_ptr<DataFile> file;
    int64_t added_sequence_number;
    std::optional<int64_t> removed_sequence_number;
  };

  // The delete file with the highest sequence number among those sharing a scope. A
  // delete file applies to a data file based on sequence numbers alone once the scope
  // matches, so the latest one decides whether any of them applies.
  struct LatestDeleteFile {
    int64_t sequence_number;
    std::string file_path;
  };

  DeletionVectorChangelogPlanner() = default;

  static std::optional<LatestDeleteFile>& LatestFor(
      PartitionMap<std::optional<LatestDeleteFile>>& deletes_by_partition,
      const DataFile& file);
  static void KeepLatest(std::optional<LatestDeleteFile>& latest, int64_t sequence_number,
                         const std::string& file_path);

  // Deletion vector history keyed by the path of the data file it references.
  std::unordered_map<std::string, std::vector<DeletionVectorVersion>> dvs_by_path_;

  std::optional<LatestDeleteFile> global_equality_deletes_;
  PartitionMap<std::optional<LatestDeleteFile>> equality_deletes_by_partition_;
  PartitionMap<std::optional<LatestDeleteFile>> position_deletes_by_partition_;
  std::unordered_map<std::string, std::optional<LatestDeleteFile>>
      position_deletes_by_path_;
};

}  // namespace iceberg
