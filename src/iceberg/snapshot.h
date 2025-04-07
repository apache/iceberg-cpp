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

#include <optional>
#include <string>
#include <unordered_map>

#include "iceberg/iceberg_export.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

/// Optional Snapshot Summary Fields
/// Metrics
/// See https://iceberg.apache.org/spec/#metrics

/// \brief Number of data files added in the snapshot
constexpr std::string_view kAddedDataFilesKey = "added-data-files";
/// \brief Number of data files deleted in the snapshot
constexpr std::string_view kDeletedDataFilesKey = "deleted-data-files";
/// \brief Total number of live data files in the snapshot
constexpr std::string_view kTotalDataFilesKey = "total-data-files";
/// \brief Number of positional/equality delete files and deletion vectors added in the
/// snapshot
constexpr std::string_view kAddedDeleteFilesKey = "added-delete-files";
/// \brief Number of equality delete files added in the snapshot
constexpr std::string_view kAddedEqDeleteFilesKey = "added-equality-delete-files";
/// \brief Number of equality delete files removed in the snapshot
constexpr std::string_view kRemovedEqDeleteFilesKey = "removed-equality-delete-files";
/// \brief Number of position delete files added in the snapshot
constexpr std::string_view kAddedPosDeleteFilesKey = "added-position-delete-files";
/// \brief Number of position delete files removed in the snapshot
constexpr std::string_view kRemovedPosDeleteFilesKey = "removed-position-delete-files";
/// \brief Number of deletion vectors added in the snapshot
constexpr std::string_view kAddedDVSKey = "added-dvs";
/// \brief Number of deletion vectors removed in the snapshot
constexpr std::string_view kRemovedDVSKey = "removed-dvs";
/// \brief Number of positional/equality delete files and deletion vectors removed in the
/// snapshot
constexpr std::string_view kRemovedDeleteFilesKey = "removed-delete-files";
/// \brief Total number of live positional/equality delete files and deletion vectors in
/// the snapshot
constexpr std::string_view kTotalDeleteFilesKey = "total-delete-files";
/// \brief Number of records added in the snapshot
constexpr std::string_view kAddedRecordsKey = "added-records";
/// \brief Number of records deleted in the snapshot
constexpr std::string_view kDeletedRecordsKey = "deleted-records";
/// \brief Total number of records in the snapshot
constexpr std::string_view kTotalRecordsKey = "total-records";
/// \brief The size of files added in the snapshot
constexpr std::string_view kAddedFileSizeKey = "added-files-size";
/// \brief The size of files removed in the snapshot
constexpr std::string_view kRemovedFileSizeKey = "removed-files-size";
/// \brief Total size of live files in the snapshot
constexpr std::string_view kTotalFileSizeKey = "total-files-size";
/// \brief Number of position delete records added in the snapshot
constexpr std::string_view kAddedPosDeletesKey = "added-position-deletes";
/// \brief Number of position delete records removed in the snapshot
constexpr std::string_view kRemovedPosDeletesKey = "removed-position-deletes";
/// \brief Total number of position delete records in the snapshot
constexpr std::string_view kTotalPosDeletesKey = "total-position-deletes";
/// \brief Number of equality delete records added in the snapshot
constexpr std::string_view kAddedEqDeletesKey = "added-equality-deletes";
/// \brief Number of equality delete records removed in the snapshot
constexpr std::string_view kRemovedEqDeletesKey = "removed-equality-deletes";
/// \brief Total number of equality delete records in the snapshot
constexpr std::string_view kTotalEqDeletesKey = "total-equality-deletes";
/// \brief Number of duplicate files deleted (duplicates are files recorded more than once
/// in the manifest)
constexpr std::string_view kDeletedDuplicatedFilesKey = "deleted-duplicate-files";
/// \brief Number of partitions with files added or removed in the snapshot
constexpr std::string_view kChangedPartitionCountProp = "changed-partition-count";

/// Other Fields
/// See https://iceberg.apache.org/spec/#other-fields

/// \brief The Write-Audit-Publish id of a staged snapshot
constexpr std::string_view kWAPIDKey = "wap.id";
/// \brief The Write-Audit-Publish id of a snapshot already been published
constexpr std::string_view kPublishedWAPIDKey = "published-wap-id";
/// \brief The original id of a cherry-picked snapshot
constexpr std::string_view kSourceSnapshotIDKey = "source-snapshot-id";
/// \brief Name of the engine that created the snapshot
constexpr std::string_view kEngineNameKey = "engine-name";
/// \brief Version of the engine that created the snapshot
constexpr std::string_view kEngineVersionKey = "engine-version";

/// \brief The operation field is used by some operations, like snapshot expiration, to
/// skip processing certain snapshots.
enum class Operation {
  /// Only data files were added and no files were removed.
  kAppend,
  /// Data and delete files were added and removed without changing table data; i.e.
  /// compaction, change the data file format, or relocating data files.
  kReplace,
  /// Data and delete files were added and removed in a logical overwrite operation.
  kOverwrite,
  /// Data files were removed and their contents logically deleted and/or delete files
  /// were added to delete rows.
  kDelete,
};

/// \brief Summarises the changes in the snapshot.
class ICEBERG_EXPORT Summary : public iceberg::util::Formattable {
 public:
  Summary() = default;
  /// \brief Construct a summary with the given operation and properties.
  Summary(Operation op, std::unordered_map<std::string, std::string> props);

  /// \brief Get the operation type of the snapshot.
  [[nodiscard]] Operation operation() const;

  /// \brief Get the additional properties of the snapshot.
  [[nodiscard]] const std::unordered_map<std::string, std::string>& properties() const;

  std::string ToString() const override;

  friend bool operator==(const Summary& lhs, const Summary& rhs) {
    return lhs.Equals(rhs);
  }

  friend bool operator!=(const Summary& lhs, const Summary& rhs) { return !(lhs == rhs); }

 private:
  /// \brief Compare two Summaries for equality.
  [[nodiscard]] bool Equals(const Summary& other) const;

  /// The type of operation in the snapshot
  Operation operation_{Operation::kAppend};
  /// Other summary data.
  std::unordered_map<std::string, std::string> additional_properties_;
};

/// \brief A snapshot of the data in a table at a point in time.
///
/// A snapshot consist of one or more file manifests, and the complete table contents is
/// the union of all the data files in those manifests.
///
/// Snapshots are created by table operations.
class ICEBERG_EXPORT Snapshot : public iceberg::util::Formattable {
 public:
  Snapshot(int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
           int64_t sequence_number, int64_t timestamp_ms, std::string manifest_list,
           std::shared_ptr<Summary> summary, std::optional<int64_t> schema_id);

  /// \brief Get the id of the snapshot.
  [[nodiscard]] int64_t snapshot_id() const;

  /// \brief Get parent snapshot id.
  [[nodiscard]] std::optional<int64_t> parent_snapshot_id() const;

  /// \brief Get the sequence number of the snapshot.
  [[nodiscard]] int64_t sequence_number() const;

  /// \brief Get the timestamp of the snapshot.
  [[nodiscard]] int64_t timestamp_ms() const;

  /// \brief Get the manifest list of the snapshot.
  [[nodiscard]] const std::string& manifest_list() const;

  /// \brief Get the summary of the snapshot.
  [[nodiscard]] const std::shared_ptr<Summary>& summary() const;

  /// \brief Get the schema ID of the snapshot.
  [[nodiscard]] std::optional<int32_t> schema_id() const;

  std::string ToString() const override;

  friend bool operator==(const Snapshot& lhs, const Snapshot& rhs) {
    return lhs.Equals(rhs);
  }

  friend bool operator!=(const Snapshot& lhs, const Snapshot& rhs) {
    return !(lhs == rhs);
  }

 private:
  /// \brief Compare two snapshots for equality.
  [[nodiscard]] bool Equals(const Snapshot& other) const;

  /// A unqiue long ID.
  int64_t snapshot_id_;
  /// The snapshot ID of the snapshot's parent. Omitted for any snapshot with no parent.
  std::optional<int64_t> parent_snapshot_id_;
  /// A monotonically increasing long that tracks the order of changes to a table.
  int64_t sequence_number_;
  /// A timestamp when the snapshot was created, used for garbage collection and table
  /// inspection.
  int64_t timestamp_ms_;
  /// The location of a manifest list for this snapshot that tracks manifest files with
  /// additional metadata.
  std::string manifest_list_;
  /// A string map that summaries the snapshot changes, including operation.
  std::shared_ptr<Summary> summary_;
  /// ID of the table's current schema when the snapshot was created.
  std::optional<int32_t> schema_id_;
};

}  // namespace iceberg
