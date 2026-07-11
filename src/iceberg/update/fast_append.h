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

/// \file iceberg/update/fast_append.h

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/snapshot_update.h"
#include "iceberg/util/data_file_set.h"

namespace iceberg {

/// \brief API for appending new files in a table.
///
/// This API accumulates file additions, produces a new Snapshot of the table,
/// and commits that snapshot as current. When committing, these changes are
/// applied to the latest table snapshot. Commit conflicts are resolved by
/// applying the changes to the new latest snapshot and reattempting the commit.
///
/// FastAppend is optimized for appending new data files to a table. It creates
/// new manifest files for the added data without compacting or rewriting
/// existing manifests.
class ICEBERG_EXPORT FastAppend : public SnapshotUpdate {
 public:
  /// \brief Create a new FastAppend instance.
  ///
  /// \param table_name The name of the table
  /// \param ctx The transaction context to use for this update
  /// \return A Result containing the FastAppend instance or an error
  static Result<std::unique_ptr<FastAppend>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  /// \brief Append a DataFile to the table.
  ///
  /// \param file A data file.
  /// \return This FastAppend for method chaining.
  FastAppend& AppendFile(const std::shared_ptr<DataFile>& file);

  /// \brief Append a ManifestFile to the table.
  ///
  /// The manifest must contain only appended files. All files in the manifest
  /// are appended to the table in the snapshot created by this update.
  ///
  /// If the manifest doesn't have a snapshot ID assigned and snapshot ID
  /// inheritance is enabled, it will be used directly. Otherwise, it will be
  /// copied with the new snapshot ID.
  ///
  /// \param manifest A manifest file of files to append.
  /// \return This FastAppend for method chaining.
  FastAppend& AppendManifest(const ManifestFile& manifest);

  std::string operation() override;

  Result<std::vector<ManifestFile>> Apply(
      const TableMetadata& metadata_to_update,
      const std::shared_ptr<Snapshot>& snapshot) override;
  std::unordered_map<std::string, std::string> Summary() override;
  void SetSummaryProperty(const std::string& property, const std::string& value) override;
  Status CleanUncommitted(const std::unordered_set<std::string>& committed) override;
  bool CleanupAfterCommit() const override;

 private:
  explicit FastAppend(std::string table_name, std::shared_ptr<TransactionContext> ctx);

  /// \brief Get the partition spec by spec ID.
  Result<std::shared_ptr<PartitionSpec>> Spec(int32_t spec_id);

  /// \brief Copy a manifest file with a new snapshot ID.
  ///
  /// \param manifest The manifest to copy
  /// \param update_summary Whether to add copied entries to the append summary
  /// \return The copied manifest file
  Result<ManifestFile> CopyManifest(const ManifestFile& manifest, bool update_summary);

  /// \brief Write new manifests for the accumulated data files.
  ///
  /// \return A vector of manifest files, or an error
  Result<std::vector<ManifestFile>> WriteNewManifests();

 private:
  std::string table_name_;
  std::unordered_map<int32_t, DataFileSet> new_data_files_by_spec_;
  // Stable input summaries for retry-safe summary_ rebuilds.
  SnapshotSummaryBuilder added_data_files_summary_;
  SnapshotSummaryBuilder appended_manifests_summary_;
  // User-provided summary properties restored after summary_ rebuilds.
  std::unordered_map<std::string, std::string> custom_summary_properties_;
  std::vector<ManifestFile> append_manifests_;
  // Original manifests kept to recreate copied manifests after retry cleanup.
  std::vector<ManifestFile> append_manifests_to_copy_;
  std::vector<ManifestFile> rewritten_append_manifests_;
  std::vector<ManifestFile> new_manifests_;
  // Manifest count summary from the latest Apply() result.
  SnapshotSummaryBuilder manifest_count_summary_;
  bool has_new_files_{false};
};

}  // namespace iceberg
