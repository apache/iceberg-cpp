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

/// \brief Appending new files in a table.
///
/// FastAppend is optimized for appending new data files to a table, it creates new
/// manifest files for the added data without compacting or rewriting existing manifests,
/// making it faster for write-heavy workloads.
class ICEBERG_EXPORT FastAppend : public SnapshotUpdate {
 public:
  /// \brief Create a new FastAppend instance.
  ///
  /// \param table_name The name of the table
  /// \param transaction The transaction to use for this update
  /// \return A Result containing the FastAppend instance or an error
  static Result<std::unique_ptr<FastAppend>> Make(
      std::string table_name, std::shared_ptr<Transaction> transaction);

  /// \brief Append a data file to this update.
  ///
  /// \param file The data file to append
  /// \return Reference to this for method chaining
  FastAppend& AppendFile(const std::shared_ptr<DataFile>& file);

  /// \brief Append a manifest file to this update.
  ///
  /// The manifest must only contain added files (no existing or deleted files).
  /// If the manifest doesn't have a snapshot ID assigned and snapshot ID inheritance
  /// is enabled, it will be used directly. Otherwise, it will be copied with the
  /// new snapshot ID.
  ///
  /// \param manifest The manifest file to append
  /// \return Reference to this for method chaining
  FastAppend& AppendManifest(const ManifestFile& manifest);

  std::string operation() override;

  Result<std::vector<ManifestFile>> Apply(
      const TableMetadata& metadata_to_update,
      const std::shared_ptr<Snapshot>& snapshot) override;
  std::unordered_map<std::string, std::string> Summary() override;
  void CleanUncommitted(const std::unordered_set<std::string>& committed) override;
  bool CleanupAfterCommit() const override;

 private:
  explicit FastAppend(std::string table_name, std::shared_ptr<Transaction> transaction);

  /// \brief Get the partition spec by spec ID.
  Result<std::shared_ptr<PartitionSpec>> Spec(int32_t spec_id);

  /// \brief Copy a manifest file with a new snapshot ID.
  ///
  /// \param manifest The manifest to copy
  /// \return The copied manifest file
  Result<ManifestFile> CopyManifest(const ManifestFile& manifest);

  /// \brief Write new manifests for the accumulated data files.
  ///
  /// \return A vector of manifest files, or an error
  Result<std::vector<ManifestFile>> WriteNewManifests();

 private:
  std::string table_name_;
  std::unordered_map<int32_t, DataFileSet> new_data_files_by_spec_;
  std::vector<ManifestFile> append_manifests_;
  std::vector<ManifestFile> rewritten_append_manifests_;
  std::vector<ManifestFile> new_manifests_;
  bool has_new_files_{false};
};

}  // namespace iceberg
