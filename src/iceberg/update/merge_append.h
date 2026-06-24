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

/// \file iceberg/update/merge_append.h

#include <memory>
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/merging_snapshot_update.h"

namespace iceberg {

/// \brief API for appending new files in a table while merging manifests.
///
/// This API accumulates file additions, produces a new Snapshot of the table,
/// and commits that snapshot as current. When committing, these changes are
/// applied to the latest table snapshot. Commit conflicts are resolved by
/// applying the changes to the new latest snapshot and reattempting the commit.
///
/// MergeAppend uses the shared MergingSnapshotUpdate pipeline, so it can merge
/// newly written and existing manifests according to table properties.
class ICEBERG_EXPORT MergeAppend : public MergingSnapshotUpdate {
 public:
  /// \brief Create a new MergeAppend instance.
  ///
  /// \param table_name The name of the table
  /// \param ctx The transaction context to use for this update
  /// \return A Result containing the MergeAppend instance or an error
  static Result<std::unique_ptr<MergeAppend>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  /// \brief Append a DataFile to the table.
  ///
  /// \param file A data file.
  /// \return This MergeAppend for method chaining.
  MergeAppend& AppendFile(const std::shared_ptr<DataFile>& file);

  /// \brief Append a ManifestFile to the table.
  ///
  /// The manifest must contain only appended files. All files in the manifest
  /// are appended to the table in the snapshot created by this update.
  /// Snapshot ID and sequence number assignment happen during commit.
  ///
  /// \param manifest A manifest file of files to append.
  /// \return This MergeAppend for method chaining.
  MergeAppend& AppendManifest(const ManifestFile& manifest);

  std::string operation() override;

 private:
  explicit MergeAppend(std::string table_name, std::shared_ptr<TransactionContext> ctx);
};

}  // namespace iceberg
