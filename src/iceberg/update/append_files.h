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

#include <memory>

#include "iceberg/iceberg_export.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief API for appending new files in a table.
///
/// This API accumulates file additions, produces a new Snapshot of the table, and commits
/// that snapshot as the current.
///
/// When committing, these changes will be applied to the latest table snapshot. Commit
/// conflicts will be resolved by applying the changes to the new latest snapshot and
/// reattempting the commit.
class ICEBERG_EXPORT AppendFiles {
 public:
  virtual ~AppendFiles() = default;

  /// \brief Append a DataFile to the table.
  ///
  /// \param file A data file
  /// \return Reference to this for method chaining
  virtual AppendFiles& AppendFile(std::shared_ptr<DataFile> file) = 0;

  /// \brief Append a ManifestFile to the table.
  ///
  /// The manifest must contain only appended files. All files in the manifest will be
  /// appended to the table in the snapshot created by this update.
  ///
  /// The manifest will be used directly if snapshot ID inheritance is enabled (all tables
  /// with the format version > 1 or if the inheritance is enabled explicitly via table
  /// properties). Otherwise, the manifest will be rewritten to assign all entries this
  /// update's snapshot ID.
  ///
  /// If the manifest is rewritten, it is always the responsibility of the caller to
  /// manage the lifecycle of the original manifest. If the manifest is used directly, it
  /// should never be deleted manually if the commit succeeds as it will become part of
  /// the table metadata and will be cleaned upon expiry. If the manifest gets merged with
  /// others while preparing a new snapshot, it will be deleted automatically if this
  /// operation is successful. If the commit fails, the manifest will never be deleted,
  /// and it is up to the caller whether to delete or reuse it.
  ///
  /// \param file A manifest file
  /// \return Reference to this for method chaining
  virtual AppendFiles& AppendManifest(const ManifestFile& file) = 0;
};

}  // namespace iceberg
