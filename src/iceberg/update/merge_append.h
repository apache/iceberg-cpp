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

/// \brief Append files while merging manifests when table properties allow it.
///
/// MergeAppend uses the shared MergingSnapshotUpdate pipeline, so it can compact
/// newly written and existing manifests into fewer manifests during commit.
class ICEBERG_EXPORT MergeAppend : public MergingSnapshotUpdate {
 public:
  /// \brief Create a new MergeAppend instance.
  ///
  /// \param table_name The name of the table
  /// \param ctx The transaction context to use for this update
  /// \return A Result containing the MergeAppend instance or an error
  static Result<std::unique_ptr<MergeAppend>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  /// \brief Append a data file to this update.
  ///
  /// \param file The data file to append
  /// \return Reference to this for method chaining
  MergeAppend& AppendFile(const std::shared_ptr<DataFile>& file);

  /// \brief Append a data manifest to this update.
  ///
  /// The manifest must contain only added files. Snapshot ID and sequence number
  /// assignment happen during commit.
  ///
  /// \param manifest The manifest file to append
  /// \return Reference to this for method chaining
  MergeAppend& AppendManifest(const ManifestFile& manifest);

  std::string operation() override;

 private:
  explicit MergeAppend(std::string table_name, std::shared_ptr<TransactionContext> ctx);
};

}  // namespace iceberg
