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

/// \file iceberg/manifest/manifest_util_internal.h
/// Internal utility functions for manifest operations.

#include <cstdint>
#include <memory>
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Copy an append manifest with a new snapshot ID.
///
/// This function copies a manifest file that contains only ADDED entries,
/// rewriting it with a new snapshot ID. This is similar to Java's
/// ManifestFiles.copyAppendManifest.
///
/// \param manifest The manifest file to copy
/// \param file_io File IO implementation to use
/// \param schema Table schema
/// \param spec Partition spec for the manifest
/// \param snapshot_id The new snapshot ID to assign to entries
/// \param output_path Path where the new manifest will be written
/// \param format_version Table format version
/// \param summary_builder Optional summary builder to update with file metrics
/// \return The copied manifest file, or an error
ICEBERG_EXPORT Result<ManifestFile> CopyAppendManifest(
    const ManifestFile& manifest, const std::shared_ptr<FileIO>& file_io,
    const std::shared_ptr<Schema>& schema, const std::shared_ptr<PartitionSpec>& spec,
    int64_t snapshot_id, const std::string& output_path, int8_t format_version,
    SnapshotSummaryBuilder* summary_builder = nullptr);

}  // namespace iceberg
