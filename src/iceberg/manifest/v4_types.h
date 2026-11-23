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

/// \file iceberg/manifest/v4_types.h
/// Foundational metadata types for the v4 manifest tree. These types do not enable
/// v4 table reads or writes. The v4 specification is still under development.
/// See https://iceberg.apache.org/spec/#manifest-schema.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Status of a tracked file in a v4 manifest.
/// Kept separate from ManifestStatus because v1-v3 manifests cannot contain
/// REPLACED or MODIFIED entries.
enum class FileTrackingStatus {
  kExisting = 0,
  kAdded = 1,
  kDeleted = 2,
  kReplaced = 3,
  kModified = 4,
};

ICEBERG_EXPORT Result<FileTrackingStatus> FileTrackingStatusFromInt(int32_t status);

/// \brief The v4 tracking struct, shared by data files and manifests.
struct ICEBERG_EXPORT FileTracking {
  /// Field id: 0. ADDED, EXISTING, and MODIFIED entries are live.
  FileTrackingStatus status = FileTrackingStatus::kAdded;
  /// Field id: 1. Snapshot where the file was added, replaced, or deleted.
  /// Inherited when null; writers should persist a non-null value.
  std::optional<int64_t> snapshot_id;
  /// Field id: 5. Snapshot where the file was last modified.
  std::optional<int64_t> modified_snapshot_id;
  /// Field id: 3. Data sequence number; inherited when null.
  std::optional<int64_t> sequence_number;
  /// Field id: 4. File sequence number; inherited when null.
  std::optional<int64_t> file_sequence_number;
  /// Field id: 142. Base row ID; subject to first row ID inheritance.
  std::optional<int64_t> first_row_id;
  /// Field id: 6. Manifest-only bitmap of positions deleted in modified_snapshot_id.
  std::optional<std::vector<uint8_t>> deleted_positions;
  /// Field id: 7. Manifest-only bitmap of positions replaced in modified_snapshot_id.
  std::optional<std::vector<uint8_t>> replaced_positions;

  constexpr bool IsAlive() const {
    return status == FileTrackingStatus::kExisting ||
           status == FileTrackingStatus::kAdded ||
           status == FileTrackingStatus::kModified;
  }

  static std::shared_ptr<StructType> Type();
};

/// \brief The v4 deletion_vector struct, colocated with a tracked data file.
/// This references a Puffin blob; it does not hold the bitmap itself.
struct ICEBERG_EXPORT DeletionVectorMetadata {
  /// Field id: 155. Location of the file containing the deletion vector.
  std::string location;
  /// Field id: 144. Must match the blob offset in the Puffin footer.
  int64_t offset = 0;
  /// Field id: 145. Must match the blob length in the Puffin footer.
  int64_t size_in_bytes = 0;
  /// Field id: 156. Number of deleted rows (set bits).
  int64_t cardinality = 0;
  /// Field id: 149. Encryption key metadata, absent for unencrypted blobs.
  std::optional<std::vector<uint8_t>> key_metadata;

  static std::shared_ptr<StructType> Type();
};

/// \brief The v4 manifest_info struct, present only for tracked manifests.
/// File counts include all five tracking statuses and sum to the manifest's
/// record_count. This data carrier does not validate tracked-file requirements.
struct ICEBERG_EXPORT ManifestInfo {
  /// Field id: 521. 0 denotes a pre-v4 manifest; 4 denotes a v4 manifest.
  int32_t format_version = 0;
  /// Field id: 522. Bitmap of positions that are no longer live in a leaf manifest.
  std::optional<std::vector<uint8_t>> dv;
  /// Field id: 504.
  int32_t added_files_count = 0;
  /// Field id: 505.
  int32_t existing_files_count = 0;
  /// Field id: 525.
  int32_t modified_files_count = 0;
  /// Field id: 506.
  int32_t deleted_files_count = 0;
  /// Field id: 523.
  int32_t replaced_files_count = 0;
  /// Field id: 512.
  int64_t added_rows_count = 0;
  /// Field id: 513.
  int64_t existing_rows_count = 0;
  /// Field id: 526.
  int64_t modified_rows_count = 0;
  /// Field id: 514.
  int64_t deleted_rows_count = 0;
  /// Field id: 524.
  int64_t replaced_rows_count = 0;
  /// Field id: 516. Minimum data sequence number of live entries.
  int64_t min_sequence_number = 0;

  static std::shared_ptr<StructType> Type();
};

}  // namespace iceberg
