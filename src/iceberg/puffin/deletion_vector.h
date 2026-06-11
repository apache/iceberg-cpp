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

/// \file iceberg/puffin/deletion_vector.h
/// Serialization helpers for the `deletion-vector-v1` Puffin blob type.

#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/deletes/roaring_position_bitmap.h"
#include "iceberg/iceberg_data_export.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/result.h"

namespace iceberg::puffin {

/// \brief Required blob properties for the `deletion-vector-v1` blob type.
struct StandardDeletionVectorProperties {
  /// Location of the data file the deletion vector applies to.
  static constexpr std::string_view kReferencedDataFile = "referenced-data-file";
  /// Number of deleted rows (set positions) in the deletion vector.
  static constexpr std::string_view kCardinality = "cardinality";
};

/// \brief Constants describing the `deletion-vector-v1` blob framing.
///
/// See the Puffin spec for the byte layout:
/// https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type
struct ICEBERG_DATA_EXPORT DeletionVectorBlob {
  /// Magic sequence preceding the serialized bitmap: 0xD1 0xD3 0x39 0x64.
  static constexpr std::array<uint8_t, 4> kMagic = {0xD1, 0xD3, 0x39, 0x64};

  /// Length of the big-endian length prefix, in bytes.
  static constexpr int32_t kLengthPrefixBytes = 4;
  /// Length of the magic sequence, in bytes.
  static constexpr int32_t kMagicBytes = 4;
  /// Length of the trailing big-endian CRC-32 checksum, in bytes.
  static constexpr int32_t kCrcBytes = 4;
};

/// \brief Serialize a position bitmap into a `deletion-vector-v1` blob.
///
/// The returned bytes include the length prefix, magic sequence, the Roaring
/// "portable" serialization of the bitmap, and the trailing CRC-32 checksum.
ICEBERG_DATA_EXPORT Result<std::vector<uint8_t>> SerializeDeletionVectorBlob(
    const RoaringPositionBitmap& bitmap);

/// \brief Deserialize a `deletion-vector-v1` blob into a position bitmap.
///
/// Validates the length prefix, magic sequence, and CRC-32 checksum before
/// decoding the bitmap.
ICEBERG_DATA_EXPORT Result<RoaringPositionBitmap> DeserializeDeletionVectorBlob(
    std::span<const uint8_t> blob);

/// \brief Build a Puffin `Blob` of type `deletion-vector-v1` from a position
/// bitmap.
///
/// Sets `snapshot-id` and `sequence-number` to -1 (required for Puffin v1),
/// requests no compression, and populates the required `referenced-data-file`
/// and `cardinality` properties.
///
/// \param bitmap The positions to delete.
/// \param referenced_data_file Location of the data file the vector applies to.
ICEBERG_DATA_EXPORT Result<Blob> MakeDeletionVectorBlob(
    const RoaringPositionBitmap& bitmap, std::string referenced_data_file);

}  // namespace iceberg::puffin
