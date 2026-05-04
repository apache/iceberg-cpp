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

/// \file iceberg/puffin/file_metadata.h
/// Data structures for Puffin files.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"

namespace iceberg::puffin {

/// \brief Compression codecs supported by Puffin files.
enum class PuffinCompressionCodec {
  kNone,
  kLz4,
  kZstd,
};

ICEBERG_DATA_EXPORT std::string_view CodecName(PuffinCompressionCodec codec);

ICEBERG_DATA_EXPORT Result<PuffinCompressionCodec> PuffinCompressionCodecFromName(
    std::string_view codec_name);

ICEBERG_DATA_EXPORT std::string ToString(PuffinCompressionCodec codec);

/// \brief Standard blob types defined by the Iceberg specification.
struct StandardBlobTypes {
  /// A serialized form of a "compact" Theta sketch produced by the
  /// Apache DataSketches library.
  static constexpr std::string_view kApacheDatasketchesThetaV1 =
      "apache-datasketches-theta-v1";

  /// A serialized deletion vector according to the Iceberg spec.
  static constexpr std::string_view kDeletionVectorV1 = "deletion-vector-v1";
};

/// \brief Standard file-level properties for Puffin files.
struct StandardPuffinProperties {
  /// Human-readable identification of the application writing the file,
  /// along with its version.
  static constexpr std::string_view kCreatedBy = "created-by";
};

/// \brief A blob in a Puffin file.
struct ICEBERG_DATA_EXPORT Blob {
  /// See StandardBlobTypes for known types.
  std::string type;
  /// Ordered list of field IDs the blob was computed from.
  std::vector<int32_t> input_fields;
  /// ID of the Iceberg table's snapshot the blob was computed from.
  int64_t snapshot_id;
  /// Sequence number of the Iceberg table's snapshot the blob was computed from.
  int64_t sequence_number;
  std::vector<uint8_t> data;
  /// If not set, the writer's default codec will be used.
  std::optional<PuffinCompressionCodec> requested_compression;
  std::unordered_map<std::string, std::string> properties;

  friend bool operator==(const Blob& lhs, const Blob& rhs) = default;
};

ICEBERG_DATA_EXPORT std::string ToString(const Blob& blob);

/// \brief Metadata about a blob stored in a Puffin file footer.
struct ICEBERG_DATA_EXPORT BlobMetadata {
  /// See StandardBlobTypes for known types.
  std::string type;
  /// Ordered list of field IDs the blob was computed from.
  std::vector<int32_t> input_fields;
  /// ID of the Iceberg table's snapshot the blob was computed from.
  int64_t snapshot_id;
  /// Sequence number of the Iceberg table's snapshot the blob was computed from.
  int64_t sequence_number;
  int64_t offset;
  int64_t length;
  /// Codec name (e.g. "lz4", "zstd"), or empty if not compressed.
  std::string compression_codec;
  std::unordered_map<std::string, std::string> properties;

  friend bool operator==(const BlobMetadata& lhs, const BlobMetadata& rhs) = default;
};

ICEBERG_DATA_EXPORT std::string ToString(const BlobMetadata& blob_metadata);

/// \brief Metadata about a Puffin file.
struct ICEBERG_DATA_EXPORT FileMetadata {
  std::vector<BlobMetadata> blobs;
  std::unordered_map<std::string, std::string> properties;

  friend bool operator==(const FileMetadata& lhs, const FileMetadata& rhs) = default;
};

ICEBERG_DATA_EXPORT std::string ToString(const FileMetadata& file_metadata);

}  // namespace iceberg::puffin
