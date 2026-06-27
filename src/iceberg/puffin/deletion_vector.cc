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

#include "iceberg/puffin/deletion_vector.h"

#include <zlib.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "iceberg/metadata_columns.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

namespace iceberg::puffin {

namespace {

// Computes a CRC-32 checksum (zlib/IEEE polynomial) over the given bytes.
uint32_t ComputeCrc32(std::span<const uint8_t> bytes) {
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, reinterpret_cast<const Bytef*>(bytes.data()),
              static_cast<uInt>(bytes.size()));
  return static_cast<uint32_t>(crc);
}

// Writes a 4-byte big-endian integer to the buffer.
template <typename T>
void WriteBigEndian(T value, uint8_t* buf) {
  T be = ToBigEndian(value);
  std::memcpy(buf, &be, sizeof(be));
}

// Reads a 4-byte big-endian integer from the buffer.
template <typename T>
T ReadBigEndian(const uint8_t* buf) {
  T value;
  std::memcpy(&value, buf, sizeof(value));
  return FromBigEndian(value);
}

}  // namespace

Result<std::vector<uint8_t>> SerializeDeletionVectorBlob(
    const RoaringPositionBitmap& bitmap) {
  ICEBERG_ASSIGN_OR_RAISE(auto serialized, bitmap.Serialize());

  // The length prefix and CRC both cover the magic sequence plus the vector.
  const size_t magic_and_vector_size =
      static_cast<size_t>(DeletionVectorBlob::kMagicBytes) + serialized.size();
  ICEBERG_PRECHECK(
      magic_and_vector_size <= static_cast<size_t>(std::numeric_limits<int32_t>::max()),
      "Deletion vector is too large to serialize: {} bytes", magic_and_vector_size);

  std::vector<uint8_t> blob(static_cast<size_t>(DeletionVectorBlob::kLengthPrefixBytes) +
                            magic_and_vector_size +
                            static_cast<size_t>(DeletionVectorBlob::kCrcBytes));
  uint8_t* buf = blob.data();

  WriteBigEndian(static_cast<int32_t>(magic_and_vector_size), buf);
  buf += DeletionVectorBlob::kLengthPrefixBytes;

  uint8_t* checksum_begin = buf;
  std::memcpy(buf, DeletionVectorBlob::kMagic.data(), DeletionVectorBlob::kMagicBytes);
  buf += DeletionVectorBlob::kMagicBytes;
  std::memcpy(buf, serialized.data(), serialized.size());
  buf += serialized.size();

  WriteBigEndian(
      ComputeCrc32(std::span<const uint8_t>(checksum_begin, magic_and_vector_size)), buf);
  return blob;
}

Result<RoaringPositionBitmap> DeserializeDeletionVectorBlob(
    std::span<const uint8_t> blob) {
  constexpr size_t kMinSize =
      static_cast<size_t>(DeletionVectorBlob::kLengthPrefixBytes) +
      static_cast<size_t>(DeletionVectorBlob::kMagicBytes) +
      static_cast<size_t>(DeletionVectorBlob::kCrcBytes);
  ICEBERG_PRECHECK(blob.size() >= kMinSize,
                   "Deletion vector blob too small: {} bytes, need at least {}",
                   blob.size(), kMinSize);

  const uint8_t* buf = blob.data();

  const auto length = ReadBigEndian<int32_t>(buf);
  buf += DeletionVectorBlob::kLengthPrefixBytes;

  ICEBERG_PRECHECK(length >= DeletionVectorBlob::kMagicBytes,
                   "Invalid deletion vector length prefix: {}", length);

  const size_t expected_total =
      static_cast<size_t>(DeletionVectorBlob::kLengthPrefixBytes) +
      static_cast<size_t>(length) + static_cast<size_t>(DeletionVectorBlob::kCrcBytes);
  ICEBERG_PRECHECK(blob.size() == expected_total,
                   "Deletion vector blob size mismatch: {} bytes, expected {}",
                   blob.size(), expected_total);

  // Magic and vector are checksummed together by the trailing CRC.
  const uint8_t* checksum_begin = buf;

  for (size_t i = 0; i < DeletionVectorBlob::kMagic.size(); ++i) {
    ICEBERG_PRECHECK(buf[i] == DeletionVectorBlob::kMagic[i],
                     "Invalid deletion vector magic byte at offset {}: got {:#04x}", i,
                     buf[i]);
  }
  buf += DeletionVectorBlob::kMagicBytes;

  const auto stored_crc =
      ReadBigEndian<uint32_t>(checksum_begin + static_cast<size_t>(length));
  const uint32_t actual_crc =
      ComputeCrc32(std::span<const uint8_t>(checksum_begin, static_cast<size_t>(length)));
  ICEBERG_PRECHECK(stored_crc == actual_crc,
                   "Deletion vector CRC mismatch: stored {:#010x}, computed {:#010x}",
                   stored_crc, actual_crc);

  const auto vector_size = static_cast<size_t>(length) - DeletionVectorBlob::kMagicBytes;
  std::string_view vector_bytes(reinterpret_cast<const char*>(buf), vector_size);
  return RoaringPositionBitmap::Deserialize(vector_bytes);
}

Result<Blob> MakeDeletionVectorBlob(const RoaringPositionBitmap& bitmap,
                                    std::string referenced_data_file) {
  ICEBERG_PRECHECK(!referenced_data_file.empty(),
                   "Deletion vector requires a non-empty referenced data file");

  ICEBERG_ASSIGN_OR_RAISE(auto data, SerializeDeletionVectorBlob(bitmap));

  Blob blob{
      .type = std::string(StandardBlobTypes::kDeletionVectorV1),
      // The deletion vector is computed over row positions, matching the Java
      // implementation which records the row-position metadata column field id.
      .input_fields = {MetadataColumns::kFilePositionColumnId},
      // Snapshot ID and sequence number are not known when the Puffin file is
      // created; the spec requires -1 for Puffin v1.
      .snapshot_id = -1,
      .sequence_number = -1,
      .data = std::move(data),
      .requested_compression = PuffinCompressionCodec::kNone,
  };
  blob.properties.emplace(
      std::string(StandardDeletionVectorProperties::kReferencedDataFile),
      std::move(referenced_data_file));
  blob.properties.emplace(std::string(StandardDeletionVectorProperties::kCardinality),
                          std::format("{}", bitmap.Cardinality()));

  return blob;
}

}  // namespace iceberg::puffin
