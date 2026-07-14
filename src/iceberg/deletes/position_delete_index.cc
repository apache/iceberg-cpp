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

#include "iceberg/deletes/position_delete_index.h"

#include <zconf.h>
#include <zlib.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "iceberg/deletes/roaring_position_bitmap.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

// `deletion-vector-v1` blob framing. See:
// https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type
constexpr std::array<uint8_t, 4> kMagic = {0xD1, 0xD3, 0x39, 0x64};
constexpr int32_t kLengthPrefixBytes = 4;
constexpr int32_t kMagicBytes = 4;
constexpr int32_t kCrcBytes = 4;

uint32_t ComputeCrc32(std::span<const uint8_t> bytes) {
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, reinterpret_cast<const Bytef*>(bytes.data()),
              static_cast<uInt>(bytes.size()));
  return static_cast<uint32_t>(crc);
}

Result<int32_t> ReadBitmapDataLength(std::span<const uint8_t> blob,
                                     const DataFile& delete_file) {
  ICEBERG_PRECHECK(delete_file.content_size_in_bytes.has_value(),
                   "Deletion vector requires content_size_in_bytes: {}",
                   delete_file.file_path);
  ICEBERG_PRECHECK(
      std::cmp_equal(blob.size(), *delete_file.content_size_in_bytes),
      "Deletion vector blob size {} does not match content_size_in_bytes {}: {}",
      blob.size(), *delete_file.content_size_in_bytes, delete_file.file_path);
  ICEBERG_PRECHECK(blob.size() >= kLengthPrefixBytes + kMagicBytes + kCrcBytes,
                   "Deletion vector blob too small: {} bytes", blob.size());

  const auto length = ReadBigEndian<int32_t>(blob.data());
  const size_t expected_size =
      kLengthPrefixBytes + static_cast<size_t>(length) + kCrcBytes;
  ICEBERG_PRECHECK(length >= kMagicBytes, "Invalid deletion vector length prefix: {}",
                   length);
  ICEBERG_PRECHECK(blob.size() == expected_size,
                   "Deletion vector blob size mismatch: {} bytes, expected {}",
                   blob.size(), expected_size);
  return length;
}

Result<RoaringPositionBitmap> DeserializeBitmap(std::span<const uint8_t> blob,
                                                int32_t length,
                                                const DataFile& delete_file) {
  const uint8_t* bitmap_data = blob.data() + kLengthPrefixBytes;
  ICEBERG_PRECHECK(std::memcmp(bitmap_data, kMagic.data(), kMagic.size()) == 0,
                   "Invalid deletion vector magic");

  std::string_view vector_bytes(reinterpret_cast<const char*>(bitmap_data + kMagicBytes),
                                static_cast<size_t>(length) - kMagicBytes);
  ICEBERG_ASSIGN_OR_RAISE(auto bitmap, RoaringPositionBitmap::Deserialize(vector_bytes));
  ICEBERG_PRECHECK(std::cmp_equal(bitmap.Cardinality(), delete_file.record_count),
                   "Deletion vector cardinality {} does not match record count {}: {}",
                   bitmap.Cardinality(), delete_file.record_count, delete_file.file_path);
  return bitmap;
}

Status ValidateChecksum(std::span<const uint8_t> blob, int32_t length) {
  const uint8_t* bitmap_data = blob.data() + kLengthPrefixBytes;
  const auto stored_crc = ReadBigEndian<uint32_t>(bitmap_data + length);
  const auto actual_crc =
      ComputeCrc32(std::span<const uint8_t>(bitmap_data, static_cast<size_t>(length)));
  ICEBERG_PRECHECK(stored_crc == actual_crc,
                   "Deletion vector CRC mismatch: stored {:#010x}, computed {:#010x}",
                   stored_crc, actual_crc);
  return {};
}

}  // namespace

PositionDeleteIndex::PositionDeleteIndex(RoaringPositionBitmap bitmap)
    : bitmap_(std::move(bitmap)) {}

PositionDeleteIndex::PositionDeleteIndex(std::shared_ptr<DataFile> delete_file) {
  if (delete_file != nullptr) {
    delete_files_.push_back(std::move(delete_file));
  }
}

PositionDeleteIndex::PositionDeleteIndex(
    std::vector<std::shared_ptr<DataFile>> delete_files)
    : delete_files_(std::move(delete_files)) {}

void PositionDeleteIndex::Delete(int64_t pos) { bitmap_.Add(pos); }

void PositionDeleteIndex::Delete(int64_t pos_start, int64_t pos_end) {
  bitmap_.AddRange(pos_start, pos_end);
}

bool PositionDeleteIndex::IsDeleted(int64_t pos) const { return bitmap_.Contains(pos); }

bool PositionDeleteIndex::IsEmpty() const { return bitmap_.IsEmpty(); }

int64_t PositionDeleteIndex::Cardinality() const {
  return static_cast<int64_t>(bitmap_.Cardinality());
}

void PositionDeleteIndex::Merge(const PositionDeleteIndex& other) {
  bitmap_.Or(other.bitmap_);
  delete_files_.insert(delete_files_.end(), other.delete_files_.begin(),
                       other.delete_files_.end());
}

Result<std::vector<uint8_t>> PositionDeleteIndex::Serialize() {
  bitmap_.Optimize();  // run-length encode before serializing
  std::vector<uint8_t> blob(kLengthPrefixBytes);
  blob.insert(blob.end(), kMagic.begin(), kMagic.end());
  ICEBERG_ASSIGN_OR_RAISE(const auto vector_size, bitmap_.SerializeTo(blob));

  const size_t magic_and_vector_size = kMagicBytes + vector_size;
  ICEBERG_PRECHECK(magic_and_vector_size <= std::numeric_limits<int32_t>::max(),
                   "Deletion vector is too large to serialize: {} bytes",
                   magic_and_vector_size);

  WriteBigEndian(static_cast<int32_t>(magic_and_vector_size), blob.data());
  const auto crc_offset = blob.size();
  blob.resize(crc_offset + kCrcBytes);
  WriteBigEndian(ComputeCrc32(std::span<const uint8_t>(blob.data() + kLengthPrefixBytes,
                                                       magic_and_vector_size)),
                 blob.data() + crc_offset);
  return blob;
}

Result<PositionDeleteIndex> PositionDeleteIndex::Deserialize(
    std::span<const uint8_t> blob, std::shared_ptr<DataFile> delete_file) {
  ICEBERG_PRECHECK(delete_file != nullptr,
                   "Deletion vector requires a source delete file");
  ICEBERG_ASSIGN_OR_RAISE(const auto length, ReadBitmapDataLength(blob, *delete_file));
  ICEBERG_ASSIGN_OR_RAISE(auto bitmap, DeserializeBitmap(blob, length, *delete_file));
  ICEBERG_RETURN_UNEXPECTED(ValidateChecksum(blob, length));
  PositionDeleteIndex index(std::move(bitmap));
  index.delete_files_.push_back(std::move(delete_file));
  return index;
}

void PositionDeleteIndex::BulkAddForKey(int32_t key,
                                        std::span<const uint32_t> positions) {
  bitmap_.AddManyForKey(key, positions);
}

}  // namespace iceberg
