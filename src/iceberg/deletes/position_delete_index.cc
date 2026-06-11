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

#include <zlib.h>

#include <array>
#include <cstddef>
#include <cstring>
#include <limits>
#include <string_view>
#include <utility>

#include "iceberg/manifest/manifest_entry.h"
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

template <typename T>
void WriteBigEndian(T value, uint8_t* buf) {
  T be = ToBigEndian(value);
  std::memcpy(buf, &be, sizeof(be));
}

template <typename T>
T ReadBigEndian(const uint8_t* buf) {
  T value;
  std::memcpy(&value, buf, sizeof(value));
  return FromBigEndian(value);
}

}  // namespace

PositionDeleteIndex::PositionDeleteIndex(RoaringPositionBitmap bitmap)
    : bitmap_(std::move(bitmap)) {}

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

void PositionDeleteIndex::AddDeleteFile(std::shared_ptr<DataFile> delete_file) {
  delete_files_.push_back(std::move(delete_file));
}

Result<std::vector<uint8_t>> PositionDeleteIndex::Serialize() {
  bitmap_.Optimize();  // run-length encode before serializing
  ICEBERG_ASSIGN_OR_RAISE(auto vector, bitmap_.Serialize());

  // The length prefix and CRC both cover the magic sequence plus the vector.
  const size_t magic_and_vector_size = static_cast<size_t>(kMagicBytes) + vector.size();
  ICEBERG_PRECHECK(
      magic_and_vector_size <= static_cast<size_t>(std::numeric_limits<int32_t>::max()),
      "Deletion vector is too large to serialize: {} bytes", magic_and_vector_size);

  std::vector<uint8_t> blob(static_cast<size_t>(kLengthPrefixBytes) +
                            magic_and_vector_size + static_cast<size_t>(kCrcBytes));
  uint8_t* buf = blob.data();

  WriteBigEndian(static_cast<int32_t>(magic_and_vector_size), buf);
  buf += kLengthPrefixBytes;

  uint8_t* checksum_begin = buf;
  std::memcpy(buf, kMagic.data(), kMagicBytes);
  buf += kMagicBytes;
  std::memcpy(buf, vector.data(), vector.size());
  buf += vector.size();

  WriteBigEndian(
      ComputeCrc32(std::span<const uint8_t>(checksum_begin, magic_and_vector_size)), buf);
  return blob;
}

Result<PositionDeleteIndex> PositionDeleteIndex::Deserialize(
    std::span<const uint8_t> blob, std::shared_ptr<DataFile> delete_file) {
  ICEBERG_PRECHECK(delete_file != nullptr,
                   "Deletion vector requires a source delete file");
  // DV metadata requires content_size_in_bytes; the blob bytes must match it.
  ICEBERG_PRECHECK(delete_file->content_size_in_bytes.has_value(),
                   "Deletion vector requires content_size_in_bytes: {}",
                   delete_file->file_path);
  ICEBERG_PRECHECK(
      std::cmp_equal(blob.size(), delete_file->content_size_in_bytes.value()),
      "Deletion vector blob size {} does not match content_size_in_bytes {}: {}",
      blob.size(), delete_file->content_size_in_bytes.value(), delete_file->file_path);

  constexpr size_t kMinSize = static_cast<size_t>(kLengthPrefixBytes) +
                              static_cast<size_t>(kMagicBytes) +
                              static_cast<size_t>(kCrcBytes);
  ICEBERG_PRECHECK(blob.size() >= kMinSize,
                   "Deletion vector blob too small: {} bytes, need at least {}",
                   blob.size(), kMinSize);

  const uint8_t* buf = blob.data();

  const auto length = ReadBigEndian<int32_t>(buf);
  buf += kLengthPrefixBytes;

  ICEBERG_PRECHECK(length >= kMagicBytes, "Invalid deletion vector length prefix: {}",
                   length);

  const size_t expected_total = static_cast<size_t>(kLengthPrefixBytes) +
                                static_cast<size_t>(length) +
                                static_cast<size_t>(kCrcBytes);
  ICEBERG_PRECHECK(blob.size() == expected_total,
                   "Deletion vector blob size mismatch: {} bytes, expected {}",
                   blob.size(), expected_total);

  // Magic and vector are checksummed together by the trailing CRC.
  const uint8_t* checksum_begin = buf;
  for (size_t i = 0; i < kMagic.size(); ++i) {
    ICEBERG_PRECHECK(buf[i] == kMagic[i],
                     "Invalid deletion vector magic byte at offset {}: got {:#04x}", i,
                     buf[i]);
  }
  buf += kMagicBytes;

  const auto stored_crc =
      ReadBigEndian<uint32_t>(checksum_begin + static_cast<size_t>(length));
  const uint32_t actual_crc =
      ComputeCrc32(std::span<const uint8_t>(checksum_begin, static_cast<size_t>(length)));
  ICEBERG_PRECHECK(stored_crc == actual_crc,
                   "Deletion vector CRC mismatch: stored {:#010x}, computed {:#010x}",
                   stored_crc, actual_crc);

  const auto vector_size = static_cast<size_t>(length) - kMagicBytes;
  std::string_view vector_bytes(reinterpret_cast<const char*>(buf), vector_size);
  ICEBERG_ASSIGN_OR_RAISE(auto bitmap, RoaringPositionBitmap::Deserialize(vector_bytes));
  PositionDeleteIndex index(std::move(bitmap));

  // The bitmap cardinality must match the record count recorded in metadata.
  ICEBERG_PRECHECK(std::cmp_equal(index.Cardinality(), delete_file->record_count),
                   "Deletion vector cardinality {} does not match record count {}: {}",
                   index.Cardinality(), delete_file->record_count,
                   delete_file->file_path);

  index.delete_files_.push_back(std::move(delete_file));
  return index;
}

void PositionDeleteIndex::BulkAddForKey(int32_t key,
                                        std::span<const uint32_t> positions) {
  bitmap_.AddManyForKey(key, positions);
}

}  // namespace iceberg
