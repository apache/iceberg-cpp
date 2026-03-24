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

#include "iceberg/deletes/roaring_position_bitmap.h"

#include <cstring>
#include <exception>
#include <limits>
#include <utility>
#include <vector>

#include <roaring/roaring.hh>

#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

constexpr size_t kBitmapCountSizeBytes = 8;
constexpr size_t kBitmapKeySizeBytes = 4;

// Extracts high 32 bits from a 64-bit position (the key).
int32_t Key(int64_t pos) { return static_cast<int32_t>(pos >> 32); }

// Extracts low 32 bits from a 64-bit position.
uint32_t Pos32Bits(int64_t pos) { return static_cast<uint32_t>(0xFFFFFFFF & pos); }

// Combines key (high 32 bits) and pos32 (low 32 bits) into a 64-bit
// position. The low 32 bits are zero-extended to avoid sign extension.
int64_t ToPosition(int32_t key, uint32_t pos32) {
  return (int64_t{key} << 32) | int64_t{pos32};
}

Status ValidatePosition(int64_t pos) {
  if (pos < 0 || pos > RoaringPositionBitmap::kMaxPosition) {
    return InvalidArgument("Bitmap supports positions that are >= 0 and <= {}: {}",
                           RoaringPositionBitmap::kMaxPosition, pos);
  }
  return {};
}

}  // namespace

struct RoaringPositionBitmap::Impl {
  std::vector<roaring::Roaring> bitmaps;

  void AllocateBitmapsIfNeeded(int32_t required_length) {
    if (std::cmp_less(bitmaps.size(), required_length)) {
      bitmaps.resize(static_cast<size_t>(required_length));
    }
  }
};

RoaringPositionBitmap::RoaringPositionBitmap() : impl_(std::make_unique<Impl>()) {}

RoaringPositionBitmap::~RoaringPositionBitmap() = default;

RoaringPositionBitmap::RoaringPositionBitmap(const RoaringPositionBitmap& other)
    : impl_(other.impl_ != nullptr ? std::make_unique<Impl>(*other.impl_)
                                   : std::make_unique<Impl>()) {}

RoaringPositionBitmap& RoaringPositionBitmap::operator=(
    const RoaringPositionBitmap& other) {
  if (this == &other) {
    return *this;
  }
  impl_ = other.impl_ != nullptr ? std::make_unique<Impl>(*other.impl_)
                                 : std::make_unique<Impl>();
  return *this;
}

RoaringPositionBitmap::RoaringPositionBitmap(RoaringPositionBitmap&&) noexcept = default;

RoaringPositionBitmap& RoaringPositionBitmap::operator=(
    RoaringPositionBitmap&&) noexcept = default;

RoaringPositionBitmap::RoaringPositionBitmap(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

Status RoaringPositionBitmap::Add(int64_t pos) {
  ICEBERG_RETURN_UNEXPECTED(ValidatePosition(pos));
  int32_t key = Key(pos);
  uint32_t pos32 = Pos32Bits(pos);
  impl_->AllocateBitmapsIfNeeded(key + 1);
  impl_->bitmaps[key].add(pos32);
  return {};
}

Status RoaringPositionBitmap::AddRange(int64_t pos_start, int64_t pos_end) {
  for (int64_t pos = pos_start; pos < pos_end; ++pos) {
    ICEBERG_RETURN_UNEXPECTED(Add(pos));
  }
  return {};
}

Result<bool> RoaringPositionBitmap::Contains(int64_t pos) const {
  ICEBERG_RETURN_UNEXPECTED(ValidatePosition(pos));
  int32_t key = Key(pos);
  uint32_t pos32 = Pos32Bits(pos);
  return std::cmp_less(key, impl_->bitmaps.size()) && impl_->bitmaps[key].contains(pos32);
}

bool RoaringPositionBitmap::IsEmpty() const { return Cardinality() == 0; }

size_t RoaringPositionBitmap::Cardinality() const {
  size_t total = 0;
  for (const auto& bitmap : impl_->bitmaps) {
    total += bitmap.cardinality();
  }
  return total;
}

void RoaringPositionBitmap::Or(const RoaringPositionBitmap& other) {
  impl_->AllocateBitmapsIfNeeded(static_cast<int32_t>(other.impl_->bitmaps.size()));
  for (size_t key = 0; key < other.impl_->bitmaps.size(); ++key) {
    impl_->bitmaps[key] |= other.impl_->bitmaps[key];
  }
}

bool RoaringPositionBitmap::Optimize() {
  bool changed = false;
  for (auto& bitmap : impl_->bitmaps) {
    changed |= bitmap.runOptimize();
  }
  return changed;
}

void RoaringPositionBitmap::ForEach(const std::function<void(int64_t)>& fn) const {
  for (size_t key = 0; key < impl_->bitmaps.size(); ++key) {
    for (uint32_t pos32 : impl_->bitmaps[key]) {
      fn(ToPosition(static_cast<int32_t>(key), pos32));
    }
  }
}

size_t RoaringPositionBitmap::SerializedSizeInBytes() const {
  size_t size = kBitmapCountSizeBytes;
  for (const auto& bitmap : impl_->bitmaps) {
    size += kBitmapKeySizeBytes + bitmap.getSizeInBytes(/*portable=*/true);
  }
  return size;
}

// Serializes using the portable format (little-endian).
// See https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type
Result<std::string> RoaringPositionBitmap::Serialize() const {
  size_t size = SerializedSizeInBytes();
  std::string result(size, '\0');
  char* buf = result.data();

  // Write bitmap count (array length including empties)
  WriteLittleEndian(static_cast<int64_t>(impl_->bitmaps.size()), buf);
  buf += kBitmapCountSizeBytes;

  // Write each bitmap with its key
  for (int32_t key = 0; std::cmp_less(key, impl_->bitmaps.size()); ++key) {
    WriteLittleEndian(key, buf);
    buf += kBitmapKeySizeBytes;
    size_t written = impl_->bitmaps[key].write(buf, /*portable=*/true);
    buf += written;
  }

  return result;
}

Result<RoaringPositionBitmap> RoaringPositionBitmap::Deserialize(std::string_view bytes) {
  const char* buf = bytes.data();
  size_t remaining = bytes.size();

  ICEBERG_PRECHECK(remaining >= kBitmapCountSizeBytes,
                   "Buffer too small for bitmap count: {} bytes", remaining);

  auto bitmap_count = ReadLittleEndian<int64_t>(buf);
  buf += kBitmapCountSizeBytes;
  remaining -= kBitmapCountSizeBytes;

  ICEBERG_PRECHECK(
      bitmap_count >= 0 && bitmap_count <= std::numeric_limits<int32_t>::max(),
      "Invalid bitmap count: {}", bitmap_count);

  auto impl = std::make_unique<Impl>();
  int32_t last_key = -1;
  auto remaining_count = static_cast<int32_t>(bitmap_count);

  while (remaining_count > 0) {
    ICEBERG_PRECHECK(remaining >= kBitmapKeySizeBytes,
                     "Buffer too small for bitmap key: {} bytes", remaining);

    auto key = ReadLittleEndian<int32_t>(buf);
    buf += kBitmapKeySizeBytes;
    remaining -= kBitmapKeySizeBytes;

    ICEBERG_PRECHECK(key >= 0, "Invalid unsigned key: {}", key);
    ICEBERG_PRECHECK(key < std::numeric_limits<int32_t>::max(), "Key is too large: {}",
                     key);
    ICEBERG_PRECHECK(key > last_key,
                     "Keys must be sorted in ascending order, got key {} after {}", key,
                     last_key);

    // Fill gaps with empty bitmaps
    while (last_key < key - 1) {
      impl->bitmaps.emplace_back();
      ++last_key;
    }

    // Read bitmap using portable safe deserialization.
    // CRoaring's readSafe may throw on corrupted data.
    roaring::Roaring bitmap;
    try {
      bitmap = roaring::Roaring::readSafe(buf, remaining);
    } catch (const std::exception& e) {
      return InvalidArgument("Failed to deserialize bitmap at key {}: {}", key, e.what());
    }
    size_t bitmap_size = bitmap.getSizeInBytes(/*portable=*/true);
    ICEBERG_PRECHECK(
        bitmap_size <= remaining,
        "Buffer too small for bitmap key {}: {} bytes needed, {} bytes available", key,
        bitmap_size, remaining);
    buf += bitmap_size;
    remaining -= bitmap_size;

    impl->bitmaps.emplace_back(std::move(bitmap));
    last_key = key;
    --remaining_count;
  }

  return RoaringPositionBitmap(std::move(impl));
}

}  // namespace iceberg
