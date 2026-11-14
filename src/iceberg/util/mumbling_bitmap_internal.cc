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

#include "iceberg/util/mumbling_bitmap_internal.h"

#include <algorithm>
#include <array>
#include <cstring>

#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

// SSE2 is part of the x86-64 baseline, so it needs no extra compiler flags.
// Define ICEBERG_MUMBLING_SSE2=0 to force the portable scalar code.
#if !defined(ICEBERG_MUMBLING_SSE2) && (defined(__SSE2__) || defined(_M_X64)) && \
    !defined(_M_ARM64EC)
#  define ICEBERG_MUMBLING_SSE2 1
#  include <emmintrin.h>
#elif !defined(ICEBERG_MUMBLING_SSE2)
#  define ICEBERG_MUMBLING_SSE2 0
#endif

namespace iceberg {

namespace {

constexpr size_t kHeaderSize = 6;
constexpr size_t kPforChunkSize = 256;
constexpr size_t kPforHeaderSize = 3;

// The 3 most significant bits of a descriptor select the container type:
// 000 is sparse, 001 is dense, and the other patterns are reserved.
constexpr uint8_t kDenseTypeBit = 0b0010'0000;
constexpr uint8_t kReservedTypeBits = 0b1100'0000;

constexpr size_t BytesForBits(size_t bits) { return (bits + 7) / 8; }

// Returns true if `value` is one of the `length` (<= 31) bytes at `container`.
// Scans all of them, so the result doesn't depend on them being sorted.
bool ContainsByte(const uint8_t* container, uint8_t length, uint8_t value) {
  for (uint8_t i = 0; i < length; ++i) {
    if (container[i] == value) {
      return true;
    }
  }
  return false;
}

// Portable SWAR (SIMD within a register) version of ContainsByte: compares 8
// bytes per 64-bit word. Reads 32 bytes at `container`, so the caller must
// make sure they are inside the buffer.
bool ContainsByte32(const uint8_t* container, uint8_t length, uint8_t value) {
  constexpr uint64_t kLow7 = 0x7F7F7F7F7F7F7F7FULL;
  const uint64_t pattern = 0x0101010101010101ULL * value;
  uint64_t found = 0;
  for (int w = 0; w < 4; ++w) {
    // Little-endian load, so byte i of the word is bits [8i, 8i + 8).
    const uint64_t x = ReadLittleEndian<uint64_t>(container + w * 8) ^ pattern;
    // 0x80 in each byte of x that is zero. Exact per byte, unlike the common
    // `(x - 0x01..) & ~x & 0x80..` trick, so bytes can be masked by position.
    const uint64_t zero = ~(((x & kLow7) + kLow7) | x | kLow7);
    const int valid_bytes = std::clamp(static_cast<int>(length) - w * 8, 0, 8);
    const uint64_t valid =
        valid_bytes == 8 ? ~uint64_t{0} : (uint64_t{1} << (valid_bytes * 8)) - 1;
    found |= zero & valid;
  }
  return found != 0;
}

#if ICEBERG_MUMBLING_SSE2
// SSE2 version of ContainsByte32. Reads 32 bytes at `container`, so the caller
// must make sure they are inside the buffer.
bool ContainsByte32Sse2(const uint8_t* container, uint8_t length, uint8_t value) {
  const uint32_t valid = (uint32_t{1} << length) - 1;
  const __m128i needle = _mm_set1_epi8(static_cast<char>(value));
  const __m128i lo = _mm_loadu_si128(reinterpret_cast<const __m128i*>(container));
  const __m128i hi = _mm_loadu_si128(reinterpret_cast<const __m128i*>(container + 16));
  const auto mask =
      static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(lo, needle))) |
      (static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(hi, needle))) << 16);
  return (mask & valid) != 0;
}
#endif

// Reads the `index`-th `width`-bit value from `packed`, where values are packed
// most significant bit first. `width` must be at most 8 and `packed` must hold
// at least BytesForBits((index + 1) * width) bytes.
uint8_t UnpackValue(std::span<const uint8_t> packed, size_t index, uint8_t width) {
  size_t bit_pos = index * width;
  size_t byte_pos = bit_pos / 8;
  uint32_t window = static_cast<uint32_t>(packed[byte_pos]) << 8;
  if (byte_pos + 1 < packed.size()) {
    window |= packed[byte_pos + 1];
  }
  uint32_t shift = 16 - static_cast<uint32_t>(bit_pos % 8) - width;
  return static_cast<uint8_t>((window >> shift) & ((1U << width) - 1));
}

// Unpacks groups of 8 `Width`-bit values. Each group is exactly `Width` bytes,
// so it is read as one big-endian word and split with constant shifts; the
// compiler fully unrolls both inner loops.
template <int Width>
void UnpackGroups(const uint8_t* in, uint8_t* out, size_t groups) {
  constexpr uint64_t kMask = (uint64_t{1} << Width) - 1;
  for (size_t group = 0; group < groups; ++group, in += Width, out += 8) {
    uint64_t word = 0;
    for (int i = 0; i < Width; ++i) {
      word = (word << 8) | in[i];
    }
    for (int i = 0; i < 8; ++i) {
      out[i] = static_cast<uint8_t>((word >> (Width * (7 - i))) & kMask);
    }
  }
}

// Unpacks `out.size()` `width`-bit values from `packed`, which must hold at
// least BytesForBits(out.size() * width) bytes.
void UnpackBits(std::span<const uint8_t> packed, uint8_t width, std::span<uint8_t> out) {
  const size_t count = out.size();
  if (width == 0) {
    std::ranges::fill(out, 0);
    return;
  }
  if (width == 8) {
    std::memcpy(out.data(), packed.data(), count);
    return;
  }

  const size_t groups = count / 8;
  size_t unpacked = groups * 8;
  switch (width) {
    case 1:
      UnpackGroups<1>(packed.data(), out.data(), groups);
      break;
    case 2:
      UnpackGroups<2>(packed.data(), out.data(), groups);
      break;
    case 3:
      UnpackGroups<3>(packed.data(), out.data(), groups);
      break;
    case 4:
      UnpackGroups<4>(packed.data(), out.data(), groups);
      break;
    case 5:
      UnpackGroups<5>(packed.data(), out.data(), groups);
      break;
    case 6:
      UnpackGroups<6>(packed.data(), out.data(), groups);
      break;
    case 7:
      UnpackGroups<7>(packed.data(), out.data(), groups);
      break;
    default:
      unpacked = 0;
      break;
  }
  for (size_t i = unpacked; i < count; ++i) {
    out[i] = UnpackValue(packed, i, width);
  }
}

// Decodes one PFOR chunk of `out.size()` values from `data`, returning the
// number of bytes read.
Result<size_t> DecodePforChunk(std::span<const uint8_t> data, std::span<uint8_t> out) {
  const size_t count = out.size();
  ICEBERG_PRECHECK(data.size() >= kPforHeaderSize,
                   "Truncated PFOR chunk header: {} bytes available, {} required",
                   data.size(), kPforHeaderSize);

  const uint8_t b1 = data[0] & 0x0F;
  const uint8_t b2 = data[0] >> 4;
  const uint8_t exception_count = data[1];
  const uint8_t base = data[2];

  ICEBERG_PRECHECK(b1 <= 8, "Invalid PFOR primary bit width: {} (must be <= 8)", b1);
  ICEBERG_PRECHECK(b2 <= 8 - b1,
                   "Invalid PFOR exception bit width: {} (must be <= {} when b1 is {})",
                   b2, 8 - b1, b1);
  ICEBERG_PRECHECK(b1 != 8 || exception_count == 0,
                   "Invalid PFOR chunk: {} exceptions with 8-bit primary values",
                   exception_count);
  ICEBERG_PRECHECK(exception_count <= count,
                   "Invalid PFOR chunk: {} exceptions for {} values", exception_count,
                   count);

  const size_t primary_size = BytesForBits(count * b1);
  const size_t exception_values_size = BytesForBits(size_t{exception_count} * b2);
  const size_t chunk_size =
      kPforHeaderSize + primary_size + exception_count + exception_values_size;
  ICEBERG_PRECHECK(data.size() >= chunk_size,
                   "Truncated PFOR chunk: {} bytes available, {} required", data.size(),
                   chunk_size);

  UnpackBits(data.subspan(kPforHeaderSize, primary_size), b1, out);

  if (exception_count > 0) {
    auto exception_offsets =
        data.subspan(kPforHeaderSize + primary_size, exception_count);
    std::array<uint8_t, kPforChunkSize> exception_values;
    UnpackBits(data.subspan(kPforHeaderSize + primary_size + exception_count,
                            exception_values_size),
               b2, std::span(exception_values).first(exception_count));
    for (size_t i = 0; i < exception_count; ++i) {
      const uint8_t offset = exception_offsets[i];
      ICEBERG_PRECHECK(offset < count,
                       "Invalid PFOR exception offset: {} (chunk has {} values)", offset,
                       count);
      // b1 + b2 <= 8, so the patched value still fits in a byte.
      out[offset] |= static_cast<uint8_t>(exception_values[i] << b1);
    }
  }

  if (base != 0) {
    // Check for overflow once with a max reduction, then add the base; both
    // loops vectorize.
    uint8_t max_value = 0;
    for (uint8_t value : out) {
      max_value = std::max(max_value, value);
    }
    if (max_value > 0xFF - base) [[unlikely]] {
      const auto it =
          std::ranges::find_if(out, [&](uint8_t v) { return v > 0xFF - base; });
      return InvalidArgument("Invalid PFOR value at index {}: {} exceeds 255",
                             it - out.begin(), uint32_t{*it} + base);
    }
    for (uint8_t& value : out) {
      value = static_cast<uint8_t>(value + base);
    }
  }

  return chunk_size;
}

}  // namespace

Result<size_t> DecodePforBytes(std::span<const uint8_t> data, std::span<uint8_t> out) {
  size_t bytes_read = 0;
  for (size_t values_read = 0; values_read < out.size(); values_read += kPforChunkSize) {
    const size_t chunk_length = std::min(kPforChunkSize, out.size() - values_read);
    ICEBERG_ASSIGN_OR_RAISE(auto chunk_size,
                            DecodePforChunk(data.subspan(bytes_read),
                                            out.subspan(values_read, chunk_length)));
    bytes_read += chunk_size;
  }
  return bytes_read;
}

Result<MumblingBitmapView> MumblingBitmapView::Make(std::span<const uint8_t> bytes) {
  ICEBERG_PRECHECK(bytes.size() >= kHeaderSize,
                   "Truncated Mumbling bitmap header: {} bytes available, {} required",
                   bytes.size(), kHeaderSize);
  if (bytes[0] != kVersion) {
    return NotSupported("Unsupported Mumbling bitmap version: {}", bytes[0]);
  }

  const int64_t cardinality = static_cast<int64_t>(bytes[1]) |
                              (static_cast<int64_t>(bytes[2]) << 8) |
                              (static_cast<int64_t>(bytes[3]) << 16);
  const int32_t container_count =
      static_cast<int32_t>(bytes[4]) | (static_cast<int32_t>(bytes[5]) << 8);
  ICEBERG_PRECHECK(container_count <= kMaxContainerCount,
                   "Invalid Mumbling bitmap container count: {} (must be <= {})",
                   container_count, kMaxContainerCount);
  ICEBERG_PRECHECK(cardinality <= kMaxCardinality,
                   "Invalid Mumbling bitmap cardinality: {} (must be <= {})", cardinality,
                   kMaxCardinality);

  MumblingBitmapView bitmap;
  bitmap.cardinality_ = cardinality;
  bitmap.containers_.resize(container_count);

  // Decode into a stack buffer; only the offset table is kept.
  std::array<uint8_t, kMaxContainerCount> descriptors;
  ICEBERG_ASSIGN_OR_RAISE(auto descriptors_size,
                          DecodePforBytes(bytes.subspan(kHeaderSize),
                                          std::span(descriptors).first(container_count)));

  // Branch-free: a dense descriptor (type 001) is 32 bytes and a sparse one
  // (type 000) is its own value. Reserved types are checked once at the end.
  uint32_t offset = 0;
  uint8_t reserved_bits = 0;
  for (int32_t i = 0; i < container_count; ++i) {
    const uint8_t descriptor = descriptors[i];
    reserved_bits |= descriptor;
    const uint8_t length = (descriptor & kDenseTypeBit) != 0 ? kDenseLength : descriptor;
    bitmap.containers_[i] = (offset << kOffsetShift) | length;
    offset += length;
  }
  if ((reserved_bits & kReservedTypeBits) != 0) [[unlikely]] {
    const auto* it =
        std::find_if(descriptors.data(), descriptors.data() + container_count,
                     [](uint8_t d) { return (d & kReservedTypeBits) != 0; });
    return InvalidArgument(
        "Invalid Mumbling bitmap descriptor for container {}: {:#04x} (not sparse "
        "or dense)",
        it - descriptors.data(), *it);
  }

  // Every container must be inside the buffer; this is what makes the
  // accessors memory-safe.
  const size_t containers_start = kHeaderSize + descriptors_size;
  const size_t available = bytes.size() - containers_start;
  ICEBERG_PRECHECK(available >= offset,
                   "Truncated Mumbling bitmap containers: {} bytes available, {} "
                   "required",
                   available, offset);
  ICEBERG_PRECHECK(available == offset,
                   "Invalid Mumbling bitmap: {} unexpected trailing bytes",
                   available - offset);

  bitmap.containers_data_ = bytes.subspan(containers_start, offset);
  return bitmap;
}

bool MumblingBitmapView::SparseContains(uint32_t ref, uint8_t pos_in_container) const {
  const uint32_t offset = ref >> kOffsetShift;
  const uint8_t* container = containers_data_.data() + offset;
  const auto length = static_cast<uint8_t>(ref & kLengthMask);
  // Compare 32 bytes at once when they are all inside the buffer (every
  // container except those in the last 31 bytes of the bitmap).
  if (containers_data_.size() - offset >= kDenseLength) {
#if ICEBERG_MUMBLING_SSE2
    return ContainsByte32Sse2(container, length, pos_in_container);
#else
    // A plain loop is faster than SWAR for short containers.
    if (length >= 8) {
      return ContainsByte32(container, length, pos_in_container);
    }
#endif
  }
  return ContainsByte(container, length, pos_in_container);
}

}  // namespace iceberg
