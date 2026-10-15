//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#include "iceberg/util/murmurhash3_internal.h"

#include <bit>

#include "iceberg/util/endian.h"

// Adapted from the SMHasher MurmurHash3 reference implementation:
// https://github.com/aappleby/smhasher/blob/07bb4de10a63e8cc2e1724865454eba635742383/src/MurmurHash3.cpp

namespace iceberg {

namespace {

uint32_t MixBlock(uint32_t block) {
  constexpr uint32_t kC1 = 0xcc9e2d51;
  constexpr uint32_t kC2 = 0x1b873593;
  block *= kC1;
  block = std::rotl(block, 15);
  return block * kC2;
}

uint32_t MixHash(uint32_t hash, uint32_t block) {
  hash ^= MixBlock(block);
  return std::rotl(hash, 13) * 5 + 0xe6546b64;
}

uint32_t Finalize(uint32_t hash, size_t len) {
  hash ^= static_cast<uint32_t>(len);
  hash ^= hash >> 16;
  hash *= 0x85ebca6b;
  hash ^= hash >> 13;
  hash *= 0xc2b2ae35;
  hash ^= hash >> 16;
  return hash;
}

}  // namespace

uint32_t MurmurHash3_x86_32(const void* key, size_t len, uint32_t seed) {
  // Process the full size_t length; only the final 32-bit length mix may truncate it.
  const auto* data = static_cast<const uint8_t*>(key);
  const size_t nblocks = len / sizeof(uint32_t);
  uint32_t hash = seed;

  for (size_t i = 0; i < nblocks; ++i) {
    hash = MixHash(hash, ReadLittleEndian<uint32_t>(data));
    data += sizeof(uint32_t);
  }

  uint32_t tail = 0;
  switch (len & 3) {
    case 3:
      tail ^= static_cast<uint32_t>(data[2]) << 16;
      [[fallthrough]];
    case 2:
      tail ^= static_cast<uint32_t>(data[1]) << 8;
      [[fallthrough]];
    case 1:
      tail ^= data[0];
  }
  hash ^= MixBlock(tail);

  return Finalize(hash, len);
}

uint32_t MurmurHash3_x86_32_Long(uint64_t value, uint32_t seed) {
  auto hash = MixHash(seed, static_cast<uint32_t>(value));
  hash = MixHash(hash, static_cast<uint32_t>(value >> 32));
  return Finalize(hash, sizeof(value));
}

}  // namespace iceberg
