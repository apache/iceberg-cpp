//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#pragma once

#include <cstddef>
#include <cstdint>

namespace iceberg {

// MurmurHash3 reads blocks in little-endian order, including unaligned input,
// and returns a numeric hash rather than a serialized byte sequence.
uint32_t MurmurHash3_x86_32(const void* key, size_t len, uint32_t seed);

// Hash a 64-bit value as two little-endian blocks, without serializing it to memory.
uint32_t MurmurHash3_x86_32_Long(uint64_t value, uint32_t seed);

}  // namespace iceberg
