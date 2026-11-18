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

/// \file iceberg/test/mumbling_bitmap_test_util.h
/// Helpers that serialize Mumbling bitmaps for tests and benchmarks. The
/// library only has a reader, so these follow the spec directly.

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <vector>

#include "iceberg/util/mumbling_bitmap_internal.h"

namespace iceberg::mumbling_test {

using Bytes = std::vector<uint8_t>;

// Packs `width`-bit values most significant bit first, padded to a byte.
inline Bytes PackBits(const std::vector<uint8_t>& values, int width) {
  Bytes out((values.size() * width + 7) / 8, 0);
  size_t bit_pos = 0;
  for (uint8_t value : values) {
    for (int bit = width - 1; bit >= 0; --bit, ++bit_pos) {
      if ((value >> bit) & 1) {
        out[bit_pos / 8] |= static_cast<uint8_t>(0x80 >> (bit_pos % 8));
      }
    }
  }
  return out;
}

// Encodes one PFOR chunk with the given primary bit width `b1` and base `m`,
// following Appendix A of the spec
// (https://github.com/apache/iceberg/blob/main/format/mumbling-spec.md#appendix-a-pfor-encoding-for-unsigned-bytes).
// Exceptions are the values that do not fit in `b1` bits after subtracting `m`.
inline Bytes EncodePforChunk(const std::vector<uint8_t>& values, int b1, uint8_t m) {
  std::vector<uint8_t> primary;
  std::vector<uint8_t> offsets;
  std::vector<uint8_t> exceptions;
  int max_width = 0;
  for (uint8_t value : values) {
    max_width = std::max(
        max_width, static_cast<int>(std::bit_width(static_cast<uint8_t>(value - m))));
  }
  const int b2 = std::max(0, max_width - b1);
  for (size_t i = 0; i < values.size(); ++i) {
    const auto normalized = static_cast<uint8_t>(values[i] - m);
    primary.push_back(b1 == 8 ? normalized
                              : static_cast<uint8_t>(normalized & ((1 << b1) - 1)));
    if (b1 < 8 && (normalized >> b1) != 0) {
      offsets.push_back(static_cast<uint8_t>(i));
      exceptions.push_back(static_cast<uint8_t>(normalized >> b1));
    }
  }
  Bytes out = {static_cast<uint8_t>((b2 << 4) | b1), static_cast<uint8_t>(offsets.size()),
               m};
  auto packed = PackBits(primary, b1);
  out.insert(out.end(), packed.begin(), packed.end());
  out.insert(out.end(), offsets.begin(), offsets.end());
  packed = PackBits(exceptions, b2);
  out.insert(out.end(), packed.begin(), packed.end());
  return out;
}

// Passed as `b1` to choose the primary bit width the way the spec recommends.
constexpr int kBestWidth = -1;

// Returns the primary bit width that minimizes the encoded size of a chunk, as
// recommended by the spec. Ties go to the larger width, as in the Java encoder.
inline int ChooseBitWidth(const std::vector<uint8_t>& chunk, uint8_t m) {
  int max_width = 0;
  for (uint8_t value : chunk) {
    max_width = std::max(
        max_width, static_cast<int>(std::bit_width(static_cast<uint8_t>(value - m))));
  }
  int best_width = 0;
  size_t best_size = std::numeric_limits<size_t>::max();
  for (int width = 0; width <= max_width; ++width) {
    size_t exceptions = 0;
    for (uint8_t value : chunk) {
      exceptions += (static_cast<uint8_t>(value - m) >> width) != 0 ? 1 : 0;
    }
    const size_t b2 = max_width - width;
    const size_t size =
        (chunk.size() * width + 7) / 8 + exceptions + (exceptions * b2 + 7) / 8;
    if (size <= best_size) {
      best_size = size;
      best_width = width;
    }
  }
  return best_width;
}

// Encodes a whole value array as PFOR chunks of 256 values each, using primary
// bit width `b1` (0-8) or kBestWidth.
inline Bytes EncodePfor(const std::vector<uint8_t>& values, int b1 = 8) {
  Bytes out;
  for (size_t start = 0; start < values.size(); start += 256) {
    std::vector<uint8_t> chunk(
        values.begin() + start,
        values.begin() + std::min(values.size(), start + size_t{256}));
    const uint8_t min = *std::ranges::min_element(chunk);
    const int width = b1 == kBestWidth ? ChooseBitWidth(chunk, min) : b1;
    // With 8-bit values the spec recommends storing the original values (m = 0).
    auto encoded = EncodePforChunk(chunk, width, width == 8 ? 0 : min);
    out.insert(out.end(), encoded.begin(), encoded.end());
  }
  return out;
}

inline Bytes Header(int64_t cardinality, int32_t container_count) {
  return {MumblingBitmapView::kVersion,
          static_cast<uint8_t>(cardinality & 0xFF),
          static_cast<uint8_t>((cardinality >> 8) & 0xFF),
          static_cast<uint8_t>((cardinality >> 16) & 0xFF),
          static_cast<uint8_t>(container_count & 0xFF),
          static_cast<uint8_t>((container_count >> 8) & 0xFF)};
}

inline int64_t ContainerCardinality(const Bytes& container) {
  if (container.size() < 32) {
    return static_cast<int64_t>(container.size());
  }
  int64_t count = 0;
  for (uint8_t b : container) {
    count += std::popcount(b);
  }
  return count;
}

// Serializes a bitmap from containers: containers with fewer than 32 bytes are
// sparse and 32-byte containers are dense.
inline Bytes BuildBitmap(const std::vector<Bytes>& containers, int b1 = 8,
                         std::optional<int64_t> cardinality = std::nullopt) {
  std::vector<uint8_t> descriptors;
  int64_t actual_cardinality = 0;
  for (const auto& container : containers) {
    descriptors.push_back(static_cast<uint8_t>(container.size()));
    actual_cardinality += ContainerCardinality(container);
  }
  Bytes out = Header(cardinality.value_or(actual_cardinality),
                     static_cast<int32_t>(containers.size()));
  auto encoded = EncodePfor(descriptors, b1);
  out.insert(out.end(), encoded.begin(), encoded.end());
  for (const auto& container : containers) {
    out.insert(out.end(), container.begin(), container.end());
  }
  return out;
}

// Builds a dense container with the given positions set.
inline Bytes Dense(const std::vector<int>& positions) {
  Bytes container(32, 0);
  for (int pos : positions) {
    container[pos / 8] |= static_cast<uint8_t>(0x80 >> (pos % 8));
  }
  return container;
}

// Builds a dense container with positions [start, end) set.
inline Bytes DenseRange(int start, int end) {
  std::vector<int> positions;
  for (int i = start; i < end; ++i) {
    positions.push_back(i);
  }
  return Dense(positions);
}

// Serializes a bitmap from sorted, unique positions in [0, 2,097,152), with
// descriptors encoded using `b1` (0-8 or kBestWidth).
inline Bytes BuildBitmapFromPositions(const std::vector<int64_t>& positions,
                                      int b1 = kBestWidth) {
  std::vector<Bytes> containers;
  if (!positions.empty()) {
    containers.resize(static_cast<size_t>(positions.back() / 256) + 1);
  }
  for (int64_t pos : positions) {
    containers[pos / 256].push_back(static_cast<uint8_t>(pos % 256));
  }
  for (auto& container : containers) {
    if (container.size() >= 32) {
      std::vector<int> in_container(container.begin(), container.end());
      container = Dense(in_container);
    }
  }
  return BuildBitmap(containers, b1);
}

}  // namespace iceberg::mumbling_test
