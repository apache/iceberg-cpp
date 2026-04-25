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

#include "iceberg/puffin/puffin_format.h"

#include <cstddef>
#include <utility>
#include <vector>

namespace iceberg::puffin {

namespace {

// Returns (byte_index, bit_index) for a given flag within the 4-byte flags field.
constexpr std::pair<int, int> GetFlagPosition(PuffinFlag flag) {
  switch (flag) {
    case PuffinFlag::kFooterPayloadCompressed:
      return {0, 0};
  }
  std::unreachable();
}

}  // namespace

bool IsFlagSet(std::span<const uint8_t, 4> flags, PuffinFlag flag) {
  auto [byte_num, bit_num] = GetFlagPosition(flag);
  return (flags[byte_num] & (1 << bit_num)) != 0;
}

void SetFlag(std::span<uint8_t, 4> flags, PuffinFlag flag) {
  auto [byte_num, bit_num] = GetFlagPosition(flag);
  flags[byte_num] |= (1 << bit_num);
}

// TODO(zhaoxuan1994): Move compression logic to a unified codec interface.
Result<std::vector<std::byte>> Compress(PuffinCompressionCodec codec,
                                        std::span<const std::byte> input) {
  switch (codec) {
    case PuffinCompressionCodec::kNone:
      return std::vector<std::byte>(input.begin(), input.end());
    case PuffinCompressionCodec::kLz4:
      return NotSupported("LZ4 compression is not yet supported");
    case PuffinCompressionCodec::kZstd:
      return NotSupported("Zstd compression is not yet supported");
  }
  std::unreachable();
}

Result<std::vector<std::byte>> Decompress(PuffinCompressionCodec codec,
                                          std::span<const std::byte> input) {
  switch (codec) {
    case PuffinCompressionCodec::kNone:
      return std::vector<std::byte>(input.begin(), input.end());
    case PuffinCompressionCodec::kLz4:
      return NotSupported("LZ4 decompression is not yet supported");
    case PuffinCompressionCodec::kZstd:
      return NotSupported("Zstd decompression is not yet supported");
  }
  std::unreachable();
}

}  // namespace iceberg::puffin
