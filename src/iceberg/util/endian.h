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

#include <algorithm>
#include <bit>
#include <cstring>
#include <span>
#include <vector>

#include "iceberg/result.h"

/// \file iceberg/util/endian.h
/// \brief Endianness conversion utilities

namespace iceberg::util {

template <typename T>
concept LittleEndianWritable =
    std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
    std::is_same_v<T, float> || std::is_same_v<T, double> || std::is_same_v<T, uint8_t>;

/// \brief Convert a value to little-endian format.
template <LittleEndianWritable T>
T ToLittleEndian(T value) {
  if constexpr (std::endian::native != std::endian::little && sizeof(T) > 1) {
    return std::byteswap(value);
  }
  return value;
}

/// \brief Convert a value from little-endian format.
template <LittleEndianWritable T>
T FromLittleEndian(T value) {
  return ToLittleEndian(value);
}

/// \brief Write a value in little-endian format to the buffer.
template <LittleEndianWritable T>
void WriteLittleEndian(std::vector<uint8_t>& buffer, T value) {
  T le_value = ToLittleEndian(value);
  const auto* bytes = reinterpret_cast<const uint8_t*>(&le_value);
  buffer.insert(buffer.end(), bytes, bytes + sizeof(T));
}

/// \brief Read a value in little-endian format from the data.
template <LittleEndianWritable T>
Result<T> ReadLittleEndian(std::span<const uint8_t> data) {
  if (data.size() < sizeof(T)) [[unlikely]] {
    return InvalidArgument("Insufficient data to read {} bytes, got {}", sizeof(T),
                           data.size());
  }

  T value;
  std::memcpy(&value, data.data(), sizeof(T));
  return FromLittleEndian(value);
}

/// \brief Write a 16-byte value in big-endian format (for UUID and Decimal).
inline void WriteBigEndian16(std::vector<uint8_t>& buffer,
                             const std::array<uint8_t, 16>& value) {
  buffer.insert(buffer.end(), value.begin(), value.end());
}

/// \brief Read a 16-byte value in big-endian format (for UUID and Decimal).
inline Result<std::array<uint8_t, 16>> ReadBigEndian16(std::span<const uint8_t> data) {
  if (data.size() < 16) {
    return InvalidArgument("Insufficient data to read 16 bytes, got {}", data.size());
  }

  std::array<uint8_t, 16> result;
  std::copy(data.begin(), data.begin() + 16, result.begin());
  return result;
}

}  // namespace iceberg::util
