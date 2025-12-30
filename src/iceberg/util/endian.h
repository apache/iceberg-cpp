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

#include <bit>
#include <concepts>
#include <cstdint>

/// \file iceberg/util/endian.h
/// \brief Endianness conversion utilities

namespace iceberg {

/// \brief Concept for values that can be converted to/from another endian format.
template <typename T>
concept EndianConvertible = std::is_arithmetic_v<T>;

/// \brief Byte-swap a value. For floating-point types, only support 32-bit and 64-bit
/// floats.
template <EndianConvertible T>
constexpr T ByteSwap(T value) {
  if constexpr (sizeof(T) <= 1) {
    return value;
  } else if constexpr (std::is_integral_v<T>) {
    return std::byteswap(value);
  } else if constexpr (std::is_floating_point_v<T>) {
    if constexpr (sizeof(T) == sizeof(uint16_t)) {
      return std::bit_cast<T>(std::byteswap(std::bit_cast<uint16_t>(value)));
    } else if constexpr (sizeof(T) == sizeof(uint32_t)) {
      return std::bit_cast<T>(std::byteswap(std::bit_cast<uint32_t>(value)));
    } else if constexpr (sizeof(T) == sizeof(uint64_t)) {
      return std::bit_cast<T>(std::byteswap(std::bit_cast<uint64_t>(value)));
    } else {
      static_assert(sizeof(T) == 0,
                    "Unsupported floating-point size for endian conversion.");
    }
  }
}

/// \brief Convert a value to little-endian format.
template <EndianConvertible T>
constexpr T ToLittleEndian(T value) {
  if constexpr (std::endian::native == std::endian::little) {
    return value;
  } else {
    return ByteSwap(value);
  }
}

/// \brief Convert a value from little-endian format.
template <EndianConvertible T>
constexpr T FromLittleEndian(T value) {
  if constexpr (std::endian::native == std::endian::little) {
    return value;
  } else {
    return ByteSwap(value);
  }
}

/// \brief Convert a value to big-endian format.
template <EndianConvertible T>
constexpr T ToBigEndian(T value) {
  if constexpr (std::endian::native == std::endian::big) {
    return value;
  } else {
    return ByteSwap(value);
  }
}

/// \brief Convert a value from big-endian format.
template <EndianConvertible T>
constexpr T FromBigEndian(T value) {
  if constexpr (std::endian::native == std::endian::big) {
    return value;
  } else {
    return ByteSwap(value);
  }
}

}  // namespace iceberg
