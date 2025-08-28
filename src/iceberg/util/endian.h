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

#include <array>
#include <bit>
#include <concepts>

/// \file iceberg/util/endian.h
/// \brief Endianness conversion utilities

namespace iceberg {

/// \brief Concept for values that can be written in little-endian format.
template <typename T>
concept EndianConvertible = std::is_arithmetic_v<T>;

/// \brief Concept for values that can be written in big-endian format,
template <typename T>
concept BigEndianWritable = std::same_as<T, std::array<uint8_t, 16>>;

/// \brief Convert a value to little-endian format.
template <EndianConvertible T>
T ToLittleEndian(T value) {
  if constexpr (std::endian::native != std::endian::little && sizeof(T) > 1) {
    return std::byteswap(value);
  }
  return value;
}

/// \brief Convert a value from little-endian format.
template <EndianConvertible T>
T FromLittleEndian(T value) {
  if constexpr (std::endian::native != std::endian::little && sizeof(T) > 1) {
    return std::byteswap(value);
  }
  return value;
}

template <EndianConvertible T>
constexpr T ToBigEndian(T value) {
  if constexpr (std::endian::native == std::endian::big || sizeof(T) <= 1) {
    return value;
  } else {
    return std::byteswap(value);
  }
}

/// \brief Convert a value from big-endian format to native.
template <EndianConvertible T>
constexpr T FromBigEndian(T value) {
  if constexpr (std::endian::native == std::endian::big || sizeof(T) <= 1) {
    return value;
  } else {
    return std::byteswap(value);
  }
}

}  // namespace iceberg
