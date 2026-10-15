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
#include <cstddef>
#include <cstdint>

#include "iceberg/compat/compiler.h"

namespace iceberg::internal {

template <std::unsigned_integral T>
constexpr T ByteSwapUnsigned(T value) {
#if !ICEBERG_CXX20_COMPAT
  return std::byteswap(value);
#else
#  if defined(__GNUC__) || defined(__clang__)
  if constexpr (sizeof(T) == sizeof(uint16_t)) {
    return static_cast<T>(__builtin_bswap16(value));
  } else if constexpr (sizeof(T) == sizeof(uint32_t)) {
    return static_cast<T>(__builtin_bswap32(value));
  } else if constexpr (sizeof(T) == sizeof(uint64_t)) {
    return static_cast<T>(__builtin_bswap64(value));
  }
#  endif
  T swapped{0};
  for (size_t byte = 0; byte < sizeof(T); ++byte) {
    swapped = static_cast<T>(swapped << 8) | static_cast<T>(value & 0xFF);
    value = static_cast<T>(value >> 8);
  }
  return swapped;
#endif
}

}  // namespace iceberg::internal
