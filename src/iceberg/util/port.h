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

/// \file iceberg/util/port.h
/// \brief Portability macros and definitions for Iceberg C++ library

#pragma once

#if defined(_WIN32) /* Windows is always little endian */ \
    || defined(__LITTLE_ENDIAN__) ||                      \
    (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#  define ICEBERG_LITTLE_ENDIAN 1
#elif defined(__BIG_ENDIAN__) || \
    (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#  define ICEBERG_LITTLE_ENDIAN 0
#else
#  error "Unsupported or unknown endianness"
#endif

#if defined(_MSC_VER)
#  include <__msvc_int128.hpp>
using int128_t = std::_Signed128;
using uint128_t = std::_Unsigned128;
#elif defined(__GNUC__) || defined(__clang__)
using int128_t = __int128;
using uint128_t = unsigned __int128;
#else
#  error "128-bit integer type is not supported on this platform"
#endif
