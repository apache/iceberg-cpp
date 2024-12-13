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

#include <cstdint>

#define ICEBERG_EXPAND(x) x
#define ICEBERG_STRINGIFY(x) #x
#define ICEBERG_CONCAT(x, y) x##y

#ifndef ICEBERG_DISALLOW_COPY_AND_ASSIGN
#  define ICEBERG_DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&) = delete;              \
    void operator=(const TypeName&) = delete
#endif

#ifndef ICEBERG_DEFAULT_MOVE_AND_ASSIGN
#  define ICEBERG_DEFAULT_MOVE_AND_ASSIGN(TypeName) \
    TypeName(TypeName&&) = default;                 \
    TypeName& operator=(TypeName&&) = default
#endif

#if defined(__GNUC__)  // GCC and compatible compilers (clang, Intel ICC)
#  define ICEBERG_NORETURN __attribute__((noreturn))
#  define ICEBERG_NOINLINE __attribute__((noinline))
#  define ICEBERG_FORCE_INLINE __attribute__((always_inline))
#  define ICEBERG_PREDICT_FALSE(x) (__builtin_expect(!!(x), 0))
#  define ICEBERG_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#  define ICEBERG_RESTRICT __restrict
#elif defined(_MSC_VER)  // MSVC
#  define ICEBERG_NORETURN __declspec(noreturn)
#  define ICEBERG_NOINLINE __declspec(noinline)
#  define ICEBERG_FORCE_INLINE __forceinline
#  define ICEBERG_PREDICT_FALSE(x) (x)
#  define ICEBERG_PREDICT_TRUE(x) (x)
#  define ICEBERG_RESTRICT __restrict
#else
#  define ICEBERG_NORETURN
#  define ICEBERG_NOINLINE
#  define ICEBERG_FORCE_INLINE
#  define ICEBERG_PREDICT_FALSE(x) (x)
#  define ICEBERG_PREDICT_TRUE(x) (x)
#  define ICEBERG_RESTRICT
#endif
