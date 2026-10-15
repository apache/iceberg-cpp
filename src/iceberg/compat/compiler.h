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

#include <version>

#include "iceberg/compat/build_config.h"

#if defined(_MSVC_LANG)
#  define ICEBERG_COMPILER_CPLUSPLUS _MSVC_LANG
#else
#  define ICEBERG_COMPILER_CPLUSPLUS __cplusplus
#endif

#if ICEBERG_COMPILER_CPLUSPLUS < 202002L
#  error "Iceberg public headers require C++20 or newer"
#endif

#if !ICEBERG_CXX20_COMPAT
#  if ICEBERG_COMPILER_CPLUSPLUS <= 202002L
#    error \
        "This Iceberg package requires C++23; rebuild with ICEBERG_CXX20_COMPAT=ON for C++20 consumers"
#  endif
#  if !defined(__cpp_explicit_this_parameter) || __cpp_explicit_this_parameter < 202110L
#    error "The default Iceberg API requires explicit object parameters"
#  endif
#  if !defined(__cpp_lib_byteswap) || __cpp_lib_byteswap < 202110L
#    error "The default Iceberg API requires std::byteswap"
#  endif
#  if !defined(__cpp_lib_unreachable) || __cpp_lib_unreachable < 202202L
#    error "The default Iceberg API requires std::unreachable"
#  endif
#endif
