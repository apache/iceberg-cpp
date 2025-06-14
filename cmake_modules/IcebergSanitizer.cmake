# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

add_library(iceberg_sanitizer_flags INTERFACE)

if(ICEBERG_ENABLE_ASAN)
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(iceberg_sanitizer_flags INTERFACE -fsanitize=address
                                                             -fno-omit-frame-pointer)
    target_link_options(iceberg_sanitizer_flags INTERFACE -fsanitize=address)
    message(STATUS "Address Sanitizer enabled")
  else()
    message(WARNING "Address Sanitizer is only supported for GCC and Clang compilers")
  endif()
endif()

if(ICEBERG_ENABLE_UBSAN)
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(iceberg_sanitizer_flags INTERFACE -fsanitize=undefined
                                                             -fno-omit-frame-pointer)
    target_link_options(iceberg_sanitizer_flags INTERFACE -fsanitize=undefined)
    message(STATUS "Undefined Behavior Sanitizer enabled")
  else()
    message(WARNING "Undefined Behavior Sanitizer is only supported for GCC and Clang compilers"
    )
  endif()
endif()
