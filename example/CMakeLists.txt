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

# Please set CMAKE_PREFIX_PATH to the install prefix of iceberg.
cmake_minimum_required(VERSION 3.25)

project(example)

# C++20 is the minimum standard iceberg-cpp's public headers support, so the
# example builds as C++20 by default to keep that contract exercised. Set this to
# 23 to also check the headers from a C++23 consumer.
set(ICEBERG_EXAMPLE_CXX_STANDARD
    20
    CACHE STRING "C++ standard used to build the example (20 or 23)")
set_property(CACHE ICEBERG_EXAMPLE_CXX_STANDARD PROPERTY STRINGS 20 23)
if(NOT ICEBERG_EXAMPLE_CXX_STANDARD MATCHES "^(20|23)$")
  message(FATAL_ERROR "ICEBERG_EXAMPLE_CXX_STANDARD must be 20 or 23, got "
                      "'${ICEBERG_EXAMPLE_CXX_STANDARD}'")
endif()

set(CMAKE_CXX_STANDARD ${ICEBERG_EXAMPLE_CXX_STANDARD})
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(iceberg CONFIG REQUIRED COMPONENTS bundle rest)

if(TARGET iceberg::iceberg_bundle_shared)
  set(ICEBERG_BUNDLE_TARGET iceberg::iceberg_bundle_shared)
else()
  set(ICEBERG_BUNDLE_TARGET iceberg::iceberg_bundle_static)
endif()

if(TARGET iceberg::iceberg_rest_shared)
  set(ICEBERG_REST_TARGET iceberg::iceberg_rest_shared)
else()
  set(ICEBERG_REST_TARGET iceberg::iceberg_rest_static)
endif()

add_executable(demo_example demo_example.cc)

target_link_libraries(demo_example PRIVATE ${ICEBERG_BUNDLE_TARGET}
                                           ${ICEBERG_REST_TARGET})

# Compile every installed public header as a consumer using
# ICEBERG_EXAMPLE_CXX_STANDARD. The installed include
# tree is the public API contract: iceberg_install_all_headers excludes internal
# headers before packaging them.
get_target_property(ICEBERG_BUNDLE_INCLUDE_DIRS ${ICEBERG_BUNDLE_TARGET}
                    INTERFACE_INCLUDE_DIRECTORIES)
foreach(ICEBERG_INCLUDE_DIR IN LISTS ICEBERG_BUNDLE_INCLUDE_DIRS)
  if(EXISTS "${ICEBERG_INCLUDE_DIR}/iceberg")
    set(ICEBERG_PUBLIC_INCLUDE_DIR "${ICEBERG_INCLUDE_DIR}")
    break()
  endif()
endforeach()

if(NOT ICEBERG_PUBLIC_INCLUDE_DIR)
  message(FATAL_ERROR "Could not locate iceberg's installed public headers")
endif()

file(GLOB_RECURSE
     ICEBERG_PUBLIC_HEADERS
     CONFIGURE_DEPENDS
     "${ICEBERG_PUBLIC_INCLUDE_DIR}/iceberg/*.h"
     "${ICEBERG_PUBLIC_INCLUDE_DIR}/iceberg/*.hpp")
list(SORT ICEBERG_PUBLIC_HEADERS)

set(ICEBERG_PUBLIC_HEADER_CHECK_SOURCE
    "// Generated from iceberg's installed public headers.\n")
foreach(ICEBERG_PUBLIC_HEADER IN LISTS ICEBERG_PUBLIC_HEADERS)
  file(RELATIVE_PATH ICEBERG_PUBLIC_HEADER_RELATIVE_PATH "${ICEBERG_PUBLIC_INCLUDE_DIR}"
       "${ICEBERG_PUBLIC_HEADER}")
  string(APPEND ICEBERG_PUBLIC_HEADER_CHECK_SOURCE
         "#include <${ICEBERG_PUBLIC_HEADER_RELATIVE_PATH}>\n")
endforeach()
string(APPEND ICEBERG_PUBLIC_HEADER_CHECK_SOURCE "\nint main() { return 0; }\n")

file(GENERATE
     OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/public_headers_check.cc"
     CONTENT "${ICEBERG_PUBLIC_HEADER_CHECK_SOURCE}")

add_executable(public_headers_check "${CMAKE_CURRENT_BINARY_DIR}/public_headers_check.cc")
target_link_libraries(public_headers_check PRIVATE ${ICEBERG_BUNDLE_TARGET}
                                                   ${ICEBERG_REST_TARGET})
