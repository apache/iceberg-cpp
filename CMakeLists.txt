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

cmake_minimum_required(VERSION 3.25)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules")

project(Iceberg
        VERSION 0.2.0
        DESCRIPTION "Iceberg C++ Project"
        LANGUAGES CXX)

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/iceberg/version.h.in"
               "${CMAKE_BINARY_DIR}/src/iceberg/version.h")

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_COMPILE_WARNING_AS_ERROR ON)

option(ICEBERG_BUILD_STATIC "Build static library" ON)
option(ICEBERG_BUILD_SHARED "Build shared library" OFF)
option(ICEBERG_BUILD_TESTS "Build tests" ON)
option(ICEBERG_BUILD_BUNDLE "Build the battery included library" ON)
option(ICEBERG_BUILD_REST "Build rest catalog client" ON)
option(ICEBERG_BUILD_REST_INTEGRATION_TESTS "Build rest catalog integration tests" OFF)
option(ICEBERG_ENABLE_ASAN "Enable Address Sanitizer" OFF)
option(ICEBERG_ENABLE_UBSAN "Enable Undefined Behavior Sanitizer" OFF)

include(GNUInstallDirs)
include(FetchContent)

set(ICEBERG_INSTALL_LIBDIR "${CMAKE_INSTALL_LIBDIR}")
set(ICEBERG_INSTALL_BINDIR "${CMAKE_INSTALL_BINDIR}")
set(ICEBERG_INSTALL_INCLUDEDIR "${CMAKE_INSTALL_INCLUDEDIR}")
set(ICEBERG_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake")
set(ICEBERG_INSTALL_DOCDIR "share/doc/iceberg")

if(WIN32 AND NOT MINGW)
  set(MSVC_TOOLCHAIN TRUE)
else()
  set(MSVC_TOOLCHAIN FALSE)
endif()

if(ICEBERG_BUILD_REST_INTEGRATION_TESTS AND WIN32)
  set(ICEBERG_BUILD_REST_INTEGRATION_TESTS OFF)
  message(WARNING "Cannot build rest integration test on Windows, turning it off.")
endif()

include(CMakeParseArguments)
include(IcebergBuildUtils)
include(IcebergSanitizer)
include(IcebergSccache)
include(IcebergThirdpartyToolchain)

if(ICEBERG_BUILD_TESTS)
  enable_testing()
endif()

add_subdirectory(src)
install(FILES LICENSE NOTICE DESTINATION ${ICEBERG_INSTALL_DOCDIR})
