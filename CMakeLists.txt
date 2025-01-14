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
        VERSION 0.1.0
        DESCRIPTION "Iceberg C++ Project"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ICEBERG_BUILD_STATIC "Build static library" ON)
option(ICEBERG_BUILD_SHARED "Build shared library" OFF)
option(ICEBERG_BUILD_TESTS "Build tests" ON)
option(ICEBERG_ARROW "Build Arrow" ON)

include(GNUInstallDirs)
include(FetchContent)

set(ICEBERG_INSTALL_LIBDIR "${CMAKE_INSTALL_LIBDIR}")
set(ICEBERG_INSTALL_BINDIR "${CMAKE_INSTALL_BINDIR}")
set(ICEBERG_INSTALL_INCLUDEDIR "${CMAKE_INSTALL_INCLUDEDIR}")
set(ICEBERG_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake")
set(ICEBERG_INSTALL_DOCDIR "share/doc/${PROJECT_NAME}")
set(ICEBERG_INCLUDES "${PROJECT_BINARY_DIR}/src" "${PROJECT_SOURCE_DIR}/src")

if(WIN32 AND NOT MINGW)
  set(MSVC_TOOLCHAIN TRUE)
else()
  set(MSVC_TOOLCHAIN FALSE)
endif()

include(CMakeParseArguments)
include(IcebergBuildUtils)
include(IcebergThirdpartyToolchain)
include(GenerateExportHeader)

add_subdirectory(src)

if(ICEBERG_BUILD_TESTS)
  enable_testing()
  add_subdirectory(test)
endif()

install(FILES LICENSE NOTICE DESTINATION ${ICEBERG_INSTALL_DOCDIR})
