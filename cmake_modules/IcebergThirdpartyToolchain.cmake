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

# Accumulate all dependencies to provide suitable static link parameters to the
# third party libraries.
set(ICEBERG_SYSTEM_DEPENDENCIES)
set(ICEBERG_ARROW_INSTALL_INTERFACE_LIBS)

# ----------------------------------------------------------------------
# Versions and URLs for toolchain builds

set(ICEBERG_ARROW_BUILD_VERSION "19.0.1")
set(ICEBERG_ARROW_BUILD_SHA256_CHECKSUM
    "acb76266e8b0c2fbb7eb15d542fbb462a73b3fd1e32b80fad6c2fafd95a51160")

if(DEFINED ENV{ICEBERG_ARROW_URL})
  set(ARROW_SOURCE_URL "$ENV{ICEBERG_ARROW_URL}")
else()
  set(ARROW_SOURCE_URL
      "https://www.apache.org/dyn/closer.lua?action=download&filename=/arrow/arrow-${ICEBERG_ARROW_BUILD_VERSION}/apache-arrow-${ICEBERG_ARROW_BUILD_VERSION}.tar.gz"
      "https://downloads.apache.org/arrow/arrow-${ICEBERG_ARROW_BUILD_VERSION}/apache-arrow-${ICEBERG_ARROW_BUILD_VERSION}.tar.gz"
  )
endif()

# ----------------------------------------------------------------------
# FetchContent

include(FetchContent)
set(FC_DECLARE_COMMON_OPTIONS)
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
  list(APPEND FC_DECLARE_COMMON_OPTIONS EXCLUDE_FROM_ALL TRUE)
endif()

macro(prepare_fetchcontent)
  set(BUILD_SHARED_LIBS OFF)
  set(BUILD_STATIC_LIBS ON)
  set(CMAKE_COMPILE_WARNING_AS_ERROR FALSE)
  set(CMAKE_EXPORT_NO_PACKAGE_REGISTRY TRUE)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endmacro()

# ----------------------------------------------------------------------
# Apache Arrow

function(resolve_arrow_dependency)
  prepare_fetchcontent()

  set(ARROW_BUILD_SHARED
      OFF
      CACHE BOOL "" FORCE)
  set(ARROW_BUILD_STATIC
      ON
      CACHE BOOL "" FORCE)
  set(ARROW_IPC
      OFF
      CACHE BOOL "" FORCE)
  set(ARROW_FILESYSTEM
      ON
      CACHE BOOL "" FORCE)
  set(ARROW_JSON
      ON
      CACHE BOOL "" FORCE)
  set(ARROW_PARQUET
      ON
      CACHE BOOL "" FORCE)
  set(ARROW_SIMD_LEVEL
      "NONE"
      CACHE STRING "" FORCE)
  set(ARROW_RUNTIME_SIMD_LEVEL
      "NONE"
      CACHE STRING "" FORCE)
  set(ARROW_POSITION_INDEPENDENT_CODE
      ON
      CACHE BOOL "" FORCE)
  set(ARROW_DEPENDENCY_SOURCE
      "BUNDLED"
      CACHE STRING "" FORCE)
  set(ARROW_WITH_ZLIB
      ON
      CACHE BOOL "" FORCE)
  set(ZLIB_SOURCE
      "SYSTEM"
      CACHE STRING "" FORCE)

  fetchcontent_declare(VendoredArrow
                       ${FC_DECLARE_COMMON_OPTIONS}
                       GIT_REPOSITORY https://github.com/apache/arrow.git
                       GIT_TAG f12356adaaabea86638407e995e73215dbb58bb2
                       #URL ${ARROW_SOURCE_URL}
                       #URL_HASH "SHA256=${ICEBERG_ARROW_BUILD_SHA256_CHECKSUM}"
                       SOURCE_SUBDIR
                       cpp
                       FIND_PACKAGE_ARGS
                       NAMES
                       Arrow
                       CONFIG)

  fetchcontent_makeavailable(VendoredArrow)

  if(vendoredarrow_SOURCE_DIR)
    if(NOT TARGET Arrow::arrow_static)
      add_library(Arrow::arrow_static INTERFACE IMPORTED)
      target_link_libraries(Arrow::arrow_static INTERFACE arrow_static)
      target_include_directories(Arrow::arrow_static
                                 INTERFACE ${vendoredarrow_BINARY_DIR}/src
                                           ${vendoredarrow_SOURCE_DIR}/cpp/src)
    endif()

    if(NOT TARGET Parquet::parquet_static)
      add_library(Parquet::parquet_static INTERFACE IMPORTED)
      target_link_libraries(Parquet::parquet_static INTERFACE parquet_static)
      target_include_directories(Parquet::parquet_static
                                 INTERFACE ${vendoredarrow_BINARY_DIR}/src
                                           ${vendoredarrow_SOURCE_DIR}/cpp/src)
    endif()

    set(ARROW_VENDORED TRUE)
    set_target_properties(arrow_static PROPERTIES OUTPUT_NAME "iceberg_vendored_arrow")
    set_target_properties(parquet_static PROPERTIES OUTPUT_NAME
                                                    "iceberg_vendored_parquet")
    install(TARGETS arrow_static parquet_static
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")

    if(TARGET arrow_bundled_dependencies)
      message(STATUS "arrow_bundled_dependencies found")
      # arrow_bundled_dependencies is only INSTALL_INTERFACE and will not be built by default.
      # We need to add it as a dependency to arrow_static so that it will be built.
      add_dependencies(arrow_static arrow_bundled_dependencies)
      # We cannot install an IMPORTED target, so we need to install the library manually.
      get_target_property(arrow_bundled_dependencies_location arrow_bundled_dependencies
                          IMPORTED_LOCATION)
      install(FILES ${arrow_bundled_dependencies_location}
              DESTINATION ${ICEBERG_INSTALL_LIBDIR})
    endif()
  else()
    set(ARROW_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES Arrow)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(ARROW_VENDORED
      ${ARROW_VENDORED}
      PARENT_SCOPE)
endfunction()

# ----------------------------------------------------------------------
# Apache Avro

function(resolve_avro_dependency)
  prepare_fetchcontent()

  set(AVRO_USE_BOOST
      OFF
      CACHE BOOL "" FORCE)

  set(AVRO_BUILD_EXECUTABLES
      OFF
      CACHE BOOL "" FORCE)

  set(AVRO_BUILD_TESTS
      OFF
      CACHE BOOL "" FORCE)

  fetchcontent_declare(Avro
                       ${FC_DECLARE_COMMON_OPTIONS}
                       # TODO: switch to Apache Avro 1.13.0 once released.
                       GIT_REPOSITORY https://github.com/apache/avro.git
                       GIT_TAG 82a2bc8b034de34626e2ab8bf091234122474d50
                       SOURCE_SUBDIR
                       lang/c++
                       FIND_PACKAGE_ARGS
                       NAMES
                       Avro
                       CONFIG)

  fetchcontent_makeavailable(Avro)

  if(avro_SOURCE_DIR)
    if(NOT TARGET Avro::avrocpp_static)
      add_library(Avro::avrocpp_static INTERFACE IMPORTED)
      target_link_libraries(Avro::avrocpp_static INTERFACE avrocpp_s)
      target_include_directories(Avro::avrocpp_static
                                 INTERFACE ${avro_BINARY_DIR} ${avro_SOURCE_DIR}/lang/c++)
    endif()

    set(AVRO_VENDORED TRUE)
    set_target_properties(avrocpp_s PROPERTIES OUTPUT_NAME "iceberg_vendored_avrocpp")
    set_target_properties(avrocpp_s PROPERTIES POSITION_INDEPENDENT_CODE ON)
    install(TARGETS avrocpp_s
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")

    # TODO: add vendored ZLIB and Snappy support
    find_package(Snappy CONFIG)
    if(Snappy_FOUND)
      list(APPEND ICEBERG_SYSTEM_DEPENDENCIES Snappy)
    endif()
  else()
    set(AVRO_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES Avro)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(AVRO_VENDORED
      ${AVRO_VENDORED}
      PARENT_SCOPE)
endfunction()

# ----------------------------------------------------------------------
# Nanoarrow

# It is also possible to vendor nanoarrow using the bundled source code.
function(resolve_nanoarrow_dependency)
  prepare_fetchcontent()

  fetchcontent_declare(nanoarrow
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL "https://dlcdn.apache.org/arrow/apache-arrow-nanoarrow-0.7.0/apache-arrow-nanoarrow-0.7.0.tar.gz"
  )
  fetchcontent_makeavailable(nanoarrow)

  set_target_properties(nanoarrow_static
                        PROPERTIES OUTPUT_NAME "iceberg_vendored_nanoarrow"
                                   POSITION_INDEPENDENT_CODE ON)
  install(TARGETS nanoarrow_static
          EXPORT iceberg_targets
          RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
          ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
          LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
endfunction()

# ----------------------------------------------------------------------
# nlohmann-json

function(resolve_nlohmann_json_dependency)
  prepare_fetchcontent()

  set(JSON_BuildTests
      OFF
      CACHE BOOL "" FORCE)

  fetchcontent_declare(nlohmann_json
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL "https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz"
  )
  fetchcontent_makeavailable(nlohmann_json)

  set_target_properties(nlohmann_json
                        PROPERTIES OUTPUT_NAME "iceberg_vendored_nlohmann_json"
                                   POSITION_INDEPENDENT_CODE ON)
  if(MSVC_TOOLCHAIN)
    set(NLOHMANN_NATVIS_FILE ${nlohmann_json_SOURCE_DIR}/nlohmann_json.natvis)
    install(FILES ${NLOHMANN_NATVIS_FILE} DESTINATION .)
  endif()

  install(TARGETS nlohmann_json
          EXPORT iceberg_targets
          RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
          ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
          LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
endfunction()

# ----------------------------------------------------------------------
# spdlog

function(resolve_spdlog_dependency)
  prepare_fetchcontent()

  find_package(Threads REQUIRED)

  set(SPDLOG_USE_STD_FORMAT
      ON
      CACHE BOOL "" FORCE)
  set(SPDLOG_BUILD_PIC
      ON
      CACHE BOOL "" FORCE)

  fetchcontent_declare(spdlog
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL "https://github.com/gabime/spdlog/archive/refs/tags/v1.15.3.tar.gz"
                           FIND_PACKAGE_ARGS
                           NAMES
                           spdlog
                           CONFIG)
  fetchcontent_makeavailable(spdlog)

  if(spdlog_SOURCE_DIR)
    set_target_properties(spdlog PROPERTIES OUTPUT_NAME "iceberg_vendored_spdlog"
                                            POSITION_INDEPENDENT_CODE ON)
    target_link_libraries(spdlog INTERFACE Threads::Threads)
    install(TARGETS spdlog
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
    set(SPDLOG_VENDORED TRUE)
  else()
    set(SPDLOG_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES spdlog)
  endif()

  list(APPEND ICEBERG_SYSTEM_DEPENDENCIES Threads)

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(SPDLOG_VENDORED
      ${SPDLOG_VENDORED}
      PARENT_SCOPE)
endfunction()

# ----------------------------------------------------------------------
# zlib

function(resolve_zlib_dependency)
  # use system zlib, zlib is required by arrow and avro
  find_package(ZLIB REQUIRED)
  if(ZLIB_FOUND)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES ZLIB)
    message(STATUS "ZLIB_FOUND ZLIB_LIBRARIES:${ZLIB_LIBRARIES} ZLIB_INCLUDE_DIR:${ZLIB_INCLUDE_DIR}"
    )
    set(ICEBERG_SYSTEM_DEPENDENCIES
        ${ICEBERG_SYSTEM_DEPENDENCIES}
        PARENT_SCOPE)
  endif()

endfunction()

# ----------------------------------------------------------------------
# Zstd

function(resolve_zstd_dependency)
  find_package(zstd CONFIG)
  if(zstd_FOUND)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES zstd)
    message(STATUS "Found zstd, version: ${zstd_VERSION}")
    set(ICEBERG_SYSTEM_DEPENDENCIES
        ${ICEBERG_SYSTEM_DEPENDENCIES}
        PARENT_SCOPE)
  endif()
endfunction()

resolve_zlib_dependency()
resolve_nanoarrow_dependency()
resolve_nlohmann_json_dependency()
resolve_spdlog_dependency()

if(ICEBERG_BUILD_BUNDLE)
  resolve_arrow_dependency()
  resolve_avro_dependency()
  resolve_zstd_dependency()
endif()
