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

set(ICEBERG_ARROW_BUILD_VERSION "18.1.0")
set(ICEBERG_ARROW_BUILD_SHA256_CHECKSUM
    "2dc8da5f8796afe213ecc5e5aba85bb82d91520eff3cf315784a52d0fa61d7fc")

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
  # To workaround https://github.com/apache/arrow/pull/45513
  set(ARROW_IPC
      ON
      CACHE BOOL "" FORCE)
  set(ARROW_FILESYSTEM
      OFF
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
      "AUTO"
      CACHE STRING "" FORCE)

  fetchcontent_declare(Arrow
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL ${ARROW_SOURCE_URL}
                       URL_HASH "SHA256=${ICEBERG_ARROW_BUILD_SHA256_CHECKSUM}"
                       SOURCE_SUBDIR
                       cpp
                       FIND_PACKAGE_ARGS
                       NAMES
                       Arrow
                       CONFIG)

  fetchcontent_makeavailable(Arrow)

  if(arrow_SOURCE_DIR)
    if(NOT TARGET Arrow::arrow_static)
      add_library(Arrow::arrow_static INTERFACE IMPORTED)
      target_link_libraries(Arrow::arrow_static INTERFACE arrow_static)
      target_include_directories(Arrow::arrow_static
                                 INTERFACE ${arrow_BINARY_DIR}/src
                                           ${arrow_SOURCE_DIR}/cpp/src)
    endif()

    set(ARROW_VENDORED TRUE)
    set_target_properties(arrow_static PROPERTIES OUTPUT_NAME "iceberg_vendored_arrow")
    install(TARGETS arrow_static
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
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

if(ICEBERG_ARROW)
  resolve_arrow_dependency()
endif()

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
                       GIT_TAG c75e6d59fbacd05df666665d9c8b7e3274cf0399
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
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES ZLIB)
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

if(ICEBERG_AVRO)
  resolve_avro_dependency()
endif()

# ----------------------------------------------------------------------
# Nanoarrow

# It is also possible to vendor nanoarrow using the bundled source code.
function(resolve_nanoarrow_dependency)
  prepare_fetchcontent()

  fetchcontent_declare(nanoarrow
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL "https://dlcdn.apache.org/arrow/apache-arrow-nanoarrow-0.6.0/apache-arrow-nanoarrow-0.6.0.tar.gz"
  )
  fetchcontent_makeavailable(nanoarrow)

  set_target_properties(nanoarrow PROPERTIES OUTPUT_NAME "iceberg_vendored_nanoarrow"
                                             POSITION_INDEPENDENT_CODE ON)
  install(TARGETS nanoarrow
          EXPORT iceberg_targets
          RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
          ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
          LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
endfunction()

resolve_nanoarrow_dependency()
