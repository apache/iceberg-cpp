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

set(ICEBERG_ARROW_BUILD_VERSION "21.0.0")
set(ICEBERG_ARROW_BUILD_SHA256_CHECKSUM
    "5d3f8db7e72fb9f65f4785b7a1634522e8d8e9657a445af53d4a34a3849857b5")

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
  # Use "NEW" for CMP0077 by default.
  #
  # https://cmake.org/cmake/help/latest/policy/CMP0077.html
  #
  # option() honors normal variables.
  set(CMAKE_POLICY_DEFAULT_CMP0077
      NEW
      CACHE STRING "")
endmacro()

# ----------------------------------------------------------------------
# Apache Arrow

function(resolve_arrow_dependency)
  prepare_fetchcontent()

  set(ARROW_BUILD_SHARED OFF)
  set(ARROW_BUILD_STATIC ON)
  # Work around undefined symbol: arrow::ipc::ReadSchema(arrow::io::InputStream*, arrow::ipc::DictionaryMemo*)
  set(ARROW_IPC ON)
  set(ARROW_FILESYSTEM ON)
  set(ARROW_JSON ON)
  set(ARROW_PARQUET ON)
  set(ARROW_SIMD_LEVEL "NONE")
  set(ARROW_RUNTIME_SIMD_LEVEL "NONE")
  set(ARROW_POSITION_INDEPENDENT_CODE ON)
  set(ARROW_DEPENDENCY_SOURCE "BUNDLED")
  set(ARROW_WITH_ZLIB ON)
  set(ZLIB_SOURCE "SYSTEM")
  set(ARROW_VERBOSE_THIRDPARTY_BUILD OFF)

  fetchcontent_declare(VendoredArrow
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL ${ARROW_SOURCE_URL}
                       URL_HASH "SHA256=${ICEBERG_ARROW_BUILD_SHA256_CHECKSUM}"
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

  fetchcontent_declare(avro-cpp
                       ${FC_DECLARE_COMMON_OPTIONS}
                       # TODO: switch to Apache Avro 1.13.0 once released.
                       GIT_REPOSITORY https://github.com/apache/avro.git
                       GIT_TAG e6c308780e876b4c11a470b9900995947f7b0fb5
                       SOURCE_SUBDIR
                       lang/c++
                       FIND_PACKAGE_ARGS
                       NAMES
                       avro-cpp
                       CONFIG)

  fetchcontent_makeavailable(avro-cpp)

  if(avro-cpp_SOURCE_DIR)
    if(NOT TARGET avro-cpp::avrocpp_static)
      add_library(avro-cpp::avrocpp_static INTERFACE IMPORTED)
      target_link_libraries(avro-cpp::avrocpp_static INTERFACE avrocpp_s)
      target_include_directories(avro-cpp::avrocpp_static
                                 INTERFACE ${avro-cpp_BINARY_DIR}
                                           ${avro-cpp_SOURCE_DIR}/lang/c++)
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
                           FIND_PACKAGE_ARGS
                           NAMES
                           nanoarrow
                           CONFIG)
  fetchcontent_makeavailable(nanoarrow)

  if(nanoarrow_SOURCE_DIR)
    if(NOT TARGET nanoarrow::nanoarrow_static)
      add_library(nanoarrow::nanoarrow_static INTERFACE IMPORTED)
      target_link_libraries(nanoarrow::nanoarrow_static INTERFACE nanoarrow_static)
      target_include_directories(nanoarrow::nanoarrow_static
                                 INTERFACE ${nanoarrow_BINARY_DIR}
                                           ${nanoarrow_SOURCE_DIR})
    endif()

    set(NANOARROW_VENDORED TRUE)
    set_target_properties(nanoarrow_static
                          PROPERTIES OUTPUT_NAME "iceberg_vendored_nanoarrow"
                                     POSITION_INDEPENDENT_CODE ON)
    install(TARGETS nanoarrow_static
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
  else()
    set(NANOARROW_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES nanoarrow)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(NANOARROW_VENDORED
      ${NANOARROW_VENDORED}
      PARENT_SCOPE)
endfunction()

# ----------------------------------------------------------------------
# CRoaring

function(resolve_croaring_dependency)
  prepare_fetchcontent()

  set(BUILD_TESTING
      OFF
      CACHE BOOL "Disable CRoaring tests" FORCE)

  fetchcontent_declare(croaring
                       ${FC_DECLARE_COMMON_OPTIONS}
                       URL "https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v4.3.11.tar.gz"
                           FIND_PACKAGE_ARGS
                           NAMES
                           roaring
                           CONFIG)
  fetchcontent_makeavailable(croaring)

  if(croaring_SOURCE_DIR)
    if(NOT TARGET roaring::roaring)
      add_library(roaring::roaring INTERFACE IMPORTED)
      target_link_libraries(roaring::roaring INTERFACE roaring)
      target_include_directories(roaring::roaring INTERFACE ${croaring_BINARY_DIR}
                                                            ${croaring_SOURCE_DIR}/cpp)
    endif()

    set(CROARING_VENDORED TRUE)
    set_target_properties(roaring PROPERTIES OUTPUT_NAME "iceberg_vendored_croaring"
                                             POSITION_INDEPENDENT_CODE ON)
    install(TARGETS roaring
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
  else()
    set(CROARING_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES roaring)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(CROARING_VENDORED
      ${CROARING_VENDORED}
      PARENT_SCOPE)
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
                           FIND_PACKAGE_ARGS
                           NAMES
                           nlohmann_json
                           CONFIG)
  fetchcontent_makeavailable(nlohmann_json)

  if(nlohmann_json_SOURCE_DIR)
    if(NOT TARGET nlohmann_json::nlohmann_json)
      add_library(nlohmann_json::nlohmann_json INTERFACE IMPORTED)
      target_link_libraries(nlohmann_json::nlohmann_json INTERFACE nlohmann_json)
      target_include_directories(nlohmann_json::nlohmann_json
                                 INTERFACE ${nlohmann_json_BINARY_DIR}
                                           ${nlohmann_json_SOURCE_DIR})
    endif()

    set(NLOHMANN_JSON_VENDORED TRUE)
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
  else()
    set(NLOHMANN_JSON_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES nlohmann_json)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(NLOHMANN_JSON_VENDORED
      ${NLOHMANN_JSON_VENDORED}
      PARENT_SCOPE)
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
# CURL

function(resolve_curl_dependency)
  prepare_fetchcontent()

  set(BUILD_CURL_EXE
      OFF
      CACHE BOOL "" FORCE)
  set(BUILD_TESTING
      OFF
      CACHE BOOL "" FORCE)
  set(CURL_ENABLE_EXPORT_TARGET
      OFF
      CACHE BOOL "" FORCE)
  set(BUILD_SHARED_LIBS
      OFF
      CACHE BOOL "" FORCE)
  set(CURL_STATICLIB
      ON
      CACHE BOOL "" FORCE)
  set(HTTP_ONLY
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_LDAP
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_LDAPS
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_RTSP
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_FTP
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_FILE
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_TELNET
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_DICT
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_TFTP
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_GOPHER
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_POP3
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_IMAP
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_SMTP
      ON
      CACHE BOOL "" FORCE)
  set(CURL_DISABLE_SMB
      ON
      CACHE BOOL "" FORCE)
  set(CURL_CA_BUNDLE
      "auto"
      CACHE STRING "" FORCE)
  set(USE_LIBIDN2
      OFF
      CACHE BOOL "" FORCE)

  fetchcontent_declare(CURL
                       ${FC_DECLARE_COMMON_OPTIONS}
                       GIT_REPOSITORY https://github.com/curl/curl.git
                       GIT_TAG curl-8_11_0
                       FIND_PACKAGE_ARGS
                       NAMES
                       CURL
                       CONFIG)

  fetchcontent_makeavailable(CURL)

  if(curl_SOURCE_DIR)
    if(NOT TARGET CURL::libcurl)
      add_library(CURL::libcurl INTERFACE IMPORTED)
      target_link_libraries(CURL::libcurl INTERFACE libcurl_static)
      target_include_directories(CURL::libcurl INTERFACE ${curl_BINARY_DIR}/include
                                                         ${curl_SOURCE_DIR}/include)
    endif()

    set(CURL_VENDORED TRUE)
    set_target_properties(libcurl_static PROPERTIES OUTPUT_NAME "iceberg_vendored_curl"
                                                    POSITION_INDEPENDENT_CODE ON)
    add_library(Iceberg::libcurl_static ALIAS libcurl_static)
    install(TARGETS libcurl_static
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
    message(STATUS "Use vendored CURL")
  else()
    set(CURL_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES CURL)
    message(STATUS "Use system CURL")
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(CURL_VENDORED
      ${CURL_VENDORED}
      PARENT_SCOPE)
endfunction()

# ----------------------------------------------------------------------
# cpr

function(resolve_cpr_dependency)
  prepare_fetchcontent()

  set(CPR_BUILD_TESTS
      OFF
      CACHE BOOL "" FORCE)
  set(CPR_BUILD_TESTS_SSL
      OFF
      CACHE BOOL "" FORCE)
  set(CPR_ENABLE_SSL
      ON
      CACHE BOOL "" FORCE)
  set(CPR_USE_SYSTEM_CURL
      ON
      CACHE BOOL "" FORCE)
  set(CPR_CURL_NOSIGNAL
      ON
      CACHE BOOL "" FORCE)

  set(CURL_VERSION_STRING
      "8.11.0"
      CACHE STRING "" FORCE)
  set(CURL_LIB
      "CURL::libcurl"
      CACHE STRING "" FORCE)

  fetchcontent_declare(cpr
                       ${FC_DECLARE_COMMON_OPTIONS}
                       GIT_REPOSITORY https://github.com/libcpr/cpr.git
                       GIT_TAG 1.11.0
                       FIND_PACKAGE_ARGS
                       NAMES
                       cpr
                       CONFIG)

  # Create a custom install command that does nothing
  # function(install)
  #   # Do nothing - effectively disables install
  # endfunction()

  fetchcontent_makeavailable(cpr)

  # Restore the original install function
  # unset(install)

  if(cpr_SOURCE_DIR)
    if(NOT TARGET cpr::cpr)
      add_library(cpr::cpr INTERFACE IMPORTED)
      target_link_libraries(cpr::cpr INTERFACE cpr)
      target_include_directories(cpr::cpr INTERFACE ${cpr_BINARY_DIR}
                                                    ${cpr_SOURCE_DIR}/include)
    endif()

    set(CPR_VENDORED TRUE)
    set_target_properties(cpr PROPERTIES OUTPUT_NAME "iceberg_vendored_cpr"
                                         POSITION_INDEPENDENT_CODE ON)
    add_library(Iceberg::cpr ALIAS cpr)
    install(TARGETS cpr
            EXPORT iceberg_targets
            RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}"
            ARCHIVE DESTINATION "${ICEBERG_INSTALL_LIBDIR}"
            LIBRARY DESTINATION "${ICEBERG_INSTALL_LIBDIR}")
  else()
    set(CPR_VENDORED FALSE)
    list(APPEND ICEBERG_SYSTEM_DEPENDENCIES cpr)
  endif()

  set(ICEBERG_SYSTEM_DEPENDENCIES
      ${ICEBERG_SYSTEM_DEPENDENCIES}
      PARENT_SCOPE)
  set(CPR_VENDORED
      ${CPR_VENDORED}
      PARENT_SCOPE)
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
resolve_croaring_dependency()
resolve_nlohmann_json_dependency()
resolve_spdlog_dependency()

resolve_curl_dependency()
resolve_cpr_dependency()

if(ICEBERG_BUILD_BUNDLE)
  resolve_arrow_dependency()
  resolve_avro_dependency()
  resolve_zstd_dependency()
endif()
