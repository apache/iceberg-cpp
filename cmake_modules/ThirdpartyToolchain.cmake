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

include(ProcessorCount)
ProcessorCount(NPROC)

# Accumulate all dependencies to provide suitable static link parameters
# to the third party libraries.
set(ICEBERG_ARROW_SYSTEM_DEPENDENCIES)
set(ICEBERG_ARROW_VENDOR_DEPENDENCIES)
set(ICEBERG_ARROW_INSTALL_INTERFACE_LIBS)

# ----------------------------------------------------------------------
# Resolve the dependencies

set(ICEBERG_THIRDPARTY_DEPENDENCIES Arrow)

message(
  STATUS "Using ${ICEBERG_DEPENDENCY_SOURCE} approach to find dependencies")

# For each dependency, set dependency source to global default, if unset
foreach(DEPENDENCY ${ICEBERG_THIRDPARTY_DEPENDENCIES})
  if("${${DEPENDENCY}_SOURCE}" STREQUAL "")
    set(${DEPENDENCY}_SOURCE ${ICEBERG_DEPENDENCY_SOURCE})
  endif()
endforeach()

macro(build_dependency DEPENDENCY_NAME)
  if("${DEPENDENCY_NAME}" STREQUAL "Arrow")
    build_arrow()
  else()
    message(
      FATAL_ERROR "Unknown thirdparty dependency to build: ${DEPENDENCY_NAME}")
  endif()
endmacro()

function(provide_cmake_module MODULE_NAME ICEBERG_CMAKE_PACKAGE_NAME)
  set(module "${CMAKE_SOURCE_DIR}/cmake_modules/${MODULE_NAME}.cmake")
  if(EXISTS "${module}")
    message(
      STATUS
        "Providing CMake module for ${MODULE_NAME} as part of ${ICEBERG_CMAKE_PACKAGE_NAME} CMake package"
    )
    install(
      FILES "${module}"
      DESTINATION "${ICEBERG_INSTALL_CMAKEDIR}/${ICEBERG_CMAKE_PACKAGE_NAME}")
  endif()
endfunction()

# Find modules are needed by the consumer in case of a static build, or if the
# linkage is PUBLIC or INTERFACE.
function(provide_find_module PACKAGE_NAME ICEBERG_CMAKE_PACKAGE_NAME)
  provide_cmake_module("Find${PACKAGE_NAME}" ${ICEBERG_CMAKE_PACKAGE_NAME})
endfunction()

macro(resolve_dependency DEPENDENCY_NAME)
  set(options)
  set(one_value_args ICEBERG_CMAKE_PACKAGE_NAME FORCE_ANY_NEWER_VERSION
                     HAVE_ALT REQUIRED_VERSION USE_CONFIG)
  set(multi_value_args COMPONENTS OPTIONAL_COMPONENTS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}"
                        "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(
      SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(ARG_HAVE_ALT)
    set(PACKAGE_NAME "${DEPENDENCY_NAME}Alt")
  else()
    set(PACKAGE_NAME ${DEPENDENCY_NAME})
  endif()
  set(FIND_PACKAGE_ARGUMENTS ${PACKAGE_NAME})
  if(ARG_REQUIRED_VERSION AND NOT ARG_FORCE_ANY_NEWER_VERSION)
    list(APPEND FIND_PACKAGE_ARGUMENTS ${ARG_REQUIRED_VERSION})
  endif()
  if(ARG_USE_CONFIG)
    list(APPEND FIND_PACKAGE_ARGUMENTS CONFIG)
  endif()
  if(ARG_COMPONENTS)
    list(APPEND FIND_PACKAGE_ARGUMENTS COMPONENTS ${ARG_COMPONENTS})
  endif()
  if(ARG_OPTIONAL_COMPONENTS)
    list(APPEND FIND_PACKAGE_ARGUMENTS OPTIONAL_COMPONENTS
         ${ARG_OPTIONAL_COMPONENTS})
  endif()

  if(${DEPENDENCY_NAME}_SOURCE STREQUAL "AUTO")
    find_package(${FIND_PACKAGE_ARGUMENTS})
    set(COMPATIBLE ${${PACKAGE_NAME}_FOUND})
    if(COMPATIBLE
       AND ARG_FORCE_ANY_NEWER_VERSION
       AND ARG_REQUIRED_VERSION)
      if(${${PACKAGE_NAME}_VERSION} VERSION_LESS ${ARG_REQUIRED_VERSION})
        message(DEBUG
                "Couldn't find ${DEPENDENCY_NAME} >= ${ARG_REQUIRED_VERSION}")
        set(COMPATIBLE FALSE)
      endif()
    endif()
    if(COMPATIBLE)
      set(${DEPENDENCY_NAME}_SOURCE "SYSTEM")
    else()
      build_dependency(${DEPENDENCY_NAME})
      set(${DEPENDENCY_NAME}_SOURCE "VENDOR")
    endif()
  elseif(${DEPENDENCY_NAME}_SOURCE STREQUAL "VENDOR")
    build_dependency(${DEPENDENCY_NAME})
  elseif(${DEPENDENCY_NAME}_SOURCE STREQUAL "SYSTEM")
    find_package(${FIND_PACKAGE_ARGUMENTS} REQUIRED)
    if(ARG_FORCE_ANY_NEWER_VERSION AND ARG_REQUIRED_VERSION)
      if(${${PACKAGE_NAME}_VERSION} VERSION_LESS ${ARG_REQUIRED_VERSION})
        message(
          FATAL_ERROR
            "Couldn't find ${DEPENDENCY_NAME} >= ${ARG_REQUIRED_VERSION}")
      endif()
    endif()
  endif()
  if(${DEPENDENCY_NAME}_SOURCE STREQUAL "SYSTEM")
    # IcebergCore -> _Iceberg_Core
    string(REGEX REPLACE "([A-Z])" "_\\1" ARG_ICEBERG_CMAKE_PACKAGE_NAME_SNAKE
                         ${ARG_ICEBERG_CMAKE_PACKAGE_NAME})
    # _Iceberg_Core -> Iceberg_Core
    string(SUBSTRING ${ARG_ICEBERG_CMAKE_PACKAGE_NAME_SNAKE} 1 -1
                     ARG_ICEBERG_CMAKE_PACKAGE_NAME_SNAKE)
    # Iceberg_Core -> ICEBERG_CORE
    string(TOUPPER ${ARG_ICEBERG_CMAKE_PACKAGE_NAME_SNAKE}
                   ARG_ICEBERG_CMAKE_PACKAGE_NAME_UPPER_SNAKE)
    provide_find_module(${PACKAGE_NAME} ${ARG_ICEBERG_CMAKE_PACKAGE_NAME})
    list(APPEND
         ${ARG_ICEBERG_CMAKE_PACKAGE_NAME_UPPER_SNAKE}_SYSTEM_DEPENDENCIES
         ${PACKAGE_NAME})
  endif()
endmacro()

# ----------------------------------------------------------------------
# Thirdparty versions, environment variables, source URLs

set(THIRDPARTY_DIR "${CMAKE_SOURCE_DIR}/thirdparty")

# ----------------------------------------------------------------------
# Versions and URLs for toolchain builds, which also can be used to configure
# offline builds Note: We should not use the Apache dist server for build
# dependencies

macro(set_urls URLS)
  set(${URLS} ${ARGN})
endmacro()

# Read toolchain versions from thirdparty/versions.txt
file(STRINGS "${THIRDPARTY_DIR}/versions.txt" TOOLCHAIN_VERSIONS_TXT)
foreach(_VERSION_ENTRY ${TOOLCHAIN_VERSIONS_TXT})
  # Exclude comments
  if(NOT ((_VERSION_ENTRY MATCHES "^[^#][A-Za-z0-9-_]+_VERSION=")
          OR (_VERSION_ENTRY MATCHES "^[^#][A-Za-z0-9-_]+_CHECKSUM=")))
    continue()
  endif()

  string(REGEX MATCH "^[^=]*" _VARIABLE_NAME ${_VERSION_ENTRY})
  string(REPLACE "${_VARIABLE_NAME}=" "" _VARIABLE_VALUE ${_VERSION_ENTRY})

  # Skip blank or malformed lines
  if(_VARIABLE_VALUE STREQUAL "")
    continue()
  endif()

  # For debugging
  message(STATUS "${_VARIABLE_NAME}: ${_VARIABLE_VALUE}")

  set(${_VARIABLE_NAME} ${_VARIABLE_VALUE})
endforeach()

if(DEFINED ENV{ICEBERG_ARROW_URL})
  set(ARROW_SOURCE_URL "$ENV{ICEBERG_ARROW_URL}")
else()
  set_urls(
    ARROW_SOURCE_URL
    "https://www.apache.org/dyn/closer.cgi?action=download&filename=/arrow/arrow-${ICEBERG_ARROW_BUILD_VERSION}/apache-arrow-${ICEBERG_ARROW_BUILD_VERSION}.tar.gz"
    "https://downloads.apache.org/arrow/arrow-${ICEBERG_ARROW_BUILD_VERSION}/apache-arrow-${ICEBERG_ARROW_BUILD_VERSION}.tar.gz"
    "https://github.com/apache/arrow/releases/download/apache-arrow-${ICEBERG_ARROW_BUILD_VERSION}/apache-arrow-${ICEBERG_ARROW_BUILD_VERSION}.tar.gz"
  )
endif()

# ----------------------------------------------------------------------
# ExternalProject options

set(EP_LIST_SEPARATOR "|")
set(EP_COMMON_OPTIONS LIST_SEPARATOR ${EP_LIST_SEPARATOR})

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS}")
if(NOT MSVC_TOOLCHAIN)
  # Set -fPIC on all external projects
  string(APPEND EP_CXX_FLAGS " -fPIC")
  string(APPEND EP_C_FLAGS " -fPIC")
endif()

# External projects are still able to override the following declarations. cmake
# command line will favor the last defined variable when a duplicate is
# encountered. This requires that `EP_COMMON_CMAKE_ARGS` is always the first
# argument.
set(EP_COMMON_CMAKE_ARGS
    -DBUILD_SHARED_LIBS=OFF
    -DBUILD_STATIC_LIBS=ON
    -DBUILD_TESTING=OFF
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}
    -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
    -DCMAKE_C_FLAGS=${EP_C_FLAGS}
    -DCMAKE_EXPORT_NO_PACKAGE_REGISTRY=${CMAKE_EXPORT_NO_PACKAGE_REGISTRY}
    -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=${CMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY}
    -DCMAKE_INSTALL_LIBDIR=lib)

# if building with a toolchain file, pass that through
if(CMAKE_TOOLCHAIN_FILE)
  list(APPEND EP_COMMON_CMAKE_ARGS
       -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE})
endif()

if(CMAKE_PROJECT_INCLUDE)
  list(APPEND EP_COMMON_CMAKE_ARGS
       -DCMAKE_PROJECT_INCLUDE=${CMAKE_PROJECT_INCLUDE})
endif()

# Enable s/ccache if set by parent.
if(CMAKE_C_COMPILER_LAUNCHER AND CMAKE_CXX_COMPILER_LAUNCHER)
  list(APPEND EP_COMMON_CMAKE_ARGS
       -DCMAKE_C_COMPILER_LAUNCHER=${CMAKE_C_COMPILER_LAUNCHER}
       -DCMAKE_CXX_COMPILER_LAUNCHER=${CMAKE_CXX_COMPILER_LAUNCHER})
endif()

if(NOT ICEBERG_VERBOSE_THIRDPARTY_BUILD)
  list(
    APPEND
    EP_COMMON_OPTIONS
    LOG_CONFIGURE
    1
    LOG_BUILD
    1
    LOG_INSTALL
    1
    LOG_DOWNLOAD
    1
    LOG_OUTPUT_ON_FAILURE
    1)
endif()

# Ensure that a default make is set
if("${MAKE}" STREQUAL "")
  if(NOT MSVC)
    find_program(MAKE make)
  endif()
endif()

# Args for external projects using make.
set(MAKE_BUILD_ARGS "-j${NPROC}")

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
endmacro()

# ----------------------------------------------------------------------
# Apache Arrow

function(build_arrow)
  message(STATUS "Building Apache Arrow from source")

  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-install")
  set(ARROW_HOME "${ARROW_PREFIX}")
  set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")
  set(ARROW_STATIC_LIB
      "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}arrow${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )

  set(ARROW_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}
      -DARROW_BUILD_SHARED=OFF
      -DARROW_FILESYSTEM=ON
      -DARROW_SIMD_LEVEL=NONE
      -DARROW_RUNTIME_SIMD_LEVEL=NONE)

  # Work around CMake bug
  file(MAKE_DIRECTORY ${ARROW_INCLUDE_DIR})

  ExternalProject_Add(
    arrow_ep
    ${EP_COMMON_OPTIONS}
    URL ${ARROW_SOURCE_URL}
    URL_HASH "SHA256=${ICEBERG_ARROW_BUILD_SHA256_CHECKSUM}"
    SOURCE_SUBDIR cpp
    CMAKE_ARGS ${ARROW_CMAKE_ARGS}
    BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS}
    BUILD_BYPRODUCTS ${ARROW_STATIC_LIB})
  add_library(Arrow::arrow_static STATIC IMPORTED)
  set_target_properties(Arrow::arrow_static PROPERTIES IMPORTED_LOCATION
                                                       "${ARROW_STATIC_LIB}")
  target_include_directories(Arrow::arrow_static BEFORE
                             INTERFACE "${ARROW_INCLUDE_DIR}")
  add_dependencies(Arrow::arrow_static arrow_ep)
  install(FILES "${ARROW_STATIC_LIB}"
          DESTINATION "${ICEBERG_INSTALL_LIBDIR}")

  set(ARROW_VENDORED
      TRUE
      PARENT_SCOPE)
endfunction()

if(ICEBERG_ARROW)
  resolve_dependency(Arrow HAVE_ALT TRUE)
  if(ARROW_VENDORED)
    set(ICEBERG_ARROW_VERSION ${ICEBERG_ARROW_BUILD_VERSION})
    list(APPEND ICEBERG_ARROW_VENDOR_DEPENDENCIES "iceberg::vendored_Arrow|${ARROW_STATIC_LIB}")
  else()
    set(ICEBERG_ARROW_VERSION ${ArrowAlt_VERSION})
    message(STATUS "Found Arrow static library: ${ARROW_STATIC_LIB}")
    message(STATUS "Found Arrow headers: ${ARROW_INCLUDE_DIR}")
    list(APPEND ICEBERG_ARROW_SYSTEM_DEPENDENCIES Arrow)
  endif()
endif()
