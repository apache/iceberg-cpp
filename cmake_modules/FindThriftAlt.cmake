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

# FindThriftAlt.cmake - locate an installed Apache Thrift C++ runtime.
#
# Named "ThriftAlt" rather than "Thrift" (following Arrow's FindThriftAlt.cmake)
# so it does not collide with a downstream project's own FindThrift.cmake, and
# so this module can itself call find_package(Thrift CONFIG) to reuse an
# upstream ThriftConfig.cmake without recursing into itself.
#
# Discovery order:
#   1. CONFIG mode (ThriftConfig.cmake), shipped by CMake-based Thrift installs
#      (>= 0.13). When present it already provides a usable thrift::thrift.
#   2. pkg-config (thrift.pc), used by autotools installs and Homebrew.
#   3. A plain library / header search as a last resort (e.g. when neither a
#      CMake config nor pkg-config is available).
#
# This module defines:
#   ThriftAlt_FOUND   - whether the Thrift C++ runtime was found
#   ThriftAlt_VERSION - the detected Thrift version, if known
#   thrift::thrift    - imported target for the Thrift C++ runtime

if(ThriftAlt_FOUND OR TARGET thrift::thrift)
  set(ThriftAlt_FOUND TRUE)
  return()
endif()

# ----------------------------------------------------------------------
# 1. CONFIG mode: reuse an upstream ThriftConfig.cmake when available.
#
# This module is intentionally NOT named FindThrift.cmake, so a CONFIG-mode
# find_package(Thrift) here resolves to the upstream package config rather than
# back to this file.

set(_thriftalt_config_args CONFIG QUIET)
if(ThriftAlt_FIND_VERSION)
  list(APPEND _thriftalt_config_args ${ThriftAlt_FIND_VERSION})
endif()
find_package(Thrift ${_thriftalt_config_args})
if(Thrift_FOUND AND TARGET thrift::thrift)
  set(ThriftAlt_FOUND TRUE)
  set(ThriftAlt_VERSION "${Thrift_VERSION}")
endif()

# ----------------------------------------------------------------------
# 2 + 3. pkg-config, then a plain library / header search.

if(NOT ThriftAlt_FOUND)
  find_package(PkgConfig QUIET)
  if(PkgConfig_FOUND)
    pkg_check_modules(THRIFT_PC QUIET thrift)
  endif()

  find_library(ThriftAlt_LIB
               NAMES thrift libthrift
               HINTS ${THRIFT_PC_LIBDIR} ${THRIFT_PC_LIBRARY_DIRS}
               PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib")
  find_path(ThriftAlt_INCLUDE_DIR
            NAMES thrift/Thrift.h
            HINTS ${THRIFT_PC_INCLUDEDIR} ${THRIFT_PC_INCLUDE_DIRS}
            PATH_SUFFIXES "include")

  if(THRIFT_PC_VERSION)
    set(ThriftAlt_VERSION "${THRIFT_PC_VERSION}")
  elseif(ThriftAlt_INCLUDE_DIR AND EXISTS "${ThriftAlt_INCLUDE_DIR}/thrift/config.h")
    file(READ "${ThriftAlt_INCLUDE_DIR}/thrift/config.h" _thrift_config_h)
    string(REGEX MATCH "#define PACKAGE_VERSION \"([0-9.]+)\"" _ "${_thrift_config_h}")
    set(ThriftAlt_VERSION "${CMAKE_MATCH_1}")
  endif()
endif()

include(FindPackageHandleStandardArgs)
if(TARGET thrift::thrift)
  # CONFIG mode already produced the target; satisfy REQUIRED_VARS with it.
  find_package_handle_standard_args(
    ThriftAlt
    REQUIRED_VARS thrift::thrift
    VERSION_VAR ThriftAlt_VERSION)
else()
  find_package_handle_standard_args(
    ThriftAlt
    REQUIRED_VARS ThriftAlt_LIB ThriftAlt_INCLUDE_DIR
    VERSION_VAR ThriftAlt_VERSION)
endif()

if(ThriftAlt_FOUND AND NOT TARGET thrift::thrift)
  add_library(thrift::thrift UNKNOWN IMPORTED)
  set_target_properties(thrift::thrift
                        PROPERTIES IMPORTED_LOCATION "${ThriftAlt_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${ThriftAlt_INCLUDE_DIR}")
  if(WIN32)
    set_property(TARGET thrift::thrift PROPERTY INTERFACE_LINK_LIBRARIES "ws2_32")
  endif()
endif()

# ----------------------------------------------------------------------
# Boost headers.
#
# Thrift's public C++ headers include <boost/numeric/conversion/cast.hpp>, but
# neither thrift.pc nor (older) ThriftConfig.cmake advertise Boost. Attach Boost
# headers so consumers of thrift::thrift can compile the generated bindings.
#
# Use CONFIG mode explicitly: the legacy FindBoost module was removed and emits
# a CMP0167 warning under cmake_minimum_required(VERSION < 3.30), which would
# surface in every downstream find_dependency(ThriftAlt).

if(ThriftAlt_FOUND)
  find_package(Boost CONFIG QUIET)
  if(TARGET Boost::headers)
    target_link_libraries(thrift::thrift INTERFACE Boost::headers)
  elseif(Boost_INCLUDE_DIRS)
    set_property(TARGET thrift::thrift APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES
                                                       "${Boost_INCLUDE_DIRS}")
  else()
    message(FATAL_ERROR "Apache Thrift's C++ headers require Boost headers, but Boost was not "
                        "found. Install Boost development headers (e.g. 'brew install boost' "
                        "or 'apt install libboost-dev').")
  endif()
endif()

mark_as_advanced(ThriftAlt_LIB ThriftAlt_INCLUDE_DIR)
