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

# FindThrift.cmake - locate an installed Apache Thrift C++ runtime.
#
# Homebrew and several distros ship Thrift without a ThriftConfig.cmake, so a
# CONFIG-mode find_package() is not enough. This module discovers Thrift via
# pkg-config (thrift.pc) when available, then falls back to a plain library /
# header search.
#
# This module defines:
#   Thrift_FOUND    - whether the Thrift C++ runtime was found
#   Thrift_VERSION  - the detected Thrift version, if known
#   thrift::thrift  - imported target for the Thrift C++ runtime

if(TARGET thrift::thrift)
  set(Thrift_FOUND TRUE)
  return()
endif()

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(THRIFT_PC QUIET thrift)
endif()

find_library(Thrift_LIB
             NAMES thrift libthrift
             HINTS ${THRIFT_PC_LIBDIR} ${THRIFT_PC_LIBRARY_DIRS}
             PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib")
find_path(Thrift_INCLUDE_DIR
          NAMES thrift/Thrift.h
          HINTS ${THRIFT_PC_INCLUDEDIR} ${THRIFT_PC_INCLUDE_DIRS}
          PATH_SUFFIXES "include")

if(THRIFT_PC_VERSION)
  set(Thrift_VERSION "${THRIFT_PC_VERSION}")
elseif(Thrift_INCLUDE_DIR AND EXISTS "${Thrift_INCLUDE_DIR}/thrift/config.h")
  file(READ "${Thrift_INCLUDE_DIR}/thrift/config.h" _thrift_config_h)
  string(REGEX MATCH "#define PACKAGE_VERSION \"([0-9.]+)\"" _ "${_thrift_config_h}")
  set(Thrift_VERSION "${CMAKE_MATCH_1}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  Thrift
  REQUIRED_VARS Thrift_LIB Thrift_INCLUDE_DIR
  VERSION_VAR Thrift_VERSION)

if(Thrift_FOUND AND NOT TARGET thrift::thrift)
  add_library(thrift::thrift UNKNOWN IMPORTED)
  set_target_properties(thrift::thrift
                        PROPERTIES IMPORTED_LOCATION "${Thrift_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${Thrift_INCLUDE_DIR}")
  if(WIN32)
    set_property(TARGET thrift::thrift PROPERTY INTERFACE_LINK_LIBRARIES "ws2_32")
  endif()

  # Thrift's public C++ headers include <boost/numeric/conversion/cast.hpp>, but
  # thrift.pc does not advertise Boost. Attach Boost headers so consumers of
  # thrift::thrift can compile against the generated bindings.
  find_package(Boost QUIET)
  if(TARGET Boost::headers)
    target_link_libraries(thrift::thrift INTERFACE Boost::headers)
  elseif(Boost_INCLUDE_DIRS)
    set_property(TARGET thrift::thrift APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES
                                                       "${Boost_INCLUDE_DIRS}")
  else()
    message(FATAL_ERROR "System Thrift requires Boost headers, but Boost was not "
                        "found. Install Boost development headers.")
  endif()
endif()

mark_as_advanced(Thrift_LIB Thrift_INCLUDE_DIR)
