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
project(IcebergArrow LANGUAGES C CXX)

# Meson's CMake module imports library targets but not header installation.
# Keep upstream's header selection and destinations in a separate component.
function(install)
  if(ARGV0 MATCHES "^(FILES|DIRECTORY)$")
    cmake_parse_arguments(HEADER_INSTALL
                          ""
                          "DESTINATION"
                          ""
                          ${ARGN})
    if(HEADER_INSTALL_DESTINATION MATCHES "^${CMAKE_INSTALL_INCLUDEDIR}(/|$)")
      set(HEADER_ARGS ${ARGV})
      list(FIND HEADER_ARGS DESTINATION DESTINATION_INDEX)
      math(EXPR COMPONENT_INDEX "${DESTINATION_INDEX} + 2")
      list(INSERT
           HEADER_ARGS
           ${COMPONENT_INDEX}
           COMPONENT
           iceberg_dependency_headers)
      _install(${HEADER_ARGS})
      return()
    endif()
  endif()
  _install(${ARGV})
endfunction()

# Older Thrift releases are removed from the live Apache mirrors.
if(NOT DEFINED ENV{ARROW_THRIFT_URL})
  set(ENV{ARROW_THRIFT_URL}
      "https://archive.apache.org/dist/thrift/0.22.0/thrift-0.22.0.tar.gz")
endif()
add_subdirectory("${CMAKE_CURRENT_LIST_DIR}/cpp" upstream)

# Meson needs compile flags to recognize PIC on archives of object libraries.
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/meson_static.cc" "")
foreach(target arrow_static parquet_static)
  target_sources(${target} PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/meson_static.cc")
  set_target_properties(${target} PROPERTIES POSITION_INDEPENDENT_CODE ON)
endforeach()

# Meson's CMake conversion does not propagate system libraries through Arrow's
# object libraries. Reuse the install interface computed by upstream CMake.
get_directory_property(ARROW_MESON_SYSTEM_LIBS
                       DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/cpp/src/arrow" DEFINITION
                                 ARROW_STATIC_INSTALL_INTERFACE_LIBS)
list(REMOVE_ITEM ARROW_MESON_SYSTEM_LIBS Arrow::arrow_bundled_dependencies)
set_property(TARGET arrow_static
             APPEND
             PROPERTY INTERFACE_LINK_LIBRARIES ${ARROW_MESON_SYSTEM_LIBS})

# Preserve Thrift when Meson flattens Arrow's object libraries.
target_link_libraries(arrow_static INTERFACE thrift)
target_link_libraries(parquet_static INTERFACE thrift)

if(WIN32)
  target_compile_definitions(thrift INTERFACE THRIFT_STATIC_DEFINE)
endif()
