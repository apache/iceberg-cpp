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

if(NOT CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  return()
endif()
# Expose the instantiated location when Meson promotes nested subprojects.
file(WRITE "${CMAKE_BINARY_DIR}/iceberg-meson-source-dir.txt" "${CMAKE_SOURCE_DIR}")
add_custom_target(iceberg_meson_paths
                  COMMAND "${CMAKE_COMMAND}" -E true
                  BYPRODUCTS "${CMAKE_BINARY_DIR}/iceberg-meson-source-dir.txt")
# Meson does not infer interface include paths from FILE_SET HEADERS.
if(PROJECT_NAME STREQUAL "sqlpp23")
  cmake_language(DEFER
                 CALL
                 target_include_directories
                 sqlpp23
                 INTERFACE
                 "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>")
endif()
