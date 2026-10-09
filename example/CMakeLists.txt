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

# Please set CMAKE_PREFIX_PATH to the install prefix of iceberg.
cmake_minimum_required(VERSION 3.25)

project(example)

set(ICEBERG_EXAMPLE_CXX_STANDARD
    23
    CACHE STRING "C++ standard used to build the example (20 or newer)")
if(NOT ICEBERG_EXAMPLE_CXX_STANDARD MATCHES "^[0-9]+$")
  message(FATAL_ERROR "ICEBERG_EXAMPLE_CXX_STANDARD must be a numeric CMake C++ standard")
endif()
if(ICEBERG_EXAMPLE_CXX_STANDARD EQUAL 98 OR ICEBERG_EXAMPLE_CXX_STANDARD LESS 20)
  message(FATAL_ERROR "ICEBERG_EXAMPLE_CXX_STANDARD must be 20 or newer, got "
                      "'${ICEBERG_EXAMPLE_CXX_STANDARD}'")
endif()

set(CMAKE_CXX_STANDARD ${ICEBERG_EXAMPLE_CXX_STANDARD})
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(iceberg CONFIG REQUIRED COMPONENTS bundle rest)
if(ICEBERG_EXAMPLE_CXX_STANDARD EQUAL 20 AND NOT ICEBERG_CXX20_COMPAT)
  message(FATAL_ERROR "C++20 examples require an Iceberg package built with "
                      "ICEBERG_CXX20_COMPAT=ON")
endif()

add_executable(demo_example demo_example.cc)

target_link_libraries(demo_example
                      PRIVATE "$<IF:$<TARGET_EXISTS:iceberg::iceberg_bundle_shared>,iceberg::iceberg_bundle_shared,iceberg::iceberg_bundle_static>"
                              "$<IF:$<TARGET_EXISTS:iceberg::iceberg_rest_shared>,iceberg::iceberg_rest_shared,iceberg::iceberg_rest_static>"
)
