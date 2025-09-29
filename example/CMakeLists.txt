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

set(CMAKE_CXX_STANDARD 23)

find_package(iceberg CONFIG REQUIRED)

add_executable(demo_example demo_example.cc)

target_link_libraries(demo_example PRIVATE iceberg::iceberg_bundle_static
                                           iceberg::iceberg_rest_static)
