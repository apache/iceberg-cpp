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

# GTest
add_executable(core_gtest)
target_sources(core_gtest PRIVATE core_gtest.cc)
target_link_libraries(core_gtest PRIVATE iceberg_core_static GTest::gtest_main)
target_include_directories(core_gtest PRIVATE "${ICEBERG_API_DIR}")
add_test(NAME core_gtest COMMAND core_gtest)

# Catch2
add_executable(core_catch2)
target_sources(core_catch2 PRIVATE core_catch2.cc)
target_link_libraries(core_catch2 PRIVATE iceberg_core_static Catch2::Catch2WithMain)
target_include_directories(core_catch2 PRIVATE "${ICEBERG_API_DIR}")
add_test(NAME core_catch2 COMMAND core_catch2)
