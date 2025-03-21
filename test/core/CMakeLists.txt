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

add_executable(core_unittest)
target_sources(core_unittest PRIVATE core_unittest.cc schema_test.cc schema_field_test.cc
                                     type_test.cc)
target_link_libraries(core_unittest PRIVATE iceberg_static GTest::gtest_main GTest::gmock)
target_include_directories(core_unittest PRIVATE "${ICEBERG_INCLUDES}")
add_test(NAME core_unittest COMMAND core_unittest)

add_executable(expected_test)
target_sources(expected_test PRIVATE expected_test.cc)
target_link_libraries(expected_test PRIVATE iceberg_static GTest::gtest_main)
target_include_directories(expected_test PRIVATE "${ICEBERG_INCLUDES}")
add_test(NAME expected_test COMMAND expected_test)
