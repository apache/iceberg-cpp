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

fetchcontent_declare(googletest
                     GIT_REPOSITORY https://github.com/google/googletest.git
                     GIT_TAG b514bdc898e2951020cbdca1304b75f5950d1f59 # release-1.15.2
                     FIND_PACKAGE_ARGS
                     NAMES
                     GTest)
fetchcontent_makeavailable(googletest)

set(ICEBERG_TEST_RESOURCES "${CMAKE_SOURCE_DIR}/test/resources")

configure_file("${CMAKE_SOURCE_DIR}/test/test_config.h.in"
               "${CMAKE_BINARY_DIR}/iceberg/test/test_config.h")

add_executable(schema_test)
target_sources(schema_test
               PRIVATE schema_test.cc
                       schema_field_test.cc
                       type_test.cc
                       transform_test.cc
                       partition_field_test.cc
                       partition_spec_test.cc
                       sort_field_test.cc
                       sort_order_test.cc
                       snapshot_test.cc)
target_link_libraries(schema_test PRIVATE iceberg_static GTest::gtest_main GTest::gmock)
add_test(NAME schema_test COMMAND schema_test)

add_executable(expected_test)
target_sources(expected_test PRIVATE expected_test.cc)
target_link_libraries(expected_test PRIVATE iceberg_static GTest::gtest_main GTest::gmock)
add_test(NAME expected_test COMMAND expected_test)

add_executable(expression_test)
target_sources(expression_test PRIVATE expression_test.cc)
target_link_libraries(expression_test PRIVATE iceberg_static GTest::gtest_main
                                              GTest::gmock)
add_test(NAME expression_test COMMAND expression_test)

add_executable(json_serde_test)
target_include_directories(json_serde_test PRIVATE "${CMAKE_BINARY_DIR}")
target_sources(json_serde_test PRIVATE json_internal_test.cc metadata_serde_test.cc
                                       schema_json_test.cc)
target_link_libraries(json_serde_test PRIVATE iceberg_static GTest::gtest_main
                                              GTest::gmock)
add_test(NAME json_serde_test COMMAND json_serde_test)

if(ICEBERG_BUILD_BUNDLE)
  add_executable(avro_test)
  target_sources(avro_test PRIVATE avro_test.cc)
  target_link_libraries(avro_test PRIVATE iceberg_bundle_static GTest::gtest_main
                                          GTest::gmock)
  add_test(NAME avro_test COMMAND avro_test)

  add_executable(arrow_test)
  target_sources(arrow_test PRIVATE arrow_test.cc arrow_fs_file_io_test.cc)
  target_link_libraries(arrow_test PRIVATE iceberg_bundle_static GTest::gtest_main
                                           GTest::gmock)
  add_test(NAME arrow_test COMMAND arrow_test)
endif()
