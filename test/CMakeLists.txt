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
               PRIVATE name_mapping_test.cc
                       schema_test.cc
                       schema_field_test.cc
                       type_test.cc
                       transform_test.cc
                       partition_field_test.cc
                       partition_spec_test.cc
                       sort_field_test.cc
                       sort_order_test.cc
                       snapshot_test.cc
                       schema_util_test.cc)
target_link_libraries(schema_test PRIVATE iceberg_static GTest::gtest_main GTest::gmock)
add_test(NAME schema_test COMMAND schema_test)

add_executable(table_test)
target_include_directories(table_test PRIVATE "${CMAKE_BINARY_DIR}")
target_sources(table_test PRIVATE test_common.cc json_internal_test.cc table_test.cc
                                  schema_json_test.cc)
target_link_libraries(table_test PRIVATE iceberg_static GTest::gtest_main GTest::gmock)
add_test(NAME table_test COMMAND table_test)

add_executable(expression_test)
target_sources(expression_test PRIVATE expression_test.cc literal_test.cc)
target_link_libraries(expression_test PRIVATE iceberg_static GTest::gtest_main
                                              GTest::gmock)
add_test(NAME expression_test COMMAND expression_test)

add_executable(json_serde_test)
target_include_directories(json_serde_test PRIVATE "${CMAKE_BINARY_DIR}")
target_sources(json_serde_test PRIVATE test_common.cc json_internal_test.cc
                                       metadata_serde_test.cc schema_json_test.cc)
target_link_libraries(json_serde_test PRIVATE iceberg_static GTest::gtest_main
                                              GTest::gmock)
add_test(NAME json_serde_test COMMAND json_serde_test)

add_executable(util_test)
target_sources(util_test PRIVATE formatter_test.cc config_test.cc visit_type_test.cc
                                 string_utils_test.cc)
target_link_libraries(util_test PRIVATE iceberg_static GTest::gtest_main GTest::gmock)
add_test(NAME util_test COMMAND util_test)

if(ICEBERG_BUILD_BUNDLE)
  add_executable(avro_test)
  target_sources(avro_test
                 PRIVATE avro_data_test.cc
                         avro_test.cc
                         avro_schema_test.cc
                         avro_stream_test.cc
                         manifest_list_reader_test.cc
                         manifest_reader_test.cc
                         test_common.cc)
  target_link_libraries(avro_test PRIVATE iceberg_bundle_static GTest::gtest_main
                                          GTest::gmock)
  target_include_directories(avro_test PRIVATE "${CMAKE_BINARY_DIR}")
  add_test(NAME avro_test COMMAND avro_test)

  add_executable(arrow_test)
  target_sources(arrow_test PRIVATE arrow_test.cc arrow_fs_file_io_test.cc
                                    metadata_io_test.cc gzip_decompress_test.cc)
  target_link_libraries(arrow_test PRIVATE iceberg_bundle_static GTest::gtest_main
                                           GTest::gmock)
  add_test(NAME arrow_test COMMAND arrow_test)

  add_executable(catalog_test)
  target_include_directories(catalog_test PRIVATE "${CMAKE_BINARY_DIR}")
  target_sources(catalog_test PRIVATE test_common.cc in_memory_catalog_test.cc)
  target_link_libraries(catalog_test PRIVATE iceberg_bundle_static GTest::gtest_main
                                             GTest::gmock)
  add_test(NAME catalog_test COMMAND catalog_test)
endif()
