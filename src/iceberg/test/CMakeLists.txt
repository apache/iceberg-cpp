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

set(ICEBERG_TEST_RESOURCES "${CMAKE_SOURCE_DIR}/src/iceberg/test/resources")

configure_file("test_config.h.in" "test_config.h")

function(add_iceberg_test test_name)
  set(options USE_BUNDLE)
  set(oneValueArgs)
  set(multiValueArgs SOURCES)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${oneValueArgs}"
                        "${multiValueArgs}"
                        ${ARGN})

  add_executable(${test_name})
  target_include_directories(${test_name} PRIVATE "${CMAKE_BINARY_DIR}")

  target_sources(${test_name} PRIVATE ${ARG_SOURCES})

  if(ARG_USE_BUNDLE)
    target_link_libraries(${test_name} PRIVATE iceberg_bundle_static GTest::gtest_main
                                               GTest::gmock)
  else()
    target_link_libraries(${test_name} PRIVATE iceberg_static GTest::gtest_main
                                               GTest::gmock)
  endif()

  add_test(NAME ${test_name} COMMAND ${test_name})
endfunction()

add_iceberg_test(schema_test
                 SOURCES
                 name_mapping_test.cc
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

add_iceberg_test(table_test
                 SOURCES
                 test_common.cc
                 json_internal_test.cc
                 table_test.cc
                 schema_json_test.cc)

add_iceberg_test(expression_test SOURCES expression_test.cc literal_test.cc)

add_iceberg_test(json_serde_test
                 SOURCES
                 test_common.cc
                 json_internal_test.cc
                 metadata_serde_test.cc
                 schema_json_test.cc)

add_iceberg_test(util_test
                 SOURCES
                 config_test.cc
                 decimal_test.cc
                 endian_test.cc
                 formatter_test.cc
                 string_util_test.cc
                 uuid_test.cc
                 visit_type_test.cc)

add_iceberg_test(roaring_test SOURCES roaring_test.cc)

if(ICEBERG_BUILD_BUNDLE)
  add_iceberg_test(avro_test
                   USE_BUNDLE
                   SOURCES
                   avro_data_test.cc
                   avro_test.cc
                   avro_schema_test.cc
                   avro_stream_test.cc
                   manifest_list_reader_test.cc
                   manifest_reader_test.cc
                   test_common.cc)

  add_iceberg_test(arrow_test
                   USE_BUNDLE
                   SOURCES
                   arrow_test.cc
                   arrow_fs_file_io_test.cc
                   metadata_io_test.cc
                   gzip_decompress_test.cc)

  add_iceberg_test(catalog_test
                   USE_BUNDLE
                   SOURCES
                   test_common.cc
                   in_memory_catalog_test.cc)

  add_iceberg_test(parquet_test
                   USE_BUNDLE
                   SOURCES
                   parquet_data_test.cc
                   parquet_schema_test.cc
                   parquet_test.cc)

  add_iceberg_test(scan_test USE_BUNDLE SOURCES file_scan_task_test.cc)

endif()
