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
  target_include_directories(${test_name} PRIVATE "${CMAKE_BINARY_DIR}/iceberg/test/")

  target_sources(${test_name} PRIVATE ${ARG_SOURCES})

  if(ARG_USE_BUNDLE)
    target_link_libraries(${test_name} PRIVATE iceberg_bundle_static GTest::gmock_main)
  else()
    target_link_libraries(${test_name} PRIVATE iceberg_static GTest::gmock_main)
  endif()

  add_test(NAME ${test_name} COMMAND ${test_name})
endfunction()

add_iceberg_test(schema_test
                 SOURCES
                 assign_id_visitor_test.cc
                 name_mapping_test.cc
                 partition_field_test.cc
                 partition_spec_test.cc
                 partition_value_test.cc
                 schema_field_test.cc
                 schema_test.cc
                 schema_util_test.cc
                 sort_field_test.cc
                 sort_order_test.cc
                 transform_test.cc
                 type_test.cc)

add_iceberg_test(table_test
                 SOURCES
                 metrics_config_test.cc
                 snapshot_test.cc
                 snapshot_util_test.cc
                 table_metadata_builder_test.cc
                 table_requirement_test.cc
                 table_requirements_test.cc
                 table_test.cc
                 table_update_test.cc)

add_iceberg_test(expression_test
                 SOURCES
                 aggregate_test.cc
                 expression_test.cc
                 expression_visitor_test.cc
                 inclusive_metrics_evaluator_test.cc
                 inclusive_metrics_evaluator_with_transform_test.cc
                 literal_test.cc
                 manifest_evaluator_test.cc
                 predicate_test.cc
                 projections_test.cc
                 residual_evaluator_test.cc
                 strict_metrics_evaluator_test.cc)

add_iceberg_test(json_serde_test
                 SOURCES
                 json_internal_test.cc
                 metadata_serde_test.cc
                 schema_json_test.cc)

add_iceberg_test(util_test
                 SOURCES
                 bucket_util_test.cc
                 config_test.cc
                 decimal_test.cc
                 endian_test.cc
                 formatter_test.cc
                 location_util_test.cc
                 string_util_test.cc
                 truncate_util_test.cc
                 url_encoder_test.cc
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
                   avro_stream_test.cc)

  add_iceberg_test(arrow_test
                   USE_BUNDLE
                   SOURCES
                   arrow_fs_file_io_test.cc
                   arrow_test.cc
                   gzip_decompress_test.cc
                   metadata_io_test.cc
                   struct_like_test.cc)

  add_iceberg_test(catalog_test USE_BUNDLE SOURCES in_memory_catalog_test.cc)

  add_iceberg_test(eval_expr_test
                   USE_BUNDLE
                   SOURCES
                   eval_expr_test.cc
                   evaluator_test.cc)

  add_iceberg_test(manifest_test
                   USE_BUNDLE
                   SOURCES
                   delete_file_index_test.cc
                   manifest_group_test.cc
                   manifest_list_versions_test.cc
                   manifest_reader_stats_test.cc
                   manifest_reader_test.cc
                   manifest_writer_versions_test.cc
                   rolling_manifest_writer_test.cc)

  add_iceberg_test(parquet_test
                   USE_BUNDLE
                   SOURCES
                   parquet_data_test.cc
                   parquet_schema_test.cc
                   parquet_test.cc)

  add_iceberg_test(scan_test USE_BUNDLE SOURCES file_scan_task_test.cc)

  add_iceberg_test(table_update_test
                   USE_BUNDLE
                   SOURCES
                   transaction_test.cc
                   update_partition_spec_test.cc
                   update_properties_test.cc
                   update_sort_order_test.cc)

  add_iceberg_test(data_writer_test USE_BUNDLE SOURCES data_writer_test.cc)

endif()

if(ICEBERG_BUILD_REST)
  function(add_rest_iceberg_test test_name)
    set(options USE_BUNDLE)
    set(oneValueArgs)
    set(multiValueArgs SOURCES)
    cmake_parse_arguments(ARG
                          "${options}"
                          "${oneValueArgs}"
                          "${multiValueArgs}"
                          ${ARGN})

    add_executable(${test_name})
    target_include_directories(${test_name} PRIVATE "${CMAKE_BINARY_DIR}/iceberg/test/")
    target_sources(${test_name} PRIVATE ${ARG_SOURCES})
    target_link_libraries(${test_name} PRIVATE GTest::gmock_main iceberg_rest_static)
    add_test(NAME ${test_name} COMMAND ${test_name})
  endfunction()

  add_rest_iceberg_test(rest_catalog_test
                        SOURCES
                        endpoint_test.cc
                        rest_json_internal_test.cc
                        rest_util_test.cc)

  if(ICEBERG_BUILD_REST_INTEGRATION_TESTS)
    add_rest_iceberg_test(rest_catalog_integration_test
                          SOURCES
                          rest_catalog_test.cc
                          util/cmd_util.cc
                          util/docker_compose_util.cc)
  endif()
endif()
