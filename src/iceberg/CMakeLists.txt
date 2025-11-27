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

set(ICEBERG_INCLUDES "$<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/src>"
                     "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>")
set(ICEBERG_SOURCES
    arrow_c_data_guard_internal.cc
    catalog/memory/in_memory_catalog.cc
    expression/binder.cc
    expression/evaluator.cc
    expression/expression.cc
    expression/expressions.cc
    expression/literal.cc
    expression/predicate.cc
    expression/rewrite_not.cc
    expression/term.cc
    file_reader.cc
    file_writer.cc
    inheritable_metadata.cc
    json_internal.cc
    manifest_adapter.cc
    manifest_entry.cc
    manifest_list.cc
    manifest_reader.cc
    manifest_reader_internal.cc
    manifest_writer.cc
    metadata_columns.cc
    name_mapping.cc
    partition_field.cc
    partition_spec.cc
    partition_summary.cc
    row/arrow_array_wrapper.cc
    row/manifest_wrapper.cc
    row/struct_like.cc
    schema.cc
    schema_field.cc
    schema_internal.cc
    schema_util.cc
    snapshot.cc
    sort_field.cc
    sort_order.cc
    statistics_file.cc
    table.cc
    table_metadata.cc
    table_properties.cc
    table_requirement.cc
    table_requirements.cc
    table_scan.cc
    table_update.cc
    transform.cc
    transform_function.cc
    type.cc
    util/bucket_util.cc
    util/conversions.cc
    util/decimal.cc
    util/gzip_internal.cc
    util/murmurhash3_internal.cc
    util/temporal_util.cc
    util/timepoint.cc
    util/truncate_util.cc
    util/uuid.cc
    v1_metadata.cc
    v2_metadata.cc
    v3_metadata.cc)

set(ICEBERG_STATIC_BUILD_INTERFACE_LIBS)
set(ICEBERG_SHARED_BUILD_INTERFACE_LIBS)
set(ICEBERG_STATIC_INSTALL_INTERFACE_LIBS)
set(ICEBERG_SHARED_INSTALL_INTERFACE_LIBS)

list(APPEND
     ICEBERG_STATIC_BUILD_INTERFACE_LIBS
     nanoarrow::nanoarrow_static
     nlohmann_json::nlohmann_json
     roaring::roaring
     spdlog::spdlog
     ZLIB::ZLIB)
list(APPEND
     ICEBERG_SHARED_BUILD_INTERFACE_LIBS
     nanoarrow::nanoarrow_shared
     nlohmann_json::nlohmann_json
     roaring::roaring
     spdlog::spdlog
     ZLIB::ZLIB)
list(APPEND
     ICEBERG_STATIC_INSTALL_INTERFACE_LIBS
     "$<IF:$<BOOL:${NANOARROW_VENDORED}>,iceberg::nanoarrow_static,$<IF:$<TARGET_EXISTS:nanoarrow::nanoarrow_static>,nanoarrow::nanoarrow_static,nanoarrow::nanoarrow_shared>>"
     "$<IF:$<BOOL:${NLOHMANN_JSON_VENDORED}>,iceberg::nlohmann_json,$<IF:$<TARGET_EXISTS:nlohmann_json::nlohmann_json>,nlohmann_json::nlohmann_json,nlohmann_json::nlohmann_json>>"
     "$<IF:$<BOOL:${CROARING_VENDORED}>,iceberg::roaring,roaring::roaring>"
     "$<IF:$<BOOL:${SPDLOG_VENDORED}>,iceberg::spdlog,spdlog::spdlog>")
list(APPEND
     ICEBERG_SHARED_INSTALL_INTERFACE_LIBS
     "$<IF:$<BOOL:${NANOARROW_VENDORED}>,iceberg::nanoarrow_shared,$<IF:$<TARGET_EXISTS:nanoarrow::nanoarrow_shared>,nanoarrow::nanoarrow_shared,nanoarrow::nanoarrow_static>>"
     "$<IF:$<BOOL:${NLOHMANN_JSON_VENDORED}>,iceberg::nlohmann_json,$<IF:$<TARGET_EXISTS:nlohmann_json::nlohmann_json>,nlohmann_json::nlohmann_json,nlohmann_json::nlohmann_json>>"
     "$<IF:$<BOOL:${CROARING_VENDORED}>,iceberg::roaring,roaring::roaring>"
     "$<IF:$<BOOL:${SPDLOG_VENDORED}>,iceberg::spdlog,spdlog::spdlog>")

add_iceberg_lib(iceberg
                SOURCES
                ${ICEBERG_SOURCES}
                EXTRA_INCLUDES
                ${ICEBERG_INCLUDES}
                SHARED_LINK_LIBS
                ${ICEBERG_SHARED_BUILD_INTERFACE_LIBS}
                STATIC_LINK_LIBS
                ${ICEBERG_STATIC_BUILD_INTERFACE_LIBS}
                STATIC_INSTALL_INTERFACE_LIBS
                ${ICEBERG_STATIC_INSTALL_INTERFACE_LIBS}
                SHARED_INSTALL_INTERFACE_LIBS
                ${ICEBERG_SHARED_INSTALL_INTERFACE_LIBS}
                OUTPUTS
                ICEBERG_LIBRARIES)

iceberg_install_all_headers(iceberg)

add_subdirectory(catalog)
add_subdirectory(expression)
add_subdirectory(row)
add_subdirectory(util)

if(ICEBERG_BUILD_BUNDLE)
  set(ICEBERG_BUNDLE_SOURCES
      arrow/arrow_fs_file_io.cc
      avro/avro_data_util.cc
      avro/avro_reader.cc
      avro/avro_writer.cc
      avro/avro_register.cc
      avro/avro_schema_util.cc
      avro/avro_stream_internal.cc
      parquet/parquet_data_util.cc
      parquet/parquet_reader.cc
      parquet/parquet_register.cc
      parquet/parquet_schema_util.cc
      parquet/parquet_writer.cc)

  # Libraries to link with exported libiceberg_bundle.{so,a}.
  set(ICEBERG_BUNDLE_STATIC_BUILD_INTERFACE_LIBS)
  set(ICEBERG_BUNDLE_SHARED_BUILD_INTERFACE_LIBS)
  set(ICEBERG_BUNDLE_STATIC_INSTALL_INTERFACE_LIBS)
  set(ICEBERG_BUNDLE_SHARED_INSTALL_INTERFACE_LIBS)

  list(APPEND
       ICEBERG_BUNDLE_STATIC_BUILD_INTERFACE_LIBS
       "$<IF:$<TARGET_EXISTS:iceberg_static>,iceberg_static,iceberg_shared>"
       "$<IF:$<TARGET_EXISTS:Arrow::arrow_static>,Arrow::arrow_static,Arrow::arrow_shared>"
       "$<IF:$<TARGET_EXISTS:Parquet::parquet_static>,Parquet::parquet_static,Parquet::parquet_shared>"
       "$<IF:$<TARGET_EXISTS:avro-cpp::avrocpp_static>,avro-cpp::avrocpp_static,avro-cpp::avrocpp_shared>"
  )
  list(APPEND
       ICEBERG_BUNDLE_SHARED_BUILD_INTERFACE_LIBS
       "$<IF:$<TARGET_EXISTS:iceberg_shared>,iceberg_shared,iceberg_static>"
       "$<IF:$<TARGET_EXISTS:Arrow::arrow_shared>,Arrow::arrow_shared,Arrow::arrow_static>"
       "$<IF:$<TARGET_EXISTS:Parquet::parquet_shared>,Parquet::parquet_shared,Parquet::parquet_static>"
       "$<IF:$<TARGET_EXISTS:avro-cpp::avrocpp_shared>,avro-cpp::avrocpp_shared,avro-cpp::avrocpp_static>"
  )

  list(APPEND
       ICEBERG_BUNDLE_STATIC_INSTALL_INTERFACE_LIBS
       "$<IF:$<TARGET_EXISTS:iceberg::iceberg_static>,iceberg::iceberg_static,iceberg::iceberg_shared>"
       "$<IF:$<BOOL:${ARROW_VENDORED}>,iceberg::arrow_static,$<IF:$<TARGET_EXISTS:Arrow::arrow_static>,Arrow::arrow_static,Arrow::arrow_shared>>"
       "$<IF:$<BOOL:${ARROW_VENDORED}>,iceberg::parquet_static,$<IF:$<TARGET_EXISTS:Parquet::parquet_static>,Parquet::parquet_static,Parquet::parquet_shared>>"
       "$<IF:$<BOOL:${AVRO_VENDORED}>,iceberg::avrocpp_s,$<IF:$<TARGET_EXISTS:avro-cpp::avrocpp_static>,avro-cpp::avrocpp_static,avro-cpp::avrocpp_shared>>"
  )
  list(APPEND
       ICEBERG_BUNDLE_SHARED_INSTALL_INTERFACE_LIBS
       "$<IF:$<TARGET_EXISTS:iceberg::iceberg_shared>,iceberg::iceberg_shared,iceberg::iceberg_static>"
       "$<IF:$<BOOL:${ARROW_VENDORED}>,iceberg::arrow_static,$<IF:$<TARGET_EXISTS:Arrow::arrow_shared>,Arrow::arrow_shared,Arrow::arrow_static>>"
       "$<IF:$<BOOL:${ARROW_VENDORED}>,iceberg::parquet_static,$<IF:$<TARGET_EXISTS:Parquet::parquet_shared>,Parquet::parquet_shared,Parquet::parquet_static>>"
       "$<IF:$<BOOL:${AVRO_VENDORED}>,iceberg::avrocpp_s,$<IF:$<TARGET_EXISTS:avro-cpp::avrocpp_shared>,avro-cpp::avrocpp_shared,avro-cpp::avrocpp_static>>"
  )

  add_iceberg_lib(iceberg_bundle
                  SOURCES
                  ${ICEBERG_BUNDLE_SOURCES}
                  SHARED_LINK_LIBS
                  ${ICEBERG_BUNDLE_SHARED_BUILD_INTERFACE_LIBS}
                  STATIC_LINK_LIBS
                  ${ICEBERG_BUNDLE_STATIC_BUILD_INTERFACE_LIBS}
                  STATIC_INSTALL_INTERFACE_LIBS
                  ${ICEBERG_BUNDLE_STATIC_INSTALL_INTERFACE_LIBS}
                  SHARED_INSTALL_INTERFACE_LIBS
                  ${ICEBERG_BUNDLE_SHARED_INSTALL_INTERFACE_LIBS}
                  OUTPUTS
                  ICEBERG_BUNDLE_LIBRARIES)

  add_subdirectory(arrow)
  add_subdirectory(avro)
  add_subdirectory(parquet)
endif()

iceberg_install_cmake_package(iceberg iceberg_targets)

if(ICEBERG_BUILD_TESTS)
  add_subdirectory(test)
endif()
