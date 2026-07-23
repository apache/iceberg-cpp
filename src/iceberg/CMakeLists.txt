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
    arrow_c_data_util.cc
    arrow_row_builder.cc
    catalog/memory/in_memory_catalog.cc
    catalog/session_catalog.cc
    catalog/session_context.cc
    puffin_dv_io.cc
    delete_file_index.cc
    expression/aggregate.cc
    expression/binder.cc
    expression/evaluator.cc
    expression/expression.cc
    expression/expressions.cc
    expression/inclusive_metrics_evaluator.cc
    expression/json_serde.cc
    expression/literal.cc
    expression/manifest_evaluator.cc
    expression/predicate.cc
    expression/projections.cc
    expression/residual_evaluator.cc
    expression/rewrite_not.cc
    expression/strict_metrics_evaluator.cc
    expression/term.cc
    file_io.cc
    file_io_registry.cc
    file_reader.cc
    file_writer.cc
    inspect/history_table.cc
    inspect/metadata_table.cc
    inspect/snapshots_table.cc
    inheritable_metadata.cc
    json_serde.cc
    location_provider.cc
    logging/cerr_logger.cc
    logging/logger.cc
    manifest/manifest_adapter.cc
    manifest/manifest_entry.cc
    manifest/manifest_filter_manager.cc
    manifest/manifest_group.cc
    manifest/manifest_list.cc
    manifest/manifest_merge_manager.cc
    manifest/manifest_reader.cc
    manifest/manifest_util.cc
    manifest/manifest_writer.cc
    manifest/rolling_manifest_writer.cc
    manifest/v1_metadata.cc
    manifest/v2_metadata.cc
    manifest/v3_metadata.cc
    metadata_columns.cc
    metrics_config.cc
    metrics/commit_report.cc
    metrics/counter.cc
    metrics/json_serde.cc
    metrics/metrics_context.cc
    metrics/metrics_reporters.cc
    metrics/scan_report.cc
    metrics/timer.cc
    name_mapping.cc
    partition_field.cc
    partition_spec.cc
    partition_summary.cc
    row/arrow_array_wrapper.cc
    row/manifest_wrapper.cc
    row/partition_values.cc
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
    table_identifier.cc
    table_metadata.cc
    table_properties.cc
    table_requirement.cc
    table_requirements.cc
    table_scan.cc
    table_update.cc
    transaction.cc
    transform.cc
    transform_function.cc
    type.cc
    update/delete_files.cc
    update/expire_snapshots.cc
    update/fast_append.cc
    update/merge_append.cc
    update/replace_partitions.cc
    update/merging_snapshot_update.cc
    update/overwrite_files.cc
    update/pending_update.cc
    update/rewrite_files.cc
    update/row_delta.cc
    update/set_snapshot.cc
    update/snapshot_manager.cc
    update/snapshot_update.cc
    update/update_location.cc
    update/update_partition_spec.cc
    update/update_partition_statistics.cc
    update/update_properties.cc
    update/update_schema.cc
    update/update_snapshot_reference.cc
    update/update_sort_order.cc
    update/update_statistics.cc
    util/base64.cc
    util/bucket_util.cc
    util/content_file_util.cc
    util/conversions.cc
    util/decimal.cc
    util/gzip_internal.cc
    util/murmurhash3_internal.cc
    util/property_util.cc
    util/retry_util.cc
    util/snapshot_util.cc
    util/string_util.cc
    util/struct_like_set.cc
    util/task_group.cc
    util/temporal_util.cc
    util/thread_util.cc
    util/timepoint.cc
    util/transform_util.cc
    util/truncate_util.cc
    util/type_util.cc
    util/url_encoder.cc
    util/uuid.cc)

set(ICEBERG_STATIC_BUILD_INTERFACE_LIBS)
set(ICEBERG_SHARED_BUILD_INTERFACE_LIBS)
set(ICEBERG_STATIC_INSTALL_INTERFACE_LIBS)
set(ICEBERG_SHARED_INSTALL_INTERFACE_LIBS)

list(APPEND
     ICEBERG_STATIC_BUILD_INTERFACE_LIBS
     "$<IF:$<BOOL:${NANOARROW_VENDORED}>,nanoarrow::nanoarrow_static,$<IF:$<TARGET_EXISTS:nanoarrow::nanoarrow_static>,nanoarrow::nanoarrow_static,nanoarrow::nanoarrow_shared>>"
     nlohmann_json::nlohmann_json
     spdlog::spdlog
     utf8proc::utf8proc
     ZLIB::ZLIB)
list(APPEND
     ICEBERG_SHARED_BUILD_INTERFACE_LIBS
     "$<IF:$<BOOL:${NANOARROW_VENDORED}>,nanoarrow::nanoarrow_static,$<IF:$<TARGET_EXISTS:nanoarrow::nanoarrow_shared>,nanoarrow::nanoarrow_shared,nanoarrow::nanoarrow_static>>"
     nlohmann_json::nlohmann_json
     spdlog::spdlog
     utf8proc::utf8proc
     ZLIB::ZLIB)
list(APPEND
     ICEBERG_STATIC_INSTALL_INTERFACE_LIBS
     "$<IF:$<BOOL:${NANOARROW_VENDORED}>,iceberg::nanoarrow_static,$<IF:$<TARGET_EXISTS:nanoarrow::nanoarrow_static>,nanoarrow::nanoarrow_static,nanoarrow::nanoarrow_shared>>"
     "$<IF:$<BOOL:${NLOHMANN_JSON_VENDORED}>,iceberg::nlohmann_json,$<IF:$<TARGET_EXISTS:nlohmann_json::nlohmann_json>,nlohmann_json::nlohmann_json,nlohmann_json::nlohmann_json>>"
     "$<IF:$<BOOL:${SPDLOG_VENDORED}>,iceberg::spdlog,spdlog::spdlog>"
     "$<IF:$<BOOL:${UTF8PROC_VENDORED}>,iceberg::utf8proc,utf8proc::utf8proc>")
list(APPEND
     ICEBERG_SHARED_INSTALL_INTERFACE_LIBS
     "$<IF:$<BOOL:${NANOARROW_VENDORED}>,iceberg::nanoarrow_static,$<IF:$<TARGET_EXISTS:nanoarrow::nanoarrow_shared>,nanoarrow::nanoarrow_shared,nanoarrow::nanoarrow_static>>"
     "$<IF:$<BOOL:${NLOHMANN_JSON_VENDORED}>,iceberg::nlohmann_json,$<IF:$<TARGET_EXISTS:nlohmann_json::nlohmann_json>,nlohmann_json::nlohmann_json,nlohmann_json::nlohmann_json>>"
     "$<IF:$<BOOL:${SPDLOG_VENDORED}>,iceberg::spdlog,spdlog::spdlog>"
     "$<IF:$<BOOL:${UTF8PROC_VENDORED}>,iceberg::utf8proc,utf8proc::utf8proc>")

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

# The logging macros in the installed public header log_macros.h use __VA_OPT__,
# which MSVC's traditional preprocessor rejects. Require the conforming
# preprocessor PUBLICly so downstream consumers that include log_macros.h get it
# on MSVC (and any library TU that includes it does too).
if(MSVC_TOOLCHAIN)
  foreach(_iceberg_lib iceberg_shared iceberg_static)
    if(TARGET ${_iceberg_lib})
      target_compile_options(${_iceberg_lib} PUBLIC /Zc:preprocessor)
    endif()
  endforeach()
endif()

set(ICEBERG_DATA_SOURCES
    data/data_writer.cc
    data/delete_filter.cc
    data/delete_loader.cc
    data/puffin_dv_register.cc
    data/deletion_vector_writer.cc
    data/dv_util.cc
    data/equality_delete_writer.cc
    data/file_scan_task_reader.cc
    data/position_delete_writer.cc
    data/writer.cc
    deletes/position_delete_index.cc
    deletes/position_delete_range_consumer.cc
    deletes/roaring_position_bitmap.cc
    puffin/file_metadata.cc
    puffin/json_serde.cc
    puffin/puffin_format.cc
    puffin/puffin_reader.cc
    puffin/puffin_writer.cc)

set(ICEBERG_DATA_STATIC_BUILD_INTERFACE_LIBS)
set(ICEBERG_DATA_SHARED_BUILD_INTERFACE_LIBS)
set(ICEBERG_DATA_STATIC_INSTALL_INTERFACE_LIBS)
set(ICEBERG_DATA_SHARED_INSTALL_INTERFACE_LIBS)

list(APPEND ICEBERG_DATA_STATIC_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_static>,iceberg_static,iceberg_shared>"
     roaring::roaring)
list(APPEND ICEBERG_DATA_SHARED_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_shared>,iceberg_shared,iceberg_static>"
     roaring::roaring)
list(APPEND
     ICEBERG_DATA_STATIC_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_static>,iceberg::iceberg_static,iceberg::iceberg_shared>"
     "$<IF:$<BOOL:${CROARING_VENDORED}>,iceberg::roaring,roaring::roaring>")
list(APPEND
     ICEBERG_DATA_SHARED_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_shared>,iceberg::iceberg_shared,iceberg::iceberg_static>"
     "$<IF:$<BOOL:${CROARING_VENDORED}>,iceberg::roaring,roaring::roaring>")

add_iceberg_lib(iceberg_data
                SOURCES
                ${ICEBERG_DATA_SOURCES}
                EXTRA_INCLUDES
                ${ICEBERG_INCLUDES}
                SHARED_LINK_LIBS
                ${ICEBERG_DATA_SHARED_BUILD_INTERFACE_LIBS}
                STATIC_LINK_LIBS
                ${ICEBERG_DATA_STATIC_BUILD_INTERFACE_LIBS}
                STATIC_INSTALL_INTERFACE_LIBS
                ${ICEBERG_DATA_STATIC_INSTALL_INTERFACE_LIBS}
                SHARED_INSTALL_INTERFACE_LIBS
                ${ICEBERG_DATA_SHARED_INSTALL_INTERFACE_LIBS})

iceberg_install_all_headers(iceberg)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/version.h"
        DESTINATION "${ICEBERG_INSTALL_INCLUDEDIR}/iceberg")

add_subdirectory(catalog)
add_subdirectory(data)
add_subdirectory(deletes)
add_subdirectory(encryption)
add_subdirectory(expression)
add_subdirectory(manifest)
add_subdirectory(puffin)
add_subdirectory(row)
add_subdirectory(update)
add_subdirectory(util)
add_subdirectory(inspect)
add_subdirectory(metrics)
add_subdirectory(logging)

if(ICEBERG_BUILD_BUNDLE)
  set(ICEBERG_BUNDLE_SOURCES
      arrow/arrow_c_data_util.cc
      arrow/arrow_io.cc
      arrow/s3/arrow_s3_file_io.cc
      arrow/arrow_register.cc
      arrow/literal_util.cc
      arrow/metadata_column_util.cc
      avro/avro_data_util.cc
      avro/avro_direct_decoder.cc
      avro/avro_direct_encoder.cc
      avro/avro_metrics.cc
      avro/avro_reader.cc
      avro/avro_writer.cc
      avro/avro_register.cc
      avro/avro_schema_util.cc
      avro/avro_stream_internal.cc
      parquet/parquet_data_util.cc
      parquet/parquet_metrics.cc
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
       "$<IF:$<TARGET_EXISTS:iceberg_data_static>,iceberg_data_static,iceberg_data_shared>"
       "$<IF:$<TARGET_EXISTS:Arrow::arrow_static>,Arrow::arrow_static,Arrow::arrow_shared>"
       "$<IF:$<TARGET_EXISTS:Parquet::parquet_static>,Parquet::parquet_static,Parquet::parquet_shared>"
       "$<IF:$<TARGET_EXISTS:avro-cpp::avrocpp_static>,avro-cpp::avrocpp_static,avro-cpp::avrocpp_shared>"
  )
  list(APPEND
       ICEBERG_BUNDLE_SHARED_BUILD_INTERFACE_LIBS
       "$<IF:$<TARGET_EXISTS:iceberg_data_shared>,iceberg_data_shared,iceberg_data_static>"
       "$<IF:$<TARGET_EXISTS:Arrow::arrow_shared>,Arrow::arrow_shared,Arrow::arrow_static>"
       "$<IF:$<TARGET_EXISTS:Parquet::parquet_shared>,Parquet::parquet_shared,Parquet::parquet_static>"
       "$<IF:$<TARGET_EXISTS:avro-cpp::avrocpp_shared>,avro-cpp::avrocpp_shared,avro-cpp::avrocpp_static>"
  )

  list(APPEND
       ICEBERG_BUNDLE_STATIC_INSTALL_INTERFACE_LIBS
       "$<IF:$<TARGET_EXISTS:iceberg::iceberg_data_static>,iceberg::iceberg_data_static,iceberg::iceberg_data_shared>"
       "$<IF:$<BOOL:${ARROW_VENDORED}>,iceberg::arrow_static,$<IF:$<TARGET_EXISTS:Arrow::arrow_static>,Arrow::arrow_static,Arrow::arrow_shared>>"
       "$<IF:$<BOOL:${ARROW_VENDORED}>,iceberg::parquet_static,$<IF:$<TARGET_EXISTS:Parquet::parquet_static>,Parquet::parquet_static,Parquet::parquet_shared>>"
       "$<IF:$<BOOL:${AVRO_VENDORED}>,iceberg::avrocpp_s,$<IF:$<TARGET_EXISTS:avro-cpp::avrocpp_static>,avro-cpp::avrocpp_static,avro-cpp::avrocpp_shared>>"
  )
  list(APPEND
       ICEBERG_BUNDLE_SHARED_INSTALL_INTERFACE_LIBS
       "$<IF:$<TARGET_EXISTS:iceberg::iceberg_data_shared>,iceberg::iceberg_data_shared,iceberg::iceberg_data_static>"
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

  foreach(target iceberg_bundle_static iceberg_bundle_shared)
    if(TARGET ${target})
      if(ICEBERG_S3)
        target_compile_definitions(${target}
                                   PUBLIC "$<BUILD_INTERFACE:ICEBERG_S3_ENABLED=1>")
      else()
        target_compile_definitions(${target}
                                   PUBLIC "$<BUILD_INTERFACE:ICEBERG_S3_ENABLED=0>")
      endif()
    endif()
  endforeach()

  add_subdirectory(arrow)
  add_subdirectory(avro)
  add_subdirectory(parquet)
endif()

iceberg_install_cmake_package(iceberg iceberg_targets)

# When linking a system Thrift, downstream find_package(Iceberg) calls
# find_dependency(ThriftAlt); ship FindThriftAlt.cmake next to the package config
# (the dir iceberg-config.cmake adds to CMAKE_MODULE_PATH) so that lookup resolves.
if(ICEBERG_BUILD_HIVE AND NOT ICEBERG_THRIFT_BUNDLED)
  install(FILES "${PROJECT_SOURCE_DIR}/cmake_modules/FindThriftAlt.cmake"
          DESTINATION "${ICEBERG_INSTALL_CMAKEDIR}/iceberg")
endif()

if(ICEBERG_BUILD_TESTS)
  add_subdirectory(test)
endif()

if(ICEBERG_BUILD_BENCHMARKS)
  add_subdirectory(benchmark)
endif()
