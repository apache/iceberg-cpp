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
    arrow_c_data_internal.cc
    demo.cc
    expression/expression.cc
    file_reader.cc
    json_internal.cc
    manifest_entry.cc
    manifest_list.cc
    partition_field.cc
    partition_spec.cc
    schema.cc
    schema_field.cc
    schema_internal.cc
    snapshot.cc
    sort_field.cc
    sort_order.cc
    statistics_file.cc
    table_metadata.cc
    transform.cc
    transform_function.cc
    type.cc
    snapshot.cc
    util/murmurhash3_internal.cc
    util/timepoint.cc)

set(ICEBERG_STATIC_BUILD_INTERFACE_LIBS)
set(ICEBERG_SHARED_BUILD_INTERFACE_LIBS)
set(ICEBERG_STATIC_INSTALL_INTERFACE_LIBS)
set(ICEBERG_SHARED_INSTALL_INTERFACE_LIBS)

list(APPEND ICEBERG_STATIC_BUILD_INTERFACE_LIBS nanoarrow::nanoarrow
     nlohmann_json::nlohmann_json)
list(APPEND ICEBERG_SHARED_BUILD_INTERFACE_LIBS nanoarrow::nanoarrow
     nlohmann_json::nlohmann_json)
list(APPEND ICEBERG_STATIC_INSTALL_INTERFACE_LIBS "Iceberg::nanoarrow"
     "Iceberg::nlohmann_json")
list(APPEND ICEBERG_SHARED_INSTALL_INTERFACE_LIBS "Iceberg::nanoarrow"
     "Iceberg::nlohmann_json")

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
                ${ICEBERG_SHARED_INSTALL_INTERFACE_LIBS})

iceberg_install_all_headers(iceberg)

add_subdirectory(expression)
add_subdirectory(util)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/iceberg_export.h
        DESTINATION ${ICEBERG_INSTALL_INCLUDEDIR}/iceberg)

if(ICEBERG_BUILD_BUNDLE)
  set(ICEBERG_BUNDLE_SOURCES arrow/demo_arrow.cc arrow/arrow_fs_file_io.cc
                             avro/demo_avro.cc)

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
       "$<IF:$<TARGET_EXISTS:Avro::avrocpp_static>,Avro::avrocpp_static,Avro::avrocpp_shared>"
  )
  list(APPEND
       ICEBERG_BUNDLE_SHARED_BUILD_INTERFACE_LIBS
       "$<IF:$<TARGET_EXISTS:iceberg_shared>,iceberg_shared,iceberg_static>"
       "$<IF:$<TARGET_EXISTS:Arrow::arrow_shared>,Arrow::arrow_shared,Arrow::arrow_static>"
       "$<IF:$<TARGET_EXISTS:Parquet::parquet_shared>,Parquet::parquet_shared,Parquet::parquet_static>"
       "$<IF:$<TARGET_EXISTS:Avro::avrocpp_shared>,Avro::avrocpp_shared,Avro::avrocpp_static>"
  )

  list(APPEND
       ICEBERG_BUNDLE_STATIC_INSTALL_INTERFACE_LIBS
       "$<IF:$<TARGET_EXISTS:Iceberg::iceberg_static>,Iceberg::iceberg_static,Iceberg::iceberg_shared>"
       "$<IF:$<BOOL:${ARROW_VENDORED}>,Iceberg::arrow_static,$<IF:$<TARGET_EXISTS:Arrow::arrow_static>,Arrow::arrow_static,Arrow::arrow_shared>>"
       "$<IF:$<BOOL:${ARROW_VENDORED}>,Iceberg::parquet_static,$<IF:$<TARGET_EXISTS:Parquet::parquet_static>,Parquet::parquet_static,Parquet::parquet_shared>>"
       "$<IF:$<BOOL:${AVRO_VENDORED}>,Iceberg::avrocpp_s,$<IF:$<TARGET_EXISTS:Avro::avrocpp_static>,Avro::avrocpp_static,Avro::avrocpp_shared>>"
  )
  list(APPEND
       ICEBERG_BUNDLE_SHARED_INSTALL_INTERFACE_LIBS
       "$<IF:$<TARGET_EXISTS:Iceberg::iceberg_shared>,Iceberg::iceberg_shared,Iceberg::iceberg_static>"
       "$<IF:$<BOOL:${ARROW_VENDORED}>,Iceberg::arrow_static,$<IF:$<TARGET_EXISTS:Arrow::arrow_shared>,Arrow::arrow_shared,Arrow::arrow_static>>"
       "$<IF:$<BOOL:${ARROW_VENDORED}>,Iceberg::parquet_static,$<IF:$<TARGET_EXISTS:Parquet::parquet_shared>,Parquet::parquet_shared,Parquet::parquet_static>>"
       "$<IF:$<BOOL:${AVRO_VENDORED}>,Iceberg::avrocpp_s,$<IF:$<TARGET_EXISTS:Avro::avrocpp_shared>,Avro::avrocpp_shared,Avro::avrocpp_static>>"
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
                  ${ICEBERG_BUNDLE_SHARED_INSTALL_INTERFACE_LIBS})

  add_subdirectory(arrow)
  add_subdirectory(avro)

  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/iceberg_bundle_export.h
          DESTINATION ${ICEBERG_INSTALL_INCLUDEDIR}/iceberg)
endif()

iceberg_install_cmake_package(Iceberg iceberg_targets)
