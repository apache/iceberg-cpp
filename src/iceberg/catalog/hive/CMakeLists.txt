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

# The iceberg_hive library: a Hive Metastore (HMS) catalog client built on
# generated Apache Thrift bindings. Layout mirrors iceberg_rest.

# ----------------------------------------------------------------------
# Hive Metastore Thrift bindings.
#
# These are checked into gen-cpp/ rather than generated at build time, so a
# normal build needs no Thrift IDL compiler — only the Thrift C++ runtime,
# which comes from Apache Arrow's bundled build. Regenerate them with
# dev/update_hive_thrift.sh whenever thirdparty/hive_metastore/*.thrift changes.

set(_thrift_gen_dir ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp)

set(ICEBERG_HIVE_THRIFT_GEN_SOURCES
    ${_thrift_gen_dir}/FacebookService.cpp
    ${_thrift_gen_dir}/fb303_types.cpp
    ${_thrift_gen_dir}/hive_metastore_constants.cpp
    ${_thrift_gen_dir}/hive_metastore_types.cpp
    ${_thrift_gen_dir}/ThriftHiveMetastore.cpp)

# Upstream-generated code: skip lint/format, and downgrade the deprecated
# std::iterator warning from Arrow's bundled Thrift 0.22 headers (removed
# upstream in Thrift 0.23, THRIFT-5698) so it does not trip -Werror.
set_source_files_properties(${ICEBERG_HIVE_THRIFT_GEN_SOURCES}
                            PROPERTIES SKIP_LINTING TRUE
                                       COMPILE_OPTIONS
                                       "$<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wno-error=deprecated-declarations>"
)

# ----------------------------------------------------------------------
# iceberg_hive library

if(NOT TARGET thrift::thrift)
  message(FATAL_ERROR "iceberg_hive requires a `thrift::thrift` target. Either keep "
                      "ICEBERG_BUNDLE_THRIFT=ON with -DICEBERG_BUILD_BUNDLE=ON (the "
                      "default, uses Arrow's bundled Thrift), or build with "
                      "-DICEBERG_BUNDLE_THRIFT=OFF against a system Thrift install.")
endif()

set(ICEBERG_HIVE_SOURCES hive_catalog.cc hive_catalog_properties.cc
                         ${ICEBERG_HIVE_THRIFT_GEN_SOURCES})

set(ICEBERG_HIVE_STATIC_BUILD_INTERFACE_LIBS)
set(ICEBERG_HIVE_SHARED_BUILD_INTERFACE_LIBS)
set(ICEBERG_HIVE_STATIC_INSTALL_INTERFACE_LIBS)
set(ICEBERG_HIVE_SHARED_INSTALL_INTERFACE_LIBS)

list(APPEND ICEBERG_HIVE_STATIC_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_static>,iceberg_static,iceberg_shared>" thrift::thrift)
list(APPEND ICEBERG_HIVE_SHARED_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_shared>,iceberg_shared,iceberg_static>" thrift::thrift)
list(APPEND
     ICEBERG_HIVE_STATIC_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_static>,iceberg::iceberg_static,iceberg::iceberg_shared>"
     thrift::thrift)
list(APPEND
     ICEBERG_HIVE_SHARED_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_shared>,iceberg::iceberg_shared,iceberg::iceberg_static>"
     thrift::thrift)

add_iceberg_lib(iceberg_hive
                SOURCES
                ${ICEBERG_HIVE_SOURCES}
                PRIVATE_INCLUDES
                ${_thrift_gen_dir}
                SHARED_LINK_LIBS
                ${ICEBERG_HIVE_SHARED_BUILD_INTERFACE_LIBS}
                STATIC_LINK_LIBS
                ${ICEBERG_HIVE_STATIC_BUILD_INTERFACE_LIBS}
                STATIC_INSTALL_INTERFACE_LIBS
                ${ICEBERG_HIVE_STATIC_INSTALL_INTERFACE_LIBS}
                SHARED_INSTALL_INTERFACE_LIBS
                ${ICEBERG_HIVE_SHARED_INSTALL_INTERFACE_LIBS})

iceberg_install_all_headers(iceberg/catalog/hive)
