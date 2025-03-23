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
    demo_table.cc
    schema.cc
    schema_field.cc
    type.cc)

set(ICEBERG_STATIC_BUILD_INTERFACE_LIBS)
set(ICEBERG_SHARED_BUILD_INTERFACE_LIBS)
set(ICEBERG_STATIC_INSTALL_INTERFACE_LIBS)
set(ICEBERG_SHARED_INSTALL_INTERFACE_LIBS)

list(APPEND ICEBERG_STATIC_BUILD_INTERFACE_LIBS nanoarrow::nanoarrow)
list(APPEND ICEBERG_SHARED_BUILD_INTERFACE_LIBS nanoarrow::nanoarrow)
list(APPEND ICEBERG_STATIC_INSTALL_INTERFACE_LIBS "Iceberg::nanoarrow")
list(APPEND ICEBERG_SHARED_INSTALL_INTERFACE_LIBS "Iceberg::nanoarrow")

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

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/iceberg_export.h
        DESTINATION ${ICEBERG_INSTALL_INCLUDEDIR}/iceberg)

add_subdirectory(arrow)
add_subdirectory(avro)
add_subdirectory(puffin)

iceberg_install_cmake_package(Iceberg iceberg_targets)
