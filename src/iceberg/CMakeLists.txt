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

set(ICEBERG_SOURCES demo_table.cc)
set(ICEBERG_INCLUDES "${CMAKE_SOURCE_DIR}/src")

add_iceberg_lib(iceberg
                SOURCES
                ${ICEBERG_SOURCES}
                OUTPUTS
                ICEBERG_LIBRARIES
                PRIVATE_INCLUDES
                ${ICEBERG_INCLUDES})

iceberg_set_export_definitions(iceberg_static "${ICEBERG_LIBRARIES}")
iceberg_install_all_headers(iceberg)

add_subdirectory(arrow)
add_subdirectory(puffin)

iceberg_install_cmake_package(Iceberg iceberg_targets)
