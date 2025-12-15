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

set(ICEBERG_REST_SOURCES
    catalog_properties.cc
    endpoint.cc
    error_handlers.cc
    http_client.cc
    json_internal.cc
    resource_paths.cc
    rest_catalog.cc
    rest_util.cc)

set(ICEBERG_REST_STATIC_BUILD_INTERFACE_LIBS)
set(ICEBERG_REST_SHARED_BUILD_INTERFACE_LIBS)
set(ICEBERG_REST_STATIC_INSTALL_INTERFACE_LIBS)
set(ICEBERG_REST_SHARED_INSTALL_INTERFACE_LIBS)

list(APPEND ICEBERG_REST_STATIC_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_static>,iceberg_static,iceberg_shared>" cpr::cpr)
list(APPEND ICEBERG_REST_SHARED_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_shared>,iceberg_shared,iceberg_static>" cpr::cpr)
list(APPEND
     ICEBERG_REST_STATIC_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_static>,iceberg::iceberg_static,iceberg::iceberg_shared>"
     "$<IF:$<BOOL:${CPR_VENDORED}>,iceberg::cpr,cpr::cpr>")
list(APPEND
     ICEBERG_REST_SHARED_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_shared>,iceberg::iceberg_shared,iceberg::iceberg_static>"
     "$<IF:$<BOOL:${CPR_VENDORED}>,iceberg::cpr,cpr::cpr>")

add_iceberg_lib(iceberg_rest
                SOURCES
                ${ICEBERG_REST_SOURCES}
                SHARED_LINK_LIBS
                ${ICEBERG_REST_SHARED_BUILD_INTERFACE_LIBS}
                STATIC_LINK_LIBS
                ${ICEBERG_REST_STATIC_BUILD_INTERFACE_LIBS}
                STATIC_INSTALL_INTERFACE_LIBS
                ${ICEBERG_REST_STATIC_INSTALL_INTERFACE_LIBS}
                SHARED_INSTALL_INTERFACE_LIBS
                ${ICEBERG_REST_SHARED_INSTALL_INTERFACE_LIBS})

iceberg_install_all_headers(iceberg/catalog/rest)
