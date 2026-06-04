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

# Select the built-in database connectors to compile. The catalog logic itself
# is database-agnostic; each connector is an optional `CatalogStore` implementation
# built on sqlpp23 and linked against its native client library.
#
# sqlpp23 is a build-time-only (header-only) dependency: it is compiled into the
# connector translation units but never appears in the installed interface, so
# downstream consumers only need the native client libraries.
set(ICEBERG_SQL_CATALOG_SOURCES connection_uri.cc sql_catalog.cc)
# Targets used while building (header-only sqlpp23 connector targets).
set(ICEBERG_SQL_CATALOG_CONNECTOR_BUILD_LIBS)
# Native client libraries required at link time, including by installed consumers.
set(ICEBERG_SQL_CATALOG_CONNECTOR_INSTALL_LIBS)

if(ICEBERG_SQL_SQLITE)
  set(BUILD_SQLITE3_CONNECTOR ON)
  list(APPEND ICEBERG_SQL_CATALOG_SOURCES catalog_store_sqlite3.cc)
  list(APPEND ICEBERG_SQL_CATALOG_CONNECTOR_BUILD_LIBS sqlpp23::sqlite3)
  list(APPEND ICEBERG_SQL_CATALOG_CONNECTOR_INSTALL_LIBS SQLite::SQLite3)
endif()

if(ICEBERG_SQL_POSTGRESQL)
  set(BUILD_POSTGRESQL_CONNECTOR ON)
  list(APPEND ICEBERG_SQL_CATALOG_SOURCES catalog_store_postgresql.cc)
  list(APPEND ICEBERG_SQL_CATALOG_CONNECTOR_BUILD_LIBS sqlpp23::postgresql)
  list(APPEND ICEBERG_SQL_CATALOG_CONNECTOR_INSTALL_LIBS PostgreSQL::PostgreSQL)
endif()

if(ICEBERG_SQL_MYSQL)
  set(BUILD_MYSQL_CONNECTOR ON)
  list(APPEND ICEBERG_SQL_CATALOG_SOURCES catalog_store_mysql.cc)
  list(APPEND ICEBERG_SQL_CATALOG_CONNECTOR_BUILD_LIBS sqlpp23::mysql)
  list(APPEND ICEBERG_SQL_CATALOG_CONNECTOR_INSTALL_LIBS MySQL::MySQL)
endif()

# config.h.in uses #cmakedefine for the BUILD_*_CONNECTOR variables set above,
# so it must be configured after the connectors are selected.
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/config.h.in"
               "${CMAKE_CURRENT_BINARY_DIR}/config.h")

set(ICEBERG_SQL_CATALOG_STATIC_BUILD_INTERFACE_LIBS)
set(ICEBERG_SQL_CATALOG_SHARED_BUILD_INTERFACE_LIBS)
set(ICEBERG_SQL_CATALOG_STATIC_INSTALL_INTERFACE_LIBS)
set(ICEBERG_SQL_CATALOG_SHARED_INSTALL_INTERFACE_LIBS)

# The sqlpp23 connector targets are header-only and used only while building.
# The installed interface exposes only the native client libraries.
list(APPEND
     ICEBERG_SQL_CATALOG_STATIC_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_static>,iceberg_static,iceberg_shared>"
     ${ICEBERG_SQL_CATALOG_CONNECTOR_BUILD_LIBS}
     ${ICEBERG_SQL_CATALOG_CONNECTOR_INSTALL_LIBS})
list(APPEND
     ICEBERG_SQL_CATALOG_SHARED_BUILD_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg_shared>,iceberg_shared,iceberg_static>"
     ${ICEBERG_SQL_CATALOG_CONNECTOR_BUILD_LIBS}
     ${ICEBERG_SQL_CATALOG_CONNECTOR_INSTALL_LIBS})
list(APPEND
     ICEBERG_SQL_CATALOG_STATIC_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_static>,iceberg::iceberg_static,iceberg::iceberg_shared>"
     ${ICEBERG_SQL_CATALOG_CONNECTOR_INSTALL_LIBS})
list(APPEND
     ICEBERG_SQL_CATALOG_SHARED_INSTALL_INTERFACE_LIBS
     "$<IF:$<TARGET_EXISTS:iceberg::iceberg_shared>,iceberg::iceberg_shared,iceberg::iceberg_static>"
     ${ICEBERG_SQL_CATALOG_CONNECTOR_INSTALL_LIBS})

add_iceberg_lib(iceberg_sql_catalog
                SOURCES
                ${ICEBERG_SQL_CATALOG_SOURCES}
                SHARED_LINK_LIBS
                ${ICEBERG_SQL_CATALOG_SHARED_BUILD_INTERFACE_LIBS}
                STATIC_LINK_LIBS
                ${ICEBERG_SQL_CATALOG_STATIC_BUILD_INTERFACE_LIBS}
                STATIC_INSTALL_INTERFACE_LIBS
                ${ICEBERG_SQL_CATALOG_STATIC_INSTALL_INTERFACE_LIBS}
                SHARED_INSTALL_INTERFACE_LIBS
                ${ICEBERG_SQL_CATALOG_SHARED_INSTALL_INTERFACE_LIBS})

iceberg_install_all_headers(iceberg/catalog/sql)
