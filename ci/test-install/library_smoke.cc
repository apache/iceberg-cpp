/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <iostream>

#include "iceberg/catalog/catalog_util.h"
#include "iceberg/catalog/session_catalog.h"
#include "iceberg/catalog/session_context.h"
#include "iceberg/data/data_writer.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "iceberg/version.h"

#ifdef INSTALL_TEST_REST
#  include "iceberg/catalog/rest/auth/token_refresh_scheduler.h"
#  include "iceberg/catalog/rest/endpoint.h"
#  include "iceberg/catalog/rest/rest_catalog.h"
#endif
#ifdef INSTALL_TEST_HIVE
#  include "iceberg/catalog/hive/hive_catalog.h"
#  include "iceberg/catalog/hive/hms_client.h"
#endif
#ifdef INSTALL_TEST_SQL_CATALOG
#  include "iceberg/catalog/sql/sql_catalog.h"
#endif

iceberg::Status CheckLibraries() {
  if (std::string_view(ICEBERG_PROJECT_NAME) != "Iceberg") {
    return iceberg::InvalidArgument("Unexpected project name in version.h");
  }
  const auto field = iceberg::SchemaField::MakeRequired(1, "id", iceberg::int32());
  if (field.field_id() != 1) return iceberg::InvalidArgument("Invalid schema field");
  // No format writers have been registered in this process.
  if (iceberg::DataWriter::Make({})) {
    return iceberg::InvalidArgument("Expected a missing writer error");
  }
#ifdef INSTALL_TEST_REST
  auto endpoint = iceberg::rest::Endpoint::FromString("GET /v1/config");
  if (!endpoint) return std::unexpected(endpoint.error());
  if (endpoint->ToString() != "GET /v1/config") {
    return iceberg::InvalidArgument("REST endpoint did not roundtrip");
  }
  iceberg::rest::auth::TokenRefreshScheduler scheduler;
  scheduler.Shutdown();
#endif
#ifdef INSTALL_TEST_HIVE
  auto endpoints = iceberg::hive::ParseHmsUris("thrift://localhost:9083");
  if (!endpoints) return std::unexpected(endpoints.error());
  if (endpoints->size() != 1 || endpoints->front().port != 9083) {
    return iceberg::InvalidArgument("Invalid Hive endpoint");
  }
#endif
#ifdef INSTALL_TEST_SQL_CATALOG
  if (iceberg::sql::SqlCatalog::Make({}, nullptr, nullptr)) {
    return iceberg::InvalidArgument("SQL catalog accepted a null store");
  }
#endif
#ifdef INSTALL_TEST_SQL_SQLITE
  auto store = iceberg::sql::MakeSqliteCatalogStore(
      {.catalog_name = "installed", .uri = ":memory:"});
  if (!store) return std::unexpected(store.error());
  auto initialized = (*store)->Initialize();
  if (!initialized) return initialized;
  auto inserted = (*store)->InsertNamespaceProperty("ns", "key", std::string("value"));
  if (!inserted) return inserted;
  auto namespaces = (*store)->ListNamespaceNames();
  if (!namespaces) return std::unexpected(namespaces.error());
  if (namespaces->size() != 1 || namespaces->front() != "ns") {
    return iceberg::InvalidArgument("SQLite namespace did not roundtrip");
  }
#endif
#ifdef INSTALL_TEST_SQL_POSTGRESQL
  if (iceberg::sql::MakePostgreSqlCatalogStore({.uri = "postgresql://"})) {
    return iceberg::InvalidArgument("PostgreSQL connector accepted an invalid URI");
  }
#endif
#ifdef INSTALL_TEST_SQL_MYSQL
  if (iceberg::sql::MakeMySqlCatalogStore({.uri = "mysql://"})) {
    return iceberg::InvalidArgument("MySQL connector accepted an invalid URI");
  }
#endif
  return {};
}

int main() {
  auto result = CheckLibraries();
  if (!result) {
    std::cerr << result.error().message << '\n';
    return 1;
  }
  return 0;
}
