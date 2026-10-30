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

#include <exception>
#include <memory>
#include <string_view>

#include <sqlpp23/postgresql/postgresql.h>

#include "iceberg/catalog/sql/catalog_store.h"
#include "iceberg/catalog/sql/catalog_store_sqlpp23_internal.h"
#include "iceberg/catalog/sql/connection_uri_internal.h"
#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg::sql {

namespace {

/// Recognizes PostgreSQL unique-constraint violations via SQLSTATE 23505
/// (unique_violation), reported on `result_exception`.
struct PostgresqlTraits {
  static bool IsUniqueViolation(const std::exception& error) {
    const auto* result_error =
        dynamic_cast<const sqlpp::postgresql::result_exception*>(&error);
    return result_error != nullptr && result_error->sql_state() == "23505";
  }

  static bool IsDuplicateColumn(const std::exception& error) {
    const auto* result_error =
        dynamic_cast<const sqlpp::postgresql::result_exception*>(&error);
    return result_error != nullptr && result_error->sql_state() == "42701";
  }
};

using PostgresqlSingleCatalogStore =
    Sqlpp23CatalogStore<SingleConnectionSource<sqlpp::postgresql::connection>,
                        PostgresqlTraits>;
using PostgresqlPooledCatalogStore =
    Sqlpp23CatalogStore<PooledConnectionSource<sqlpp::postgresql::connection_pool>,
                        PostgresqlTraits>;

}  // namespace

Result<std::shared_ptr<CatalogStore>> MakePostgreSqlCatalogStore(
    const CatalogStoreOptions& options) {
  ICEBERG_ASSIGN_OR_RAISE(const auto parsed, ParseConnectionUri(options.uri));

  auto config = std::make_shared<sqlpp::postgresql::connection_config>();
  config->host = parsed.host;
  if (parsed.port.has_value()) {
    config->port = *parsed.port;
  }
  config->dbname = parsed.database;
  config->user = parsed.user;
  config->password = parsed.password;

  try {
    if (options.max_connections > 1) {
      sqlpp::postgresql::connection_pool pool(
          config, static_cast<std::size_t>(options.max_connections));
      return std::make_shared<PostgresqlPooledCatalogStore>(
          options.catalog_name, std::move(pool), options.max_connections);
    }

    sqlpp::postgresql::connection connection(config);
    return std::make_shared<PostgresqlSingleCatalogStore>(options.catalog_name,
                                                          std::move(connection));
  } catch (const std::exception& e) {
    return IOError("Failed to connect to PostgreSQL '{}': {}",
                   RedactConnectionUri(options.uri), e.what());
  }
}

}  // namespace iceberg::sql
