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
#include <mutex>

#include <sqlpp23/mysql/mysql.h>

#include "iceberg/catalog/sql/catalog_store.h"
#include "iceberg/catalog/sql/catalog_store_sqlpp23_internal.h"
#include "iceberg/catalog/sql/connection_uri_internal.h"
#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg::sql {

namespace {

// MySQL server error code for a duplicate key (ER_DUP_ENTRY).
constexpr unsigned int kMysqlErrDupEntry = 1062;
// MySQL server error code for a duplicate column (ER_DUP_FIELDNAME).
constexpr unsigned int kMysqlErrDupFieldName = 1060;

/// Recognizes MySQL unique-constraint violations via the ER_DUP_ENTRY code.
struct MysqlTraits {
  static bool IsUniqueViolation(const std::exception& error) {
    const auto* mysql_error = dynamic_cast<const sqlpp::mysql::exception*>(&error);
    return mysql_error != nullptr && mysql_error->error_code() == kMysqlErrDupEntry;
  }

  static bool IsDuplicateColumn(const std::exception& error) {
    const auto* mysql_error = dynamic_cast<const sqlpp::mysql::exception*>(&error);
    return mysql_error != nullptr && mysql_error->error_code() == kMysqlErrDupFieldName;
  }
};

using MysqlSingleCatalogStore =
    Sqlpp23CatalogStore<SingleConnectionSource<sqlpp::mysql::connection>, MysqlTraits>;
using MysqlPooledCatalogStore =
    Sqlpp23CatalogStore<PooledConnectionSource<sqlpp::mysql::connection_pool>,
                        MysqlTraits>;

void EnsureLibraryInitialized() {
  static std::once_flag flag;
  std::call_once(flag, [] { sqlpp::mysql::global_library_init(); });
}

}  // namespace

Result<std::shared_ptr<CatalogStore>> MakeMySqlCatalogStore(
    const CatalogStoreOptions& options) {
  EnsureLibraryInitialized();

  ICEBERG_ASSIGN_OR_RAISE(const auto parsed, ParseConnectionUri(options.uri));

  auto config = std::make_shared<sqlpp::mysql::connection_config>();
  if (!parsed.host.empty()) {
    config->host = parsed.host;
  }
  if (parsed.port.has_value()) {
    config->port = *parsed.port;
  }
  config->database = parsed.database;
  config->user = parsed.user;
  config->password = parsed.password;

  try {
    if (options.max_connections > 1) {
      sqlpp::mysql::connection_pool pool(
          config, static_cast<std::size_t>(options.max_connections));
      return std::make_shared<MysqlPooledCatalogStore>(
          options.catalog_name, std::move(pool), options.max_connections);
    }

    sqlpp::mysql::connection connection(config);
    return std::make_shared<MysqlSingleCatalogStore>(options.catalog_name,
                                                     std::move(connection));
  } catch (const std::exception& e) {
    return IOError("Failed to connect to MySQL '{}': {}",
                   RedactConnectionUri(options.uri), e.what());
  }
}

}  // namespace iceberg::sql
