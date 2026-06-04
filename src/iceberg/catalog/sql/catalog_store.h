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

#pragma once

/// \file iceberg/catalog/sql/catalog_store.h
/// Storage abstraction for the SQL catalog.
///
/// `CatalogStore` persists SQL catalog rows. `SqlCatalog` owns Iceberg
/// catalog semantics and uses this interface for database-specific row
/// operations.

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/catalog/sql/iceberg_sql_catalog_export.h"
#include "iceberg/result.h"

namespace iceberg::sql {

/// \brief A namespace property row.
struct ICEBERG_SQL_CATALOG_EXPORT NamespaceProperty {
  /// Property key.
  std::string key;
  /// Property value, or `std::nullopt` for SQL NULL.
  std::optional<std::string> value;
};

/// \brief Connection parameters used to construct a built-in `CatalogStore`.
struct ICEBERG_SQL_CATALOG_EXPORT CatalogStoreOptions {
  /// Logical catalog name used to scope rows.
  std::string catalog_name;
  /// Connector-specific connection string.
  std::string uri;
  /// Maximum number of database connections to use.
  int32_t max_connections = 1;
  /// Additional connector-specific properties.
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Semantic, driver-agnostic storage interface for the SQL catalog.
///
/// A store instance is scoped to one catalog name and persists namespace and
/// table rows. Implementations must be safe for concurrent calls from
/// `SqlCatalog`.
///
/// Unique or primary-key constraint violations from insert and rename
/// operations must be reported as `ErrorKind::kAlreadyExists`. Other database
/// failures should be reported as `ErrorKind::kIOError`.
class ICEBERG_SQL_CATALOG_EXPORT CatalogStore {
 public:
  virtual ~CatalogStore() = default;

  /// \brief Create the backing tables if they do not already exist.
  ///
  /// \return Status indicating success if the tables are ready.
  virtual Status Initialize() = 0;

  /// \brief Return the distinct namespace identifiers known to this catalog.
  ///
  /// \return A list of namespace identifiers in the store's string form.
  virtual Result<std::vector<std::string>> ListNamespaceNames() = 0;

  /// \brief Return all property rows stored for `ns`.
  ///
  /// \param ns A namespace identifier in the store's string form.
  /// \return The property rows stored for the namespace.
  virtual Result<std::vector<NamespaceProperty>> GetNamespaceProperties(
      std::string_view ns) = 0;

  /// \brief Insert one property row for `ns`.
  ///
  /// A primary-key collision (same namespace + key) must be reported as
  /// `ErrorKind::kAlreadyExists`.
  ///
  /// \param ns A namespace identifier in the store's string form.
  /// \param key Property key.
  /// \param value Property value, or `std::nullopt` for SQL NULL.
  /// \return Status indicating success if the row was inserted.
  virtual Status InsertNamespaceProperty(std::string_view ns, std::string_view key,
                                         std::optional<std::string_view> value) = 0;

  /// \brief Delete the property row identified by (`ns`, `key`), if present.
  ///
  /// \param ns A namespace identifier in the store's string form.
  /// \param key Property key.
  /// \return Status indicating success if the row was deleted or did not exist.
  virtual Status DeleteNamespaceProperty(std::string_view ns, std::string_view key) = 0;

  /// \brief Delete all property rows for `ns`.
  ///
  /// \param ns A namespace identifier in the store's string form.
  /// \return The number of rows deleted.
  virtual Result<int64_t> DeleteNamespace(std::string_view ns) = 0;

  /// \brief Return the table names stored directly under `ns`.
  ///
  /// \param ns A namespace identifier in the store's string form.
  /// \return A list of table names.
  virtual Result<std::vector<std::string>> ListTableNames(std::string_view ns) = 0;

  /// \brief Whether a row exists for the table (`ns`, `name`).
  ///
  /// \param ns A namespace identifier in the store's string form.
  /// \param name Table name.
  /// \return true if the table row exists, false otherwise.
  virtual Result<bool> TableExists(std::string_view ns, std::string_view name) = 0;

  /// \brief Return the current metadata location of (`ns`, `name`).
  ///
  /// \param ns A namespace identifier in the store's string form.
  /// \param name Table name.
  /// \return The metadata location, or `std::nullopt` if the table has no row
  /// or its `metadata_location` column is NULL.
  virtual Result<std::optional<std::string>> GetTableMetadataLocation(
      std::string_view ns, std::string_view name) = 0;

  /// \brief Insert a new table row with the given metadata location.
  ///
  /// The `previous_metadata_location` column is set to NULL. A primary-key
  /// collision must be reported as `ErrorKind::kAlreadyExists`.
  ///
  /// \param ns A namespace identifier in the store's string form.
  /// \param name Table name.
  /// \param metadata_location Table metadata location.
  /// \return Status indicating success if the row was inserted.
  virtual Status InsertTable(std::string_view ns, std::string_view name,
                             std::string_view metadata_location) = 0;

  /// \brief Conditionally update a table's metadata location (optimistic CAS).
  ///
  /// The update only applies when the stored `metadata_location` still equals
  /// `expected_current_location`.
  ///
  /// \param ns A namespace identifier in the store's string form.
  /// \param name Table name.
  /// \param new_location New table metadata location.
  /// \param new_previous_location New previous metadata location.
  /// \param expected_current_location Expected current metadata location.
  /// \return The number of rows updated (1 on success, 0 on a stale base).
  virtual Result<int64_t> UpdateTableMetadataLocation(
      std::string_view ns, std::string_view name, std::string_view new_location,
      std::string_view new_previous_location,
      std::string_view expected_current_location) = 0;

  /// \brief Delete the table row (`ns`, `name`), if present.
  ///
  /// \param ns A namespace identifier in the store's string form.
  /// \param name Table name.
  /// \return The number of rows deleted.
  virtual Result<int64_t> DeleteTable(std::string_view ns, std::string_view name) = 0;

  /// \brief Move a table row to a new namespace and/or name.
  ///
  /// A primary-key collision with an existing target must be reported as
  /// `ErrorKind::kAlreadyExists`.
  ///
  /// \param from_ns Source namespace identifier in the store's string form.
  /// \param from_name Source table name.
  /// \param to_ns Target namespace identifier in the store's string form.
  /// \param to_name Target table name.
  /// \return The number of rows updated (1 on success, 0 if the source is gone).
  virtual Result<int64_t> RenameTable(std::string_view from_ns,
                                      std::string_view from_name, std::string_view to_ns,
                                      std::string_view to_name) = 0;

  /// \brief Execute `body` atomically inside a single transaction.
  ///
  /// The implementation begins a transaction, invokes `body` (which issues
  /// store operations on this same instance), then commits if `body` returns
  /// success or rolls back otherwise. Implementations are not required to
  /// support nesting.
  ///
  /// \param body Transaction body to execute.
  /// \return Status returned by `body`, or a transaction error.
  virtual Status RunInTransaction(const std::function<Status()>& body) = 0;
};

/// \brief Build the built-in SQLite catalog store (sqlpp23 + SQLite3).
///
/// `options.uri` is the SQLite database file path, or ":memory:".
///
/// \param options Store connection options.
/// \return A catalog store instance, or ErrorKind::kNotSupported if the SQLite
///         connector was not built.
ICEBERG_SQL_CATALOG_EXPORT Result<std::shared_ptr<CatalogStore>> MakeSqliteCatalogStore(
    const CatalogStoreOptions& options);

/// \brief Build the built-in PostgreSQL catalog store (sqlpp23 + libpq).
///
/// `options.uri` is parsed as
/// `[scheme://][user[:password]@]host[:port][/database]`.
///
/// \param options Store connection options.
/// \return A catalog store instance, or ErrorKind::kNotSupported if the
///         PostgreSQL connector was not built.
ICEBERG_SQL_CATALOG_EXPORT Result<std::shared_ptr<CatalogStore>>
MakePostgreSqlCatalogStore(const CatalogStoreOptions& options);

/// \brief Build the built-in MySQL catalog store (sqlpp23 + libmysqlclient).
///
/// `options.uri` is parsed as
/// `[scheme://][user[:password]@]host[:port][/database]`.
///
/// \param options Store connection options.
/// \return A catalog store instance, or ErrorKind::kNotSupported if the MySQL
///         connector was not built.
ICEBERG_SQL_CATALOG_EXPORT Result<std::shared_ptr<CatalogStore>> MakeMySqlCatalogStore(
    const CatalogStoreOptions& options);

}  // namespace iceberg::sql
