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

/// \file iceberg/catalog/sql/sql_catalog.h
/// SQL catalog implementation.
///
/// `SqlCatalog` implements the Iceberg `Catalog` API on top of a relational
/// database. Database access goes through the `CatalogStore` interface.

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/catalog.h"
#include "iceberg/catalog/sql/catalog_store.h"
#include "iceberg/catalog/sql/iceberg_sql_catalog_export.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"

namespace iceberg {
class MetricsReporter;
}  // namespace iceberg

namespace iceberg::sql {

/// \brief Default maximum number of database connections for built-in SQL stores.
constexpr static int32_t kMaxConnections = 10;

/// \brief Configuration for the SQL catalog.
struct ICEBERG_SQL_CATALOG_EXPORT SqlCatalogConfig {
  /// Logical catalog name. Scopes all rows via the `catalog_name` column so that
  /// multiple catalogs can share one database.
  std::string name = "sql_catalog";
  /// Database connection string interpreted by the chosen `CatalogStore`.
  std::string uri;
  /// Base location used to derive table locations when none is supplied.
  std::string warehouse_location;
  /// Maximum number of database connections. Built-in stores use a single
  /// connection when this is 1 and a bounded sqlpp23 connection pool otherwise.
  int32_t max_connections = kMaxConnections;
  /// Additional connector-specific properties.
  std::unordered_map<std::string, std::string> props;
};

/// \brief SQL-backed Iceberg catalog.
class ICEBERG_SQL_CATALOG_EXPORT SqlCatalog
    : public Catalog,
      public std::enable_shared_from_this<SqlCatalog> {
 public:
  ~SqlCatalog() override;

  SqlCatalog(const SqlCatalog&) = delete;
  SqlCatalog& operator=(const SqlCatalog&) = delete;
  SqlCatalog(SqlCatalog&&) = delete;
  SqlCatalog& operator=(SqlCatalog&&) = delete;

  /// \brief Create a catalog backed by a user-supplied `CatalogStore`.
  ///
  /// This is the extension point for custom databases/drivers: implement
  /// `CatalogStore` and pass it here. The catalog initializes its schema
  /// (creating the backing tables if they do not exist) before returning.
  ///
  /// \param config Catalog configuration (name and warehouse location are used;
  ///        `uri` is informational because the store is already constructed).
  /// \param file_io File IO used to read and write table metadata files.
  /// \param store The catalog metadata store. Must not be null.
  static Result<std::shared_ptr<SqlCatalog>> Make(const SqlCatalogConfig& config,
                                                  std::shared_ptr<FileIO> file_io,
                                                  std::shared_ptr<CatalogStore> store);

  std::string_view name() const override;

  Status CreateNamespace(
      const Namespace& ns,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<std::vector<Namespace>> ListNamespaces(const Namespace& ns) const override;

  Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
      const Namespace& ns) const override;

  Status DropNamespace(const Namespace& ns) override;

  Result<bool> NamespaceExists(const Namespace& ns) const override;

  Status UpdateNamespaceProperties(
      const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
      const std::unordered_set<std::string>& removals) override;

  Result<std::vector<TableIdentifier>> ListTables(const Namespace& ns) const override;

  Result<std::shared_ptr<Table>> CreateTable(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
      const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<std::shared_ptr<Table>> UpdateTable(
      const TableIdentifier& identifier,
      const std::vector<std::unique_ptr<TableRequirement>>& requirements,
      const std::vector<std::unique_ptr<TableUpdate>>& updates) override;

  Result<std::shared_ptr<Transaction>> StageCreateTable(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
      const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<bool> TableExists(const TableIdentifier& identifier) const override;

  /// \brief Drop a table.
  ///
  /// SqlCatalog currently removes only the catalog entry. Support for deleting
  /// table data and metadata files when `purge` is true is not implemented yet.
  Status DropTable(const TableIdentifier& identifier, bool purge) override;

  Status RenameTable(const TableIdentifier& from, const TableIdentifier& to) override;

  Result<std::shared_ptr<Table>> LoadTable(const TableIdentifier& identifier) override;

  Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier,
      const std::string& metadata_file_location) override;

  /// \brief Create a catalog backed by the built-in SQLite client.
  ///
  /// \param config `uri` is the SQLite database file path (or ":memory:").
  /// \param file_io File IO used to read and write table metadata files.
  /// \return A catalog instance, or ErrorKind::kNotSupported if the SQLite
  ///         connector was not built.
  static Result<std::shared_ptr<SqlCatalog>> MakeSqliteCatalog(
      const SqlCatalogConfig& config, std::shared_ptr<FileIO> file_io);

  /// \brief Create a catalog backed by the built-in PostgreSQL (libpq) client.
  ///
  /// \param config `uri` is parsed as
  ///        `[scheme://][user[:password]@]host[:port][/database]`.
  /// \param file_io File IO used to read and write table metadata files.
  /// \return A catalog instance, or ErrorKind::kNotSupported if the PostgreSQL
  ///         connector was not built.
  static Result<std::shared_ptr<SqlCatalog>> MakePostgreSqlCatalog(
      const SqlCatalogConfig& config, std::shared_ptr<FileIO> file_io);

  /// \brief Create a catalog backed by the built-in MySQL client.
  ///
  /// \param config `uri` is parsed as
  ///        `[scheme://][user[:password]@]host[:port][/database]`.
  /// \param file_io File IO used to read and write table metadata files.
  /// \return A catalog instance, or ErrorKind::kNotSupported if the MySQL
  ///         connector was not built.
  static Result<std::shared_ptr<SqlCatalog>> MakeMySqlCatalog(
      const SqlCatalogConfig& config, std::shared_ptr<FileIO> file_io);

 private:
  SqlCatalog(SqlCatalogConfig config, std::shared_ptr<FileIO> file_io,
             std::shared_ptr<CatalogStore> store);

  /// \brief Resolve the current metadata location for a table, or NoSuchTable.
  Result<std::string> GetTableMetadataLocation(const TableIdentifier& identifier) const;

  /// \brief Build a Table object from a metadata location.
  Result<std::shared_ptr<Table>> LoadTableFrom(const TableIdentifier& identifier,
                                               const std::string& metadata_location);

  SqlCatalogConfig config_;
  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<CatalogStore> store_;
  std::shared_ptr<iceberg::MetricsReporter> reporter_;
};

}  // namespace iceberg::sql
