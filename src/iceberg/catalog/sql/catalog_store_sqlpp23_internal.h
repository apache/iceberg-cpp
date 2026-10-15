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

/// \file iceberg/catalog/sql/catalog_store_sqlpp23_internal.h
/// Generic `CatalogStore` implementation on top of sqlpp23.
///
/// The query bodies are identical across connectors: sqlpp23 serializes the
/// same typed statements into each SQL dialect automatically. Only two things
/// vary per connector, both supplied by the `Traits` policy:
///   - how driver errors are recognized as unique-constraint violations
///     (`Traits::IsUniqueViolation`).
///   - how driver errors are recognized as duplicate-column migrations
///     (`Traits::IsDuplicateColumn`).
///
/// This is an internal header that pulls in sqlpp23; it is only included by the
/// per-connector factory translation units (`catalog_store_*.cc`).

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <sqlpp23/sqlpp23.h>

#include "iceberg/catalog/sql/catalog_store.h"
#include "iceberg/catalog/sql/sql_catalog_tables_internal.h"
#include "iceberg/result.h"

namespace iceberg::sql {

template <typename Connection>
class SingleConnectionSource {
 public:
  explicit SingleConnectionSource(Connection connection)
      : connection_(std::move(connection)) {}

  template <typename Fn>
  decltype(auto) WithConnection(Fn&& fn) {
    std::lock_guard lock(mutex_);
    return std::forward<Fn>(fn)(connection_);
  }

  Status RunInTransaction(const std::function<Status()>& body) {
    std::lock_guard lock(mutex_);
    auto transaction = sqlpp::start_transaction(connection_);
    try {
      Status status = body();
      if (status.has_value()) {
        transaction.commit();
      } else {
        transaction.rollback();
      }
      return status;
    } catch (...) {
      try {
        transaction.rollback();
      } catch (...) {
      }
      throw;
    }
  }

 private:
  Connection connection_;
  std::recursive_mutex mutex_;
};

template <typename Pool>
class PooledConnectionSource {
 public:
  using Connection = typename Pool::_pooled_connection_t;

  PooledConnectionSource(Pool pool, int32_t max_connections)
      : pool_(std::move(pool)), max_connections_(std::max<int32_t>(1, max_connections)) {}

  template <typename Fn>
  decltype(auto) WithConnection(Fn&& fn) {
    if (auto* connection = ActiveTransactionConnection()) {
      return std::forward<Fn>(fn)(*connection);
    }

    ConnectionLease lease(*this);
    auto connection = pool_.get();
    return std::forward<Fn>(fn)(connection);
  }

  Status RunInTransaction(const std::function<Status()>& body) {
    if (ActiveTransactionConnection() != nullptr) {
      return InvalidArgument("Nested SQL catalog transactions are not supported");
    }

    ConnectionLease lease(*this);
    auto connection = pool_.get();
    TransactionContext context{this, &connection};
    ScopedTransactionContext scope(context);

    auto transaction = sqlpp::start_transaction(connection);
    try {
      Status status = body();
      if (status.has_value()) {
        transaction.commit();
      } else {
        transaction.rollback();
      }
      return status;
    } catch (...) {
      try {
        transaction.rollback();
      } catch (...) {
      }
      throw;
    }
  }

 private:
  struct TransactionContext {
    PooledConnectionSource* owner;
    Connection* connection;
  };

  class ScopedTransactionContext {
   public:
    explicit ScopedTransactionContext(TransactionContext& context)
        : previous_(active_context_) {
      active_context_ = &context;
    }

    ScopedTransactionContext(const ScopedTransactionContext&) = delete;
    ScopedTransactionContext& operator=(const ScopedTransactionContext&) = delete;
    ScopedTransactionContext(ScopedTransactionContext&&) = delete;
    ScopedTransactionContext& operator=(ScopedTransactionContext&&) = delete;

    ~ScopedTransactionContext() { active_context_ = previous_; }

   private:
    TransactionContext* previous_;
  };

  class ConnectionLease {
   public:
    explicit ConnectionLease(PooledConnectionSource& source) : source_(source) {
      std::unique_lock lock(source_.mutex_);
      source_.available_.wait(
          lock, [&] { return source_.active_connections_ < source_.max_connections_; });
      ++source_.active_connections_;
    }

    ConnectionLease(const ConnectionLease&) = delete;
    ConnectionLease& operator=(const ConnectionLease&) = delete;
    ConnectionLease(ConnectionLease&&) = delete;
    ConnectionLease& operator=(ConnectionLease&&) = delete;

    ~ConnectionLease() {
      {
        std::lock_guard lock(source_.mutex_);
        --source_.active_connections_;
      }
      source_.available_.notify_one();
    }

   private:
    PooledConnectionSource& source_;
  };

  Connection* ActiveTransactionConnection() {
    if (active_context_ != nullptr && active_context_->owner == this) {
      return active_context_->connection;
    }
    return nullptr;
  }

  Pool pool_;
  const int32_t max_connections_;
  std::mutex mutex_;
  std::condition_variable available_;
  int32_t active_connections_ = 0;

  inline static thread_local TransactionContext* active_context_ = nullptr;
};

/// \brief `CatalogStore` backed by a concrete sqlpp23 connection source.
///
/// \tparam ConnectionSource A single connection or pooled connection source.
/// \tparam Traits     A policy type providing
///                    `static bool IsUniqueViolation(const std::exception&)`
///                    and optionally
///                    `static bool IsDuplicateColumn(const std::exception&)`.
template <typename ConnectionSource, typename Traits>
class Sqlpp23CatalogStore final : public CatalogStore {
 public:
  template <typename... SourceArgs>
  explicit Sqlpp23CatalogStore(std::string catalog_name, SourceArgs&&... source_args)
      : source_(std::forward<SourceArgs>(source_args)...),
        catalog_(std::move(catalog_name)) {}

  Status Initialize() override {
    return Guard("initialize catalog tables", [&] {
      source_.WithConnection([&](auto& connection) {
        connection(std::string(kCreateTablesSql));
        connection(std::string(kCreateNamespacePropertiesSql));
        try {
          connection(std::string(kAddRecordTypeSql));
        } catch (const std::exception& e) {
          if (!IsDuplicateColumn(e)) {
            throw;
          }
        }
      });
    });
  }

  // --- Namespaces --------------------------------------------------------

  Result<std::vector<std::string>> ListNamespaceNames() override {
    const IcebergNamespaceProperties namespaces{};
    const IcebergTables tables{};
    std::vector<std::string> names;
    auto status = Guard("list namespaces", [&] {
      source_.WithConnection([&](auto& connection) {
        for (const auto& row :
             connection(sqlpp::select(sqlpp::distinct, namespaces.ns)
                            .from(namespaces)
                            .where(namespaces.catalogName == catalog_))) {
          names.emplace_back(row.ns);
        }
        for (const auto& row :
             connection(sqlpp::select(sqlpp::distinct, tables.tableNamespace)
                            .from(tables)
                            .where(tables.catalogName == catalog_ and
                                   TableRecordFilter(tables)))) {
          names.emplace_back(row.tableNamespace);
        }
      });
    });
    if (!status.has_value()) return ::iceberg::unexpected(status.error());
    return names;
  }

  Result<std::vector<NamespaceProperty>> GetNamespaceProperties(
      std::string_view ns) override {
    const IcebergNamespaceProperties t{};
    std::vector<NamespaceProperty> properties;
    auto status = Guard("get namespace properties", [&] {
      source_.WithConnection([&](auto& connection) {
        for (const auto& row : connection(
                 sqlpp::select(t.propertyKey, t.propertyValue)
                     .from(t)
                     .where(t.catalogName == catalog_ and t.ns == std::string(ns)))) {
          NamespaceProperty property;
          property.key = std::string(row.propertyKey);
          if (row.propertyValue.has_value()) {
            property.value = std::string(*row.propertyValue);
          }
          properties.push_back(std::move(property));
        }
      });
    });
    if (!status.has_value()) return ::iceberg::unexpected(status.error());
    return properties;
  }

  Status InsertNamespaceProperty(std::string_view ns, std::string_view key,
                                 std::optional<std::string_view> value) override {
    const IcebergNamespaceProperties t{};
    std::optional<std::string> value_str;
    if (value.has_value()) value_str = std::string(*value);
    return Guard("insert namespace property", [&] {
      source_.WithConnection([&](auto& connection) {
        connection(sqlpp::insert_into(t).set(
            t.catalogName = catalog_, t.ns = std::string(ns),
            t.propertyKey = std::string(key), t.propertyValue = value_str));
      });
    });
  }

  Status DeleteNamespaceProperty(std::string_view ns, std::string_view key) override {
    const IcebergNamespaceProperties t{};
    return Guard("delete namespace property", [&] {
      source_.WithConnection([&](auto& connection) {
        connection(sqlpp::delete_from(t).where(t.catalogName == catalog_ and
                                               t.ns == std::string(ns) and
                                               t.propertyKey == std::string(key)));
      });
    });
  }

  Result<int64_t> DeleteNamespace(std::string_view ns) override {
    const IcebergNamespaceProperties t{};
    int64_t affected = 0;
    auto status = Guard("delete namespace", [&] {
      source_.WithConnection([&](auto& connection) {
        affected = static_cast<int64_t>(
            connection(sqlpp::delete_from(t).where(t.catalogName == catalog_ and
                                                   t.ns == std::string(ns)))
                .affected_rows);
      });
    });
    if (!status.has_value()) return ::iceberg::unexpected(status.error());
    return affected;
  }

  // --- Tables ------------------------------------------------------------

  Result<std::vector<std::string>> ListTableNames(std::string_view ns) override {
    const IcebergTables t{};
    std::vector<std::string> names;
    auto status = Guard("list tables", [&] {
      source_.WithConnection([&](auto& connection) {
        for (const auto& row :
             connection(sqlpp::select(t.tableName)
                            .from(t)
                            .where(t.catalogName == catalog_ and
                                   t.tableNamespace == std::string(ns) and
                                   TableRecordFilter(t)))) {
          names.emplace_back(row.tableName);
        }
      });
    });
    if (!status.has_value()) return ::iceberg::unexpected(status.error());
    return names;
  }

  Result<bool> TableExists(std::string_view ns, std::string_view name) override {
    const IcebergTables t{};
    bool exists = false;
    auto status = Guard("check table exists", [&] {
      source_.WithConnection([&](auto& connection) {
        auto result = connection(sqlpp::select(t.tableName)
                                     .from(t)
                                     .where(t.catalogName == catalog_ and
                                            t.tableNamespace == std::string(ns) and
                                            t.tableName == std::string(name) and
                                            TableRecordFilter(t)));
        exists = !result.empty();
      });
    });
    if (!status.has_value()) return ::iceberg::unexpected(status.error());
    return exists;
  }

  Result<std::optional<std::string>> GetTableMetadataLocation(
      std::string_view ns, std::string_view name) override {
    const IcebergTables t{};
    std::optional<std::string> location;
    auto status = Guard("get table metadata location", [&] {
      source_.WithConnection([&](auto& connection) {
        auto result = connection(sqlpp::select(t.metadataLocation)
                                     .from(t)
                                     .where(t.catalogName == catalog_ and
                                            t.tableNamespace == std::string(ns) and
                                            t.tableName == std::string(name) and
                                            TableRecordFilter(t)));
        if (!result.empty()) {
          const auto& row = result.front();
          if (row.metadataLocation.has_value()) {
            location = std::string(*row.metadataLocation);
          }
        }
      });
    });
    if (!status.has_value()) return ::iceberg::unexpected(status.error());
    return location;
  }

  Status InsertTable(std::string_view ns, std::string_view name,
                     std::string_view metadata_location) override {
    const IcebergTables t{};
    return Guard("insert table", [&] {
      source_.WithConnection([&](auto& connection) {
        connection(sqlpp::insert_into(t).set(
            t.catalogName = catalog_, t.tableNamespace = std::string(ns),
            t.tableName = std::string(name),
            t.metadataLocation = std::string(metadata_location),
            t.recordType = std::string(kTableRecordType)));
      });
    });
  }

  Result<int64_t> UpdateTableMetadataLocation(
      std::string_view ns, std::string_view name, std::string_view new_location,
      std::string_view new_previous_location,
      std::string_view expected_current_location) override {
    const IcebergTables t{};
    int64_t affected = 0;
    auto status = Guard("update table metadata location", [&] {
      source_.WithConnection([&](auto& connection) {
        affected = static_cast<int64_t>(
            connection(
                sqlpp::update(t)
                    .set(t.metadataLocation = std::string(new_location),
                         t.previousMetadataLocation = std::string(new_previous_location))
                    .where(t.catalogName == catalog_ and
                           t.tableNamespace == std::string(ns) and
                           t.tableName == std::string(name) and
                           t.metadataLocation ==
                               std::string(expected_current_location) and
                           TableRecordFilter(t)))
                .affected_rows);
      });
    });
    if (!status.has_value()) return ::iceberg::unexpected(status.error());
    return affected;
  }

  Result<int64_t> DeleteTable(std::string_view ns, std::string_view name) override {
    const IcebergTables t{};
    int64_t affected = 0;
    auto status = Guard("delete table", [&] {
      source_.WithConnection([&](auto& connection) {
        affected = static_cast<int64_t>(
            connection(sqlpp::delete_from(t).where(t.catalogName == catalog_ and
                                                   t.tableNamespace == std::string(ns) and
                                                   t.tableName == std::string(name) and
                                                   TableRecordFilter(t)))
                .affected_rows);
      });
    });
    if (!status.has_value()) return ::iceberg::unexpected(status.error());
    return affected;
  }

  Result<int64_t> RenameTable(std::string_view from_ns, std::string_view from_name,
                              std::string_view to_ns, std::string_view to_name) override {
    const IcebergTables t{};
    int64_t affected = 0;
    auto status = Guard("rename table", [&] {
      source_.WithConnection([&](auto& connection) {
        affected = static_cast<int64_t>(
            connection(sqlpp::update(t)
                           .set(t.tableNamespace = std::string(to_ns),
                                t.tableName = std::string(to_name))
                           .where(t.catalogName == catalog_ and
                                  t.tableNamespace == std::string(from_ns) and
                                  t.tableName == std::string(from_name) and
                                  TableRecordFilter(t)))
                .affected_rows);
      });
    });
    if (!status.has_value()) return ::iceberg::unexpected(status.error());
    return affected;
  }

  Status RunInTransaction(const std::function<Status()>& body) override {
    try {
      return source_.RunInTransaction(body);
    } catch (const std::exception& e) {
      return Translate(e, "transaction");
    }
  }

 private:
  static constexpr std::string_view kTableRecordType = "TABLE";

  auto TableRecordFilter(const IcebergTables& t) const {
    return t.recordType == std::string(kTableRecordType) or t.recordType.is_null();
  }

  // Schema compatible with the Apache Iceberg Java JdbcCatalog. sqlpp23 cannot
  // emit DDL, so these run as plain string statements.
  static constexpr std::string_view kCreateTablesSql =
      "CREATE TABLE IF NOT EXISTS iceberg_tables ("
      "catalog_name VARCHAR(255) NOT NULL, "
      "table_namespace VARCHAR(255) NOT NULL, "
      "table_name VARCHAR(255) NOT NULL, "
      "metadata_location VARCHAR(1000), "
      "previous_metadata_location VARCHAR(1000), "
      "iceberg_type VARCHAR(5), "
      "PRIMARY KEY (catalog_name, table_namespace, table_name))";

  static constexpr std::string_view kAddRecordTypeSql =
      "ALTER TABLE iceberg_tables ADD COLUMN iceberg_type VARCHAR(5)";

  static constexpr std::string_view kCreateNamespacePropertiesSql =
      "CREATE TABLE IF NOT EXISTS iceberg_namespace_properties ("
      "catalog_name VARCHAR(255) NOT NULL, "
      "namespace VARCHAR(255) NOT NULL, "
      "property_key VARCHAR(255) NOT NULL, "
      "property_value VARCHAR(1000), "
      "PRIMARY KEY (catalog_name, namespace, property_key))";

  /// \brief Run `fn`, translating any thrown exception into a `Status`.
  template <typename Fn>
  Status Guard(std::string_view op, Fn&& fn) {
    try {
      std::forward<Fn>(fn)();
      return {};
    } catch (const std::exception& e) {
      return Translate(e, op);
    }
  }

  Status Translate(const std::exception& e, std::string_view op) const {
    if (Traits::IsUniqueViolation(e)) {
      return AlreadyExists("{}: unique constraint violation: {}", op, e.what());
    }
    return IOError("{}: {}", op, e.what());
  }

  bool IsDuplicateColumn(const std::exception& e) const {
    if constexpr (requires { Traits::IsDuplicateColumn(e); }) {
      return Traits::IsDuplicateColumn(e);
    }
    return false;
  }

  ConnectionSource source_;
  std::string catalog_;
};

}  // namespace iceberg::sql
