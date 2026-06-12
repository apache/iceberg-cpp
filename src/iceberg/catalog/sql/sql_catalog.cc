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

#include "iceberg/catalog/sql/sql_catalog.h"

#include <algorithm>
#include <set>
#include <string>

#include "iceberg/catalog/sql/config.h"
#include "iceberg/metrics/metrics_reporters.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/transaction.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"

namespace iceberg::sql {

namespace {

// Sentinel property guaranteeing a namespace row exists even with no user
// properties. Hidden from callers.
constexpr std::string_view kNamespaceExistsProperty = "exists";
constexpr std::string_view kNamespaceLocationProperty = "location";

std::string NamespaceToString(const Namespace& ns) { return ns.ToString(); }

std::string NamespaceToPath(const Namespace& ns) {
  std::string path;
  for (const auto& level : ns.levels) {
    if (!path.empty()) {
      path += '/';
    }
    path += level;
  }
  return path;
}

std::string JoinLocation(std::string_view parent, std::string_view child) {
  std::string location(LocationUtil::StripTrailingSlash(parent));
  if (location.empty()) {
    return std::string(child);
  }
  if (child.empty()) {
    return location;
  }
  location += '/';
  location += child;
  return location;
}

// Returns true when `candidate` is a strict descendant of `parent`
// (i.e. parent is a non-empty prefix path, or parent is the root).
bool IsDescendant(std::string_view parent, std::string_view candidate) {
  if (parent.empty()) {
    return !candidate.empty();
  }
  return candidate.size() > parent.size() && candidate.starts_with(parent) &&
         candidate[parent.size()] == '.';
}

Status ValidateNamespaceLevels(const Namespace& ns) {
  for (const auto& level : ns.levels) {
    if (level.empty()) {
      return InvalidArgument("SQL catalog namespace levels cannot be empty");
    }
    if (level.find('.') != std::string::npos) {
      return InvalidArgument("SQL catalog namespace level '{}' cannot contain '.'",
                             level);
    }
  }
  return {};
}

Status ValidateTableIdentifier(const TableIdentifier& identifier) {
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());
  return ValidateNamespaceLevels(identifier.ns);
}

Result<std::string> ResolveTableLocation(
    const SqlCatalogConfig& config, const TableIdentifier& identifier,
    const std::unordered_map<std::string, std::string>& namespace_properties,
    std::string_view explicit_location) {
  if (!explicit_location.empty()) {
    return std::string(explicit_location);
  }

  auto location = namespace_properties.find(std::string(kNamespaceLocationProperty));
  const std::string namespace_location =
      location != namespace_properties.end()
          ? location->second
          : JoinLocation(config.warehouse_location, NamespaceToPath(identifier.ns));
  return JoinLocation(namespace_location, identifier.name);
}

[[maybe_unused]] CatalogStoreOptions ToCatalogStoreOptions(
    const SqlCatalogConfig& config) {
  CatalogStoreOptions options;
  options.catalog_name = config.name;
  options.uri = config.uri;
  options.max_connections = config.max_connections;
  options.properties = config.props;
  return options;
}

}  // namespace

SqlCatalog::SqlCatalog(SqlCatalogConfig config, std::shared_ptr<FileIO> file_io,
                       std::shared_ptr<CatalogStore> store)
    : config_(std::move(config)), file_io_(std::move(file_io)), store_(std::move(store)) {
  auto it = config_.props.find(std::string(kMetricsReporterImpl));
  if (it != config_.props.end() && !it->second.empty() &&
      it->second != kMetricsReporterTypeNoop) {
    if (auto r = MetricsReporters::Load(config_.props); r.has_value()) {
      reporter_ = std::shared_ptr<MetricsReporter>(std::move(r.value()));
    }
  }
}

SqlCatalog::~SqlCatalog() = default;

Result<std::shared_ptr<SqlCatalog>> SqlCatalog::Make(
    const SqlCatalogConfig& config, std::shared_ptr<FileIO> file_io,
    std::shared_ptr<CatalogStore> store) {
  if (store == nullptr) {
    return InvalidArgument("SqlCatalog requires a non-null CatalogStore");
  }
  if (file_io == nullptr) {
    return InvalidArgument("SqlCatalog requires a non-null FileIO");
  }
  auto catalog = std::shared_ptr<SqlCatalog>(
      new SqlCatalog(config, std::move(file_io), std::move(store)));
  ICEBERG_RETURN_UNEXPECTED(catalog->store_->Initialize());
  return catalog;
}

std::string_view SqlCatalog::name() const { return config_.name; }

// --------------------------------------------------------------------------
// Namespaces
// --------------------------------------------------------------------------

Result<bool> SqlCatalog::NamespaceExists(const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespaceLevels(ns));
  if (ns.levels.empty()) {
    // The implicit root namespace always exists.
    return true;
  }
  const std::string ns_str = NamespaceToString(ns);
  ICEBERG_ASSIGN_OR_RAISE(auto names, store_->ListNamespaceNames());
  for (const auto& candidate : names) {
    if (candidate == ns_str || IsDescendant(ns_str, candidate)) {
      return true;
    }
  }
  return false;
}

Status SqlCatalog::CreateNamespace(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespaceLevels(ns));
  if (ns.levels.empty()) {
    return InvalidArgument("Cannot create namespace with empty identifier");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto exists, NamespaceExists(ns));
  if (exists) {
    return AlreadyExists("Namespace already exists: {}", ns.ToString());
  }
  if (properties.contains(std::string(kNamespaceExistsProperty))) {
    return InvalidArgument("Property '{}' is reserved", kNamespaceExistsProperty);
  }

  const std::string ns_str = NamespaceToString(ns);
  return store_->RunInTransaction([&]() -> Status {
    // Sentinel row so the namespace exists even when it has no user properties.
    ICEBERG_RETURN_UNEXPECTED(store_->InsertNamespaceProperty(
        ns_str, kNamespaceExistsProperty, std::string_view{"true"}));
    for (const auto& [key, value] : properties) {
      ICEBERG_RETURN_UNEXPECTED(
          store_->InsertNamespaceProperty(ns_str, key, std::string_view{value}));
    }
    return {};
  });
}

Result<std::vector<Namespace>> SqlCatalog::ListNamespaces(const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespaceLevels(ns));
  if (!ns.levels.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto exists, NamespaceExists(ns));
    if (!exists) {
      return NoSuchNamespace("Namespace does not exist: {}", ns.ToString());
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto names, store_->ListNamespaceNames());

  const std::string parent = NamespaceToString(ns);
  std::set<std::string> children;
  for (const auto& candidate : names) {
    if (!IsDescendant(parent, candidate)) {
      continue;
    }
    // Extract the immediate child level under `parent`.
    const size_t start = parent.empty() ? 0 : parent.size() + 1;
    const size_t dot = candidate.find('.', start);
    children.insert(candidate.substr(
        start, dot == std::string::npos ? std::string::npos : dot - start));
  }

  std::vector<Namespace> result_namespaces;
  result_namespaces.reserve(children.size());
  for (const auto& child : children) {
    Namespace child_ns = ns;
    child_ns.levels.push_back(child);
    result_namespaces.push_back(std::move(child_ns));
  }
  return result_namespaces;
}

Result<std::unordered_map<std::string, std::string>> SqlCatalog::GetNamespaceProperties(
    const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespaceLevels(ns));
  ICEBERG_ASSIGN_OR_RAISE(auto exists, NamespaceExists(ns));
  if (!exists) {
    return NoSuchNamespace("Namespace does not exist: {}", ns.ToString());
  }

  const std::string ns_str = NamespaceToString(ns);
  ICEBERG_ASSIGN_OR_RAISE(auto rows, store_->GetNamespaceProperties(ns_str));

  std::unordered_map<std::string, std::string> properties;
  for (const auto& row : rows) {
    if (row.key == kNamespaceExistsProperty) {
      continue;
    }
    properties.emplace(row.key, row.value.value_or(""));
  }
  return properties;
}

Status SqlCatalog::DropNamespace(const Namespace& ns) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespaceLevels(ns));
  if (ns.levels.empty()) {
    return InvalidArgument("Cannot drop the root namespace");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto exists, NamespaceExists(ns));
  if (!exists) {
    return NoSuchNamespace("Namespace does not exist: {}", ns.ToString());
  }

  const std::string ns_str = NamespaceToString(ns);

  // Reject if the namespace still contains tables.
  {
    ICEBERG_ASSIGN_OR_RAISE(auto tables, store_->ListTableNames(ns_str));
    if (!tables.empty()) {
      return NamespaceNotEmpty("Namespace {} is not empty: it contains tables",
                               ns.ToString());
    }
  }

  // Reject if the namespace still has child namespaces.
  {
    ICEBERG_ASSIGN_OR_RAISE(auto names, store_->ListNamespaceNames());
    for (const auto& candidate : names) {
      if (IsDescendant(ns_str, candidate)) {
        return NamespaceNotEmpty("Namespace {} is not empty: it contains sub-namespaces",
                                 ns.ToString());
      }
    }
  }

  ICEBERG_RETURN_UNEXPECTED(store_->DeleteNamespace(ns_str));
  return {};
}

Status SqlCatalog::UpdateNamespaceProperties(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
    const std::unordered_set<std::string>& removals) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespaceLevels(ns));
  ICEBERG_ASSIGN_OR_RAISE(auto exists, NamespaceExists(ns));
  if (!exists) {
    return NoSuchNamespace("Namespace does not exist: {}", ns.ToString());
  }
  if (updates.contains(std::string(kNamespaceExistsProperty)) ||
      removals.contains(std::string(kNamespaceExistsProperty))) {
    return InvalidArgument("Property '{}' is reserved", kNamespaceExistsProperty);
  }
  for (const auto& [key, value] : updates) {
    if (removals.contains(key)) {
      return InvalidArgument("Property '{}' is both updated and removed", key);
    }
  }

  const std::string ns_str = NamespaceToString(ns);
  return store_->RunInTransaction([&]() -> Status {
    for (const auto& key : removals) {
      ICEBERG_RETURN_UNEXPECTED(store_->DeleteNamespaceProperty(ns_str, key));
    }
    for (const auto& [key, value] : updates) {
      // Portable upsert: delete then insert.
      ICEBERG_RETURN_UNEXPECTED(store_->DeleteNamespaceProperty(ns_str, key));
      ICEBERG_RETURN_UNEXPECTED(
          store_->InsertNamespaceProperty(ns_str, key, std::string_view{value}));
    }
    return {};
  });
}

// --------------------------------------------------------------------------
// Tables
// --------------------------------------------------------------------------

Result<std::vector<TableIdentifier>> SqlCatalog::ListTables(const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespaceLevels(ns));
  if (!ns.levels.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto exists, NamespaceExists(ns));
    if (!exists) {
      return NoSuchNamespace("Namespace does not exist: {}", ns.ToString());
    }
  }

  const std::string ns_str = NamespaceToString(ns);
  ICEBERG_ASSIGN_OR_RAISE(auto names, store_->ListTableNames(ns_str));

  std::vector<TableIdentifier> identifiers;
  identifiers.reserve(names.size());
  for (auto& table_name : names) {
    identifiers.push_back(TableIdentifier{.ns = ns, .name = std::move(table_name)});
  }
  std::ranges::sort(identifiers,
                    [](const auto& lhs, const auto& rhs) { return lhs.name < rhs.name; });
  return identifiers;
}

Result<bool> SqlCatalog::TableExists(const TableIdentifier& identifier) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateTableIdentifier(identifier));
  const std::string ns_str = NamespaceToString(identifier.ns);
  return store_->TableExists(ns_str, identifier.name);
}

Result<std::string> SqlCatalog::GetTableMetadataLocation(
    const TableIdentifier& identifier) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateTableIdentifier(identifier));
  const std::string ns_str = NamespaceToString(identifier.ns);
  ICEBERG_ASSIGN_OR_RAISE(auto location,
                          store_->GetTableMetadataLocation(ns_str, identifier.name));
  if (!location.has_value()) {
    return NoSuchTable("Table does not exist: {}", identifier.ToString());
  }
  return *location;
}

Result<std::shared_ptr<Table>> SqlCatalog::LoadTableFrom(
    const TableIdentifier& identifier, const std::string& metadata_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto metadata,
                          TableMetadataUtil::Read(*file_io_, metadata_location));
  return Table::Make(identifier, std::move(metadata), metadata_location, file_io_,
                     shared_from_this(), reporter_);
}

Result<std::shared_ptr<Table>> SqlCatalog::LoadTable(const TableIdentifier& identifier) {
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_location, GetTableMetadataLocation(identifier));
  return LoadTableFrom(identifier, metadata_location);
}

Result<std::shared_ptr<Table>> SqlCatalog::CreateTable(
    const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto ns_exists, NamespaceExists(identifier.ns));
  if (!ns_exists) {
    return NoSuchNamespace("Namespace does not exist: {}", identifier.ns.ToString());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto table_exists, TableExists(identifier));
  if (table_exists) {
    return AlreadyExists("Table already exists: {}", identifier.ToString());
  }

  ICEBERG_ASSIGN_OR_RAISE(auto namespace_properties,
                          GetNamespaceProperties(identifier.ns));
  ICEBERG_ASSIGN_OR_RAISE(
      auto base_location,
      ResolveTableLocation(config_, identifier, namespace_properties, location));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, TableMetadata::Make(*schema, *spec, *order,
                                                             base_location, properties));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_location,
                          TableMetadataUtil::Write(*file_io_, nullptr, "", *metadata));

  const std::string ns_str = NamespaceToString(identifier.ns);
  ICEBERG_RETURN_UNEXPECTED(
      store_->InsertTable(ns_str, identifier.name, metadata_location));

  return Table::Make(identifier, std::move(metadata), metadata_location, file_io_,
                     shared_from_this(), reporter_);
}

Result<std::shared_ptr<Table>> SqlCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<TableRequirement>>& requirements,
    const std::vector<std::unique_ptr<TableUpdate>>& updates) {
  ICEBERG_RETURN_UNEXPECTED(ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto is_create, TableRequirements::IsCreate(requirements));

  std::unique_ptr<TableMetadata> base;
  std::unique_ptr<TableMetadataBuilder> builder;
  std::string base_location;

  if (is_create) {
    ICEBERG_ASSIGN_OR_RAISE(auto exists, TableExists(identifier));
    if (exists) {
      return AlreadyExists("Table already exists: {}", identifier.ToString());
    }
    int8_t format_version = TableMetadata::kDefaultTableFormatVersion;
    for (const auto& update : updates) {
      if (update->kind() == TableUpdate::Kind::kUpgradeFormatVersion) {
        format_version =
            iceberg::internal::checked_cast<const table::UpgradeFormatVersion&>(*update)
                .format_version();
      }
    }
    builder = TableMetadataBuilder::BuildFromEmpty(format_version);
  } else {
    ICEBERG_ASSIGN_OR_RAISE(base_location, GetTableMetadataLocation(identifier));
    ICEBERG_ASSIGN_OR_RAISE(base, TableMetadataUtil::Read(*file_io_, base_location));
    builder = TableMetadataBuilder::BuildFrom(base.get());
  }

  for (const auto& requirement : requirements) {
    ICEBERG_RETURN_UNEXPECTED(requirement->Validate(base.get()));
  }
  for (const auto& update : updates) {
    update->ApplyTo(*builder);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto updated, builder->Build());
  ICEBERG_ASSIGN_OR_RAISE(
      auto new_metadata_location,
      TableMetadataUtil::Write(*file_io_, base.get(), base_location, *updated));

  const std::string ns_str = NamespaceToString(identifier.ns);
  if (is_create) {
    ICEBERG_RETURN_UNEXPECTED(
        store_->InsertTable(ns_str, identifier.name, new_metadata_location));
  } else {
    // Optimistic concurrency: only succeed if the stored metadata location is
    // still the base we read.
    ICEBERG_ASSIGN_OR_RAISE(
        auto affected, store_->UpdateTableMetadataLocation(ns_str, identifier.name,
                                                           new_metadata_location,
                                                           base_location, base_location));
    if (affected != 1) {
      return CommitFailed(
          "Failed to commit to table {}: stale metadata location (concurrent update)",
          identifier.ToString());
    }
    TableMetadataUtil::DeleteRemovedMetadataFiles(*file_io_, base.get(), *updated);
  }

  return Table::Make(identifier, std::move(updated), new_metadata_location, file_io_,
                     shared_from_this(), reporter_);
}

Result<std::shared_ptr<Transaction>> SqlCatalog::StageCreateTable(
    const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto ns_exists, NamespaceExists(identifier.ns));
  if (!ns_exists) {
    return NoSuchNamespace("Namespace does not exist: {}", identifier.ns.ToString());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto table_exists, TableExists(identifier));
  if (table_exists) {
    return AlreadyExists("Table already exists: {}", identifier.ToString());
  }

  ICEBERG_ASSIGN_OR_RAISE(auto namespace_properties,
                          GetNamespaceProperties(identifier.ns));
  ICEBERG_ASSIGN_OR_RAISE(
      auto base_location,
      ResolveTableLocation(config_, identifier, namespace_properties, location));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, TableMetadata::Make(*schema, *spec, *order,
                                                             base_location, properties));
  ICEBERG_ASSIGN_OR_RAISE(auto table,
                          StagedTable::Make(identifier, std::move(metadata), "", file_io_,
                                            shared_from_this(), reporter_));
  return Transaction::Make(std::move(table), TransactionKind::kCreate);
}

Status SqlCatalog::DropTable(const TableIdentifier& identifier, bool purge) {
  ICEBERG_RETURN_UNEXPECTED(ValidateTableIdentifier(identifier));
  if (purge) {
    // TODO(zhjwpku): Delete the table data and metadata files when purge is requested.
  }

  const std::string ns_str = NamespaceToString(identifier.ns);
  ICEBERG_ASSIGN_OR_RAISE(auto affected, store_->DeleteTable(ns_str, identifier.name));
  if (affected == 0) {
    return NoSuchTable("Table does not exist: {}", identifier.ToString());
  }
  return {};
}

Status SqlCatalog::RenameTable(const TableIdentifier& from, const TableIdentifier& to) {
  ICEBERG_RETURN_UNEXPECTED(ValidateTableIdentifier(from));
  ICEBERG_RETURN_UNEXPECTED(ValidateTableIdentifier(to));
  if (from == to) {
    return {};
  }
  ICEBERG_ASSIGN_OR_RAISE(auto to_ns_exists, NamespaceExists(to.ns));
  if (!to_ns_exists) {
    return NoSuchNamespace("Target namespace does not exist: {}", to.ns.ToString());
  }

  const std::string from_ns = NamespaceToString(from.ns);
  const std::string to_ns = NamespaceToString(to.ns);

  return store_->RunInTransaction([&]() -> Status {
    ICEBERG_ASSIGN_OR_RAISE(auto to_exists, TableExists(to));
    if (to_exists) {
      return AlreadyExists("Table already exists: {}", to.ToString());
    }
    ICEBERG_ASSIGN_OR_RAISE(auto affected,
                            store_->RenameTable(from_ns, from.name, to_ns, to.name));
    if (affected == 0) {
      return NoSuchTable("Table does not exist: {}", from.ToString());
    }
    return {};
  });
}

Result<std::shared_ptr<Table>> SqlCatalog::RegisterTable(
    const TableIdentifier& identifier, const std::string& metadata_file_location) {
  ICEBERG_RETURN_UNEXPECTED(ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto ns_exists, NamespaceExists(identifier.ns));
  if (!ns_exists) {
    return NoSuchNamespace("Namespace does not exist: {}", identifier.ns.ToString());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto table_exists, TableExists(identifier));
  if (table_exists) {
    return AlreadyExists("Table already exists: {}", identifier.ToString());
  }

  ICEBERG_ASSIGN_OR_RAISE(auto metadata,
                          TableMetadataUtil::Read(*file_io_, metadata_file_location));

  const std::string ns_str = NamespaceToString(identifier.ns);
  ICEBERG_RETURN_UNEXPECTED(
      store_->InsertTable(ns_str, identifier.name, metadata_file_location));

  return Table::Make(identifier, std::move(metadata), metadata_file_location, file_io_,
                     shared_from_this(), reporter_);
}

// --------------------------------------------------------------------------
// Built-in catalog store factories
// --------------------------------------------------------------------------

Result<std::shared_ptr<SqlCatalog>> SqlCatalog::MakeSqliteCatalog(
    [[maybe_unused]] const SqlCatalogConfig& config,
    [[maybe_unused]] std::shared_ptr<FileIO> file_io) {
#ifdef BUILD_SQLITE3_CONNECTOR
  ICEBERG_ASSIGN_OR_RAISE(auto store,
                          MakeSqliteCatalogStore(ToCatalogStoreOptions(config)));
  return Make(config, std::move(file_io), std::move(store));
#else
  return NotSupported("SQLite SQL catalog connector is not built");
#endif  // BUILD_SQLITE3_CONNECTOR
}

Result<std::shared_ptr<SqlCatalog>> SqlCatalog::MakePostgreSqlCatalog(
    [[maybe_unused]] const SqlCatalogConfig& config,
    [[maybe_unused]] std::shared_ptr<FileIO> file_io) {
#ifdef BUILD_POSTGRESQL_CONNECTOR
  ICEBERG_ASSIGN_OR_RAISE(auto store,
                          MakePostgreSqlCatalogStore(ToCatalogStoreOptions(config)));
  return Make(config, std::move(file_io), std::move(store));
#else
  return NotSupported("PostgreSQL SQL catalog connector is not built");
#endif  // BUILD_POSTGRESQL_CONNECTOR
}

Result<std::shared_ptr<SqlCatalog>> SqlCatalog::MakeMySqlCatalog(
    [[maybe_unused]] const SqlCatalogConfig& config,
    [[maybe_unused]] std::shared_ptr<FileIO> file_io) {
#ifdef BUILD_MYSQL_CONNECTOR
  ICEBERG_ASSIGN_OR_RAISE(auto store,
                          MakeMySqlCatalogStore(ToCatalogStoreOptions(config)));
  return Make(config, std::move(file_io), std::move(store));
#else
  return NotSupported("MySQL SQL catalog connector is not built");
#endif  // BUILD_MYSQL_CONNECTOR
}

#ifndef BUILD_SQLITE3_CONNECTOR
Result<std::shared_ptr<CatalogStore>> MakeSqliteCatalogStore(const CatalogStoreOptions&) {
  return NotSupported("SQLite SQL catalog connector is not built");
}
#endif  // BUILD_SQLITE3_CONNECTOR

#ifndef BUILD_POSTGRESQL_CONNECTOR
Result<std::shared_ptr<CatalogStore>> MakePostgreSqlCatalogStore(
    const CatalogStoreOptions&) {
  return NotSupported("PostgreSQL SQL catalog connector is not built");
}
#endif  // BUILD_POSTGRESQL_CONNECTOR

#ifndef BUILD_MYSQL_CONNECTOR
Result<std::shared_ptr<CatalogStore>> MakeMySqlCatalogStore(const CatalogStoreOptions&) {
  return NotSupported("MySQL SQL catalog connector is not built");
}
#endif  // BUILD_MYSQL_CONNECTOR

}  // namespace iceberg::sql
