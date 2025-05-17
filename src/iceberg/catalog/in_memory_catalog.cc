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

#include "iceberg/catalog/in_memory_catalog.h"

#include <algorithm>
#include <iterator>  // IWYU pragma: keep
#include <mutex>
#include <optional>
#include <unordered_map>

#include "iceberg/exception.h"
#include "iceberg/table.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

/**
 * \brief A hierarchical namespace that manages namespaces and table metadata in-memory.
 *
 * Each InMemoryNamespace represents a namespace level and can contain properties,
 * tables, and child namespaces. This structure enables a tree-like representation
 * of nested namespaces.
 */
class ICEBERG_EXPORT InMemoryNamespace {
 public:
  /**
   * \brief Checks whether the given namespace exists.
   * \param[in] namespace_ident The namespace to check.
   * \return Status indicating success or failure.
   */
  Status NamespaceExists(const Namespace& namespace_ident) const;

  /**
   * \brief Lists immediate child namespaces under the given parent namespace.
   * \param[in] parent_namespace_ident The optional parent namespace. If not provided,
   *                                the children of the root are returned.
   * \return A vector of child namespace names.
   */
  Result<std::vector<std::string>> ListChildrenNamespaces(
      const std::optional<Namespace>& parent_namespace_ident = std::nullopt) const;

  /**
   * \brief Creates a new namespace with the specified properties.
   * \param[in] namespace_ident The namespace to create.
   * \param[in] properties A map of key-value pairs to associate with the namespace.
   * \return Status indicating success or failure.
   */
  Status CreateNamespace(const Namespace& namespace_ident,
                         const std::unordered_map<std::string, std::string>& properties);

  /**
   * \brief Deletes an existing namespace.
   * \param[in] namespace_ident The namespace to delete.
   * \return Status indicating success or failure.
   */
  Status DeleteNamespace(const Namespace& namespace_ident);

  /**
   * \brief Retrieves the properties of the specified namespace.
   * \param[in] namespace_ident The namespace whose properties to retrieve.
   * \return An  containing the properties map if the namespace exists;
   *         Errpr otherwise.
   */
  Result<std::unordered_map<std::string, std::string>> GetProperties(
      const Namespace& namespace_ident) const;

  /**
   * \brief Replaces all properties of the given namespace.
   * \param[in] namespace_ident The namespace whose properties will be replaced.
   * \param[in] properties The new properties map.
   * \return Status indicating success or failure.
   */
  Status ReplaceProperties(
      const Namespace& namespace_ident,
      const std::unordered_map<std::string, std::string>& properties);

  /**
   * \brief Lists all table names under the specified namespace.
   * \param[in] namespace_ident The namespace from which to list tables.
   * \return A vector of table names or error.
   */
  Result<std::vector<std::string>> ListTables(const Namespace& namespace_ident) const;

  /**
   * \brief Registers a table in the given namespace with a metadata location.
   * \param[in] table_ident The fully qualified identifier of the table.
   * \param[in] metadata_location The path to the table's metadata.
   * \return Status indicating success or failure.
   */
  Status RegisterTable(TableIdentifier const& table_ident,
                       const std::string& metadata_location);

  /**
   * \brief Unregisters a table from the specified namespace.
   * \param[in] table_ident The identifier of the table to unregister.
   * \return Status indicating success or failure.
   */
  Status UnregisterTable(TableIdentifier const& table_ident);

  /**
   * \brief Checks if a table exists in the specified namespace.
   * \param[in] table_ident The identifier of the table to check.
   * \return Result<bool> indicating table exists or not.
   */
  Result<bool> TableExists(TableIdentifier const& table_ident) const;

  /**
   * \brief Gets the metadata location for the specified table.
   * \param[in] table_ident The identifier of the table.
   * \return An string containing the metadata location if the table exists;
   *         Error otherwise.
   */
  Result<std::string> GetTableMetadataLocation(TableIdentifier const& table_ident) const;

  template <typename NamespacePtr>
  static Result<NamespacePtr> GetNamespaceImpl(NamespacePtr root,
                                               const Namespace& namespace_ident) {
    auto node = root;
    for (const auto& part_level : namespace_ident.levels) {
      auto it = node->children_.find(part_level);
      if (it == node->children_.end()) {
        return NoSuchNamespace("{}", part_level);
      }
      node = &it->second;
    }
    return node;
  }

 private:
  /// Map of child namespace names to their corresponding namespace instances.
  std::unordered_map<std::string, InMemoryNamespace> children_;

  /// Key-value property map for this namespace.
  std::unordered_map<std::string, std::string> properties_;

  /// Mapping of table names to metadata file locations.
  std::unordered_map<std::string, std::string> table_metadata_locations_;
};

Result<InMemoryNamespace*> GetNamespace(InMemoryNamespace* root,
                                        const Namespace& namespace_ident) {
  return InMemoryNamespace::GetNamespaceImpl(root, namespace_ident);
}

Result<const InMemoryNamespace*> GetNamespace(const InMemoryNamespace* root,
                                              const Namespace& namespace_ident) {
  return InMemoryNamespace::GetNamespaceImpl(root, namespace_ident);
}

Status InMemoryNamespace::NamespaceExists(const Namespace& namespace_ident) const {
  const auto ns = GetNamespace(this, namespace_ident);
  ICEBERG_RETURN_UNEXPECTED(ns);
  return {};
}

Result<std::vector<std::string>> InMemoryNamespace::ListChildrenNamespaces(
    const std::optional<Namespace>& parent_namespace_ident) const {
  auto ns = this;
  if (parent_namespace_ident.has_value()) {
    const auto nsRs = GetNamespace(this, *parent_namespace_ident);
    ICEBERG_RETURN_UNEXPECTED(nsRs);
    ns = *nsRs;
  }

  std::vector<std::string> names;
  auto const& children = ns->children_;
  names.reserve(children.size());
  std::ranges::transform(children, std::back_inserter(names),
                         [](const auto& pair) { return pair.first; });
  return names;
}

Status InMemoryNamespace::CreateNamespace(
    const Namespace& namespace_ident,
    const std::unordered_map<std::string, std::string>& properties) {
  if (namespace_ident.levels.empty()) {
    return InvalidArgument("namespace identifier is empty");
  }

  auto ns = this;
  bool newly_created = false;
  for (const auto& part_level : namespace_ident.levels) {
    if (auto it = ns->children_.find(part_level); it == ns->children_.end()) {
      ns = &ns->children_[part_level];
      newly_created = true;
    } else {
      ns = &it->second;
    }
  }
  if (!newly_created) {
    return AlreadyExists("{}", namespace_ident.levels.back());
  }

  ns->properties_ = properties;
  return {};
}

Status InMemoryNamespace::DeleteNamespace(const Namespace& namespace_ident) {
  if (namespace_ident.levels.empty()) {
    return InvalidArgument("namespace identifier is empty");
  }

  auto parent_namespace_ident = namespace_ident;
  const auto to_delete = parent_namespace_ident.levels.back();
  parent_namespace_ident.levels.pop_back();

  const auto parentRs = GetNamespace(this, parent_namespace_ident);
  ICEBERG_RETURN_UNEXPECTED(parentRs);

  const auto it = parentRs.value()->children_.find(to_delete);
  if (it == parentRs.value()->children_.end()) {
    return NotFound("namespace {} is not found", to_delete);
  }

  const auto& target = it->second;
  if (!target.children_.empty() || !target.table_metadata_locations_.empty()) {
    return NotAllowed("{} has other sub-namespaces and cannot be deleted", to_delete);
  }

  parentRs.value()->children_.erase(to_delete);
  return {};
}

Result<std::unordered_map<std::string, std::string>> InMemoryNamespace::GetProperties(
    const Namespace& namespace_ident) const {
  const auto ns = GetNamespace(this, namespace_ident);
  ICEBERG_RETURN_UNEXPECTED(ns);
  return ns.value()->properties_;
}

Status InMemoryNamespace::ReplaceProperties(
    const Namespace& namespace_ident,
    const std::unordered_map<std::string, std::string>& properties) {
  const auto ns = GetNamespace(this, namespace_ident);
  ICEBERG_RETURN_UNEXPECTED(ns);
  ns.value()->properties_ = properties;
  return {};
}

Result<std::vector<std::string>> InMemoryNamespace::ListTables(
    const Namespace& namespace_ident) const {
  const auto ns = GetNamespace(this, namespace_ident);
  ICEBERG_RETURN_UNEXPECTED(ns);

  const auto& locations = ns.value()->table_metadata_locations_;
  std::vector<std::string> table_names;
  table_names.reserve(locations.size());

  std::ranges::transform(locations, std::back_inserter(table_names),
                         [](const auto& pair) { return pair.first; });
  std::ranges::sort(table_names);

  return table_names;
}

Status InMemoryNamespace::RegisterTable(TableIdentifier const& table_ident,
                                        const std::string& metadata_location) {
  const auto ns = GetNamespace(this, table_ident.ns);
  ICEBERG_RETURN_UNEXPECTED(ns);
  if (ns.value()->table_metadata_locations_.contains(table_ident.name)) {
    return AlreadyExists("{} already exists", table_ident.name);
  }
  ns.value()->table_metadata_locations_[table_ident.name] = metadata_location;
  return {};
}

Status InMemoryNamespace::UnregisterTable(TableIdentifier const& table_ident) {
  const auto ns = GetNamespace(this, table_ident.ns);
  ICEBERG_RETURN_UNEXPECTED(ns);
  ns.value()->table_metadata_locations_.erase(table_ident.name);
  return {};
}

Result<bool> InMemoryNamespace::TableExists(TableIdentifier const& table_ident) const {
  const auto ns = GetNamespace(this, table_ident.ns);
  ICEBERG_RETURN_UNEXPECTED(ns);
  return ns.value()->table_metadata_locations_.contains(table_ident.name);
}

Result<std::string> InMemoryNamespace::GetTableMetadataLocation(
    TableIdentifier const& table_ident) const {
  const auto ns = GetNamespace(this, table_ident.ns);
  ICEBERG_RETURN_UNEXPECTED(ns);
  const auto it = ns.value()->table_metadata_locations_.find(table_ident.name);
  if (it == ns.value()->table_metadata_locations_.end()) {
    return NotFound("{} does not exist", table_ident.name);
  }
  return it->second;
}

}  // namespace

class ICEBERG_EXPORT InMemoryCatalogImpl {
 public:
  InMemoryCatalogImpl(std::string name, std::shared_ptr<FileIO> file_io,
                      std::string warehouse_location,
                      std::unordered_map<std::string, std::string> properties);

  std::string_view name() const;

  Result<std::vector<TableIdentifier>> ListTables(const Namespace& ns) const;

  Result<std::unique_ptr<Table>> CreateTable(
      const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties);

  Result<std::unique_ptr<Table>> UpdateTable(
      const TableIdentifier& identifier,
      const std::vector<std::unique_ptr<UpdateRequirement>>& requirements,
      const std::vector<std::unique_ptr<MetadataUpdate>>& updates);

  Result<std::shared_ptr<Transaction>> StageCreateTable(
      const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties);

  Result<bool> TableExists(const TableIdentifier& identifier) const;

  Status DropTable(const TableIdentifier& identifier, bool purge);

  Result<std::shared_ptr<Table>> LoadTable(const TableIdentifier& identifier) const;

  Result<std::shared_ptr<Table>> RegisterTable(const TableIdentifier& identifier,
                                               const std::string& metadata_file_location);

  std::unique_ptr<TableBuilder> BuildTable(const TableIdentifier& identifier,
                                           const Schema& schema) const;

 private:
  std::string catalog_name_;
  std::unordered_map<std::string, std::string> properties_;
  std::shared_ptr<FileIO> file_io_;
  std::string warehouse_location_;
  std::unique_ptr<class InMemoryNamespace> root_namespace_;
  mutable std::recursive_mutex mutex_;
};

InMemoryCatalogImpl::InMemoryCatalogImpl(
    std::string name, std::shared_ptr<FileIO> file_io, std::string warehouse_location,
    std::unordered_map<std::string, std::string> properties)
    : catalog_name_(std::move(name)),
      properties_(std::move(properties)),
      file_io_(std::move(file_io)),
      warehouse_location_(std::move(warehouse_location)),
      root_namespace_(std::make_unique<InMemoryNamespace>()) {}

std::string_view InMemoryCatalogImpl::name() const { return catalog_name_; }

Result<std::vector<TableIdentifier>> InMemoryCatalogImpl::ListTables(
    const Namespace& ns) const {
  std::unique_lock lock(mutex_);
  const auto& table_names = root_namespace_->ListTables(ns);
  ICEBERG_RETURN_UNEXPECTED(table_names);
  std::vector<TableIdentifier> table_idents;
  table_idents.reserve(table_names.value().size());
  std::ranges::transform(
      table_names.value(), std::back_inserter(table_idents),
      [&ns](auto const& table_name) { return TableIdentifier(ns, table_name); });
  return table_idents;
}

Result<std::unique_ptr<Table>> InMemoryCatalogImpl::CreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("create table");
}

Result<std::unique_ptr<Table>> InMemoryCatalogImpl::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<UpdateRequirement>>& requirements,
    const std::vector<std::unique_ptr<MetadataUpdate>>& updates) {
  return NotImplemented("update table");
}

Result<std::shared_ptr<Transaction>> InMemoryCatalogImpl::StageCreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("stage create table");
}

Result<bool> InMemoryCatalogImpl::TableExists(const TableIdentifier& identifier) const {
  std::unique_lock lock(mutex_);
  return root_namespace_->TableExists(identifier);
}

Status InMemoryCatalogImpl::DropTable(const TableIdentifier& identifier, bool purge) {
  std::unique_lock lock(mutex_);
  // TODO(Guotao): Delete all metadata files if purge is true.
  return root_namespace_->UnregisterTable(identifier);
}

Result<std::shared_ptr<Table>> InMemoryCatalogImpl::LoadTable(
    const TableIdentifier& identifier) const {
  return NotImplemented("load table");
}

Result<std::shared_ptr<Table>> InMemoryCatalogImpl::RegisterTable(
    const TableIdentifier& identifier, const std::string& metadata_file_location) {
  std::unique_lock lock(mutex_);
  if (!root_namespace_->NamespaceExists(identifier.ns)) {
    return NoSuchNamespace("table namespace does not exist.");
  }
  if (!root_namespace_->RegisterTable(identifier, metadata_file_location)) {
    return UnknownError("The registry failed.");
  }
  return LoadTable(identifier);
}

std::unique_ptr<TableBuilder> InMemoryCatalogImpl::BuildTable(
    const TableIdentifier& identifier, const Schema& schema) const {
  throw IcebergError("not implemented");
}

InMemoryCatalog::InMemoryCatalog(
    std::string const& name, std::shared_ptr<FileIO> const& file_io,
    std::string const& warehouse_location,
    std::unordered_map<std::string, std::string> const& properties)
    : impl_(std::make_unique<InMemoryCatalogImpl>(name, file_io, warehouse_location,
                                                  properties)) {}

InMemoryCatalog::~InMemoryCatalog() = default;

std::string_view InMemoryCatalog::name() const { return impl_->name(); }

Result<std::vector<TableIdentifier>> InMemoryCatalog::ListTables(
    const Namespace& ns) const {
  return impl_->ListTables(ns);
}

Result<std::unique_ptr<Table>> InMemoryCatalog::CreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return impl_->CreateTable(identifier, schema, spec, location, properties);
}

Result<std::unique_ptr<Table>> InMemoryCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<UpdateRequirement>>& requirements,
    const std::vector<std::unique_ptr<MetadataUpdate>>& updates) {
  return impl_->UpdateTable(identifier, requirements, updates);
}

Result<std::shared_ptr<Transaction>> InMemoryCatalog::StageCreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return impl_->StageCreateTable(identifier, schema, spec, location, properties);
}

Result<bool> InMemoryCatalog::TableExists(const TableIdentifier& identifier) const {
  return impl_->TableExists(identifier);
}

Status InMemoryCatalog::DropTable(const TableIdentifier& identifier, bool purge) {
  return impl_->DropTable(identifier, purge);
}

Result<std::shared_ptr<Table>> InMemoryCatalog::LoadTable(
    const TableIdentifier& identifier) const {
  return impl_->LoadTable(identifier);
}

Result<std::shared_ptr<Table>> InMemoryCatalog::RegisterTable(
    const TableIdentifier& identifier, const std::string& metadata_file_location) {
  return impl_->RegisterTable(identifier, metadata_file_location);
}

std::unique_ptr<TableBuilder> InMemoryCatalog::BuildTable(
    const TableIdentifier& identifier, const Schema& schema) const {
  throw IcebergError("not implemented");
}

}  // namespace iceberg
