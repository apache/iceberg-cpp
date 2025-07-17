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

#include "iceberg/exception.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg {

/// \brief A hierarchical namespace that manages namespaces and table metadata in-memory.
///
/// Each InMemoryNamespace represents a namespace level and can contain properties,
/// tables, and child namespaces. This structure enables a tree-like representation
/// of nested namespaces.
class ICEBERG_EXPORT InMemoryNamespace {
 public:
  /// \brief Checks whether the given namespace exists.
  ///
  /// \param namespace_ident The namespace to check.
  /// \return Result<bool> indicating whether the namespace exists.
  Result<bool> NamespaceExists(const Namespace& namespace_ident) const;

  /// \brief Lists immediate child namespaces under the given parent namespace.
  ///
  /// \param parent_namespace_ident The parent namespace to list children for.
  /// \return A vector of child namespaces if found; error otherwise.
  Result<std::vector<Namespace>> ListNamespaces(
      const Namespace& parent_namespace_ident) const;

  /// \brief Creates a new namespace with the specified properties.
  ///
  /// \param namespace_ident The namespace to create.
  /// \param properties A map of key-value pairs to associate with the namespace.
  /// \return Status::OK if the namespace is created;
  ///         ErrorKind::kAlreadyExists if the namespace already exists.
  Status CreateNamespace(const Namespace& namespace_ident,
                         const std::unordered_map<std::string, std::string>& properties);

  /// \brief Deletes an existing namespace.
  ///
  /// \param namespace_ident The namespace to delete.
  /// \return Status::OK if the namespace is deleted;
  ///         ErrorKind::kNoSuchNamespace if the namespace does not exist;
  ///         ErrorKind::kNotAllowed if the namespace is not empty.
  Status DropNamespace(const Namespace& namespace_ident);

  /// \brief Retrieves the properties of the specified namespace.
  ///
  /// \param namespace_ident The namespace whose properties are to be retrieved.
  /// \return A map of property key-value pairs if the namespace exists;
  ///         error otherwise.
  Result<std::unordered_map<std::string, std::string>> GetProperties(
      const Namespace& namespace_ident) const;

  /// \brief Updates a namespace's properties by applying additions and removals.
  ///
  /// \param namespace_ident The namespace to update.
  /// \param updates Properties to add or overwrite.
  /// \param removals Property keys to remove.
  /// \return Status::OK if the update is successful;
  ///         ErrorKind::kNoSuchNamespace if the namespace does not exist;
  ///         ErrorKind::kUnsupported if the operation is not supported.
  Status UpdateNamespaceProperties(
      const Namespace& namespace_ident,
      const std::unordered_map<std::string, std::string>& updates,
      const std::unordered_set<std::string>& removals);

  /// \brief Lists all table names under the specified namespace.
  ///
  /// \param namespace_ident The namespace to list tables from.
  /// \return A vector of table names if successful; error otherwise.
  Result<std::vector<std::string>> ListTables(const Namespace& namespace_ident) const;

  /// \brief Registers a table in the given namespace with a metadata location.
  ///
  /// \param table_ident The fully qualified identifier of the table.
  /// \param metadata_location The path to the table's metadata.
  /// \return Status::OK if the table is registered;
  ///         Error otherwise.
  Status RegisterTable(const TableIdentifier& table_ident,
                       const std::string& metadata_location);

  /// \brief Unregisters a table from the specified namespace.
  ///
  /// \param table_ident The identifier of the table to unregister.
  /// \return Status::OK if the table is removed;
  ///         ErrorKind::kNoSuchTable if the table does not exist.
  Status UnregisterTable(const TableIdentifier& table_ident);

  /// \brief Checks if a table exists in the specified namespace.
  ///
  /// \param table_ident The identifier of the table to check.
  /// \return Result<bool> indicating whether the table exists.
  Result<bool> TableExists(const TableIdentifier& table_ident) const;

  /// \brief Gets the metadata location for the specified table.
  ///
  /// \param table_ident The identifier of the table.
  /// \return The metadata location if the table exists; error otherwise.
  Result<std::string> GetTableMetadataLocation(const TableIdentifier& table_ident) const;

  /// \brief Internal utility for retrieving a namespace node pointer from the tree.
  ///
  /// \tparam NamespacePtr The type of the namespace node pointer.
  /// \param root The root namespace node.
  /// \param namespace_ident The fully qualified namespace to resolve.
  /// \return A pointer to the namespace node if it exists; error otherwise.
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

Result<bool> InMemoryNamespace::NamespaceExists(const Namespace& namespace_ident) const {
  const auto& ns = GetNamespace(this, namespace_ident);
  if (ns.has_value()) {
    return true;
  }
  if (ns.error().kind == ErrorKind::kNoSuchNamespace) {
    return false;
  }
  return std::unexpected<Error>(ns.error());
}

Result<std::vector<Namespace>> InMemoryNamespace::ListNamespaces(
    const Namespace& parent_namespace_ident) const {
  const auto nsRs = GetNamespace(this, parent_namespace_ident);
  ICEBERG_RETURN_UNEXPECTED(nsRs);
  auto ns = *nsRs;

  std::vector<Namespace> names;
  auto const& children = ns->children_;
  names.reserve(children.size());
  std::ranges::transform(children, std::back_inserter(names), [&](const auto& pair) {
    auto childNs = parent_namespace_ident;
    childNs.levels.emplace_back(pair.first);
    return childNs;
  });
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

Status InMemoryNamespace::DropNamespace(const Namespace& namespace_ident) {
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

Status InMemoryNamespace::UpdateNamespaceProperties(
    const Namespace& namespace_ident,
    const std::unordered_map<std::string, std::string>& updates,
    const std::unordered_set<std::string>& removals) {
  const auto ns = GetNamespace(this, namespace_ident);
  ICEBERG_RETURN_UNEXPECTED(ns);

  std::ranges::for_each(updates, [&](const auto& prop) {
    ns.value()->properties_[prop.first] = prop.second;
  });
  std::ranges::for_each(removals,
                        [&](const auto& prop) { ns.value()->properties_.erase(prop); });
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

InMemoryCatalog::InMemoryCatalog(
    std::string const& name, std::shared_ptr<FileIO> const& file_io,
    std::string const& warehouse_location,
    std::unordered_map<std::string, std::string> const& properties)
    : catalog_name_(std::move(name)),
      properties_(std::move(properties)),
      file_io_(std::move(file_io)),
      warehouse_location_(std::move(warehouse_location)),
      root_namespace_(std::make_unique<InMemoryNamespace>()) {}

InMemoryCatalog::~InMemoryCatalog() = default;

std::string_view InMemoryCatalog::name() const { return catalog_name_; }

Status InMemoryCatalog::CreateNamespace(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  std::unique_lock lock(mutex_);
  return root_namespace_->CreateNamespace(ns, properties);
}

Result<std::unordered_map<std::string, std::string>>
InMemoryCatalog::GetNamespaceProperties(const Namespace& ns) const {
  std::unique_lock lock(mutex_);
  return root_namespace_->GetProperties(ns);
}

Result<std::vector<Namespace>> InMemoryCatalog::ListNamespaces(
    const Namespace& ns) const {
  std::unique_lock lock(mutex_);
  return root_namespace_->ListNamespaces(ns);
}

Status InMemoryCatalog::DropNamespace(const Namespace& ns) {
  std::unique_lock lock(mutex_);
  return root_namespace_->DropNamespace(ns);
}

Result<bool> InMemoryCatalog::NamespaceExists(const Namespace& ns) const {
  std::unique_lock lock(mutex_);
  return root_namespace_->NamespaceExists(ns);
}

Status InMemoryCatalog::UpdateNamespaceProperties(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
    const std::unordered_set<std::string>& removals) {
  std::unique_lock lock(mutex_);
  return root_namespace_->UpdateNamespaceProperties(ns, updates, removals);
}

Result<std::vector<TableIdentifier>> InMemoryCatalog::ListTables(
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

Result<std::unique_ptr<Table>> InMemoryCatalog::CreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("create table");
}

Result<std::unique_ptr<Table>> InMemoryCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<UpdateRequirement>>& requirements,
    const std::vector<std::unique_ptr<MetadataUpdate>>& updates) {
  return NotImplemented("update table");
}

Result<std::shared_ptr<Transaction>> InMemoryCatalog::StageCreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("stage create table");
}

Result<bool> InMemoryCatalog::TableExists(const TableIdentifier& identifier) const {
  std::unique_lock lock(mutex_);
  return root_namespace_->TableExists(identifier);
}

Status InMemoryCatalog::DropTable(const TableIdentifier& identifier, bool purge) {
  std::unique_lock lock(mutex_);
  // TODO(Guotao): Delete all metadata files if purge is true.
  return root_namespace_->UnregisterTable(identifier);
}

Result<std::unique_ptr<Table>> InMemoryCatalog::LoadTable(
    const TableIdentifier& identifier) {
  if (!file_io_) [[unlikely]] {
    return InvalidArgument("file_io is not set for catalog {}", catalog_name_);
  }

  Result<std::string> metadata_location;
  {
    std::unique_lock lock(mutex_);
    ICEBERG_ASSIGN_OR_RAISE(metadata_location,
                            root_namespace_->GetTableMetadataLocation(identifier));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto metadata,
                          TableMetadataUtil::Read(*file_io_, metadata_location.value()));

  return std::make_unique<Table>(identifier, std::move(metadata),
                                 metadata_location.value(), file_io_,
                                 std::static_pointer_cast<Catalog>(shared_from_this()));
}

Result<std::shared_ptr<Table>> InMemoryCatalog::RegisterTable(
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

std::unique_ptr<TableBuilder> InMemoryCatalog::BuildTable(
    const TableIdentifier& identifier, const Schema& schema) const {
  throw IcebergError("not implemented");
}

}  // namespace iceberg
