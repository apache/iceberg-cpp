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

#include "iceberg/catalog/memory_catalog.h"

#include <algorithm>
#include <iterator>  // IWYU pragma: keep

#include "iceberg/exception.h"
#include "iceberg/table.h"

namespace iceberg {

namespace {

InMemoryNamespace* GetNamespace(InMemoryNamespace* root,
                                const Namespace& namespace_ident) {
  return InMemoryNamespace::GetNamespaceImpl(root, namespace_ident);
}

const InMemoryNamespace* GetNamespace(const InMemoryNamespace* root,
                                      const Namespace& namespace_ident) {
  return InMemoryNamespace::GetNamespaceImpl(root, namespace_ident);
}

}  // namespace

InMemoryCatalog::InMemoryCatalog(std::shared_ptr<FileIO> file_io,
                                 std::string warehouse_location)
    : file_io_(std::move(file_io)),
      warehouse_location_(std::move(warehouse_location)),
      root_namespace_(std::make_unique<InMemoryNamespace>()) {}

void InMemoryCatalog::Initialize(
    const std::string& name,
    const std::unordered_map<std::string, std::string>& properties) {
  catalog_name_ = name;
  properties_ = properties;
}

std::string_view InMemoryCatalog::name() const { return catalog_name_; }

Result<std::vector<TableIdentifier>> InMemoryCatalog::ListTables(
    const Namespace& ns) const {
  std::unique_lock lock(mutex_);
  const auto& table_names = root_namespace_->ListTables(ns);
  std::vector<TableIdentifier> table_idents;
  table_idents.reserve(table_names.size());
  std::ranges::transform(
      table_names, std::back_inserter(table_idents),
      [&ns](auto const& table_name) { return TableIdentifier(ns, table_name); });
  return table_idents;
}

Result<std::unique_ptr<Table>> InMemoryCatalog::CreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "CreateTable"});
}

Result<std::unique_ptr<Table>> InMemoryCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<UpdateRequirement>>& requirements,
    const std::vector<std::unique_ptr<MetadataUpdate>>& updates) {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "UpdateTable"});
}

Result<std::shared_ptr<Transaction>> InMemoryCatalog::StageCreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "StageCreateTable"});
}

bool InMemoryCatalog::TableExists(const TableIdentifier& identifier) const {
  std::unique_lock lock(mutex_);
  return root_namespace_->TableExists(identifier);
}

bool InMemoryCatalog::DropTable(const TableIdentifier& identifier, bool purge) {
  std::unique_lock lock(mutex_);
  // TODO(Guotao): Delete all metadata files if purge is true.
  return root_namespace_->UnregisterTable(identifier);
}

Result<std::shared_ptr<Table>> InMemoryCatalog::LoadTable(
    const TableIdentifier& identifier) const {
  return unexpected<Error>({.kind = ErrorKind::kNotImplemented, .message = "LoadTable"});
}

Result<std::shared_ptr<Table>> InMemoryCatalog::RegisterTable(
    const TableIdentifier& identifier, const std::string& metadata_file_location) {
  std::unique_lock lock(mutex_);
  if (!root_namespace_->NamespaceExists(identifier.ns)) {
    return unexpected<Error>({.kind = ErrorKind::kNoSuchNamespace,
                              .message = "table namespace does not exist"});
  }
  if (!root_namespace_->RegisterTable(identifier, metadata_file_location)) {
    return unexpected<Error>(
        {.kind = ErrorKind::kUnknownError, .message = "The registry failed."});
  }
  return LoadTable(identifier);
}

std::unique_ptr<TableBuilder> InMemoryCatalog::BuildTable(
    const TableIdentifier& identifier, const Schema& schema) const {
  throw IcebergError("not implemented");
}

bool InMemoryNamespace::NamespaceExists(const Namespace& namespace_ident) const {
  return GetNamespace(this, namespace_ident) != nullptr;
}

std::vector<std::string> InMemoryNamespace::ListChildrenNamespaces(
    const std::optional<Namespace>& parent_namespace_ident) const {
  auto ns = this;
  if (parent_namespace_ident.has_value()) {
    ns = GetNamespace(this, *parent_namespace_ident);
    if (!ns) return {};
  }

  std::vector<std::string> names;
  auto const& children = ns->children_;
  names.reserve(children.size());
  std::ranges::transform(children, std::back_inserter(names),
                         [](const auto& pair) { return pair.first; });
  return names;
}

bool InMemoryNamespace::CreateNamespace(
    const Namespace& namespace_ident,
    const std::unordered_map<std::string, std::string>& properties) {
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
    return false;
  }

  ns->properties_ = properties;
  return true;
}

bool InMemoryNamespace::DeleteNamespace(const Namespace& namespace_ident) {
  if (namespace_ident.levels.empty()) return false;

  auto parent_namespace_ident = namespace_ident;
  const auto to_delete = parent_namespace_ident.levels.back();
  parent_namespace_ident.levels.pop_back();

  auto* parent = GetNamespace(this, parent_namespace_ident);
  if (!parent) return false;

  auto it = parent->children_.find(to_delete);
  if (it == parent->children_.end()) return false;

  const auto& target = it->second;
  if (!target.children_.empty() || !target.table_metadata_locations_.empty()) {
    return false;
  }

  return parent->children_.erase(to_delete) > 0;
}

std::optional<std::unordered_map<std::string, std::string>>
InMemoryNamespace::GetProperties(const Namespace& namespace_ident) const {
  const auto ns = GetNamespace(this, namespace_ident);
  if (!ns) return std::nullopt;
  return ns->properties_;
}

bool InMemoryNamespace::ReplaceProperties(
    const Namespace& namespace_ident,
    const std::unordered_map<std::string, std::string>& properties) {
  const auto ns = GetNamespace(this, namespace_ident);
  if (!ns) return false;
  ns->properties_ = properties;
  return true;
}

std::vector<std::string> InMemoryNamespace::ListTables(
    const Namespace& namespace_ident) const {
  const auto ns = GetNamespace(this, namespace_ident);
  if (!ns) return {};

  const auto& locations = ns->table_metadata_locations_;
  std::vector<std::string> table_names;
  table_names.reserve(locations.size());

  std::ranges::transform(locations, std::back_inserter(table_names),
                         [](const auto& pair) { return pair.first; });
  std::ranges::sort(table_names);

  return table_names;
}

bool InMemoryNamespace::RegisterTable(TableIdentifier const& table_ident,
                                      const std::string& metadata_location) {
  const auto ns = GetNamespace(this, table_ident.ns);
  if (!ns) return false;
  if (ns->table_metadata_locations_.contains(table_ident.name)) return false;
  ns->table_metadata_locations_[table_ident.name] = metadata_location;
  return true;
}

bool InMemoryNamespace::UnregisterTable(TableIdentifier const& table_ident) {
  const auto ns = GetNamespace(this, table_ident.ns);
  if (!ns) return false;
  return ns->table_metadata_locations_.erase(table_ident.name) > 0;
}

bool InMemoryNamespace::TableExists(TableIdentifier const& table_ident) const {
  const auto ns = GetNamespace(this, table_ident.ns);
  if (!ns) return false;
  return ns->table_metadata_locations_.contains(table_ident.name);
}

std::optional<std::string> InMemoryNamespace::GetTableMetadataLocation(
    TableIdentifier const& table_ident) const {
  const auto ns = GetNamespace(this, table_ident.ns);
  if (!ns) return std::nullopt;
  const auto it = ns->table_metadata_locations_.find(table_ident.name);
  if (it == ns->table_metadata_locations_.end()) return std::nullopt;
  return it->second;
}
}  // namespace iceberg
