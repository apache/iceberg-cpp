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

#include <mutex>
#include <optional>
#include <unordered_map>

#include "iceberg/catalog.h"

namespace iceberg {

class NamespaceContainer;

class ICEBERG_EXPORT MemoryCatalog : public Catalog {
 public:
  MemoryCatalog(std::shared_ptr<FileIO> file_io, std::string warehouse_location);

  void Initialize(
      const std::string& name,
      const std::unordered_map<std::string, std::string>& properties) override;

  std::string_view name() const override;

  Result<std::vector<TableIdentifier>> ListTables(const Namespace& ns) const override;

  Result<std::unique_ptr<Table>> CreateTable(
      const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<std::unique_ptr<Table>> UpdateTable(
      const TableIdentifier& identifier,
      const std::vector<std::unique_ptr<UpdateRequirement>>& requirements,
      const std::vector<std::unique_ptr<MetadataUpdate>>& updates) override;

  Result<std::shared_ptr<Transaction>> StageCreateTable(
      const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override;

  bool TableExists(const TableIdentifier& identifier) const override;

  bool DropTable(const TableIdentifier& identifier, bool purge) override;

  Result<std::shared_ptr<Table>> LoadTable(
      const TableIdentifier& identifier) const override;

  Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier,
      const std::string& metadata_file_location) override;

  std::unique_ptr<iceberg::TableBuilder> BuildTable(const TableIdentifier& identifier,
                                                    const Schema& schema) const override;

 private:
  std::string catalog_name_;
  std::unordered_map<std::string, std::string> properties_;
  std::shared_ptr<FileIO> file_io_;
  std::string warehouse_location_;
  std::unique_ptr<NamespaceContainer> root_container_;
  mutable std::recursive_mutex mutex_;
};

/**
 * \brief A hierarchical container that manages namespaces and table metadata in-memory.
 *
 * Each NamespaceContainer represents a namespace level and can contain properties,
 * tables, and child namespaces. This structure enables a tree-like representation
 * of nested namespaces.
 */
class ICEBERG_EXPORT NamespaceContainer {
 public:
  /**
   * \brief Checks whether the given namespace exists.
   * \param[in] namespace_ident The namespace to check.
   * \return True if the namespace exists; false otherwise.
   */
  bool NamespaceExists(const Namespace& namespace_ident) const;

  /**
   * \brief Lists immediate child namespaces under the given parent namespace.
   * \param[in] parent_namespace_ident The optional parent namespace. If not provided,
   *                                the children of the root are returned.
   * \return A vector of child namespace names.
   */
  std::vector<std::string> ListChildrenNamespaces(
      const std::optional<Namespace>& parent_namespace_ident = std::nullopt) const;

  /**
   * \brief Creates a new namespace with the specified properties.
   * \param[in] namespace_ident The namespace to create.
   * \param[in] properties A map of key-value pairs to associate with the namespace.
   * \return True if the namespace was successfully created; false if it already exists.
   */
  bool CreateNamespace(const Namespace& namespace_ident,
                       const std::unordered_map<std::string, std::string>& properties);

  /**
   * \brief Deletes an existing namespace.
   * \param[in] namespace_ident The namespace to delete.
   * \return True if the namespace was successfully deleted; false if it does not exist.
   */
  bool DeleteNamespace(const Namespace& namespace_ident);

  /**
   * \brief Retrieves the properties of the specified namespace.
   * \param[in] namespace_ident The namespace whose properties to retrieve.
   * \return An optional containing the properties map if the namespace exists;
   *         std::nullopt otherwise.
   */
  std::optional<std::unordered_map<std::string, std::string>> GetProperties(
      const Namespace& namespace_ident) const;

  /**
   * \brief Replaces all properties of the given namespace.
   * \param[in] namespace_ident The namespace whose properties will be replaced.
   * \param[in] properties The new properties map.
   * \return True if the namespace exists and properties were replaced; false otherwise.
   */
  bool ReplaceProperties(const Namespace& namespace_ident,
                         const std::unordered_map<std::string, std::string>& properties);

  /**
   * \brief Lists all table names under the specified namespace.
   * \param[in] namespace_ident The namespace from which to list tables.
   * \return A vector of table names.
   */
  std::vector<std::string> ListTables(const Namespace& namespace_ident) const;

  /**
   * \brief Registers a table in the given namespace with a metadata location.
   * \param[in] table_ident The fully qualified identifier of the table.
   * \param[in] metadata_location The path to the table's metadata.
   * \return True if the table was registered successfully; false otherwise.
   */
  bool RegisterTable(TableIdentifier const& table_ident,
                     const std::string& metadata_location);

  /**
   * \brief Unregisters a table from the specified namespace.
   * \param[in] table_ident The identifier of the table to unregister.
   * \return True if the table existed and was removed; false otherwise.
   */
  bool UnregisterTable(TableIdentifier const& table_ident);

  /**
   * \brief Checks if a table exists in the specified namespace.
   * \param[in] table_ident The identifier of the table to check.
   * \return True if the table exists; false otherwise.
   */
  bool TableExists(TableIdentifier const& table_ident) const;

  /**
   * \brief Gets the metadata location for the specified table.
   * \param[in] table_ident The identifier of the table.
   * \return An optional string containing the metadata location if the table exists;
   *         std::nullopt otherwise.
   */
  std::optional<std::string> GetTableMetadataLocation(
      TableIdentifier const& table_ident) const;

  template <typename ContainerPtr>
  static ContainerPtr GetNamespaceContainerImpl(ContainerPtr root,
                                                const Namespace& namespace_ident) {
    auto node = root;
    for (const auto& part_level : namespace_ident.levels) {
      auto it = node->children_.find(part_level);
      if (it == node->children_.end()) {
        return nullptr;
      }
      node = &it->second;
    }
    return node;
  }

 private:
  /// Map of child namespace names to their corresponding container instances.
  std::unordered_map<std::string, NamespaceContainer> children_;

  /// Key-value property map for this namespace.
  std::unordered_map<std::string, std::string> properties_;

  /// Mapping of table names to metadata file locations.
  std::unordered_map<std::string, std::string> table_metadata_locations_;
};
}  // namespace iceberg
