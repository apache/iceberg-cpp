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

#include "iceberg/catalog.h"

namespace iceberg {

/**
 * @brief An in-memory implementation of the Iceberg Catalog interface.
 *
 * This catalog stores all metadata purely in memory, with no persistence to disk
 * or external systems. It is primarily intended for unit tests, prototyping, or
 * demonstration purposes.
 *
 * @note This class is **not** suitable for production use.
 *       All data will be lost when the process exits.
 */
class ICEBERG_EXPORT InMemoryCatalog
    : public Catalog,
      public std::enable_shared_from_this<InMemoryCatalog> {
 public:
  InMemoryCatalog(std::string const& name, std::shared_ptr<FileIO> const& file_io,
                  std::string const& warehouse_location,
                  std::unordered_map<std::string, std::string> const& properties);
  ~InMemoryCatalog() override;

  static std::shared_ptr<InMemoryCatalog> Make(
      std::string const& name, std::shared_ptr<FileIO> const& file_io,
      std::string const& warehouse_location,
      std::unordered_map<std::string, std::string> const& properties);

  std::string_view name() const override;

  Status CreateNamespace(
      const Namespace& ns,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<std::vector<Namespace>> ListNamespaces(const Namespace& ns) const override;

  Status DropNamespace(const Namespace& ns) override;

  Result<bool> NamespaceExists(const Namespace& ns) const override;

  Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
      const Namespace& ns) const override;

  Status UpdateNamespaceProperties(
      const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
      const std::unordered_set<std::string>& removals) override;

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

  Result<bool> TableExists(const TableIdentifier& identifier) const override;

  Status DropTable(const TableIdentifier& identifier, bool purge) override;

  Result<std::unique_ptr<Table>> LoadTable(const TableIdentifier& identifier) override;

  Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier,
      const std::string& metadata_file_location) override;

  std::unique_ptr<TableBuilder> BuildTable(const TableIdentifier& identifier,
                                           const Schema& schema) const override;

 private:
  std::string catalog_name_;
  std::unordered_map<std::string, std::string> properties_;
  std::shared_ptr<FileIO> file_io_;
  std::string warehouse_location_;
  std::unique_ptr<class InMemoryNamespace> root_namespace_;
  mutable std::recursive_mutex mutex_;
};

}  // namespace iceberg
