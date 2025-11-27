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

#include <memory>
#include <string>

#include "iceberg/catalog.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/rest_catalog.h
/// RestCatalog implementation for Iceberg REST API.

namespace iceberg::rest {

/// \brief Rest catalog implementation.
class ICEBERG_REST_EXPORT RestCatalog : public Catalog {
 public:
  ~RestCatalog() override;

  RestCatalog(const RestCatalog&) = delete;
  RestCatalog& operator=(const RestCatalog&) = delete;
  RestCatalog(RestCatalog&&) = delete;
  RestCatalog& operator=(RestCatalog&&) = delete;

  /// \brief Create a RestCatalog instance
  ///
  /// \param config the configuration for the RestCatalog
  /// \return a unique_ptr to RestCatalog instance
  static Result<std::unique_ptr<RestCatalog>> Make(const RestCatalogProperties& config);

  std::string_view name() const override;

  Result<std::vector<Namespace>> ListNamespaces(const Namespace& ns) const override;

  Status CreateNamespace(
      const Namespace& ns,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
      const Namespace& ns) const override;

  Status DropNamespace(const Namespace& ns) override;

  Result<bool> NamespaceExists(const Namespace& ns) const override;

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
      const std::vector<std::unique_ptr<TableRequirement>>& requirements,
      const std::vector<std::unique_ptr<TableUpdate>>& updates) override;

  Result<std::shared_ptr<Transaction>> StageCreateTable(
      const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<bool> TableExists(const TableIdentifier& identifier) const override;

  Status RenameTable(const TableIdentifier& from, const TableIdentifier& to) override;

  Status DropTable(const TableIdentifier& identifier, bool purge) override;

  Result<std::unique_ptr<Table>> LoadTable(const TableIdentifier& identifier) override;

  Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier,
      const std::string& metadata_file_location) override;

  std::unique_ptr<RestCatalog::TableBuilder> BuildTable(
      const TableIdentifier& identifier, const Schema& schema) const override;

 private:
  RestCatalog(std::unique_ptr<RestCatalogProperties> config,
              std::unique_ptr<ResourcePaths> paths);

  std::unique_ptr<RestCatalogProperties> config_;
  std::unique_ptr<HttpClient> client_;
  std::unique_ptr<ResourcePaths> paths_;
  std::string name_;
};

}  // namespace iceberg::rest
