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
#include <unordered_set>

#include "iceberg/catalog.h"
#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/rest_catalog.h
/// RestCatalog implementation for Iceberg REST API.

namespace iceberg::rest {

/// \brief Rest catalog implementation.
class ICEBERG_REST_EXPORT RestCatalog : public Catalog,
                                        public std::enable_shared_from_this<RestCatalog> {
 public:
  ~RestCatalog() override;

  RestCatalog(const RestCatalog&) = delete;
  RestCatalog& operator=(const RestCatalog&) = delete;
  RestCatalog(RestCatalog&&) = delete;
  RestCatalog& operator=(RestCatalog&&) = delete;

  /// \brief Create a RestCatalog instance.
  ///
  /// FileIO is resolved in this order:
  /// 1. If config.GetFileIO() is set, use it directly.
  /// 2. If the "io-impl" property is set, load from FileIORegistry.
  /// 3. Otherwise, auto-detect based on warehouse URI scheme:
  ///    - "s3://..." -> "s3" (ArrowS3FileIO)
  ///    - anything else -> "local" (ArrowLocalFileIO)
  ///
  /// Users can register custom FileIO implementations via FileIORegistry::Register():
  /// \code
  /// FileIORegistry::Register("myfs",
  ///     [](const auto& props) -> Result<std::unique_ptr<FileIO>> {
  ///         return std::make_unique<MyFileIO>(props);
  ///     });
  /// \endcode
  ///
  /// \param config the configuration for the RestCatalog, including warehouse location
  ///        and optional "io-impl" property or pre-configured FileIO
  /// \return a shared_ptr to RestCatalog instance, or an error if FileIO creation fails
  static Result<std::shared_ptr<RestCatalog>> Make(const RestCatalogProperties& config);

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

  Status RenameTable(const TableIdentifier& from, const TableIdentifier& to) override;

  Status DropTable(const TableIdentifier& identifier, bool purge) override;

  Result<std::shared_ptr<Table>> LoadTable(const TableIdentifier& identifier) override;

  Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier,
      const std::string& metadata_file_location) override;

 private:
  RestCatalog(RestCatalogProperties config, std::shared_ptr<FileIO> file_io,
              std::unique_ptr<HttpClient> client, std::unique_ptr<ResourcePaths> paths,
              std::unordered_set<Endpoint> endpoints,
              std::unique_ptr<auth::AuthManager> auth_manager,
              std::shared_ptr<auth::AuthSession> catalog_session,
              SnapshotMode snapshot_mode);

  Result<std::string> LoadTableInternal(const TableIdentifier& identifier) const;

  Result<LoadTableResult> CreateTableInternal(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
      const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties, bool stage_create);

  RestCatalogProperties config_;
  std::shared_ptr<FileIO> file_io_;
  std::unique_ptr<HttpClient> client_;
  std::unique_ptr<ResourcePaths> paths_;
  std::string name_;
  std::unordered_set<Endpoint> supported_endpoints_;
  std::unique_ptr<auth::AuthManager> auth_manager_;
  std::shared_ptr<auth::AuthSession> catalog_session_;
  SnapshotMode snapshot_mode_;
};

}  // namespace iceberg::rest
