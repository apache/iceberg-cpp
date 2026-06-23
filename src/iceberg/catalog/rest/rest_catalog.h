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
#include "iceberg/catalog/session_catalog.h"
#include "iceberg/catalog/session_context.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/rest_catalog.h
/// RestCatalog implementation for Iceberg REST API.

namespace iceberg::rest {

/// \brief Session-aware REST catalog root.
class ICEBERG_REST_EXPORT RestCatalog final
    : public SessionCatalog,
      public std::enable_shared_from_this<RestCatalog> {
 public:
  ~RestCatalog() override;

  RestCatalog(const RestCatalog&) = delete;
  RestCatalog& operator=(const RestCatalog&) = delete;
  RestCatalog(RestCatalog&&) = delete;
  RestCatalog& operator=(RestCatalog&&) = delete;

  /// \brief Create a RestCatalog instance.
  static Result<std::shared_ptr<RestCatalog>> Make(const RestCatalogProperties& config);

  std::string_view name() const override;

  Result<std::shared_ptr<Catalog>> AsCatalog() override;

  Result<std::shared_ptr<Catalog>> WithContext(SessionContext context) override;

 private:
  class ContextCatalog;
  class TableScopedCatalog;

  RestCatalog(RestCatalogProperties config, std::shared_ptr<FileIO> file_io,
              std::unique_ptr<HttpClient> client, std::unique_ptr<ResourcePaths> paths,
              std::unordered_set<Endpoint> endpoints,
              std::unique_ptr<auth::AuthManager> auth_manager,
              std::shared_ptr<auth::AuthSession> catalog_session,
              SnapshotMode snapshot_mode, SessionContext default_context);

  Result<std::shared_ptr<auth::AuthSession>> ContextualAuthSession(
      const SessionContext& context);

  Result<std::shared_ptr<auth::AuthSession>> TableAuthSession(
      const TableIdentifier& identifier,
      const std::unordered_map<std::string, std::string>& table_config,
      std::shared_ptr<auth::AuthSession> contextual_session);

  Result<std::shared_ptr<FileIO>> TableFileIO(
      const SessionContext& context,
      const std::unordered_map<std::string, std::string>& table_config,
      const std::vector<StorageCredential>& storage_credentials,
      std::string_view storage_location) const;

  Result<std::vector<Namespace>> ListNamespaces(const Namespace& ns,
                                                auth::AuthSession& session) const;

  Status CreateNamespace(const Namespace& ns,
                         const std::unordered_map<std::string, std::string>& properties,
                         auth::AuthSession& session);

  Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
      const Namespace& ns, auth::AuthSession& session) const;

  Status DropNamespace(const Namespace& ns, auth::AuthSession& session);

  Result<bool> NamespaceExists(const Namespace& ns, auth::AuthSession& session) const;

  Status UpdateNamespaceProperties(
      const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
      const std::unordered_set<std::string>& removals, auth::AuthSession& session);

  Result<std::vector<TableIdentifier>> ListTables(const Namespace& ns,
                                                  auth::AuthSession& session) const;

  Result<std::shared_ptr<Table>> CreateTable(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
      const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties,
      const SessionContext& context,
      std::shared_ptr<auth::AuthSession> contextual_session);

  Result<std::shared_ptr<Table>> UpdateTable(
      const TableIdentifier& identifier,
      const std::vector<std::unique_ptr<TableRequirement>>& requirements,
      const std::vector<std::unique_ptr<TableUpdate>>& updates,
      const SessionContext& context,
      std::shared_ptr<auth::AuthSession> contextual_session);

  Result<std::shared_ptr<Transaction>> StageCreateTable(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
      const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties,
      const SessionContext& context,
      std::shared_ptr<auth::AuthSession> contextual_session);

  Result<bool> TableExists(const TableIdentifier& identifier,
                           auth::AuthSession& session) const;

  Status RenameTable(const TableIdentifier& from, const TableIdentifier& to,
                     auth::AuthSession& session);

  Status DropTable(const TableIdentifier& identifier, bool purge,
                   auth::AuthSession& session);

  Result<std::shared_ptr<Table>> LoadTable(
      const TableIdentifier& identifier, const SessionContext& context,
      std::shared_ptr<auth::AuthSession> request_session,
      std::shared_ptr<auth::AuthSession> contextual_session);

  Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier, const std::string& metadata_file_location,
      const SessionContext& context,
      std::shared_ptr<auth::AuthSession> contextual_session);

  Result<LoadTableResult> LoadTableInternal(const TableIdentifier& identifier,
                                            auth::AuthSession& session) const;

  Result<LoadTableResult> CreateTableInternal(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
      const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties, bool stage_create,
      auth::AuthSession& session);

  Result<CommitTableResponse> UpdateTableInternal(
      const TableIdentifier& identifier,
      const std::vector<std::unique_ptr<TableRequirement>>& requirements,
      const std::vector<std::unique_ptr<TableUpdate>>& updates,
      auth::AuthSession& session);

  Result<std::shared_ptr<Table>> MakeTableFromLoadResult(
      const TableIdentifier& identifier, LoadTableResult result,
      const SessionContext& context,
      std::shared_ptr<auth::AuthSession> contextual_session);

  Result<std::shared_ptr<Table>> MakeTableFromCommitResponse(
      const TableIdentifier& identifier, CommitTableResponse response,
      const SessionContext& context,
      const std::unordered_map<std::string, std::string>& table_config,
      std::shared_ptr<auth::AuthSession> table_session, std::shared_ptr<FileIO> table_io);

  RestCatalogProperties config_;
  std::shared_ptr<FileIO> file_io_;
  std::unique_ptr<HttpClient> client_;
  std::unique_ptr<ResourcePaths> paths_;
  std::string name_;
  std::unordered_set<Endpoint> supported_endpoints_;
  std::unique_ptr<auth::AuthManager> auth_manager_;
  std::shared_ptr<auth::AuthSession> catalog_session_;
  SnapshotMode snapshot_mode_;
  SessionContext default_context_;
  std::weak_ptr<Catalog> default_catalog_;
};

}  // namespace iceberg::rest
