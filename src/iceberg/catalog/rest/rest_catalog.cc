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

#include "iceberg/catalog/rest/rest_catalog.h"

#include <memory>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_managers.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/constant.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/catalog/rest/resource_paths.h"
#include "iceberg/catalog/rest/rest_file_io.h"
#include "iceberg/catalog/rest/rest_util.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/transaction.h"
#include "iceberg/util/formatter_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

/// \brief Get the default set of endpoints for backwards compatibility according to the
/// iceberg rest spec.
std::unordered_set<Endpoint> GetDefaultEndpoints() {
  return {
      Endpoint::ListNamespaces(),  Endpoint::GetNamespaceProperties(),
      Endpoint::CreateNamespace(), Endpoint::UpdateNamespace(),
      Endpoint::DropNamespace(),   Endpoint::ListTables(),
      Endpoint::LoadTable(),       Endpoint::CreateTable(),
      Endpoint::UpdateTable(),     Endpoint::DeleteTable(),
      Endpoint::RenameTable(),     Endpoint::RegisterTable(),
      Endpoint::ReportMetrics(),   Endpoint::CommitTransaction(),
  };
}

/// \brief Fetch server configuration from the REST catalog server.
Result<CatalogConfig> FetchServerConfig(const ResourcePaths& paths,
                                        const RestCatalogProperties& current_config,
                                        auth::AuthSession& session) {
  ICEBERG_ASSIGN_OR_RAISE(auto config_path, paths.Config());
  HttpClient client(current_config.ExtractHeaders());

  // Send the client's warehouse location to the service to keep in sync.
  // This is needed for cases where the warehouse is configured client side, but may
  // be used on the server side, like the Hive Metastore, where both client and service
  // may have a warehouse location.
  std::unordered_map<std::string, std::string> params;
  std::string warehouse = current_config.Get(RestCatalogProperties::kWarehouse);
  if (!warehouse.empty()) {
    params[RestCatalogProperties::kWarehouse.key()] = std::move(warehouse);
  }

  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client.Get(config_path, params, /*headers=*/{},
                                     *ConfigErrorHandler::Instance(), session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  return CatalogConfigFromJson(json);
}

#define ICEBERG_ENDPOINT_CHECK(endpoints, endpoint)                           \
  do {                                                                        \
    if (!endpoints.contains(endpoint)) {                                      \
      return NotSupported("Not supported endpoint: {}", endpoint.ToString()); \
    }                                                                         \
  } while (0)

Result<bool> CaptureNoSuchObject(const auto& status, ErrorKind kind) {
  ICEBERG_DCHECK(kind == ErrorKind::kNoSuchTable || kind == ErrorKind::kNoSuchNamespace,
                 "Invalid kind for CaptureNoSuchObject");
  if (status.has_value()) {
    return true;
  }
  if (status.error().kind == kind) {
    return false;
  }
  return std::unexpected(status.error());
}

Result<bool> CaptureNoSuchTable(const auto& status) {
  return CaptureNoSuchObject(status, ErrorKind::kNoSuchTable);
}

Result<bool> CaptureNoSuchNamespace(const auto& status) {
  return CaptureNoSuchObject(status, ErrorKind::kNoSuchNamespace);
}

Status CheckBoundTable(const TableIdentifier& requested, const TableIdentifier& bound) {
  if (requested == bound) {
    return {};
  }
  return InvalidArgument("Table-scoped catalog is bound to '{}', got '{}'",
                         ToString(bound), ToString(requested));
}

}  // namespace

class RestCatalog::ContextCatalog final
    : public Catalog,
      public std::enable_shared_from_this<RestCatalog::ContextCatalog> {
 public:
  ContextCatalog(std::shared_ptr<RestCatalog> root, SessionContext context)
      : root_(std::move(root)), context_(std::move(context)) {}

  std::string_view name() const override { return root_->name(); }

  Result<std::vector<Namespace>> ListNamespaces(const Namespace& ns) const override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->ListNamespaces(ns, *session);
  }

  Status CreateNamespace(
      const Namespace& ns,
      const std::unordered_map<std::string, std::string>& properties) override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->CreateNamespace(ns, properties, *session);
  }

  Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
      const Namespace& ns) const override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->GetNamespaceProperties(ns, *session);
  }

  Status DropNamespace(const Namespace& ns) override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->DropNamespace(ns, *session);
  }

  Result<bool> NamespaceExists(const Namespace& ns) const override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->NamespaceExists(ns, *session);
  }

  Status UpdateNamespaceProperties(
      const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
      const std::unordered_set<std::string>& removals) override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->UpdateNamespaceProperties(ns, updates, removals, *session);
  }

  Result<std::vector<TableIdentifier>> ListTables(const Namespace& ns) const override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->ListTables(ns, *session);
  }

  Result<std::shared_ptr<Table>> CreateTable(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
      const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->CreateTable(identifier, schema, spec, order, location, properties,
                              context_, std::move(session));
  }

  Result<std::shared_ptr<Table>> UpdateTable(
      const TableIdentifier& identifier,
      const std::vector<std::unique_ptr<TableRequirement>>& requirements,
      const std::vector<std::unique_ptr<TableUpdate>>& updates) override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->UpdateTable(identifier, requirements, updates, context_,
                              std::move(session));
  }

  Result<std::shared_ptr<Transaction>> StageCreateTable(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
      const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->StageCreateTable(identifier, schema, spec, order, location, properties,
                                   context_, std::move(session));
  }

  Result<bool> TableExists(const TableIdentifier& identifier) const override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->TableExists(identifier, *session);
  }

  Status RenameTable(const TableIdentifier& from, const TableIdentifier& to) override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->RenameTable(from, to, *session);
  }

  Status DropTable(const TableIdentifier& identifier, bool purge) override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->DropTable(identifier, purge, *session);
  }

  Result<std::shared_ptr<Table>> LoadTable(const TableIdentifier& identifier) override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->LoadTable(identifier, context_, session, session);
  }

  Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier,
      const std::string& metadata_file_location) override {
    ICEBERG_ASSIGN_OR_RAISE(auto session, root_->ContextualAuthSession(context_));
    return root_->RegisterTable(identifier, metadata_file_location, context_,
                                std::move(session));
  }

 private:
  std::shared_ptr<RestCatalog> root_;
  SessionContext context_;
};

class RestCatalog::TableScopedCatalog final
    : public Catalog,
      public std::enable_shared_from_this<RestCatalog::TableScopedCatalog> {
 public:
  TableScopedCatalog(std::shared_ptr<RestCatalog> root, SessionContext context,
                     TableIdentifier identifier,
                     std::unordered_map<std::string, std::string> table_config,
                     std::shared_ptr<auth::AuthSession> table_session,
                     std::shared_ptr<FileIO> table_io)
      : root_(std::move(root)),
        context_(std::move(context)),
        identifier_(std::move(identifier)),
        table_config_(std::move(table_config)),
        table_session_(std::move(table_session)),
        table_io_(std::move(table_io)) {}

  std::string_view name() const override { return root_->name(); }

  Result<std::vector<Namespace>> ListNamespaces(const Namespace& /*ns*/) const override {
    return NotSupported("Table-scoped catalog does not support ListNamespaces");
  }

  Status CreateNamespace(
      const Namespace& /*ns*/,
      const std::unordered_map<std::string, std::string>& /*properties*/) override {
    return NotSupported("Table-scoped catalog does not support CreateNamespace");
  }

  Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
      const Namespace& /*ns*/) const override {
    return NotSupported("Table-scoped catalog does not support GetNamespaceProperties");
  }

  Status DropNamespace(const Namespace& /*ns*/) override {
    return NotSupported("Table-scoped catalog does not support DropNamespace");
  }

  Result<bool> NamespaceExists(const Namespace& /*ns*/) const override {
    return NotSupported("Table-scoped catalog does not support NamespaceExists");
  }

  Status UpdateNamespaceProperties(
      const Namespace& /*ns*/,
      const std::unordered_map<std::string, std::string>& /*updates*/,
      const std::unordered_set<std::string>& /*removals*/) override {
    return NotSupported(
        "Table-scoped catalog does not support UpdateNamespaceProperties");
  }

  Result<std::vector<TableIdentifier>> ListTables(
      const Namespace& /*ns*/) const override {
    return NotSupported("Table-scoped catalog does not support ListTables");
  }

  Result<std::shared_ptr<Table>> CreateTable(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& /*schema*/,
      const std::shared_ptr<PartitionSpec>& /*spec*/,
      const std::shared_ptr<SortOrder>& /*order*/, const std::string& /*location*/,
      const std::unordered_map<std::string, std::string>& /*properties*/) override {
    ICEBERG_RETURN_UNEXPECTED(CheckBoundTable(identifier, identifier_));
    return NotSupported("Table-scoped catalog does not support CreateTable");
  }

  Result<std::shared_ptr<Table>> UpdateTable(
      const TableIdentifier& identifier,
      const std::vector<std::unique_ptr<TableRequirement>>& requirements,
      const std::vector<std::unique_ptr<TableUpdate>>& updates) override {
    ICEBERG_RETURN_UNEXPECTED(CheckBoundTable(identifier, identifier_));
    ICEBERG_ASSIGN_OR_RAISE(
        auto response,
        root_->UpdateTableInternal(identifier, requirements, updates, *table_session_));
    return root_->MakeTableFromCommitResponse(identifier, std::move(response), context_,
                                              table_config_, table_session_, table_io_);
  }

  Result<std::shared_ptr<Transaction>> StageCreateTable(
      const TableIdentifier& identifier, const std::shared_ptr<Schema>& /*schema*/,
      const std::shared_ptr<PartitionSpec>& /*spec*/,
      const std::shared_ptr<SortOrder>& /*order*/, const std::string& /*location*/,
      const std::unordered_map<std::string, std::string>& /*properties*/) override {
    ICEBERG_RETURN_UNEXPECTED(CheckBoundTable(identifier, identifier_));
    return NotSupported("Table-scoped catalog does not support StageCreateTable");
  }

  Result<bool> TableExists(const TableIdentifier& identifier) const override {
    ICEBERG_RETURN_UNEXPECTED(CheckBoundTable(identifier, identifier_));
    return root_->TableExists(identifier, *table_session_);
  }

  Status RenameTable(const TableIdentifier& from,
                     const TableIdentifier& /*to*/) override {
    ICEBERG_RETURN_UNEXPECTED(CheckBoundTable(from, identifier_));
    return NotSupported("Table-scoped catalog does not support RenameTable");
  }

  Status DropTable(const TableIdentifier& identifier, bool /*purge*/) override {
    ICEBERG_RETURN_UNEXPECTED(CheckBoundTable(identifier, identifier_));
    return NotSupported("Table-scoped catalog does not support DropTable");
  }

  Result<std::shared_ptr<Table>> LoadTable(const TableIdentifier& identifier) override {
    ICEBERG_RETURN_UNEXPECTED(CheckBoundTable(identifier, identifier_));
    ICEBERG_ASSIGN_OR_RAISE(auto contextual, root_->ContextualAuthSession(context_));
    return root_->LoadTable(identifier, context_, table_session_, std::move(contextual));
  }

  Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier,
      const std::string& /*metadata_file_location*/) override {
    ICEBERG_RETURN_UNEXPECTED(CheckBoundTable(identifier, identifier_));
    return NotSupported("Table-scoped catalog does not support RegisterTable");
  }

 private:
  std::shared_ptr<RestCatalog> root_;
  SessionContext context_;
  TableIdentifier identifier_;
  std::unordered_map<std::string, std::string> table_config_;
  std::shared_ptr<auth::AuthSession> table_session_;
  std::shared_ptr<FileIO> table_io_;
};

RestCatalog::~RestCatalog() {
  if (catalog_session_) {
    std::ignore = catalog_session_->Close();
  }
  if (auth_manager_) {
    std::ignore = auth_manager_->Close();
  }
}

Result<std::shared_ptr<RestCatalog>> RestCatalog::Make(
    const RestCatalogProperties& config) {
  ICEBERG_ASSIGN_OR_RAISE(auto uri, config.Uri());

  std::string catalog_name = config.Get(RestCatalogProperties::kName);
  ICEBERG_ASSIGN_OR_RAISE(auto auth_manager,
                          auth::AuthManagers::Load(catalog_name, config.configs()));
  ICEBERG_ASSIGN_OR_RAISE(
      auto paths,
      ResourcePaths::Make(std::string(TrimTrailingSlash(uri)),
                          config.Get(RestCatalogProperties::kPrefix),
                          config.Get(RestCatalogProperties::kNamespaceSeparator)));

  // Create init session for fetching server configuration
  HttpClient init_client(config.ExtractHeaders());
  ICEBERG_ASSIGN_OR_RAISE(auto init_session,
                          auth_manager->InitSession(init_client, config.configs()));
  ICEBERG_ASSIGN_OR_RAISE(auto server_config,
                          FetchServerConfig(*paths, config, *init_session));

  RestCatalogProperties final_config = RestCatalogProperties::FromMap(
      MergeConfigs(server_config.defaults, config.configs(), server_config.overrides));

  std::unordered_set<Endpoint> endpoints;
  if (!server_config.endpoints.empty()) {
    // Endpoints are already parsed during JSON deserialization, just convert to set
    endpoints = std::unordered_set<Endpoint>(server_config.endpoints.begin(),
                                             server_config.endpoints.end());
  } else {
    // If a server does not send the endpoints field, use default set of endpoints
    // for backwards compatibility with legacy servers
    endpoints = GetDefaultEndpoints();
  }

  // Update resource paths based on the final config
  ICEBERG_ASSIGN_OR_RAISE(auto final_uri, final_config.Uri());
  ICEBERG_ASSIGN_OR_RAISE(
      paths,
      ResourcePaths::Make(std::string(TrimTrailingSlash(final_uri)),
                          final_config.Get(RestCatalogProperties::kPrefix),
                          final_config.Get(RestCatalogProperties::kNamespaceSeparator)));

  // Get snapshot loading mode
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot_mode, final_config.SnapshotLoadingMode());

  auto client = std::make_unique<HttpClient>(final_config.ExtractHeaders());
  ICEBERG_ASSIGN_OR_RAISE(auto catalog_session,
                          auth_manager->CatalogSession(*client, final_config.configs()));

  // Create FileIO with the final configuration
  ICEBERG_ASSIGN_OR_RAISE(auto file_io, MakeCatalogFileIO(final_config));

  auto default_context = SessionContext::Empty();
  return std::shared_ptr<RestCatalog>(new RestCatalog(
      std::move(final_config), std::move(file_io), std::move(client), std::move(paths),
      std::move(endpoints), std::move(auth_manager), std::move(catalog_session),
      snapshot_mode, std::move(default_context)));
}

RestCatalog::RestCatalog(RestCatalogProperties config, std::shared_ptr<FileIO> file_io,
                         std::unique_ptr<HttpClient> client,
                         std::unique_ptr<ResourcePaths> paths,
                         std::unordered_set<Endpoint> endpoints,
                         std::unique_ptr<auth::AuthManager> auth_manager,
                         std::shared_ptr<auth::AuthSession> catalog_session,
                         SnapshotMode snapshot_mode, SessionContext default_context)
    : config_(std::move(config)),
      file_io_(std::move(file_io)),
      client_(std::move(client)),
      paths_(std::move(paths)),
      name_(config_.Get(RestCatalogProperties::kName)),
      supported_endpoints_(std::move(endpoints)),
      auth_manager_(std::move(auth_manager)),
      catalog_session_(std::move(catalog_session)),
      snapshot_mode_(snapshot_mode),
      default_context_(std::move(default_context)) {
  ICEBERG_DCHECK(catalog_session_ != nullptr, "catalog_session must not be null");
}

std::string_view RestCatalog::name() const { return name_; }

Result<std::shared_ptr<Catalog>> RestCatalog::AsCatalog() {
  if (auto catalog = default_catalog_.lock()) {
    return catalog;
  }

  auto catalog = std::make_shared<ContextCatalog>(shared_from_this(), default_context_);
  default_catalog_ = catalog;
  return catalog;
}

Result<std::shared_ptr<Catalog>> RestCatalog::WithContext(SessionContext context) {
  if (context.session_id.empty()) {
    return InvalidArgument("Session context session_id must not be empty");
  }
  return std::make_shared<ContextCatalog>(shared_from_this(), std::move(context));
}

Result<std::shared_ptr<auth::AuthSession>> RestCatalog::ContextualAuthSession(
    const SessionContext& context) {
  return auth_manager_->ContextualSession(context, catalog_session_);
}

Result<std::shared_ptr<auth::AuthSession>> RestCatalog::TableAuthSession(
    const TableIdentifier& identifier,
    const std::unordered_map<std::string, std::string>& table_config,
    std::shared_ptr<auth::AuthSession> contextual_session) {
  return auth_manager_->TableSession(identifier, table_config,
                                     std::move(contextual_session));
}

Result<std::shared_ptr<FileIO>> RestCatalog::TableFileIO(
    const SessionContext& /*context*/,
    const std::unordered_map<std::string, std::string>& table_config,
    const std::vector<StorageCredential>& storage_credentials) const {
  if (!table_config.empty() || !storage_credentials.empty()) {
    return MakeTableFileIO(config_.configs(), table_config, storage_credentials);
  }

  return file_io_;
}

Result<std::vector<Namespace>> RestCatalog::ListNamespaces(
    const Namespace& ns, auth::AuthSession& session) const {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::ListNamespaces());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespaces());
  std::vector<Namespace> result;
  std::string next_token;
  while (true) {
    std::unordered_map<std::string, std::string> params;
    if (!ns.levels.empty()) {
      ICEBERG_ASSIGN_OR_RAISE(
          params[kQueryParamParent],
          EncodeNamespace(ns, config_.Get(RestCatalogProperties::kNamespaceSeparator)));
    }
    if (!next_token.empty()) {
      params[kQueryParamPageToken] = next_token;
    }
    ICEBERG_ASSIGN_OR_RAISE(const auto response,
                            client_->Get(path, params, /*headers=*/{},
                                         *NamespaceErrorHandler::Instance(), session));
    ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
    ICEBERG_ASSIGN_OR_RAISE(auto list_response, ListNamespacesResponseFromJson(json));
    result.insert(result.end(), list_response.namespaces.begin(),
                  list_response.namespaces.end());
    if (list_response.next_page_token.empty()) {
      return result;
    }
    next_token = std::move(list_response.next_page_token);
  }
  return result;
}

Status RestCatalog::CreateNamespace(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties,
    auth::AuthSession& session) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::CreateNamespace());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespaces());
  CreateNamespaceRequest request{.namespace_ = ns, .properties = properties};
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Post(path, json_request, /*headers=*/{},
                                        *NamespaceErrorHandler::Instance(), session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto create_response, CreateNamespaceResponseFromJson(json));
  return {};
}

Result<std::unordered_map<std::string, std::string>> RestCatalog::GetNamespaceProperties(
    const Namespace& ns, auth::AuthSession& session) const {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::GetNamespaceProperties());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespace_(ns));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Get(path, /*params=*/{}, /*headers=*/{},
                                       *NamespaceErrorHandler::Instance(), session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto get_response, GetNamespaceResponseFromJson(json));
  return get_response.properties;
}

Status RestCatalog::DropNamespace(const Namespace& ns, auth::AuthSession& session) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::DropNamespace());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespace_(ns));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      client_->Delete(path, /*params=*/{}, /*headers=*/{},
                      *DropNamespaceErrorHandler::Instance(), session));
  return {};
}

Result<bool> RestCatalog::NamespaceExists(const Namespace& ns,
                                          auth::AuthSession& session) const {
  if (!supported_endpoints_.contains(Endpoint::NamespaceExists())) {
    // Fall back to GetNamespaceProperties
    return CaptureNoSuchNamespace(GetNamespaceProperties(ns, session));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespace_(ns));
  return CaptureNoSuchNamespace(
      client_->Head(path, /*headers=*/{}, *NamespaceErrorHandler::Instance(), session));
}

Status RestCatalog::UpdateNamespaceProperties(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
    const std::unordered_set<std::string>& removals, auth::AuthSession& session) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::UpdateNamespace());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->NamespaceProperties(ns));
  UpdateNamespacePropertiesRequest request{
      .removals = std::vector<std::string>(removals.begin(), removals.end()),
      .updates = updates};
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Post(path, json_request, /*headers=*/{},
                                        *NamespaceErrorHandler::Instance(), session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto update_response,
                          UpdateNamespacePropertiesResponseFromJson(json));
  return {};
}

Result<std::vector<TableIdentifier>> RestCatalog::ListTables(
    const Namespace& ns, auth::AuthSession& session) const {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::ListTables());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Tables(ns));
  std::vector<TableIdentifier> result;
  std::string next_token;
  while (true) {
    std::unordered_map<std::string, std::string> params;
    if (!next_token.empty()) {
      params[kQueryParamPageToken] = next_token;
    }
    ICEBERG_ASSIGN_OR_RAISE(const auto response,
                            client_->Get(path, params, /*headers=*/{},
                                         *TableErrorHandler::Instance(), session));
    ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
    ICEBERG_ASSIGN_OR_RAISE(auto list_response, ListTablesResponseFromJson(json));
    result.insert(result.end(), list_response.identifiers.begin(),
                  list_response.identifiers.end());
    if (list_response.next_page_token.empty()) {
      return result;
    }
    next_token = std::move(list_response.next_page_token);
  }
  return result;
}

Result<LoadTableResult> RestCatalog::CreateTableInternal(
    const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties, bool stage_create,
    auth::AuthSession& session) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::CreateTable());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Tables(identifier.ns));

  CreateTableRequest request{
      .name = identifier.name,
      .location = location,
      .schema = schema,
      .partition_spec = spec,
      .write_order = order,
      .stage_create = stage_create,
      .properties = properties,
  };

  ICEBERG_ASSIGN_OR_RAISE(auto request_json, ToJson(request));
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(request_json));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Post(path, json_request, /*headers=*/{},
                                        *TableErrorHandler::Instance(), session));

  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto load_result, LoadTableResultFromJson(json));
  return load_result;
}

Result<std::shared_ptr<Table>> RestCatalog::CreateTable(
    const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties,
    const SessionContext& context,
    std::shared_ptr<auth::AuthSession> contextual_session) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto result,
      CreateTableInternal(identifier, schema, spec, order, location, properties,
                          /*stage_create=*/false, *contextual_session));
  return MakeTableFromLoadResult(identifier, std::move(result), context,
                                 std::move(contextual_session));
}

Result<CommitTableResponse> RestCatalog::UpdateTableInternal(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<TableRequirement>>& requirements,
    const std::vector<std::unique_ptr<TableUpdate>>& updates,
    auth::AuthSession& session) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::UpdateTable());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Table(identifier));

  CommitTableRequest request{.identifier = identifier};
  request.requirements.reserve(requirements.size());
  for (const auto& req : requirements) {
    request.requirements.push_back(req->Clone());
  }
  request.updates.reserve(updates.size());
  for (const auto& update : updates) {
    request.updates.push_back(update->Clone());
  }

  ICEBERG_ASSIGN_OR_RAISE(auto request_json, ToJson(request));
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(request_json));
  ICEBERG_ASSIGN_OR_RAISE(auto is_create, TableRequirements::IsCreate(requirements));
  const ErrorHandler* error_handler = TableCommitErrorHandler::Instance().get();
  if (is_create) {
    error_handler = CreateTableErrorHandler::Instance().get();
  }
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      client_->Post(path, json_request, /*headers=*/{}, *error_handler, session));

  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto commit_response, CommitTableResponseFromJson(json));
  return commit_response;
}

Result<std::shared_ptr<Table>> RestCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<TableRequirement>>& requirements,
    const std::vector<std::unique_ptr<TableUpdate>>& updates,
    const SessionContext& context,
    std::shared_ptr<auth::AuthSession> contextual_session) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto response,
      UpdateTableInternal(identifier, requirements, updates, *contextual_session));
  std::unordered_map<std::string, std::string> table_config;
  ICEBERG_ASSIGN_OR_RAISE(
      auto table_session,
      TableAuthSession(identifier, table_config, std::move(contextual_session)));
  // Top-level updates have no loaded table FileIO to reuse.
  return MakeTableFromCommitResponse(identifier, std::move(response), context,
                                     table_config, std::move(table_session), file_io_);
}

Result<std::shared_ptr<Transaction>> RestCatalog::StageCreateTable(
    const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties,
    const SessionContext& context,
    std::shared_ptr<auth::AuthSession> contextual_session) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto result,
      CreateTableInternal(identifier, schema, spec, order, location, properties,
                          /*stage_create=*/true, *contextual_session));
  auto table_config = std::move(result.config);
  auto storage_credentials = std::move(result.storage_credentials);
  ICEBERG_ASSIGN_OR_RAISE(auto table_io,
                          TableFileIO(context, table_config, storage_credentials));
  ICEBERG_ASSIGN_OR_RAISE(
      auto table_session,
      TableAuthSession(identifier, table_config, std::move(contextual_session)));
  auto table_catalog = std::make_shared<TableScopedCatalog>(
      shared_from_this(), context, identifier, table_config, std::move(table_session),
      table_io);
  ICEBERG_ASSIGN_OR_RAISE(
      auto staged_table,
      StagedTable::Make(identifier, std::move(result.metadata),
                        std::move(result.metadata_location), std::move(table_io),
                        std::move(table_catalog)));
  return Transaction::Make(std::move(staged_table), TransactionKind::kCreate);
}

Status RestCatalog::DropTable(const TableIdentifier& identifier, bool purge,
                              auth::AuthSession& session) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::DeleteTable());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Table(identifier));

  std::unordered_map<std::string, std::string> params;
  if (purge) {
    params["purgeRequested"] = "true";
  }
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Delete(path, params, /*headers=*/{},
                                          *TableErrorHandler::Instance(), session));
  return {};
}

Result<bool> RestCatalog::TableExists(const TableIdentifier& identifier,
                                      auth::AuthSession& session) const {
  if (!supported_endpoints_.contains(Endpoint::TableExists())) {
    // Fall back to call LoadTable
    return CaptureNoSuchTable(LoadTableInternal(identifier, session));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Table(identifier));
  return CaptureNoSuchTable(
      client_->Head(path, /*headers=*/{}, *TableErrorHandler::Instance(), session));
}

Status RestCatalog::RenameTable(const TableIdentifier& from, const TableIdentifier& to,
                                auth::AuthSession& session) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::RenameTable());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Rename());

  RenameTableRequest request{.source = from, .destination = to};
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Post(path, json_request, /*headers=*/{},
                                        *TableErrorHandler::Instance(), session));

  return {};
}

Result<LoadTableResult> RestCatalog::LoadTableInternal(const TableIdentifier& identifier,
                                                       auth::AuthSession& session) const {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::LoadTable());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Table(identifier));

  std::unordered_map<std::string, std::string> params;
  if (snapshot_mode_ == SnapshotMode::kRefs) {
    params["snapshots"] = "refs";
  } else {
    params["snapshots"] = "all";
  }

  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Get(path, params, /*headers=*/{},
                                       *TableErrorHandler::Instance(), session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  return LoadTableResultFromJson(json);
}

Result<std::shared_ptr<Table>> RestCatalog::LoadTable(
    const TableIdentifier& identifier, const SessionContext& context,
    std::shared_ptr<auth::AuthSession> request_session,
    std::shared_ptr<auth::AuthSession> contextual_session) {
  ICEBERG_ASSIGN_OR_RAISE(auto load_result,
                          LoadTableInternal(identifier, *request_session));
  return MakeTableFromLoadResult(identifier, std::move(load_result), context,
                                 std::move(contextual_session));
}

Result<std::shared_ptr<Table>> RestCatalog::RegisterTable(
    const TableIdentifier& identifier, const std::string& metadata_file_location,
    const SessionContext& context,
    std::shared_ptr<auth::AuthSession> contextual_session) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::RegisterTable());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Register(identifier.ns));

  RegisterTableRequest request{
      .name = identifier.name,
      .metadata_location = metadata_file_location,
  };

  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      client_->Post(path, json_request, /*headers=*/{}, *TableErrorHandler::Instance(),
                    *contextual_session));

  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto load_result, LoadTableResultFromJson(json));
  return MakeTableFromLoadResult(identifier, std::move(load_result), context,
                                 std::move(contextual_session));
}

Result<std::shared_ptr<Table>> RestCatalog::MakeTableFromLoadResult(
    const TableIdentifier& identifier, LoadTableResult result,
    const SessionContext& context,
    std::shared_ptr<auth::AuthSession> contextual_session) {
  auto table_config = std::move(result.config);
  auto storage_credentials = std::move(result.storage_credentials);
  ICEBERG_ASSIGN_OR_RAISE(auto table_io,
                          TableFileIO(context, table_config, storage_credentials));
  ICEBERG_ASSIGN_OR_RAISE(
      auto table_session,
      TableAuthSession(identifier, table_config, std::move(contextual_session)));
  auto table_catalog = std::make_shared<TableScopedCatalog>(
      shared_from_this(), context, identifier, table_config, table_session, table_io);
  return Table::Make(identifier, std::move(result.metadata),
                     std::move(result.metadata_location), std::move(table_io),
                     std::move(table_catalog));
}

Result<std::shared_ptr<Table>> RestCatalog::MakeTableFromCommitResponse(
    const TableIdentifier& identifier, CommitTableResponse response,
    const SessionContext& context,
    const std::unordered_map<std::string, std::string>& table_config,
    std::shared_ptr<auth::AuthSession> table_session, std::shared_ptr<FileIO> table_io) {
  // Reuse the bound FileIO because commit responses carry no config or credentials.
  auto table_catalog = std::make_shared<TableScopedCatalog>(
      shared_from_this(), context, identifier, table_config, table_session, table_io);
  return Table::Make(identifier, std::move(response.metadata),
                     std::move(response.metadata_location), std::move(table_io),
                     std::move(table_catalog));
}

}  // namespace iceberg::rest
