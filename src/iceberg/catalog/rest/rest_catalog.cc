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
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/constant.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/catalog/rest/resource_paths.h"
#include "iceberg/catalog/rest/rest_catalog.h"
#include "iceberg/catalog/rest/rest_util.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/transaction.h"
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

/// \brief Fetch server config and merge it with client config
Result<CatalogConfig> FetchServerConfig(const ResourcePaths& paths,
                                        const RestCatalogProperties& current_config) {
  ICEBERG_ASSIGN_OR_RAISE(auto config_path, paths.Config());
  HttpClient client(current_config.ExtractHeaders());
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client.Get(config_path, /*params=*/{}, /*headers=*/{},
                                     *DefaultErrorHandler::Instance()));
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

}  // namespace

RestCatalog::~RestCatalog() = default;

Result<std::shared_ptr<RestCatalog>> RestCatalog::Make(
    const RestCatalogProperties& config, std::shared_ptr<FileIO> file_io) {
  ICEBERG_ASSIGN_OR_RAISE(auto uri, config.Uri());
  if (!file_io) {
    return InvalidArgument("FileIO is required to create RestCatalog");
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto paths, ResourcePaths::Make(std::string(TrimTrailingSlash(uri)),
                                      config.Get(RestCatalogProperties::kPrefix)));
  ICEBERG_ASSIGN_OR_RAISE(auto server_config, FetchServerConfig(*paths, config));

  std::unique_ptr<RestCatalogProperties> final_config = RestCatalogProperties::FromMap(
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
  ICEBERG_ASSIGN_OR_RAISE(auto final_uri, final_config->Uri());
  ICEBERG_ASSIGN_OR_RAISE(
      paths, ResourcePaths::Make(std::string(TrimTrailingSlash(final_uri)),
                                 final_config->Get(RestCatalogProperties::kPrefix)));

  return std::shared_ptr<RestCatalog>(
      new RestCatalog(std::move(final_config), std::move(file_io), std::move(paths),
                      std::move(endpoints)));
}

RestCatalog::RestCatalog(std::unique_ptr<RestCatalogProperties> config,
                         std::shared_ptr<FileIO> file_io,
                         std::unique_ptr<ResourcePaths> paths,
                         std::unordered_set<Endpoint> endpoints)
    : config_(std::move(config)),
      file_io_(std::move(file_io)),
      client_(std::make_unique<HttpClient>(config_->ExtractHeaders())),
      paths_(std::move(paths)),
      name_(config_->Get(RestCatalogProperties::kName)),
      supported_endpoints_(std::move(endpoints)) {}

std::string_view RestCatalog::name() const { return name_; }

Result<std::vector<Namespace>> RestCatalog::ListNamespaces(const Namespace& ns) const {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::ListNamespaces());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespaces());
  std::vector<Namespace> result;
  std::string next_token;
  while (true) {
    std::unordered_map<std::string, std::string> params;
    if (!ns.levels.empty()) {
      ICEBERG_ASSIGN_OR_RAISE(params[kQueryParamParent], EncodeNamespace(ns));
    }
    if (!next_token.empty()) {
      params[kQueryParamPageToken] = next_token;
    }
    ICEBERG_ASSIGN_OR_RAISE(
        const auto response,
        client_->Get(path, params, /*headers=*/{}, *NamespaceErrorHandler::Instance()));
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
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::CreateNamespace());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespaces());
  CreateNamespaceRequest request{.namespace_ = ns, .properties = properties};
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Post(path, json_request, /*headers=*/{},
                                        *NamespaceErrorHandler::Instance()));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto create_response, CreateNamespaceResponseFromJson(json));
  return {};
}

Result<std::unordered_map<std::string, std::string>> RestCatalog::GetNamespaceProperties(
    const Namespace& ns) const {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::GetNamespaceProperties());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespace_(ns));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Get(path, /*params=*/{}, /*headers=*/{},
                                       *NamespaceErrorHandler::Instance()));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto get_response, GetNamespaceResponseFromJson(json));
  return get_response.properties;
}

Status RestCatalog::DropNamespace(const Namespace& ns) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::DropNamespace());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespace_(ns));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Delete(path, /*params=*/{}, /*headers=*/{},
                                          *DropNamespaceErrorHandler::Instance()));
  return {};
}

Result<bool> RestCatalog::NamespaceExists(const Namespace& ns) const {
  if (!supported_endpoints_.contains(Endpoint::NamespaceExists())) {
    // Fall back to GetNamespaceProperties
    return CaptureNoSuchNamespace(GetNamespaceProperties(ns));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespace_(ns));
  return CaptureNoSuchNamespace(
      client_->Head(path, /*headers=*/{}, *NamespaceErrorHandler::Instance()));
}

Status RestCatalog::UpdateNamespaceProperties(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
    const std::unordered_set<std::string>& removals) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::UpdateNamespace());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->NamespaceProperties(ns));
  UpdateNamespacePropertiesRequest request{
      .removals = std::vector<std::string>(removals.begin(), removals.end()),
      .updates = updates};
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Post(path, json_request, /*headers=*/{},
                                        *NamespaceErrorHandler::Instance()));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto update_response,
                          UpdateNamespacePropertiesResponseFromJson(json));
  return {};
}

Result<std::vector<TableIdentifier>> RestCatalog::ListTables(const Namespace& ns) const {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::ListTables());

  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Tables(ns));
  std::vector<TableIdentifier> result;
  std::string next_token;
  while (true) {
    std::unordered_map<std::string, std::string> params;
    if (!next_token.empty()) {
      params[kQueryParamPageToken] = next_token;
    }
    ICEBERG_ASSIGN_OR_RAISE(
        const auto response,
        client_->Get(path, params, /*headers=*/{}, *TableErrorHandler::Instance()));
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
    const std::unordered_map<std::string, std::string>& properties, bool stage_create) {
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

  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      client_->Post(path, json_request, /*headers=*/{}, *TableErrorHandler::Instance()));

  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  return LoadTableResultFromJson(json);
}

Result<std::shared_ptr<Table>> RestCatalog::CreateTable(
    const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_ASSIGN_OR_RAISE(auto result,
                          CreateTableInternal(identifier, schema, spec, order, location,
                                              properties, /*stage_create=*/false));
  return Table::Make(identifier, std::move(result.metadata),
                     std::move(result.metadata_location), file_io_, shared_from_this());
}

Result<std::shared_ptr<Table>> RestCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<TableRequirement>>& requirements,
    const std::vector<std::unique_ptr<TableUpdate>>& updates) {
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

  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      client_->Post(path, json_request, /*headers=*/{}, *TableErrorHandler::Instance()));

  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto commit_response, CommitTableResponseFromJson(json));

  return Table::Make(identifier, std::move(commit_response.metadata),
                     std::move(commit_response.metadata_location), file_io_,
                     shared_from_this());
}

Result<std::shared_ptr<Transaction>> RestCatalog::StageCreateTable(
    const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_ASSIGN_OR_RAISE(auto result,
                          CreateTableInternal(identifier, schema, spec, order, location,
                                              properties, /*stage_create=*/true));
  ICEBERG_ASSIGN_OR_RAISE(auto staged_table,
                          StagedTable::Make(identifier, std::move(result.metadata),
                                            std::move(result.metadata_location), file_io_,
                                            shared_from_this()));
  return Transaction::Make(std::move(staged_table), Transaction::Kind::kCreate,
                           /*auto_commit=*/false);
}

Status RestCatalog::DropTable(const TableIdentifier& identifier, bool purge) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::DeleteTable());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Table(identifier));

  std::unordered_map<std::string, std::string> params;
  if (purge) {
    params["purgeRequested"] = "true";
  }
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      client_->Delete(path, params, /*headers=*/{}, *TableErrorHandler::Instance()));
  return {};
}

Result<bool> RestCatalog::TableExists(const TableIdentifier& identifier) const {
  if (!supported_endpoints_.contains(Endpoint::TableExists())) {
    // Fall back to call LoadTable
    return CaptureNoSuchTable(LoadTableInternal(identifier));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Table(identifier));
  return CaptureNoSuchTable(
      client_->Head(path, /*headers=*/{}, *TableErrorHandler::Instance()));
}

Status RestCatalog::RenameTable(const TableIdentifier& from, const TableIdentifier& to) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::RenameTable());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Rename());

  RenameTableRequest request{.source = from, .destination = to};
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      client_->Post(path, json_request, /*headers=*/{}, *TableErrorHandler::Instance()));

  return {};
}

Result<std::string> RestCatalog::LoadTableInternal(
    const TableIdentifier& identifier) const {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::LoadTable());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Table(identifier));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      client_->Get(path, /*params=*/{}, /*headers=*/{}, *TableErrorHandler::Instance()));
  return response.body();
}

Result<std::shared_ptr<Table>> RestCatalog::LoadTable(const TableIdentifier& identifier) {
  ICEBERG_ASSIGN_OR_RAISE(const auto body, LoadTableInternal(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(body));
  ICEBERG_ASSIGN_OR_RAISE(auto load_result, LoadTableResultFromJson(json));

  return Table::Make(identifier, std::move(load_result.metadata),
                     std::move(load_result.metadata_location), file_io_,
                     shared_from_this());
}

Result<std::shared_ptr<Table>> RestCatalog::RegisterTable(
    const TableIdentifier& identifier, const std::string& metadata_file_location) {
  ICEBERG_ENDPOINT_CHECK(supported_endpoints_, Endpoint::RegisterTable());
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Register(identifier.ns));

  RegisterTableRequest request{
      .name = identifier.name,
      .metadata_location = metadata_file_location,
  };

  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      client_->Post(path, json_request, /*headers=*/{}, *TableErrorHandler::Instance()));

  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto load_result, LoadTableResultFromJson(json));
  return Table::Make(identifier, std::move(load_result.metadata),
                     std::move(load_result.metadata_location), file_io_,
                     shared_from_this());
}

}  // namespace iceberg::rest
