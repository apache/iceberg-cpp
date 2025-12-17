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
#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/catalog/rest/resource_paths.h"
#include "iceberg/catalog/rest/rest_catalog.h"
#include "iceberg/catalog/rest/rest_util.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/json_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
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

}  // namespace

RestCatalog::~RestCatalog() = default;

Result<std::unique_ptr<RestCatalog>> RestCatalog::Make(
    const RestCatalogProperties& config) {
  ICEBERG_ASSIGN_OR_RAISE(auto uri, config.Uri());
  ICEBERG_ASSIGN_OR_RAISE(
      auto paths, ResourcePaths::Make(std::string(TrimTrailingSlash(uri)),
                                      config.Get(RestCatalogProperties::kPrefix)));
  ICEBERG_ASSIGN_OR_RAISE(auto server_config, FetchServerConfig(*paths, config));

  std::unique_ptr<RestCatalogProperties> final_config = RestCatalogProperties::FromMap(
      MergeConfigs(server_config.overrides, config.configs(), server_config.defaults));

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
  ICEBERG_RETURN_UNEXPECTED(paths->SetBaseUri(std::string(TrimTrailingSlash(final_uri))));

  return std::unique_ptr<RestCatalog>(
      new RestCatalog(std::move(final_config), std::move(paths), std::move(endpoints)));
}

RestCatalog::RestCatalog(std::unique_ptr<RestCatalogProperties> config,
                         std::unique_ptr<ResourcePaths> paths,
                         std::unordered_set<Endpoint> endpoints)
    : config_(std::move(config)),
      client_(std::make_unique<HttpClient>(config_->ExtractHeaders())),
      paths_(std::move(paths)),
      name_(config_->Get(RestCatalogProperties::kName)),
      supported_endpoints_(std::move(endpoints)) {}

std::string_view RestCatalog::name() const { return name_; }

Result<std::vector<Namespace>> RestCatalog::ListNamespaces(const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(
      CheckEndpoint(supported_endpoints_, Endpoint::ListNamespaces()));

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
    next_token = list_response.next_page_token;
  }
  return result;
}

Status RestCatalog::CreateNamespace(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(
      CheckEndpoint(supported_endpoints_, Endpoint::CreateNamespace()));

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
  ICEBERG_RETURN_UNEXPECTED(
      CheckEndpoint(supported_endpoints_, Endpoint::GetNamespaceProperties()));

  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespace_(ns));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client_->Get(path, /*params=*/{}, /*headers=*/{},
                                       *NamespaceErrorHandler::Instance()));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto get_response, GetNamespaceResponseFromJson(json));
  return get_response.properties;
}

Status RestCatalog::DropNamespace(const Namespace& ns) {
  ICEBERG_RETURN_UNEXPECTED(
      CheckEndpoint(supported_endpoints_, Endpoint::DropNamespace()));
  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespace_(ns));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      client_->Delete(path, /*headers=*/{}, *DropNamespaceErrorHandler::Instance()));
  return {};
}

Result<bool> RestCatalog::NamespaceExists(const Namespace& ns) const {
  auto check = CheckEndpoint(supported_endpoints_, Endpoint::NamespaceExists());
  if (!check.has_value()) {
    // Fall back to GetNamespaceProperties
    auto result = GetNamespaceProperties(ns);
    if (!result.has_value() && result.error().kind == ErrorKind::kNoSuchNamespace) {
      return false;
    }
    ICEBERG_RETURN_UNEXPECTED(result);
    // GET succeeded, namespace exists
    return true;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto path, paths_->Namespace_(ns));
  auto response_or_error =
      client_->Head(path, /*headers=*/{}, *NamespaceErrorHandler::Instance());
  if (!response_or_error.has_value()) {
    const auto& error = response_or_error.error();
    // catch NoSuchNamespaceException/404 and return false
    if (error.kind == ErrorKind::kNoSuchNamespace) {
      return false;
    }
    ICEBERG_RETURN_UNEXPECTED(response_or_error);
  }
  return true;
}

Status RestCatalog::UpdateNamespaceProperties(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
    const std::unordered_set<std::string>& removals) {
  ICEBERG_RETURN_UNEXPECTED(
      CheckEndpoint(supported_endpoints_, Endpoint::UpdateNamespace()));

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

Result<std::vector<TableIdentifier>> RestCatalog::ListTables(
    [[maybe_unused]] const Namespace& ns) const {
  return NotImplemented("Not implemented");
}

Result<std::shared_ptr<Table>> RestCatalog::CreateTable(
    [[maybe_unused]] const TableIdentifier& identifier,
    [[maybe_unused]] const Schema& schema, [[maybe_unused]] const PartitionSpec& spec,
    [[maybe_unused]] const std::string& location,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("Not implemented");
}

Result<std::shared_ptr<Table>> RestCatalog::UpdateTable(
    [[maybe_unused]] const TableIdentifier& identifier,
    [[maybe_unused]] const std::vector<std::unique_ptr<TableRequirement>>& requirements,
    [[maybe_unused]] const std::vector<std::unique_ptr<TableUpdate>>& updates) {
  return NotImplemented("Not implemented");
}

Result<std::shared_ptr<Transaction>> RestCatalog::StageCreateTable(
    [[maybe_unused]] const TableIdentifier& identifier,
    [[maybe_unused]] const Schema& schema, [[maybe_unused]] const PartitionSpec& spec,
    [[maybe_unused]] const std::string& location,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("Not implemented");
}

Status RestCatalog::DropTable([[maybe_unused]] const TableIdentifier& identifier,
                              [[maybe_unused]] bool purge) {
  return NotImplemented("Not implemented");
}

Result<bool> RestCatalog::TableExists(
    [[maybe_unused]] const TableIdentifier& identifier) const {
  return NotImplemented("Not implemented");
}

Status RestCatalog::RenameTable([[maybe_unused]] const TableIdentifier& from,
                                [[maybe_unused]] const TableIdentifier& to) {
  return NotImplemented("Not implemented");
}

Result<std::shared_ptr<Table>> RestCatalog::LoadTable(
    [[maybe_unused]] const TableIdentifier& identifier) {
  return NotImplemented("Not implemented");
}

Result<std::shared_ptr<Table>> RestCatalog::RegisterTable(
    [[maybe_unused]] const TableIdentifier& identifier,
    [[maybe_unused]] const std::string& metadata_file_location) {
  return NotImplemented("Not implemented");
}

}  // namespace iceberg::rest
