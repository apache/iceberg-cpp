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
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/constant.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/catalog/rest/resource_paths.h"
#include "iceberg/catalog/rest/rest_catalog.h"
#include "iceberg/catalog/rest/rest_util.h"
#include "iceberg/json_internal.h"
#include "iceberg/result.h"
#include "iceberg/table.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

// Fetch server config and merge it with client config
Result<std::unique_ptr<RestCatalogProperties>> FetchConfig(
    const ResourcePaths& paths, const RestCatalogProperties& config) {
  ICEBERG_ASSIGN_OR_RAISE(auto config_endpoint, paths.Config());
  HttpClient client(config.ExtractHeaders());
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          client.Get(config_endpoint, /*params=*/{}, /*headers=*/{},
                                     *DefaultErrorHandler::Instance()));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto server_config, CatalogConfigFromJson(json));

  // Merge priorities: server overrides > client config > server defaults
  return RestCatalogProperties::FromMap(
      MergeConfigs(server_config.overrides, config.configs(), server_config.defaults));
}

}  // namespace

RestCatalog::~RestCatalog() = default;

Result<std::unique_ptr<RestCatalog>> RestCatalog::Make(
    const RestCatalogProperties& config) {
  ICEBERG_ASSIGN_OR_RAISE(auto uri, config.Uri());
  ICEBERG_ASSIGN_OR_RAISE(
      auto paths, ResourcePaths::Make(std::string(TrimTrailingSlash(uri)),
                                      config.Get(RestCatalogProperties::kPrefix)));
  ICEBERG_ASSIGN_OR_RAISE(auto final_config, FetchConfig(*paths, config));

  // Update resource paths based on the final config
  ICEBERG_ASSIGN_OR_RAISE(auto final_uri, final_config->Uri());
  ICEBERG_RETURN_UNEXPECTED(paths->SetBaseUri(std::string(TrimTrailingSlash(final_uri))));

  return std::unique_ptr<RestCatalog>(
      new RestCatalog(std::move(final_config), std::move(paths)));
}

RestCatalog::RestCatalog(std::unique_ptr<RestCatalogProperties> config,
                         std::unique_ptr<ResourcePaths> paths)
    : config_(std::move(config)),
      client_(std::make_unique<HttpClient>(config_->ExtractHeaders())),
      paths_(std::move(paths)),
      name_(config_->Get(RestCatalogProperties::kName)) {}

std::string_view RestCatalog::name() const { return name_; }

Result<std::vector<Namespace>> RestCatalog::ListNamespaces(const Namespace& ns) const {
  ICEBERG_ASSIGN_OR_RAISE(auto endpoint, paths_->Namespaces());
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
    ICEBERG_ASSIGN_OR_RAISE(const auto& response,
                            client_->Get(endpoint, params, /*headers=*/{},
                                         *NamespaceErrorHandler::Instance()));
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
    [[maybe_unused]] const Namespace& ns,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("Not implemented");
}

Result<std::unordered_map<std::string, std::string>> RestCatalog::GetNamespaceProperties(
    [[maybe_unused]] const Namespace& ns) const {
  return NotImplemented("Not implemented");
}

Status RestCatalog::DropNamespace([[maybe_unused]] const Namespace& ns) {
  return NotImplemented("Not implemented");
}

Result<bool> RestCatalog::NamespaceExists([[maybe_unused]] const Namespace& ns) const {
  return NotImplemented("Not implemented");
}

Status RestCatalog::UpdateNamespaceProperties(
    [[maybe_unused]] const Namespace& ns,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& updates,
    [[maybe_unused]] const std::unordered_set<std::string>& removals) {
  return NotImplemented("Not implemented");
}

Result<std::vector<TableIdentifier>> RestCatalog::ListTables(
    [[maybe_unused]] const Namespace& ns) const {
  return NotImplemented("Not implemented");
}

Result<std::unique_ptr<Table>> RestCatalog::CreateTable(
    [[maybe_unused]] const TableIdentifier& identifier,
    [[maybe_unused]] const Schema& schema, [[maybe_unused]] const PartitionSpec& spec,
    [[maybe_unused]] const std::string& location,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("Not implemented");
}

Result<std::unique_ptr<Table>> RestCatalog::UpdateTable(
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

Result<std::unique_ptr<Table>> RestCatalog::LoadTable(
    [[maybe_unused]] const TableIdentifier& identifier) {
  return NotImplemented("Not implemented");
}

Result<std::shared_ptr<Table>> RestCatalog::RegisterTable(
    [[maybe_unused]] const TableIdentifier& identifier,
    [[maybe_unused]] const std::string& metadata_file_location) {
  return NotImplemented("Not implemented");
}

std::unique_ptr<RestCatalog::TableBuilder> RestCatalog::BuildTable(
    [[maybe_unused]] const TableIdentifier& identifier,
    [[maybe_unused]] const Schema& schema) const {
  return nullptr;
}

}  // namespace iceberg::rest
