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

#include <cpr/cpr.h>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/constant.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/http_response.h"
#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/catalog/rest/rest_catalog.h"
#include "iceberg/catalog/rest/rest_util.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/json_internal.h"
#include "iceberg/result.h"
#include "iceberg/table.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

Result<std::unique_ptr<RestCatalogConfig>> RestCatalog::FetchAndMergeConfig(
    const RestCatalogConfig& config, const ResourcePaths& paths) {
  // Fetch server configuration
  auto tmp_client = std::make_unique<HttpClient>(config);
  const std::string endpoint = paths.Config();
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          tmp_client->Get(endpoint, {}, {}, DefaultErrorHandler()));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto server_config, CatalogConfigFromJson(json));

  // Merge server config into client config, server config overrides > client config
  // properties > server config defaults
  auto final_props =
      MergeConfigs(server_config.defaults, config.configs(), server_config.overrides);
  return RestCatalogConfig::FromMap(final_props);
}

Result<std::unique_ptr<RestCatalog>> RestCatalog::Make(const RestCatalogConfig& config) {
  ICEBERG_ASSIGN_OR_RAISE(auto paths, ResourcePaths::Make(config));
  // Fetch and merge server configuration
  ICEBERG_ASSIGN_OR_RAISE(auto final_config, FetchAndMergeConfig(config, *paths));

  auto client = std::make_unique<HttpClient>(*final_config);
  ICEBERG_ASSIGN_OR_RAISE(auto final_paths, ResourcePaths::Make(*final_config));

  std::string catalog_name = final_config->Get(RestCatalogConfig::kName);
  return std::unique_ptr<RestCatalog>(
      new RestCatalog(std::move(final_config), std::move(client), std::move(*final_paths),
                      std::move(catalog_name)));
}

RestCatalog::RestCatalog(std::unique_ptr<RestCatalogConfig> config,
                         std::unique_ptr<HttpClient> client, ResourcePaths paths,
                         std::string name)
    : config_(std::move(config)),
      client_(std::move(client)),
      paths_(std::move(paths)),
      name_(std::move(name)) {}

std::string_view RestCatalog::name() const { return name_; }

Result<std::vector<Namespace>> RestCatalog::ListNamespaces(const Namespace& ns) const {
  const std::string endpoint = paths_.Namespaces();
  std::vector<Namespace> result;
  std::string next_token;
  while (true) {
    std::unordered_map<std::string, std::string> params;
    if (!ns.levels.empty()) {
      params[kQueryParamParent] = EncodeNamespaceForUrl(ns);
    }
    if (!next_token.empty()) {
      params[kQueryParamPageToken] = next_token;
    }
    ICEBERG_ASSIGN_OR_RAISE(const auto& response,
                            client_->Get(endpoint, params, {}, NamespaceErrorHandler()));
    ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
    ICEBERG_ASSIGN_OR_RAISE(auto list_response, ListNamespacesResponseFromJson(json));
    result.insert(result.end(), list_response.namespaces.begin(),
                  list_response.namespaces.end());
    if (list_response.next_page_token.empty()) {
      return result;
    }
    next_token = list_response.next_page_token;
    continue;
  }
  return result;
}

Status RestCatalog::CreateNamespace(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("Not implemented");
}

Result<std::unordered_map<std::string, std::string>> RestCatalog::GetNamespaceProperties(
    const Namespace& ns) const {
  return NotImplemented("Not implemented");
}

Status RestCatalog::DropNamespace(const Namespace& ns) {
  return NotImplemented("Not implemented");
}

Result<bool> RestCatalog::NamespaceExists(const Namespace& ns) const {
  return NotImplemented("Not implemented");
}

Status RestCatalog::UpdateNamespaceProperties(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
    const std::unordered_set<std::string>& removals) {
  return NotImplemented("Not implemented");
}

Result<std::vector<TableIdentifier>> RestCatalog::ListTables(const Namespace& ns) const {
  return NotImplemented("Not implemented");
}

Result<std::unique_ptr<Table>> RestCatalog::CreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("Not implemented");
}

Result<std::unique_ptr<Table>> RestCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<TableRequirement>>& requirements,
    const std::vector<std::unique_ptr<TableUpdate>>& updates) {
  return NotImplemented("Not implemented");
}

Result<std::shared_ptr<Transaction>> RestCatalog::StageCreateTable(
    const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  return NotImplemented("Not implemented");
}

Status RestCatalog::DropTable(const TableIdentifier& identifier, bool purge) {
  return NotImplemented("Not implemented");
}

Result<bool> RestCatalog::TableExists(const TableIdentifier& identifier) const {
  return NotImplemented("Not implemented");
}

Status RestCatalog::RenameTable(const TableIdentifier& from, const TableIdentifier& to) {
  return NotImplemented("Not implemented");
}

Result<std::unique_ptr<Table>> RestCatalog::LoadTable(const TableIdentifier& identifier) {
  return NotImplemented("Not implemented");
}

Result<std::shared_ptr<Table>> RestCatalog::RegisterTable(
    const TableIdentifier& identifier, const std::string& metadata_file_location) {
  return NotImplemented("Not implemented");
}

std::unique_ptr<RestCatalog::TableBuilder> RestCatalog::BuildTable(
    const TableIdentifier& identifier, const Schema& schema) const {
  return nullptr;
}

}  // namespace iceberg::rest
