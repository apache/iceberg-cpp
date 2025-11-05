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

#include "iceberg/catalog/rest/catalog.h"

#include <memory>
#include <utility>

#include <cpr/cpr.h>

#include "iceberg/catalog/rest/config.h"
#include "iceberg/catalog/rest/http_client_interal.h"
#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/catalog/rest/util.h"
#include "iceberg/json_internal.h"
#include "iceberg/result.h"
#include "iceberg/table.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

Result<RestCatalog> RestCatalog::Make(RestCatalogConfig config) {
  // Validate that uri is not empty
  if (config.uri.empty()) {
    return InvalidArgument("Rest catalog configuration property 'uri' is required.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto tmp_client, HttpClient::Make(config));
  const std::string endpoint = config.GetConfigEndpoint();
  cpr::Parameters params;
  if (config.warehouse.has_value()) {
    params.Add({"warehouse", config.warehouse.value()});
  }
  ICEBERG_ASSIGN_OR_RAISE(const auto& response, tmp_client->Get(endpoint, params));
  switch (response.status_code) {
    case cpr::status::HTTP_OK: {
      ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.text));
      ICEBERG_ASSIGN_OR_RAISE(auto server_config, CatalogConfigFromJson(json));
      // Merge server config into client config, server config overrides > client config
      // properties > server config defaults
      auto final_props = std::move(server_config.defaults);
      for (const auto& kv : config.properties_) {
        final_props.insert_or_assign(kv.first, kv.second);
      }

      for (const auto& kv : server_config.overrides) {
        final_props.insert_or_assign(kv.first, kv.second);
      }
      RestCatalogConfig final_config = {
          .uri = config.uri,
          .name = config.name,
          .warehouse = config.warehouse,
          .properties_ = std::move(final_props),
      };
      ICEBERG_ASSIGN_OR_RAISE(auto client, HttpClient::Make(final_config));
      return RestCatalog(std::make_shared<RestCatalogConfig>(std::move(final_config)),
                         std::move(client));
    };
    default: {
      ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.text));
      ICEBERG_ASSIGN_OR_RAISE(auto list_response, ErrorResponseFromJson(json));
      return UnknownError("Error listing namespaces: {}", list_response.error.message);
    }
  }
}

RestCatalog::RestCatalog(std::shared_ptr<RestCatalogConfig> config,
                         std::unique_ptr<HttpClient> client)
    : config_(std::move(config)), client_(std::move(client)) {}

std::string_view RestCatalog::name() const {
  return config_->name.has_value() ? std::string_view(config_->name.value())
                                   : std::string_view("");
}

Result<std::vector<Namespace>> RestCatalog::ListNamespaces(const Namespace& ns) const {
  const std::string endpoint = config_->GetNamespacesEndpoint();
  std::vector<Namespace> result;
  std::string next_token;
  while (true) {
    cpr::Parameters params;
    if (!ns.levels.empty()) {
      params.Add({"parent", EncodeNamespaceForUrl(ns)});
    }
    if (!next_token.empty()) {
      params.Add({"page_token", next_token});
    }
    ICEBERG_ASSIGN_OR_RAISE(const auto& response, client_->Get(endpoint, params));
    switch (response.status_code) {
      case cpr::status::HTTP_OK: {
        ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.text));
        ICEBERG_ASSIGN_OR_RAISE(auto list_response, ListNamespacesResponseFromJson(json));
        result.insert(result.end(), list_response.namespaces.begin(),
                      list_response.namespaces.end());
        if (list_response.next_page_token.empty()) {
          return result;
        }
        next_token = list_response.next_page_token;
        continue;
      }
      case cpr::status::HTTP_NOT_FOUND: {
        return NoSuchNamespace("Namespace not found");
      }
      default:
        ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.text));
        ICEBERG_ASSIGN_OR_RAISE(auto list_response, ErrorResponseFromJson(json));
        return UnknownError("Error listing namespaces: {}", list_response.error.message);
    }
  }
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
