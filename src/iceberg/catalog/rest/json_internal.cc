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

#include "iceberg/catalog/rest/json_internal.h"

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/types.h"
#include "iceberg/json_internal.h"
#include "iceberg/table_identifier.h"
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

// REST API JSON field constants
constexpr std::string_view kNamespace = "namespace";
constexpr std::string_view kNamespaces = "namespaces";
constexpr std::string_view kProperties = "properties";
constexpr std::string_view kRemovals = "removals";
constexpr std::string_view kUpdates = "updates";
constexpr std::string_view kUpdated = "updated";
constexpr std::string_view kRemoved = "removed";
constexpr std::string_view kMissing = "missing";
constexpr std::string_view kNextPageToken = "next-page-token";
constexpr std::string_view kName = "name";
constexpr std::string_view kLocation = "location";
constexpr std::string_view kSchema = "schema";
constexpr std::string_view kPartitionSpec = "partition-spec";
constexpr std::string_view kWriteOrder = "write-order";
constexpr std::string_view kStageCreate = "stage-create";
constexpr std::string_view kMetadataLocation = "metadata-location";
constexpr std::string_view kOverwrite = "overwrite";
constexpr std::string_view kSource = "source";
constexpr std::string_view kDestination = "destination";
constexpr std::string_view kMetadata = "metadata";
constexpr std::string_view kConfig = "config";
constexpr std::string_view kIdentifiers = "identifiers";

}  // namespace

nlohmann::json ToJson(const CreateNamespaceRequest& request) {
  nlohmann::json json;
  json[kNamespace] = request.namespace_.levels;
  if (!request.properties.empty()) {
    json[kProperties] = request.properties;
  }
  return json;
}

Result<CreateNamespaceRequest> CreateNamespaceRequestFromJson(
    const nlohmann::json& json) {
  CreateNamespaceRequest request;
  ICEBERG_ASSIGN_OR_RAISE(request.namespace_.levels,
                          GetJsonValue<std::vector<std::string>>(json, kNamespace));
  ICEBERG_ASSIGN_OR_RAISE(
      request.properties,
      GetJsonValueOrDefault<decltype(request.properties)>(json, kProperties));
  return request;
}

nlohmann::json ToJson(const UpdateNamespacePropertiesRequest& request) {
  // Initialize as an empty object so that when all optional fields are absent we return
  // {} instead of null
  nlohmann::json json = nlohmann::json::object();
  if (!request.removals.empty()) {
    json[kRemovals] = request.removals;
  }
  if (!request.updates.empty()) {
    json[kUpdates] = request.updates;
  }
  return json;
}

Result<UpdateNamespacePropertiesRequest> UpdateNamespacePropertiesRequestFromJson(
    const nlohmann::json& json) {
  UpdateNamespacePropertiesRequest request;
  ICEBERG_ASSIGN_OR_RAISE(
      request.removals, GetJsonValueOrDefault<std::vector<std::string>>(json, kRemovals));
  ICEBERG_ASSIGN_OR_RAISE(
      request.updates, GetJsonValueOrDefault<decltype(request.updates)>(json, kUpdates));
  return request;
}

nlohmann::json ToJson(const RegisterTableRequest& request) {
  nlohmann::json json;
  json[kName] = request.name;
  json[kMetadataLocation] = request.metadata_location;
  if (request.overwrite) {
    json[kOverwrite] = request.overwrite;
  }
  return json;
}

Result<RegisterTableRequest> RegisterTableRequestFromJson(const nlohmann::json& json) {
  RegisterTableRequest request;
  ICEBERG_ASSIGN_OR_RAISE(request.name, GetJsonValue<std::string>(json, kName));
  ICEBERG_ASSIGN_OR_RAISE(request.metadata_location,
                          GetJsonValue<std::string>(json, kMetadataLocation));
  ICEBERG_ASSIGN_OR_RAISE(request.overwrite,
                          GetJsonValueOrDefault<bool>(json, kOverwrite, false));
  return request;
}

nlohmann::json ToJson(const RenameTableRequest& request) {
  nlohmann::json json;
  json[kSource] = ToJson(request.source);
  json[kDestination] = ToJson(request.destination);
  return json;
}

Result<RenameTableRequest> RenameTableRequestFromJson(const nlohmann::json& json) {
  RenameTableRequest request;
  ICEBERG_ASSIGN_OR_RAISE(auto source_json, GetJsonValue<nlohmann::json>(json, kSource));
  ICEBERG_ASSIGN_OR_RAISE(request.source, TableIdentifierFromJson(source_json));
  ICEBERG_ASSIGN_OR_RAISE(auto dest_json,
                          GetJsonValue<nlohmann::json>(json, kDestination));
  ICEBERG_ASSIGN_OR_RAISE(request.destination, TableIdentifierFromJson(dest_json));
  return request;
}

// LoadTableResult (used by CreateTableResponse, LoadTableResponse)
nlohmann::json ToJson(const LoadTableResult& result) {
  nlohmann::json json;
  if (!result.metadata_location.empty()) {
    json[kMetadataLocation] = result.metadata_location;
  }
  json[kMetadata] = ToJson(*result.metadata);
  if (!result.config.empty()) {
    json[kConfig] = result.config;
  }
  return json;
}

Result<LoadTableResult> LoadTableResultFromJson(const nlohmann::json& json) {
  LoadTableResult result;
  ICEBERG_ASSIGN_OR_RAISE(result.metadata_location,
                          GetJsonValueOrDefault<std::string>(json, kMetadataLocation));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_json,
                          GetJsonValue<nlohmann::json>(json, kMetadata));
  ICEBERG_ASSIGN_OR_RAISE(result.metadata, TableMetadataFromJson(metadata_json));
  ICEBERG_ASSIGN_OR_RAISE(
      result.config, (GetJsonValueOrDefault<std::unordered_map<std::string, std::string>>(
                         json, kConfig)));
  return result;
}

nlohmann::json ToJson(const ListNamespacesResponse& response) {
  nlohmann::json json;
  if (!response.next_page_token.empty()) {
    json[kNextPageToken] = response.next_page_token;
  }
  nlohmann::json namespaces = nlohmann::json::array();
  for (const auto& ns : response.namespaces) {
    namespaces.push_back(ToJson(ns));
  }
  json[kNamespaces] = std::move(namespaces);
  return json;
}

Result<ListNamespacesResponse> ListNamespacesResponseFromJson(
    const nlohmann::json& json) {
  ListNamespacesResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.next_page_token,
                          GetJsonValueOrDefault<std::string>(json, kNextPageToken));
  ICEBERG_ASSIGN_OR_RAISE(auto namespaces_json,
                          GetJsonValue<nlohmann::json>(json, kNamespaces));
  for (const auto& ns_json : namespaces_json) {
    ICEBERG_ASSIGN_OR_RAISE(auto ns, NamespaceFromJson(ns_json));
    response.namespaces.push_back(std::move(ns));
  }
  return response;
}

nlohmann::json ToJson(const CreateNamespaceResponse& response) {
  nlohmann::json json;
  json[kNamespace] = response.namespace_.levels;
  if (!response.properties.empty()) {
    json[kProperties] = response.properties;
  }
  return json;
}

Result<CreateNamespaceResponse> CreateNamespaceResponseFromJson(
    const nlohmann::json& json) {
  CreateNamespaceResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.namespace_.levels,
                          GetJsonValue<std::vector<std::string>>(json, kNamespace));
  ICEBERG_ASSIGN_OR_RAISE(
      response.properties,
      GetJsonValueOrDefault<decltype(response.properties)>(json, kProperties));
  return response;
}

nlohmann::json ToJson(const GetNamespaceResponse& response) {
  nlohmann::json json;
  json[kNamespace] = response.namespace_.levels;
  if (!response.properties.empty()) {
    json[kProperties] = response.properties;
  }
  return json;
}

Result<GetNamespaceResponse> GetNamespaceResponseFromJson(const nlohmann::json& json) {
  GetNamespaceResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.namespace_.levels,
                          GetJsonValue<std::vector<std::string>>(json, kNamespace));
  ICEBERG_ASSIGN_OR_RAISE(
      response.properties,
      GetJsonValueOrDefault<decltype(response.properties)>(json, kProperties));
  return response;
}

nlohmann::json ToJson(const UpdateNamespacePropertiesResponse& response) {
  nlohmann::json json;
  json[kUpdated] = response.updated;
  json[kRemoved] = response.removed;
  if (!response.missing.empty()) {
    json[kMissing] = response.missing;
  }
  return json;
}

Result<UpdateNamespacePropertiesResponse> UpdateNamespacePropertiesResponseFromJson(
    const nlohmann::json& json) {
  UpdateNamespacePropertiesResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.updated,
                          GetJsonValue<std::vector<std::string>>(json, kUpdated));
  ICEBERG_ASSIGN_OR_RAISE(response.removed,
                          GetJsonValue<std::vector<std::string>>(json, kRemoved));
  ICEBERG_ASSIGN_OR_RAISE(
      response.missing, GetJsonValueOrDefault<std::vector<std::string>>(json, kMissing));
  return response;
}

nlohmann::json ToJson(const ListTablesResponse& response) {
  nlohmann::json json;
  if (!response.next_page_token.empty()) {
    json[kNextPageToken] = response.next_page_token;
  }
  nlohmann::json identifiers_json = nlohmann::json::array();
  for (const auto& identifier : response.identifiers) {
    identifiers_json.push_back(ToJson(identifier));
  }
  json[kIdentifiers] = identifiers_json;
  return json;
}

Result<ListTablesResponse> ListTablesResponseFromJson(const nlohmann::json& json) {
  ListTablesResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.next_page_token,
                          GetJsonValueOrDefault<std::string>(json, kNextPageToken));
  ICEBERG_ASSIGN_OR_RAISE(auto identifiers_json,
                          GetJsonValue<nlohmann::json>(json, kIdentifiers));
  for (const auto& id_json : identifiers_json) {
    ICEBERG_ASSIGN_OR_RAISE(auto identifier, TableIdentifierFromJson(id_json));
    response.identifiers.push_back(std::move(identifier));
  }
  return response;
}

}  // namespace iceberg::rest
