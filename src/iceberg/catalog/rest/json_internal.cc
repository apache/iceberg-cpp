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

#include "iceberg/json_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
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

using MapType = std::unordered_map<std::string, std::string>;

/// Helper function to convert TableIdentifier to JSON
nlohmann::json ToJson(const TableIdentifier& identifier) {
  nlohmann::json json;
  json[kNamespace] = identifier.ns.levels;
  json[kName] = identifier.name;
  return json;
}

/// Helper function to parse TableIdentifier from JSON
Result<TableIdentifier> TableIdentifierFromJson(const nlohmann::json& json) {
  TableIdentifier identifier;

  ICEBERG_ASSIGN_OR_RAISE(identifier.ns.levels,
                          GetJsonValue<std::vector<std::string>>(json, kNamespace));
  ICEBERG_ASSIGN_OR_RAISE(identifier.name, GetJsonValue<std::string>(json, kName));

  return identifier;
}

}  // namespace

// CreateNamespaceRequest
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

  ICEBERG_ASSIGN_OR_RAISE(auto levels,
                          GetJsonValue<std::vector<std::string>>(json, kNamespace));
  request.namespace_.levels = std::move(levels);

  ICEBERG_ASSIGN_OR_RAISE(request.properties,
                          GetJsonValueOrDefault<MapType>(json, kProperties));

  return request;
}

// UpdateNamespacePropertiesRequest
nlohmann::json ToJson(const UpdateNamespacePropertiesRequest& request) {
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
  ICEBERG_ASSIGN_OR_RAISE(request.updates,
                          GetJsonValueOrDefault<MapType>(json, kUpdates));

  return request;
}

// CreateTableRequest
nlohmann::json ToJson(const CreateTableRequest& request) {
  nlohmann::json json;

  json[kName] = request.name;

  if (!request.location.empty()) {
    json[kLocation] = request.location;
  }

  json[kSchema] = iceberg::ToJson(*request.schema);

  if (request.partition_spec) {
    json[kPartitionSpec] = iceberg::ToJson(*request.partition_spec);
  }

  if (request.write_order) {
    json[kWriteOrder] = iceberg::ToJson(*request.write_order);
  }

  SetOptionalField(json, kStageCreate, request.stage_create);

  if (!request.properties.empty()) {
    json[kProperties] = request.properties;
  }

  return json;
}

Result<CreateTableRequest> CreateTableRequestFromJson(const nlohmann::json& json) {
  CreateTableRequest request;

  ICEBERG_ASSIGN_OR_RAISE(request.name, GetJsonValue<std::string>(json, kName));

  ICEBERG_ASSIGN_OR_RAISE(auto location,
                          GetJsonValueOptional<std::string>(json, kLocation));
  request.location = location.value_or("");

  ICEBERG_ASSIGN_OR_RAISE(auto schema_json, GetJsonValue<nlohmann::json>(json, kSchema));
  ICEBERG_ASSIGN_OR_RAISE(auto schema_ptr, iceberg::SchemaFromJson(schema_json));
  request.schema = std::move(schema_ptr);

  if (json.contains(kPartitionSpec)) {
    ICEBERG_ASSIGN_OR_RAISE(auto partition_spec_json,
                            GetJsonValue<nlohmann::json>(json, kPartitionSpec));
    ICEBERG_ASSIGN_OR_RAISE(
        request.partition_spec,
        iceberg::PartitionSpecFromJson(request.schema, partition_spec_json));
  } else {
    request.partition_spec = nullptr;
  }

  if (json.contains(kWriteOrder)) {
    ICEBERG_ASSIGN_OR_RAISE(auto write_order_json,
                            GetJsonValue<nlohmann::json>(json, kWriteOrder));
    ICEBERG_ASSIGN_OR_RAISE(request.write_order,
                            iceberg::SortOrderFromJson(write_order_json));
  } else {
    request.write_order = nullptr;
  }

  ICEBERG_ASSIGN_OR_RAISE(request.stage_create,
                          GetJsonValueOptional<bool>(json, kStageCreate));

  ICEBERG_ASSIGN_OR_RAISE(request.properties,
                          GetJsonValueOrDefault<MapType>(json, kProperties));

  return request;
}

// RegisterTableRequest
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

  // Default to false if not present
  ICEBERG_ASSIGN_OR_RAISE(auto overwrite_opt,
                          GetJsonValueOptional<bool>(json, kOverwrite));
  request.overwrite = overwrite_opt.value_or(false);

  return request;
}

// RenameTableRequest
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

  if (result.metadata_location.has_value() && !result.metadata_location->empty()) {
    json[kMetadataLocation] = result.metadata_location.value();
  }

  json[kMetadata] = iceberg::ToJson(*result.metadata);

  if (!result.config.empty()) {
    json[kConfig] = result.config;
  }

  return json;
}

Result<LoadTableResult> LoadTableResultFromJson(const nlohmann::json& json) {
  LoadTableResult result;

  ICEBERG_ASSIGN_OR_RAISE(auto metadata_location,
                          GetJsonValueOptional<std::string>(json, kMetadataLocation));
  result.metadata_location = metadata_location;

  ICEBERG_ASSIGN_OR_RAISE(auto metadata_json,
                          GetJsonValue<nlohmann::json>(json, kMetadata));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_ptr,
                          iceberg::TableMetadataFromJson(metadata_json));
  result.metadata = std::move(metadata_ptr);

  ICEBERG_ASSIGN_OR_RAISE(result.config, GetJsonValueOrDefault<MapType>(json, kConfig));

  return result;
}

// ListNamespacesResponse
nlohmann::json ToJson(const ListNamespacesResponse& response) {
  nlohmann::json json;

  if (!response.next_page_token.empty()) {
    json[kNextPageToken] = response.next_page_token;
  }

  nlohmann::json namespaces = nlohmann::json::array();
  for (const auto& ns : response.namespaces) {
    namespaces.push_back(ns.levels);
  }
  json[kNamespaces] = std::move(namespaces);

  return json;
}

Result<ListNamespacesResponse> ListNamespacesResponseFromJson(
    const nlohmann::json& json) {
  ListNamespacesResponse response;

  ICEBERG_ASSIGN_OR_RAISE(auto next_page_token,
                          GetJsonValueOptional<std::string>(json, kNextPageToken));
  response.next_page_token = next_page_token.value_or("");

  ICEBERG_ASSIGN_OR_RAISE(
      auto namespace_levels,
      GetJsonValue<std::vector<std::vector<std::string>>>(json, kNamespaces));
  response.namespaces.reserve(namespace_levels.size());
  for (auto& levels : namespace_levels) {
    response.namespaces.push_back(Namespace{.levels = std::move(levels)});
  }

  return response;
}

// CreateNamespaceResponse
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

  ICEBERG_ASSIGN_OR_RAISE(auto levels,
                          GetJsonValue<std::vector<std::string>>(json, kNamespace));
  response.namespace_.levels = std::move(levels);

  ICEBERG_ASSIGN_OR_RAISE(response.properties,
                          GetJsonValueOrDefault<MapType>(json, kProperties));

  return response;
}

// GetNamespaceResponse
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

  ICEBERG_ASSIGN_OR_RAISE(auto levels,
                          GetJsonValue<std::vector<std::string>>(json, kNamespace));
  response.namespace_.levels = std::move(levels);

  ICEBERG_ASSIGN_OR_RAISE(response.properties,
                          GetJsonValueOrDefault<MapType>(json, kProperties));

  return response;
}

// UpdateNamespacePropertiesResponse
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

// ListTablesResponse
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

  ICEBERG_ASSIGN_OR_RAISE(auto next_page_token,
                          GetJsonValueOptional<std::string>(json, kNextPageToken));
  response.next_page_token = next_page_token.value_or("");

  ICEBERG_ASSIGN_OR_RAISE(auto identifiers_json,
                          GetJsonValue<nlohmann::json>(json, kIdentifiers));

  for (const auto& id_json : identifiers_json) {
    ICEBERG_ASSIGN_OR_RAISE(auto identifier, TableIdentifierFromJson(id_json));
    response.identifiers.push_back(std::move(identifier));
  }

  return response;
}

}  // namespace iceberg::rest
