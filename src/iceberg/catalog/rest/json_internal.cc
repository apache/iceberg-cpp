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
#include <vector>

#include <nlohmann/json.hpp>

#include "iceberg/json_internal.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

// REST API JSON field constants
constexpr std::string_view kNamespaces = "namespaces";
constexpr std::string_view kRemovals = "removals";
constexpr std::string_view kUpdates = "updates";
constexpr std::string_view kUpdated = "updated";
constexpr std::string_view kRemoved = "removed";
constexpr std::string_view kMissing = "missing";
constexpr std::string_view kIdentifiers = "identifiers";
constexpr std::string_view kSource = "source";
constexpr std::string_view kDestination = "destination";
constexpr std::string_view kMetadataLocation = "metadata-location";
constexpr std::string_view kMetadata = "metadata";
constexpr std::string_view kConfig = "config";
constexpr std::string_view kName = "name";
constexpr std::string_view kLocation = "location";
constexpr std::string_view kSchema = "schema";
constexpr std::string_view kPartitionSpec = "partition-spec";
constexpr std::string_view kWriteOrder = "write-order";
constexpr std::string_view kStageCreate = "stage-create";
constexpr std::string_view kProperties = "properties";
constexpr std::string_view kOverwrite = "overwrite";
constexpr std::string_view kNamespace = "namespace";

/// Helper function to convert TableIdentifier to JSON
nlohmann::json TableIdentifierToJson(const TableIdentifier& identifier) {
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

nlohmann::json ToJson(const ListNamespaceResponse& response) {
  nlohmann::json json;
  json[kNamespaces] = response.namespaces;
  return json;
}

Result<ListNamespaceResponse> ListNamespaceResponseFromJson(const nlohmann::json& json) {
  ListNamespaceResponse response;

  ICEBERG_ASSIGN_OR_RAISE(
      response.namespaces,
      GetJsonValue<std::vector<std::vector<std::string>>>(json, kNamespaces));
  return response;
}

nlohmann::json ToJson(const CreateNamespaceRequest& request) {
  nlohmann::json json;
  json[kNamespaces] = request.namespaces;
  SetOptionalField(json, kProperties, request.properties);
  return json;
}

Result<CreateNamespaceRequest> CreateNamespaceRequestFromJson(
    const nlohmann::json& json) {
  CreateNamespaceRequest request;

  ICEBERG_ASSIGN_OR_RAISE(request.namespaces,
                          GetJsonValue<std::vector<std::string>>(json, kNamespaces));
  using MapType = std::unordered_map<std::string, std::string>;
  ICEBERG_ASSIGN_OR_RAISE(request.properties,
                          GetJsonValueOptional<MapType>(json, kProperties));

  return request;
}

nlohmann::json ToJson(const CreateNamespaceResponse& response) {
  nlohmann::json json;
  json[kNamespaces] = response.namespaces;
  SetOptionalField(json, kProperties, response.properties);
  return json;
}

Result<CreateNamespaceResponse> CreateNamespaceResponseFromJson(
    const nlohmann::json& json) {
  CreateNamespaceResponse response;

  ICEBERG_ASSIGN_OR_RAISE(response.namespaces,
                          GetJsonValue<std::vector<std::string>>(json, kNamespaces));
  using MapType = std::unordered_map<std::string, std::string>;
  ICEBERG_ASSIGN_OR_RAISE(response.properties,
                          GetJsonValueOptional<MapType>(json, kProperties));

  return response;
}

nlohmann::json ToJson(const GetNamespacePropertiesResponse& response) {
  nlohmann::json json;
  json[kNamespaces] = response.namespaces;
  json[kProperties] = response.properties;
  return json;
}

Result<GetNamespacePropertiesResponse> GetNamespacePropertiesResponseFromJson(
    const nlohmann::json& json) {
  GetNamespacePropertiesResponse response;

  ICEBERG_ASSIGN_OR_RAISE(response.namespaces,
                          GetJsonValue<std::vector<std::string>>(json, kNamespaces));
  using MapType = std::unordered_map<std::string, std::string>;
  ICEBERG_ASSIGN_OR_RAISE(response.properties, GetJsonValue<MapType>(json, kProperties));

  return response;
}

nlohmann::json ToJson(const UpdateNamespacePropsRequest& request) {
  nlohmann::json json;
  SetOptionalField(json, kRemovals, request.removals);
  SetOptionalField(json, kUpdates, request.updates);
  return json;
}

Result<UpdateNamespacePropsRequest> UpdateNamespacePropsRequestFromJson(
    const nlohmann::json& json) {
  UpdateNamespacePropsRequest request;

  ICEBERG_ASSIGN_OR_RAISE(
      request.removals, GetJsonValueOptional<std::vector<std::string>>(json, kRemovals));
  using MapType = std::unordered_map<std::string, std::string>;
  ICEBERG_ASSIGN_OR_RAISE(request.updates, GetJsonValueOptional<MapType>(json, kUpdates));

  return request;
}

nlohmann::json ToJson(const UpdateNamespacePropsResponse& response) {
  nlohmann::json json;
  json[kUpdated] = response.updated;
  json[kRemoved] = response.removed;
  SetOptionalField(json, kMissing, response.missing);
  return json;
}

Result<UpdateNamespacePropsResponse> UpdateNamespacePropsResponseFromJson(
    const nlohmann::json& json) {
  UpdateNamespacePropsResponse response;

  ICEBERG_ASSIGN_OR_RAISE(response.updated,
                          GetJsonValue<std::vector<std::string>>(json, kUpdated));
  ICEBERG_ASSIGN_OR_RAISE(response.removed,
                          GetJsonValue<std::vector<std::string>>(json, kRemoved));
  ICEBERG_ASSIGN_OR_RAISE(response.missing,
                          GetJsonValueOptional<std::vector<std::string>>(json, kMissing));

  return response;
}

nlohmann::json ToJson(const ListTableResponse& response) {
  nlohmann::json json;

  nlohmann::json identifiers_json = nlohmann::json::array();
  for (const auto& identifier : response.identifiers) {
    identifiers_json.push_back(TableIdentifierToJson(identifier));
  }
  json[kIdentifiers] = identifiers_json;
  return json;
}

Result<ListTableResponse> ListTableResponseFromJson(const nlohmann::json& json) {
  ListTableResponse response;

  for (const auto& id_json : json[kIdentifiers]) {
    ICEBERG_ASSIGN_OR_RAISE(auto identifier, TableIdentifierFromJson(id_json));
    response.identifiers.push_back(std::move(identifier));
  }
  return response;
}

nlohmann::json ToJson(const CreateTableRequest& request) {
  nlohmann::json json;
  json[kName] = request.name;
  SetOptionalField(json, kLocation, request.location);
  json[kSchema] = ToJson(*request.schema);

  if (request.partition_spec) {
    json[kPartitionSpec] = ToJson(*request.partition_spec);
  }

  if (request.write_order) {
    json[kWriteOrder] = ToJson(*request.write_order);
  }

  SetOptionalField(json, kStageCreate, request.stage_create);
  SetOptionalField(json, kProperties, request.properties);
  return json;
}

Result<CreateTableRequest> CreateTableRequestFromJson(const nlohmann::json& json) {
  CreateTableRequest request;

  ICEBERG_ASSIGN_OR_RAISE(request.name, GetJsonValue<std::string>(json, kName));

  ICEBERG_ASSIGN_OR_RAISE(request.location,
                          GetJsonValueOptional<std::string>(json, kLocation));

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

  using MapType = std::unordered_map<std::string, std::string>;
  ICEBERG_ASSIGN_OR_RAISE(request.properties,
                          GetJsonValueOptional<MapType>(json, kProperties));

  return request;
}

nlohmann::json ToJson(const RegisterTableRequest& request) {
  nlohmann::json json;
  json[kName] = request.name;
  json[kMetadataLocation] = request.metadata_location;
  SetOptionalField(json, kOverwrite, request.overwrite);
  return json;
}

Result<RegisterTableRequest> RegisterTableRequestFromJson(const nlohmann::json& json) {
  RegisterTableRequest request;

  ICEBERG_ASSIGN_OR_RAISE(request.name, GetJsonValue<std::string>(json, kName));
  ICEBERG_ASSIGN_OR_RAISE(request.metadata_location,
                          GetJsonValue<std::string>(json, kMetadataLocation));
  ICEBERG_ASSIGN_OR_RAISE(request.overwrite,
                          GetJsonValueOptional<bool>(json, kOverwrite));

  return request;
}

nlohmann::json ToJson(const RenameTableRequest& request) {
  nlohmann::json json;
  json[kSource] = TableIdentifierToJson(request.source);
  json[kDestination] = TableIdentifierToJson(request.destination);
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

nlohmann::json ToJson(const LoadTableResponse& response) {
  nlohmann::json json;

  SetOptionalField(json, kMetadataLocation, response.metadata_location);
  json[kMetadata] = iceberg::ToJson(response.metadata);
  SetOptionalField(json, kConfig, response.config);

  return json;
}

Result<LoadTableResponse> LoadTableResponseFromJson(const nlohmann::json& json) {
  LoadTableResponse response;

  ICEBERG_ASSIGN_OR_RAISE(response.metadata_location,
                          GetJsonValueOptional<std::string>(json, kMetadataLocation));

  ICEBERG_ASSIGN_OR_RAISE(auto metadata_json,
                          GetJsonValue<nlohmann::json>(json, kMetadata));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_ptr,
                          iceberg::TableMetadataFromJson(metadata_json));
  response.metadata = std::move(*metadata_ptr);

  using MapType = std::unordered_map<std::string, std::string>;
  ICEBERG_ASSIGN_OR_RAISE(response.config, GetJsonValueOptional<MapType>(json, kConfig));

  return response;
}

}  // namespace iceberg::rest
