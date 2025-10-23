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
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"

/// \file iceberg/catalog/rest/types.h
/// Request and response types for Iceberg REST Catalog API.

namespace iceberg::rest {

/// \brief Request to create a namespace.
struct ICEBERG_REST_EXPORT CreateNamespaceRequest {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Update or delete namespace properties request.
struct ICEBERG_REST_EXPORT UpdateNamespacePropertiesRequest {
  std::vector<std::string> removals;
  std::unordered_map<std::string, std::string> updates;
};

/// \brief Request to create a table.
struct ICEBERG_REST_EXPORT CreateTableRequest {
  std::string name;  // required
  std::string location;
  std::shared_ptr<Schema> schema;  // required
  std::shared_ptr<PartitionSpec> partition_spec;
  std::shared_ptr<SortOrder> write_order;
  std::optional<bool> stage_create;
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Request to register a table.
struct ICEBERG_REST_EXPORT RegisterTableRequest {
  std::string name;               // required
  std::string metadata_location;  // required
  bool overwrite = false;
};

/// \brief Request to rename a table.
struct ICEBERG_REST_EXPORT RenameTableRequest {
  TableIdentifier source;       // required
  TableIdentifier destination;  // required
};

/// \brief An opaque token that allows clients to make use of pagination for list APIs.
using PageToken = std::string;

/// \brief Result body for table create/load/register APIs.
struct ICEBERG_REST_EXPORT LoadTableResult {
  std::optional<std::string> metadata_location;
  std::shared_ptr<TableMetadata> metadata;  // required  // required
  std::unordered_map<std::string, std::string> config;
  // TODO(Li Feiyang): Add std::shared_ptr<StorageCredential> storage_credential;
};

/// \brief Alias of LoadTableResult used as the body of CreateTableResponse
using CreateTableResponse = LoadTableResult;

/// \brief Alias of LoadTableResult used as the body of LoadTableResponse
using LoadTableResponse = LoadTableResult;

/// \brief Response body for listing namespaces.
struct ICEBERG_REST_EXPORT ListNamespacesResponse {
  PageToken next_page_token;
  std::vector<Namespace> namespaces;
};

/// \brief Response body after creating a namespace.
struct ICEBERG_REST_EXPORT CreateNamespaceResponse {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Response body for loading namespace properties.
struct ICEBERG_REST_EXPORT GetNamespaceResponse {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Response body after updating namespace properties.
struct ICEBERG_REST_EXPORT UpdateNamespacePropertiesResponse {
  std::vector<std::string> updated;  // required
  std::vector<std::string> removed;  // required
  std::vector<std::string> missing;
};

/// \brief Response body for listing tables in a namespace.
struct ICEBERG_REST_EXPORT ListTablesResponse {
  PageToken next_page_token;
  std::vector<TableIdentifier> identifiers;
};

}  // namespace iceberg::rest
