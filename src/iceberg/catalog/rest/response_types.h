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

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"

namespace iceberg::rest {

/// \brief An opaque token that allows clients to make use of pagination for list APIs.
using PageToken = std::string;

/// \brief Result body for table create/load/register APIs.
/// \details Matches **components/schemas/LoadTableResult** in the REST spec.
struct ICEBERG_REST_EXPORT LoadTableResult {
  std::optional<std::string> metadata_location;
  TableMetadata metadata;  // required
  std::unordered_map<std::string, std::string> config;
  // TODO(Li Feiyang): Add std::vector<StorageCredential> storage_credentials;
};

/// \brief Alias of LoadTableResult used as the body of CreateTableResponse
using CreateTableResponse = LoadTableResult;

/// \brief Alias of LoadTableResult used as the body of LoadTableResponse
using LoadTableResponse = LoadTableResult;

/// \brief Alias of LoadTableResult used as the body of RegisterTableResponse
using RegisterTableResponse = LoadTableResult;

/// \brief Response body for listing namespaces.
/// Contains all namespaces and an optional pagination token.
struct ICEBERG_REST_EXPORT ListNamespacesResponse {
  std::optional<PageToken> next_page_token;
  std::vector<Namespace> namespaces;
};

/// \brief Response body after creating a namespace.
/// \details Contains the created namespace and its resolved properties.
struct ICEBERG_REST_EXPORT CreateNamespaceResponse {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Response body for loading namespace properties.
/// \details Contains stored properties, may be null if unsupported by the server.
struct ICEBERG_REST_EXPORT GetNamespaceResponse {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Response body after updating namespace properties.
/// \details Lists keys that were updated, removed, or missing.
struct ICEBERG_REST_EXPORT UpdateNamespacePropertiesResponse {
  std::vector<std::string> updated;  // required
  std::vector<std::string> removed;  // required
  std::vector<std::string> missing;
};

/// \brief Response body for listing tables in a namespace.
/// \details Contains all table identifiers and an optional pagination token.
struct ICEBERG_REST_EXPORT ListTablesResponse {
  std::optional<PageToken> next_page_token;
  std::vector<TableIdentifier> identifiers;
};

}  // namespace iceberg::rest
