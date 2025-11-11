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

#include <algorithm>
#include <format>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/macros.h"

/// \file iceberg/catalog/rest/types.h
/// Request and response types for Iceberg REST Catalog API.

namespace iceberg::rest {

/// \brief Server-provided configuration for the catalog.
struct ICEBERG_REST_EXPORT CatalogConfig {
  std::unordered_map<std::string, std::string> defaults;   // required
  std::unordered_map<std::string, std::string> overrides;  // required
  std::vector<std::string> endpoints;

  /// \brief Validates the CatalogConfig.
  Status Validate() const {
    // TODO(Li Feiyang): Add an invalidEndpoint test that validates endpoint format.
    // See:
    // https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/rest/responses/TestConfigResponseParser.java#L164
    // for reference.
    return {};
  }
};

/// \brief JSON error payload returned in a response with further details on the error.
struct ICEBERG_REST_EXPORT ErrorModel {
  std::string message;  // required
  std::string type;     // required
  uint32_t code;        // required
  std::vector<std::string> stack;

  /// \brief Validates the ErrorModel.
  Status Validate() const {
    if (message.empty() || type.empty()) {
      return Invalid("Invalid error model: missing required fields");
    }

    if (code < 400 || code > 600) {
      return Invalid("Invalid error model: code {} is out of range [400, 600]", code);
    }

    // stack is optional, no validation needed
    return {};
  }
};

/// \brief Error response body returned in a response.
struct ICEBERG_REST_EXPORT ErrorResponse {
  ErrorModel error;  // required

  /// \brief Validates the ErrorResponse.
  // We don't validate the error field because ErrorModel::Validate has been called in the
  // FromJson.
  Status Validate() const { return {}; }
};

/// \brief Request to create a namespace.
struct ICEBERG_REST_EXPORT CreateNamespaceRequest {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;

  /// \brief Validates the CreateNamespaceRequest.
  Status Validate() const { return {}; }
};

/// \brief Update or delete namespace properties request.
struct ICEBERG_REST_EXPORT UpdateNamespacePropertiesRequest {
  std::vector<std::string> removals;
  std::unordered_map<std::string, std::string> updates;

  /// \brief Validates the UpdateNamespacePropertiesRequest.
  Status Validate() const {
    for (const auto& key : removals) {
      if (updates.contains(key)) {
        return Invalid("Duplicate key to update and remove: {}", key);
      }
    }
    return {};
  }
};

/// \brief Request to register a table.
struct ICEBERG_REST_EXPORT RegisterTableRequest {
  std::string name;               // required
  std::string metadata_location;  // required
  bool overwrite = false;

  /// \brief Validates the RegisterTableRequest.
  Status Validate() const {
    if (name.empty()) {
      return Invalid("Missing table name");
    }

    if (metadata_location.empty()) {
      return Invalid("Empty metadata location");
    }

    return {};
  }
};

/// \brief Request to rename a table.
struct ICEBERG_REST_EXPORT RenameTableRequest {
  TableIdentifier source;       // required
  TableIdentifier destination;  // required

  /// \brief Validates the RenameTableRequest.
  Status Validate() const {
    ICEBERG_RETURN_UNEXPECTED(source.Validate());
    ICEBERG_RETURN_UNEXPECTED(destination.Validate());
    return {};
  }
};

/// \brief An opaque token that allows clients to make use of pagination for list APIs.
using PageToken = std::string;

/// \brief Result body for table create/load/register APIs.
struct ICEBERG_REST_EXPORT LoadTableResult {
  std::string metadata_location;
  std::shared_ptr<TableMetadata> metadata;  // required
  std::unordered_map<std::string, std::string> config;
  // TODO(Li Feiyang): Add std::shared_ptr<StorageCredential> storage_credential;

  /// \brief Validates the LoadTableResult.
  Status Validate() const {
    if (!metadata) {
      return Invalid("Invalid metadata: null");
    }
    return {};
  }
};

/// \brief Alias of LoadTableResult used as the body of CreateTableResponse
using CreateTableResponse = LoadTableResult;

/// \brief Alias of LoadTableResult used as the body of LoadTableResponse
using LoadTableResponse = LoadTableResult;

/// \brief Response body for listing namespaces.
struct ICEBERG_REST_EXPORT ListNamespacesResponse {
  PageToken next_page_token;
  std::vector<Namespace> namespaces;

  /// \brief Validates the ListNamespacesResponse.
  Status Validate() const { return {}; }
};

/// \brief Response body after creating a namespace.
struct ICEBERG_REST_EXPORT CreateNamespaceResponse {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;

  /// \brief Validates the CreateNamespaceResponse.
  Status Validate() const { return {}; }
};

/// \brief Response body for loading namespace properties.
struct ICEBERG_REST_EXPORT GetNamespaceResponse {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;

  /// \brief Validates the GetNamespaceResponse.
  Status Validate() const { return {}; }
};

/// \brief Response body after updating namespace properties.
struct ICEBERG_REST_EXPORT UpdateNamespacePropertiesResponse {
  std::vector<std::string> updated;  // required
  std::vector<std::string> removed;  // required
  std::vector<std::string> missing;

  /// \brief Validates the UpdateNamespacePropertiesResponse.
  Status Validate() const { return {}; }
};

/// \brief Response body for listing tables in a namespace.
struct ICEBERG_REST_EXPORT ListTablesResponse {
  PageToken next_page_token;
  std::vector<TableIdentifier> identifiers;

  /// \brief Validates the ListTablesResponse.
  Status Validate() const { return {}; }
};

}  // namespace iceberg::rest
