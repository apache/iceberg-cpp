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
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
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
  std::vector<Endpoint> endpoints;

  /// \brief Validates the CatalogConfig.
  Status Validate() const { return {}; }

  bool operator==(const CatalogConfig&) const = default;
};

/// \brief JSON error payload returned in a response with further details on the error.
struct ICEBERG_REST_EXPORT ErrorResponse {
  uint32_t code;        // required
  std::string type;     // required
  std::string message;  // required
  std::vector<std::string> stack;

  /// \brief Validates the ErrorResponse.
  Status Validate() const {
    if (message.empty() || type.empty()) {
      return Invalid("Invalid error response: missing required fields");
    }

    if (code < 400 || code > 600) {
      return Invalid("Invalid error response: code {} is out of range [400, 600]", code);
    }

    // stack is optional, no validation needed
    return {};
  }

  bool operator==(const ErrorResponse&) const = default;
};

/// \brief Request to create a namespace.
struct ICEBERG_REST_EXPORT CreateNamespaceRequest {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;

  /// \brief Validates the CreateNamespaceRequest.
  Status Validate() const { return {}; }

  bool operator==(const CreateNamespaceRequest&) const = default;
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

  bool operator==(const UpdateNamespacePropertiesRequest&) const = default;
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

  bool operator==(const RegisterTableRequest&) const = default;
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

  bool operator==(const RenameTableRequest&) const = default;
};

/// \brief Request to create a table.
struct ICEBERG_REST_EXPORT CreateTableRequest {
  std::string name;  // required
  std::string location;
  std::shared_ptr<Schema> schema;  // required
  std::shared_ptr<PartitionSpec> partition_spec;
  std::shared_ptr<SortOrder> write_order;
  bool stage_create = false;
  std::unordered_map<std::string, std::string> properties;

  /// \brief Validates the CreateTableRequest.
  Status Validate() const {
    if (name.empty()) {
      return Invalid("Missing table name");
    }
    if (!schema) {
      return Invalid("Missing schema");
    }
    return {};
  }

  bool operator==(const CreateTableRequest& other) const;
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

  bool operator==(const LoadTableResult& other) const;
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

  bool operator==(const ListNamespacesResponse&) const = default;
};

/// \brief Response body after creating a namespace.
struct ICEBERG_REST_EXPORT CreateNamespaceResponse {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;

  /// \brief Validates the CreateNamespaceResponse.
  Status Validate() const { return {}; }

  bool operator==(const CreateNamespaceResponse&) const = default;
};

/// \brief Response body for loading namespace properties.
struct ICEBERG_REST_EXPORT GetNamespaceResponse {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;

  /// \brief Validates the GetNamespaceResponse.
  Status Validate() const { return {}; }

  bool operator==(const GetNamespaceResponse&) const = default;
};

/// \brief Response body after updating namespace properties.
struct ICEBERG_REST_EXPORT UpdateNamespacePropertiesResponse {
  std::vector<std::string> updated;  // required
  std::vector<std::string> removed;  // required
  std::vector<std::string> missing;

  /// \brief Validates the UpdateNamespacePropertiesResponse.
  Status Validate() const { return {}; }

  bool operator==(const UpdateNamespacePropertiesResponse&) const = default;
};

/// \brief Response body for listing tables in a namespace.
struct ICEBERG_REST_EXPORT ListTablesResponse {
  PageToken next_page_token;
  std::vector<TableIdentifier> identifiers;

  /// \brief Validates the ListTablesResponse.
  Status Validate() const { return {}; }

  bool operator==(const ListTablesResponse&) const = default;
};

}  // namespace iceberg::rest
