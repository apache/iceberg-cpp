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
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_identifier.h"

namespace iceberg::rest {

/// \brief Request to create a namespace.
/// \details Corresponds to **POST /v1/{prefix}/namespaces**.
/// Allows creating a new namespace with optional properties.
struct ICEBERG_REST_EXPORT CreateNamespaceRequest {
  Namespace namespace_;  // required
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Request to update or remove namespace properties.
/// \details Corresponds to **POST /v1/{prefix}/namespaces/{namespace}/properties**.
/// Allows setting and/or removing namespace properties in a single call.
/// Properties not listed in this request are left unchanged.
struct ICEBERG_REST_EXPORT UpdateNamespaceRequest {
  std::vector<std::string> removals;
  std::unordered_map<std::string, std::string> updates;
};

/// \brief Request to create a table.
/// \details Corresponds to **POST /v1/{prefix}/namespaces/{namespace}/tables**.
/// If `stage_create` is false, the table is created immediately.
/// If `stage_create` is true, metadata is prepared and returned without committing,
/// allowing a later transaction commit via the table commit endpoint.
struct ICEBERG_REST_EXPORT CreateTableRequest {
  std::string name;  // required
  std::string location;
  std::shared_ptr<Schema> schema;  // required
  std::shared_ptr<PartitionSpec> partition_spec;
  std::shared_ptr<SortOrder> write_order;
  std::optional<bool> stage_create;
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Request to register an existing table.
/// \details Corresponds to **POST /v1/{prefix}/namespaces/{namespace}/register**.
/// Registers an existing table using a given metadata file location.
struct ICEBERG_REST_EXPORT RegisterTableRequest {
  std::string name;               // required
  std::string metadata_location;  // required
  std::optional<bool> overwrite;
};

/// \brief Request to rename a table.
/// \details Corresponds to **POST /v1/{prefix}/tables/rename**.
/// Moves or renames a table from the source identifier to the destination identifier.
struct ICEBERG_REST_EXPORT RenameTableRequest {
  TableIdentifier source;       // required
  TableIdentifier destination;  // required
};

}  // namespace iceberg::rest
