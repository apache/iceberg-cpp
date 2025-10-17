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

#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"

namespace iceberg::rest {

struct ListNamespaceResponse {
  std::vector<std::vector<std::string>> namespaces;
};

struct CreateNamespaceRequest {
  std::vector<std::string> namespaces;
  std::optional<std::unordered_map<std::string, std::string>> properties;
};

struct CreateNamespaceResponse {
  std::vector<std::string> namespaces;
  std::optional<std::unordered_map<std::string, std::string>> properties;
};

struct GetNamespacePropertiesResponse {
  std::vector<std::string> namespaces;
  std::unordered_map<std::string, std::string> properties;
};

struct UpdateNamespacePropsRequest {
  std::optional<std::vector<std::string>> removals;
  std::optional<std::unordered_map<std::string, std::string>> updates;
};

struct UpdateNamespacePropsResponse {
  std::vector<std::string> updated;
  std::vector<std::string> removed;
  std::optional<std::vector<std::string>> missing;
};

struct ListTableResponse {
  std::vector<TableIdentifier> identifiers;
};

struct CreateTableRequest {
  std::string name;
  std::optional<std::string> location;
  std::shared_ptr<Schema> schema;
  std::shared_ptr<PartitionSpec> partition_spec;  // optional
  std::shared_ptr<SortOrder> write_order;         // optional
  std::optional<bool> stage_create;
  std::optional<std::unordered_map<std::string, std::string>> properties;
};

struct RegisterTableRequest {
  std::string name;
  std::string metadata_location;
  std::optional<bool> overwrite;
};

struct RenameTableRequest {
  TableIdentifier source;
  TableIdentifier destination;
};

// This is also used for CreateTableResponse and RegisterTableResponse
struct LoadTableResponse {
  std::optional<std::string> metadata_location;
  TableMetadata metadata;
  std::optional<std::unordered_map<std::string, std::string>> config;
};

// TODO(Li Feiyang): Add UpdateTable request and response

}  // namespace iceberg::rest
