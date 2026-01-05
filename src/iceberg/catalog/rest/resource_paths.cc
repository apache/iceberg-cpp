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

#include "iceberg/catalog/rest/resource_paths.h"

#include <format>

#include "iceberg/catalog/rest/rest_util.h"
#include "iceberg/table_identifier.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

Result<std::unique_ptr<ResourcePaths>> ResourcePaths::Make(std::string base_uri,
                                                           const std::string& prefix) {
  if (base_uri.empty()) {
    return InvalidArgument("Base URI is empty");
  }
  return std::unique_ptr<ResourcePaths>(new ResourcePaths(std::move(base_uri), prefix));
}

ResourcePaths::ResourcePaths(std::string base_uri, const std::string& prefix)
    : base_uri_(std::move(base_uri)), prefix_(prefix.empty() ? "" : (prefix + "/")) {}

Result<std::string> ResourcePaths::Config() const {
  return std::format("{}/v1/config", base_uri_);
}

Result<std::string> ResourcePaths::OAuth2Tokens() const {
  return std::format("{}/v1/{}oauth/tokens", base_uri_, prefix_);
}

Result<std::string> ResourcePaths::Namespaces() const {
  return std::format("{}/v1/{}namespaces", base_uri_, prefix_);
}

Result<std::string> ResourcePaths::Namespace_(const Namespace& ns) const {
  ICEBERG_ASSIGN_OR_RAISE(std::string encoded_namespace, EncodeNamespace(ns));
  return std::format("{}/v1/{}namespaces/{}", base_uri_, prefix_, encoded_namespace);
}

Result<std::string> ResourcePaths::NamespaceProperties(const Namespace& ns) const {
  ICEBERG_ASSIGN_OR_RAISE(std::string encoded_namespace, EncodeNamespace(ns));
  return std::format("{}/v1/{}namespaces/{}/properties", base_uri_, prefix_,
                     encoded_namespace);
}

Result<std::string> ResourcePaths::Tables(const Namespace& ns) const {
  ICEBERG_ASSIGN_OR_RAISE(std::string encoded_namespace, EncodeNamespace(ns));
  return std::format("{}/v1/{}namespaces/{}/tables", base_uri_, prefix_,
                     encoded_namespace);
}

Result<std::string> ResourcePaths::Table(const TableIdentifier& ident) const {
  ICEBERG_ASSIGN_OR_RAISE(std::string encoded_namespace, EncodeNamespace(ident.ns));
  ICEBERG_ASSIGN_OR_RAISE(std::string encoded_table_name, EncodeString(ident.name));
  return std::format("{}/v1/{}namespaces/{}/tables/{}", base_uri_, prefix_,
                     encoded_namespace, encoded_table_name);
}

Result<std::string> ResourcePaths::Register(const Namespace& ns) const {
  ICEBERG_ASSIGN_OR_RAISE(std::string encoded_namespace, EncodeNamespace(ns));
  return std::format("{}/v1/{}namespaces/{}/register", base_uri_, prefix_,
                     encoded_namespace);
}

Result<std::string> ResourcePaths::Rename() const {
  return std::format("{}/v1/{}tables/rename", base_uri_, prefix_);
}

Result<std::string> ResourcePaths::Metrics(const TableIdentifier& ident) const {
  ICEBERG_ASSIGN_OR_RAISE(std::string encoded_namespace, EncodeNamespace(ident.ns));
  ICEBERG_ASSIGN_OR_RAISE(std::string encoded_table_name, EncodeString(ident.name));
  return std::format("{}/v1/{}namespaces/{}/tables/{}/metrics", base_uri_, prefix_,
                     encoded_namespace, encoded_table_name);
}

Result<std::string> ResourcePaths::Credentials(const TableIdentifier& ident) const {
  ICEBERG_ASSIGN_OR_RAISE(std::string encoded_namespace, EncodeNamespace(ident.ns));
  ICEBERG_ASSIGN_OR_RAISE(std::string encoded_table_name, EncodeString(ident.name));
  return std::format("{}/v1/{}namespaces/{}/tables/{}/credentials", base_uri_, prefix_,
                     encoded_namespace, encoded_table_name);
}

Result<std::string> ResourcePaths::CommitTransaction() const {
  return std::format("{}/v1/{}transactions/commit", base_uri_, prefix_);
}

}  // namespace iceberg::rest
