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

#include "iceberg/catalog/rest/config.h"
#include "iceberg/catalog/rest/endpoint_util.h"
#include "iceberg/result.h"

namespace iceberg::rest {

Result<std::unique_ptr<ResourcePaths>> ResourcePaths::Make(
    const RestCatalogConfig& config) {
  // Validate and extract URI
  auto it = config.configs().find(std::string(RestCatalogConfig::kUri));
  if (it == config.configs().end() || it->second.empty()) {
    return InvalidArgument("Rest catalog configuration property 'uri' is required.");
  }

  std::string base_uri = std::string(TrimTrailingSlash(it->second));
  std::string prefix;
  auto prefix_it = config.configs().find("prefix");
  if (prefix_it != config.configs().end() && !prefix_it->second.empty()) {
    prefix = prefix_it->second;
  }

  return std::unique_ptr<ResourcePaths>(
      new ResourcePaths(std::move(base_uri), std::move(prefix)));
}

ResourcePaths::ResourcePaths(std::string base_uri, std::string prefix)
    : base_uri_(std::move(base_uri)), prefix_(std::move(prefix)) {}

std::string ResourcePaths::BuildPath(std::string_view path) const {
  if (prefix_.empty()) {
    return std::format("{}/v1/{}", base_uri_, path);
  }
  return std::format("{}/v1/{}/{}", base_uri_, prefix_, path);
}

std::string ResourcePaths::V1Config() const {
  return std::format("{}/v1/config", base_uri_);
}

std::string ResourcePaths::V1OAuth2Tokens() const {
  return std::format("{}/v1/oauth/tokens", base_uri_);
}

std::string ResourcePaths::V1Namespaces() const { return BuildPath("namespaces"); }

std::string ResourcePaths::V1Namespace(const Namespace& ns) const {
  return BuildPath(std::format("namespaces/{}", EncodeNamespaceForUrl(ns)));
}

std::string ResourcePaths::V1NamespaceProperties(const Namespace& ns) const {
  return BuildPath(std::format("namespaces/{}/properties", EncodeNamespaceForUrl(ns)));
}

std::string ResourcePaths::V1Tables(const Namespace& ns) const {
  return BuildPath(std::format("namespaces/{}/tables", EncodeNamespaceForUrl(ns)));
}

std::string ResourcePaths::V1Table(const TableIdentifier& table) const {
  return BuildPath(std::format("namespaces/{}/tables/{}", EncodeNamespaceForUrl(table.ns),
                               table.name));
}

std::string ResourcePaths::V1RegisterTable(const Namespace& ns) const {
  return BuildPath(std::format("namespaces/{}/register", EncodeNamespaceForUrl(ns)));
}

std::string ResourcePaths::V1RenameTable() const { return BuildPath("tables/rename"); }

std::string ResourcePaths::V1TableMetrics(const TableIdentifier& table) const {
  return BuildPath(std::format("namespaces/{}/tables/{}/metrics",
                               EncodeNamespaceForUrl(table.ns), table.name));
}

std::string ResourcePaths::V1TableCredentials(const TableIdentifier& table) const {
  return BuildPath(std::format("namespaces/{}/tables/{}/credentials",
                               EncodeNamespaceForUrl(table.ns), table.name));
}

std::string ResourcePaths::V1TableScanPlan(const TableIdentifier& table) const {
  return BuildPath(std::format("namespaces/{}/tables/{}/plan",
                               EncodeNamespaceForUrl(table.ns), table.name));
}

std::string ResourcePaths::V1TableScanPlanResult(const TableIdentifier& table,
                                                 const std::string& plan_id) const {
  return BuildPath(std::format("namespaces/{}/tables/{}/plan/{}",
                               EncodeNamespaceForUrl(table.ns), table.name, plan_id));
}

std::string ResourcePaths::V1TableTasks(const TableIdentifier& table) const {
  return BuildPath(std::format("namespaces/{}/tables/{}/tasks",
                               EncodeNamespaceForUrl(table.ns), table.name));
}

std::string ResourcePaths::V1TransactionCommit() const {
  return BuildPath("transactions/commit");
}

std::string ResourcePaths::V1Views(const Namespace& ns) const {
  return BuildPath(std::format("namespaces/{}/views", EncodeNamespaceForUrl(ns)));
}

std::string ResourcePaths::V1View(const TableIdentifier& view) const {
  return BuildPath(
      std::format("namespaces/{}/views/{}", EncodeNamespaceForUrl(view.ns), view.name));
}

std::string ResourcePaths::V1RenameView() const { return BuildPath("views/rename"); }

}  // namespace iceberg::rest
