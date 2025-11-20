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

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/rest_util.h"
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
  return std::format("{}/v1/{}{}", base_uri_, (prefix_.empty() ? "" : (prefix_ + "/")),
                     path);
}

std::string ResourcePaths::Config() const {
  return std::format("{}/v1/config", base_uri_);
}

std::string ResourcePaths::OAuth2Tokens() const {
  return std::format("{}/v1/oauth/tokens", base_uri_);
}

std::string ResourcePaths::Namespaces() const { return BuildPath("namespaces"); }

std::string ResourcePaths::Namespace_(const Namespace& ns) const {
  return BuildPath(std::format("namespaces/{}", EncodeNamespaceForUrl(ns)));
}

std::string ResourcePaths::NamespaceProperties(const Namespace& ns) const {
  return BuildPath(std::format("namespaces/{}/properties", EncodeNamespaceForUrl(ns)));
}

std::string ResourcePaths::Tables(const Namespace& ns) const {
  return BuildPath(std::format("namespaces/{}/tables", EncodeNamespaceForUrl(ns)));
}

std::string ResourcePaths::Table(const TableIdentifier& ident) const {
  return BuildPath(std::format("namespaces/{}/tables/{}", EncodeNamespaceForUrl(ident.ns),
                               ident.name));
}

std::string ResourcePaths::Register(const Namespace& ns) const {
  return BuildPath(std::format("namespaces/{}/register", EncodeNamespaceForUrl(ns)));
}

std::string ResourcePaths::Rename() const { return BuildPath("tables/rename"); }

std::string ResourcePaths::Metrics(const TableIdentifier& ident) const {
  return BuildPath(std::format("namespaces/{}/tables/{}/metrics",
                               EncodeNamespaceForUrl(ident.ns), ident.name));
}

std::string ResourcePaths::Credentials(const TableIdentifier& ident) const {
  return BuildPath(std::format("namespaces/{}/tables/{}/credentials",
                               EncodeNamespaceForUrl(ident.ns), ident.name));
}

std::string ResourcePaths::ScanPlan(const TableIdentifier& ident) const {
  return BuildPath(std::format("namespaces/{}/tables/{}/plan",
                               EncodeNamespaceForUrl(ident.ns), ident.name));
}

std::string ResourcePaths::ScanPlanResult(const TableIdentifier& ident,
                                          const std::string& plan_id) const {
  return BuildPath(std::format("namespaces/{}/tables/{}/plan/{}",
                               EncodeNamespaceForUrl(ident.ns), ident.name, plan_id));
}

std::string ResourcePaths::Tasks(const TableIdentifier& ident) const {
  return BuildPath(std::format("namespaces/{}/tables/{}/tasks",
                               EncodeNamespaceForUrl(ident.ns), ident.name));
}

std::string ResourcePaths::CommitTransaction() const {
  return BuildPath("transactions/commit");
}

std::string ResourcePaths::Views(const Namespace& ns) const {
  return BuildPath(std::format("namespaces/{}/views", EncodeNamespaceForUrl(ns)));
}

std::string ResourcePaths::View(const TableIdentifier& ident) const {
  return BuildPath(
      std::format("namespaces/{}/views/{}", EncodeNamespaceForUrl(ident.ns), ident.name));
}

std::string ResourcePaths::RenameView() const { return BuildPath("views/rename"); }

}  // namespace iceberg::rest
