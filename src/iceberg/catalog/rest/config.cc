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

#include "iceberg/catalog/rest/config.h"

#include <cctype>
#include <ranges>

#include "iceberg/catalog/rest/constant.h"

namespace iceberg::rest {

std::string RestCatalogConfig::GetConfigEndpoint() const {
  return std::format("{}/{}/config", TrimTrailingSlash(uri), kPathV1);
}

std::string RestCatalogConfig::GetOAuth2TokensEndpoint() const {
  return std::format("{}/{}/oauth/tokens", TrimTrailingSlash(uri), kPathV1);
}

std::string RestCatalogConfig::GetNamespacesEndpoint() const {
  return std::format("{}/{}/namespaces", TrimTrailingSlash(uri), kPathV1);
}

std::string RestCatalogConfig::GetNamespaceEndpoint(const Namespace& ns) const {
  return std::format("{}/{}/namespaces/{}", TrimTrailingSlash(uri), kPathV1,
                     EncodeNamespaceForUrl(ns));
}

std::string RestCatalogConfig::GetNamespacePropertiesEndpoint(const Namespace& ns) const {
  return std::format("{}/{}/namespaces/{}/properties", TrimTrailingSlash(uri), kPathV1,
                     EncodeNamespaceForUrl(ns));
}

std::string RestCatalogConfig::GetTablesEndpoint(const Namespace& ns) const {
  return std::format("{}/{}/namespaces/{}/tables", TrimTrailingSlash(uri), kPathV1,
                     EncodeNamespaceForUrl(ns));
}

std::string RestCatalogConfig::GetTableScanPlanEndpoint(
    const TableIdentifier& table) const {
  return std::format("{}/{}/namespaces/{}/tables/{}/plan", TrimTrailingSlash(uri),
                     kPathV1, EncodeNamespaceForUrl(table.ns), table.name);
}

std::string RestCatalogConfig::GetTableScanPlanResultEndpoint(
    const TableIdentifier& table, const std::string& plan_id) const {
  return std::format("{}/{}/namespaces/{}/tables/{}/plan/{}", TrimTrailingSlash(uri),
                     kPathV1, EncodeNamespaceForUrl(table.ns), table.name, plan_id);
}

std::string RestCatalogConfig::GetTableTasksEndpoint(const TableIdentifier& table) const {
  return std::format("{}/{}/namespaces/{}/tables/{}/tasks", TrimTrailingSlash(uri),
                     kPathV1, EncodeNamespaceForUrl(table.ns), table.name);
}

std::string RestCatalogConfig::GetRegisterTableEndpoint(const Namespace& ns) const {
  return std::format("{}/{}/namespaces/{}/register", TrimTrailingSlash(uri), kPathV1,
                     EncodeNamespaceForUrl(ns));
}

std::string RestCatalogConfig::GetTableEndpoint(const TableIdentifier& table) const {
  return std::format("{}/{}/namespaces/{}/tables/{}", TrimTrailingSlash(uri), kPathV1,
                     EncodeNamespaceForUrl(table.ns), table.name);
}

std::string RestCatalogConfig::GetTableCredentialsEndpoint(
    const TableIdentifier& table) const {
  return std::format("{}/{}/namespaces/{}/tables/{}/credentials", TrimTrailingSlash(uri),
                     kPathV1, EncodeNamespaceForUrl(table.ns), table.name);
}

std::string RestCatalogConfig::GetRenameTableEndpoint() const {
  return std::format("{}/{}/tables/rename", TrimTrailingSlash(uri), kPathV1);
}

std::string RestCatalogConfig::GetTableMetricsEndpoint(
    const TableIdentifier& table) const {
  return std::format("{}/{}/namespaces/{}/tables/{}/metrics", TrimTrailingSlash(uri),
                     kPathV1, EncodeNamespaceForUrl(table.ns), table.name);
}

std::string RestCatalogConfig::GetTransactionCommitEndpoint() const {
  return std::format("{}/{}/transactions/commit", TrimTrailingSlash(uri), kPathV1);
}

std::string RestCatalogConfig::GetViewsEndpoint(const Namespace& ns) const {
  return std::format("{}/{}/namespaces/{}/views", TrimTrailingSlash(uri), kPathV1,
                     EncodeNamespaceForUrl(ns));
}

std::string RestCatalogConfig::GetViewEndpoint(const TableIdentifier& view) const {
  return std::format("{}/{}/namespaces/{}/views/{}", TrimTrailingSlash(uri), kPathV1,
                     EncodeNamespaceForUrl(view.ns), view.name);
}

std::string RestCatalogConfig::GetRenameViewEndpoint() const {
  return std::format("{}/{}/views/rename", TrimTrailingSlash(uri), kPathV1);
}

Result<cpr::Header> RestCatalogConfig::GetExtraHeaders() const {
  cpr::Header headers;

  headers[std::string(kHeaderContentType)] = std::string(kMimeTypeApplicationJson);
  headers[std::string(kHeaderUserAgent)] = std::string(kUserAgent);
  headers[std::string(kHeaderXClientVersion)] = std::string(kClientVersion);

  constexpr std::string_view prefix = "header.";
  for (const auto& [key, value] : properties_) {
    if (key.starts_with(prefix)) {
      std::string_view header_name = std::string_view(key).substr(prefix.length());

      if (header_name.empty()) {
        return InvalidArgument("Header name cannot be empty after '{}' prefix", prefix);
      }

      if (value.empty()) {
        return InvalidArgument("Header value for '{}' cannot be empty", header_name);
      }
      headers[std::string(header_name)] = value;
    }
  }
  return headers;
}

}  // namespace iceberg::rest
