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

#include <string>
#include <string_view>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"

/// \file iceberg/catalog/rest/resource_paths.h
/// \brief Resource path construction for Iceberg REST API endpoints.

namespace iceberg::rest {

/// \brief Resource path builder for Iceberg REST catalog endpoints.
///
/// This class constructs REST API endpoint URLs for various catalog operations.
class ICEBERG_REST_EXPORT ResourcePaths {
 public:
  /// \brief Construct a ResourcePaths with REST catalog configuration.
  /// \param config The REST catalog configuration containing the base URI
  static Result<std::unique_ptr<ResourcePaths>> Make(const RestCatalogConfig& config);

  /// \brief Get the /v1/{prefix}/config endpoint path.
  std::string Config() const;

  /// \brief Get the /v1/{prefix}/oauth/tokens endpoint path.
  std::string OAuth2Tokens() const;

  /// \brief Get the /v1/{prefix}/namespaces endpoint path.
  std::string Namespaces() const;

  /// \brief Get the /v1/{prefix}/namespaces/{namespace} endpoint path.
  std::string Namespace_(const Namespace& ns) const;

  /// \brief Get the /v1/{prefix}/namespaces/{namespace}/properties endpoint path.
  std::string NamespaceProperties(const Namespace& ns) const;

  /// \brief Get the /v1/{prefix}/namespaces/{namespace}/tables endpoint path.
  std::string Tables(const Namespace& ns) const;

  /// \brief Get the /v1/{prefix}/namespaces/{namespace}/tables/{table} endpoint path.
  std::string Table(const TableIdentifier& ident) const;

  /// \brief Get the /v1/{prefix}/namespaces/{namespace}/register endpoint path.
  std::string Register(const Namespace& ns) const;

  /// \brief Get the /v1/{prefix}/tables/rename endpoint path.
  std::string Rename() const;

  /// \brief Get the /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics endpoint
  /// path.
  std::string Metrics(const TableIdentifier& ident) const;

  /// \brief Get the /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials
  /// endpoint path.
  std::string Credentials(const TableIdentifier& ident) const;

  /// \brief Get the /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan endpoint
  /// path.
  std::string ScanPlan(const TableIdentifier& ident) const;

  /// \brief Get the /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{planId}
  /// endpoint path.
  std::string ScanPlanResult(const TableIdentifier& ident,
                             const std::string& plan_id) const;

  /// \brief Get the /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks endpoint
  /// path.
  std::string Tasks(const TableIdentifier& ident) const;

  /// \brief Get the /v1/{prefix}/transactions/commit endpoint path.
  std::string CommitTransaction() const;

 private:
  explicit ResourcePaths(std::string base_uri, std::string prefix);

  // Helper to build path with optional prefix: {base_uri_}/{prefix_?}/{path}
  std::string BuildPath(std::string_view path) const;

  std::string base_uri_;
  std::string prefix_;  // Optional prefix from config
};

}  // namespace iceberg::rest
