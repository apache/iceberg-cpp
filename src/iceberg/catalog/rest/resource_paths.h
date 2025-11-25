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

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/table_identifier.h"

/// \file iceberg/catalog/rest/resource_paths.h
/// \brief Resource path construction for Iceberg REST API endpoints.

namespace iceberg::rest {

/// \brief Resource path builder for Iceberg REST catalog endpoints.
///
/// This class constructs REST API endpoint URLs for various catalog operations.
class ICEBERG_REST_EXPORT ResourcePaths {
 public:
  /// \brief Construct a ResourcePaths with base URI and optional prefix.
  /// \param base_uri The base URI of the REST catalog server (without trailing slash)
  /// \param prefix Optional prefix for REST API paths (default: empty)
  explicit ResourcePaths(std::string base_uri, std::string prefix = "");

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
  /// \brief Base URI of the REST catalog server.
  std::string base_uri_;

  /// \brief Optional prefix for REST API paths from configuration.
  std::string prefix_;
};

}  // namespace iceberg::rest
