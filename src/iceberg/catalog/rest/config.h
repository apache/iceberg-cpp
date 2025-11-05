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

#include "cpr/cpr.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/util.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"

/// \file iceberg/catalog/rest/config.h
/// \brief RestCatalogConfig implementation for Iceberg REST API.

namespace iceberg::rest {

/// \brief Configuration struct for a Rest Catalog.
///
/// This struct holds all the necessary configuration for connecting and interacting with
/// a Rest service. It's a simple data structure with public members that can be
/// initialized directly via aggregate initialization. It provides helper methods to
/// construct specific endpoint URLs and HTTP headers from the configuration properties.
struct ICEBERG_REST_EXPORT RestCatalogConfig {
  /// \brief Returns the endpoint string for listing all catalog configuration settings.
  std::string GetConfigEndpoint() const;

  /// \brief Returns the endpoint string for OAuth2 token operations (DEPRECATED).
  std::string GetOAuth2TokensEndpoint() const;

  /// \brief Returns the endpoint string for listing or creating namespaces.
  std::string GetNamespacesEndpoint() const;

  /// \brief Returns the endpoint string for loading or managing a specific namespace.
  /// \param ns The namespace to get the endpoint for.
  std::string GetNamespaceEndpoint(const Namespace& ns) const;

  /// \brief Returns the endpoint string for setting or removing namespace properties.
  /// \param ns The namespace to get the properties endpoint for.
  std::string GetNamespacePropertiesEndpoint(const Namespace& ns) const;

  /// \brief Returns the endpoint string for listing or creating tables in a namespace.
  /// \param ns The namespace to get the tables endpoint for.
  std::string GetTablesEndpoint(const Namespace& ns) const;

  /// \brief Returns the endpoint string for submitting a table scan for planning.
  /// \param table The table identifier to get the scan plan endpoint for.
  std::string GetTableScanPlanEndpoint(const TableIdentifier& table) const;

  /// \brief Returns the endpoint string for fetching scan planning results.
  /// \param table The table identifier to get the scan plan result endpoint for.
  /// \param plan_id The plan ID to fetch results for.
  std::string GetTableScanPlanResultEndpoint(const TableIdentifier& table,
                                             const std::string& plan_id) const;

  /// \brief Returns the endpoint string for fetching execution tasks for a plan.
  /// \param table The table identifier to get the tasks endpoint for.
  std::string GetTableTasksEndpoint(const TableIdentifier& table) const;

  /// \brief Returns the endpoint string for registering a table using metadata file
  /// location.
  /// \param ns The namespace to register the table in.
  std::string GetRegisterTableEndpoint(const Namespace& ns) const;

  /// \brief Returns the endpoint string for loading, committing, or dropping a table.
  /// \param table The table identifier to get the endpoint for.
  std::string GetTableEndpoint(const TableIdentifier& table) const;

  /// \brief Returns the endpoint string for loading vended credentials for a table.
  /// \param table The table identifier to get the credentials endpoint for.
  std::string GetTableCredentialsEndpoint(const TableIdentifier& table) const;

  /// \brief Returns the endpoint string for renaming a table.
  std::string GetRenameTableEndpoint() const;

  /// \brief Returns the endpoint string for submitting a metrics report for a table.
  /// \param table The table identifier to get the metrics endpoint for.
  std::string GetTableMetricsEndpoint(const TableIdentifier& table) const;

  /// \brief Returns the endpoint string for atomic multi-table commit operations.
  std::string GetTransactionCommitEndpoint() const;

  /// \brief Returns the endpoint string for listing or creating views in a namespace.
  /// \param ns The namespace to get the views endpoint for.
  std::string GetViewsEndpoint(const Namespace& ns) const;

  /// \brief Returns the endpoint string for loading, replacing, or dropping a view.
  /// \param view The view identifier to get the endpoint for.
  std::string GetViewEndpoint(const TableIdentifier& view) const;

  /// \brief Returns the endpoint string for renaming a view.
  std::string GetRenameViewEndpoint() const;

  /// \brief Generates extra HTTP headers to be added to every request from the
  /// configuration.
  ///
  /// This includes default headers like Content-Type, User-Agent, X-Client-Version and
  /// any custom headers prefixed with "header." in the properties.
  /// \return A Result containing cpr::Header object, or an error if names/values are
  /// invalid.
  Result<cpr::Header> GetExtraHeaders() const;

  /// \brief The catalog's URI (required).
  std::string uri;

  /// \brief The logical name of the catalog (optional).
  std::optional<std::string> name;

  /// \brief The warehouse location associated with the catalog (optional).
  std::optional<std::string> warehouse;

  /// \brief A string-to-string map of properties to store all other configurations.
  std::unordered_map<std::string, std::string> properties_;
};

}  // namespace iceberg::rest
