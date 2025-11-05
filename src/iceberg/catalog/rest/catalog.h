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

#include <cpr/cpr.h>

#include "iceberg/catalog.h"
#include "iceberg/catalog/rest/config.h"
#include "iceberg/catalog/rest/http_client_interal.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/catalog.h
/// RestCatalog implementation for Iceberg REST API.

namespace iceberg::rest {

class ICEBERG_REST_EXPORT RestCatalog : public Catalog {
 public:
  RestCatalog(const RestCatalog&) = delete;
  RestCatalog& operator=(const RestCatalog&) = delete;
  RestCatalog(RestCatalog&&) = default;
  RestCatalog& operator=(RestCatalog&&) = default;

  /// \brief Create a RestCatalog instance
  ///
  /// \param config the configuration for the RestCatalog
  /// \return a RestCatalog instance
  static Result<RestCatalog> Make(RestCatalogConfig config);

  /// \brief Return the name for this catalog
  std::string_view name() const override;

  /// \brief List child namespaces from the given namespace.
  Result<std::vector<Namespace>> ListNamespaces(const Namespace& ns) const override;

  /// \brief Create a namespace with associated properties.
  Status CreateNamespace(
      const Namespace& ns,
      const std::unordered_map<std::string, std::string>& properties) override;

  /// \brief Get metadata properties for a namespace.
  Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
      const Namespace& ns) const override;

  /// \brief Drop a namespace.
  Status DropNamespace(const Namespace& ns) override;

  /// \brief Check whether the namespace exists.
  Result<bool> NamespaceExists(const Namespace& ns) const override;

  /// \brief Update a namespace's properties by applying additions and removals.
  Status UpdateNamespaceProperties(
      const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
      const std::unordered_set<std::string>& removals) override;

  /// \brief Return all the identifiers under this namespace
  Result<std::vector<TableIdentifier>> ListTables(const Namespace& ns) const override;

  /// \brief Create a table
  Result<std::unique_ptr<Table>> CreateTable(
      const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override;

  /// \brief Update a table
  ///
  /// \param identifier a table identifier
  /// \param requirements a list of table requirements
  /// \param updates a list of table updates
  /// \return a Table instance or ErrorKind::kAlreadyExists if the table already exists
  Result<std::unique_ptr<Table>> UpdateTable(
      const TableIdentifier& identifier,
      const std::vector<std::unique_ptr<TableRequirement>>& requirements,
      const std::vector<std::unique_ptr<TableUpdate>>& updates) override;

  /// \brief Start a transaction to create a table
  ///
  /// \param identifier a table identifier
  /// \param schema a schema
  /// \param spec a partition spec
  /// \param location a location for the table; leave empty if unspecified
  /// \param properties a string map of table properties
  /// \return a Transaction to create the table or ErrorKind::kAlreadyExists if the
  /// table already exists
  Result<std::shared_ptr<Transaction>> StageCreateTable(
      const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override;

  /// \brief Check whether table exists
  ///
  /// \param identifier a table identifier
  /// \return Result<bool> indicating table exists or not.
  ///         - On success, the table existence was successfully checked (actual
  ///         existence may be inferred elsewhere).
  ///         - On failure, contains error information.
  Result<bool> TableExists(const TableIdentifier& identifier) const override;

  /// \brief Drop a table; optionally delete data and metadata files
  ///
  /// If purge is set to true the implementation should delete all data and metadata
  /// files.
  ///
  /// \param identifier a table identifier
  /// \param purge if true, delete all data and metadata files in the table
  /// \return Status indicating the outcome of the operation.
  ///         - On success, the table was dropped (or did not exist).
  ///         - On failure, contains error information.
  Status DropTable(const TableIdentifier& identifier, bool purge) override;

  /// \brief Load a table
  ///
  /// \param identifier a table identifier
  /// \return instance of Table implementation referred to by identifier or
  /// ErrorKind::kNoSuchTable if the table does not exist
  Result<std::unique_ptr<Table>> LoadTable(const TableIdentifier& identifier) override;

  /// \brief Register a table with the catalog if it does not exist
  ///
  /// \param identifier a table identifier
  /// \param metadata_file_location the location of a metadata file
  /// \return a Table instance or ErrorKind::kAlreadyExists if the table already exists
  Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier,
      const std::string& metadata_file_location) override;

  /// \brief A builder used to create valid tables or start create/replace transactions
  ///
  /// \param identifier a table identifier
  /// \param schema a schema
  /// \return the builder to create a table or start a create/replace transaction
  std::unique_ptr<RestCatalog::TableBuilder> BuildTable(
      const TableIdentifier& identifier, const Schema& schema) const override;

 private:
  RestCatalog(std::shared_ptr<RestCatalogConfig> config,
              std::unique_ptr<HttpClient> client);

  std::shared_ptr<RestCatalogConfig> config_;
  std::unique_ptr<HttpClient> client_;
};

}  // namespace iceberg::rest
