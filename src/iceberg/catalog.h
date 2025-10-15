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
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief A Catalog API for table create, drop, and load operations.
///
/// Note that these functions are named after the corresponding operationId
/// specified by the Iceberg Rest Catalog API.
class ICEBERG_EXPORT Catalog {
 public:
  virtual ~Catalog() = default;

  /// \brief Return the name for this catalog
  virtual std::string_view name() const = 0;

  /// \brief Create a namespace with associated properties.
  ///
  /// \param ns the namespace to create
  /// \param properties a key-value map of metadata for the namespace
  /// \return Status::OK if created successfully;
  ///         ErrorKind::kAlreadyExists if the namespace already exists;
  ///         ErrorKind::kNotSupported if the operation is not supported
  virtual Status CreateNamespace(
      const Namespace& ns,
      const std::unordered_map<std::string, std::string>& properties) = 0;

  /// \brief List child namespaces from the given namespace.
  ///
  /// \param ns the parent namespace
  /// \return a list of child namespaces;
  ///         ErrorKind::kNoSuchNamespace if the given namespace does not exist
  virtual Result<std::vector<Namespace>> ListNamespaces(const Namespace& ns) const = 0;

  /// \brief Get metadata properties for a namespace.
  ///
  /// \param ns the namespace to look up
  /// \return a key-value map of metadata properties;
  ///         ErrorKind::kNoSuchNamespace if the namespace does not exist
  virtual Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
      const Namespace& ns) const = 0;

  /// \brief Drop a namespace.
  ///
  /// \param ns the namespace to drop
  /// \return Status::OK if dropped successfully;
  ///         ErrorKind::kNoSuchNamespace if the namespace does not exist;
  ///         ErrorKind::kNotAllowed if the namespace is not empty
  virtual Status DropNamespace(const Namespace& ns) = 0;

  /// \brief Check whether the namespace exists.
  ///
  /// \param ns the namespace to check
  /// \return true if the namespace exists, false otherwise
  virtual Result<bool> NamespaceExists(const Namespace& ns) const = 0;

  /// \brief Update a namespace's properties by applying additions and removals.
  ///
  /// \param ns the namespace to update
  /// \param updates a set of properties to add or overwrite
  /// \param removals a set of property keys to remove
  /// \return Status::OK if the update is successful;
  ///         ErrorKind::kNoSuchNamespace if the namespace does not exist;
  ///         ErrorKind::kUnsupported if the operation is not supported
  virtual Status UpdateNamespaceProperties(
      const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
      const std::unordered_set<std::string>& removals) = 0;

  /// \brief Return all the identifiers under this namespace
  ///
  /// \param ns a namespace
  /// \return a list of identifiers for tables or ErrorKind::kNoSuchNamespace
  /// if the namespace does not exist
  virtual Result<std::vector<TableIdentifier>> ListTables(const Namespace& ns) const = 0;

  /// \brief Create a table
  ///
  /// \param identifier a table identifier
  /// \param schema a schema
  /// \param spec a partition spec
  /// \param location a location for the table; leave empty if unspecified
  /// \param properties a string map of table properties
  /// \return a Table instance or ErrorKind::kAlreadyExists if the table already exists
  virtual Result<std::unique_ptr<Table>> CreateTable(
      const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) = 0;

  /// \brief Update a table
  ///
  /// \param identifier a table identifier
  /// \param requirements a list of table requirements
  /// \param updates a list of table updates
  /// \return a Table instance or ErrorKind::kAlreadyExists if the table already exists
  virtual Result<std::unique_ptr<Table>> UpdateTable(
      const TableIdentifier& identifier,
      const std::vector<std::unique_ptr<TableRequirement>>& requirements,
      const std::vector<std::unique_ptr<TableUpdate>>& updates) = 0;

  /// \brief Start a transaction to create a table
  ///
  /// \param identifier a table identifier
  /// \param schema a schema
  /// \param spec a partition spec
  /// \param location a location for the table; leave empty if unspecified
  /// \param properties a string map of table properties
  /// \return a Transaction to create the table or ErrorKind::kAlreadyExists if the
  /// table already exists
  virtual Result<std::shared_ptr<Transaction>> StageCreateTable(
      const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) = 0;

  /// \brief Check whether table exists
  ///
  /// \param identifier a table identifier
  /// \return Result<bool> indicating table exists or not.
  ///         - On success, the table existence was successfully checked (actual
  ///         existence may be inferred elsewhere).
  ///         - On failure, contains error information.
  virtual Result<bool> TableExists(const TableIdentifier& identifier) const = 0;

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
  virtual Status DropTable(const TableIdentifier& identifier, bool purge) = 0;

  /// \brief Load a table
  ///
  /// \param identifier a table identifier
  /// \return instance of Table implementation referred to by identifier or
  /// ErrorKind::kNoSuchTable if the table does not exist
  virtual Result<std::unique_ptr<Table>> LoadTable(const TableIdentifier& identifier) = 0;

  /// \brief Register a table with the catalog if it does not exist
  ///
  /// \param identifier a table identifier
  /// \param metadata_file_location the location of a metadata file
  /// \return a Table instance or ErrorKind::kAlreadyExists if the table already exists
  virtual Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier, const std::string& metadata_file_location) = 0;

  /// \brief A builder used to create valid tables or start create/replace transactions
  class TableBuilder {
   public:
    virtual ~TableBuilder() = default;

    /// \brief Sets a partition spec for the table
    ///
    /// \param spec a partition spec
    /// \return this for method chaining
    virtual TableBuilder& WithPartitionSpec(const PartitionSpec& spec) = 0;

    /// \brief Sets a sort order for the table
    ///
    /// \param sort_order a sort order
    /// \return this for method chaining
    virtual TableBuilder& WithSortOrder(const SortOrder& sort_order) = 0;

    /// \brief Sets a location for the table
    ///
    /// \param location a location
    /// \return this for method chaining
    virtual TableBuilder& WithLocation(const std::string& location) = 0;

    /// \brief Adds key/value properties to the table
    ///
    /// \param properties key/value properties
    /// \return this for method chaining
    virtual TableBuilder& WithProperties(
        const std::unordered_map<std::string, std::string>& properties) = 0;

    /// \brief Adds a key/value property to the table
    ///
    /// \param key a key
    /// \param value a value
    /// \return this for method chaining
    virtual TableBuilder& WithProperty(const std::string& key,
                                       const std::string& value) = 0;

    /// \brief Creates the table
    ///
    /// \return the created table
    virtual std::unique_ptr<Table> Create() = 0;

    /// \brief Starts a transaction to create the table
    ///
    /// \return the Transaction to create the table
    virtual std::unique_ptr<Transaction> StageCreate() = 0;
  };

  /// \brief Instantiate a builder to either create a table or start a create/replace
  /// transaction
  ///
  /// \param identifier a table identifier
  /// \param schema a schema
  /// \return the builder to create a table or start a create/replace transaction
  virtual std::unique_ptr<TableBuilder> BuildTable(const TableIdentifier& identifier,
                                                   const Schema& schema) const = 0;
};

}  // namespace iceberg
