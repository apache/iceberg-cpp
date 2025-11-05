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

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/validator.h
/// Validator for REST Catalog API types.

namespace iceberg::rest {

/// \brief Validator for REST Catalog API types. Validation should be called after
/// deserializing objects from external sources to ensure data integrity before the
/// objects are used.
class ICEBERG_REST_EXPORT Validator {
 public:
  // Configuration and Error types

  /// \brief Validates a CatalogConfig object.
  static Status Validate(const CatalogConfig& config);

  /// \brief Validates an ErrorModel object.
  static Status Validate(const ErrorModel& error);

  /// \brief Validates an ErrorResponse object.
  static Status Validate(const ErrorResponse& response);

  // Namespace operations

  /// \brief Validates a ListNamespacesResponse object.
  static Status Validate(const ListNamespacesResponse& response);

  /// \brief Validates a CreateNamespaceRequest object.
  static Status Validate(const CreateNamespaceRequest& request);

  /// \brief Validates a CreateNamespaceResponse object.
  static Status Validate(const CreateNamespaceResponse& response);

  /// \brief Validates a GetNamespaceResponse object.
  static Status Validate(const GetNamespaceResponse& response);

  /// \brief Validates an UpdateNamespacePropertiesRequest object.
  static Status Validate(const UpdateNamespacePropertiesRequest& request);

  /// \brief Validates an UpdateNamespacePropertiesResponse object.
  static Status Validate(const UpdateNamespacePropertiesResponse& response);

  // Table operations

  /// \brief Validates a ListTablesResponse object.
  static Status Validate(const ListTablesResponse& response);

  /// \brief Validates a LoadTableResult object.
  static Status Validate(const LoadTableResult& result);

  /// \brief Validates a RegisterTableRequest object.
  static Status Validate(const RegisterTableRequest& request);

  /// \brief Validates a RenameTableRequest object.
  static Status Validate(const RenameTableRequest& request);

  // Other types

  /// \brief Validates a TableIdentifier object.
  static Status Validate(const TableIdentifier& identifier);
};

}  // namespace iceberg::rest
