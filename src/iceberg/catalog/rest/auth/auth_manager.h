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
#include <unordered_map>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"

/// \file iceberg/catalog/rest/auth/auth_manager.h
/// \brief Authentication manager interface for REST catalog.

namespace iceberg::rest::auth {

/// \brief Produces authentication sessions for catalog and table requests.
///
/// AuthManager is responsible for creating authentication sessions at different scopes:
/// - InitSession: Short-lived session for catalog initialization (optional)
/// - CatalogSession: Long-lived session for catalog-level operations (required)
/// - TableSession: Optional table-specific session or reuse of catalog session
///
/// Implementations are registered via AuthManagers::Register() and loaded by auth type.
class ICEBERG_REST_EXPORT AuthManager {
 public:
  virtual ~AuthManager() = default;

  /// \brief Create a short-lived session used to contact the configuration endpoint.
  ///
  /// This session is used only during catalog initialization to fetch server
  /// configuration and perform initial authentication. It is typically discarded after
  /// initialization.
  ///
  /// \param init_client HTTP client used for initialization requests.
  /// \param properties Client configuration supplied by the catalog.
  /// \return Session for initialization or an error if credentials cannot be acquired.
  virtual Result<std::unique_ptr<AuthSession>> InitSession(
      HttpClient& init_client,
      const std::unordered_map<std::string, std::string>& properties);

  /// \brief Create the long-lived catalog session that acts as the parent session.
  ///
  /// This session is used for all catalog-level operations (list namespaces, list tables,
  /// etc.) and serves as the parent session for table-specific operations. It is owned
  /// by the catalog and reused throughout the catalog's lifetime.
  ///
  /// \param shared_client HTTP client owned by the catalog and reused for auth calls.
  /// \param properties Catalog properties (client config + server defaults).
  /// \return Session for catalog operations or an error if authentication cannot be set
  /// up.
  virtual Result<std::unique_ptr<AuthSession>> CatalogSession(
      HttpClient& shared_client,
      const std::unordered_map<std::string, std::string>& properties) = 0;

  /// \brief Create or reuse a session scoped to a single table/view.
  ///
  /// This method can return a new table-specific session or indicate that the parent
  /// catalog session should be reused by returning nullptr.
  ///
  /// \param table Target table identifier.
  /// \param properties Table-specific auth properties returned by the server.
  /// \param parent Catalog session to inherit from or extract information from.
  /// \return A new session for the table, nullptr to reuse parent session, or an error.
  virtual Result<std::unique_ptr<AuthSession>> TableSession(
      const TableIdentifier& table,
      const std::unordered_map<std::string, std::string>& properties,
      const AuthSession& parent);

  /// \brief Release resources held by the manager.
  ///
  /// \return Status of the close operation.
  virtual Status Close() { return {}; }
};

}  // namespace iceberg::rest::auth
