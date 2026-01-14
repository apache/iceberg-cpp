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

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"

/// \file iceberg/catalog/rest/auth/auth_manager.h
/// \brief Authentication manager interface for REST catalog.

namespace iceberg::rest {
class HttpClient;
}  // namespace iceberg::rest

namespace iceberg::rest::auth {

/// \brief Manager for authentication sessions.
///
/// This interface is used to create sessions for the catalog, tables/views,
/// and any other context that requires authentication.
///
/// Managers are typically stateful and may require initialization and cleanup.
/// The manager is created by the catalog and is closed when the catalog is closed.
///
/// This interface is modeled after Java Iceberg's AuthManager interface.
class ICEBERG_REST_EXPORT AuthManager {
 public:
  virtual ~AuthManager() = default;

  /// \brief Return a temporary session for contacting the configuration endpoint.
  ///
  /// This session is used only during catalog initialization to fetch server
  /// configuration. The returned session will be closed after the configuration
  /// endpoint is contacted and should not be cached.
  ///
  /// The provided HTTP client is a short-lived client; it should only be used
  /// to fetch initial credentials if required, and must be discarded after that.
  ///
  /// By default, it returns the catalog session.
  ///
  /// \param init_client A short-lived HTTP client for initialization.
  /// \param properties Configuration properties.
  /// \return A session for initialization, or an error if session creation fails.
  virtual Result<std::shared_ptr<AuthSession>> InitSession(
      HttpClient* init_client,
      const std::unordered_map<std::string, std::string>& properties);

  /// \brief Return a long-lived session for catalog operations.
  ///
  /// This session's lifetime is tied to the owning catalog. It serves as the
  /// parent session for all other sessions (contextual and table-specific).
  /// It is closed when the owning catalog is closed.
  ///
  /// The provided HTTP client is a long-lived, shared client. Implementors may
  /// store it and reuse it for subsequent requests to the authorization server
  /// (e.g., for renewing or refreshing credentials). It is not necessary to
  /// close it when Close() is called.
  ///
  /// It is not required to cache the returned session internally, as the catalog
  /// will keep it alive for the lifetime of the catalog.
  ///
  /// \param shared_client A long-lived, shared HTTP client.
  /// \param properties Configuration properties (merged with server config).
  /// \return A session for catalog operations, or an error if session creation fails
  ///         (e.g., missing required credentials, network failure during token fetch).
  virtual Result<std::shared_ptr<AuthSession>> CatalogSession(
      HttpClient* shared_client,
      const std::unordered_map<std::string, std::string>& properties) = 0;

  /// \brief Return a session for a specific table or view.
  ///
  /// If the table or view requires a specific AuthSession (e.g., vended credentials),
  /// this method should return a new AuthSession instance. Otherwise, it should
  /// return the parent session.
  ///
  /// By default, it returns the parent session.
  ///
  /// Implementors should cache table sessions internally, as the catalog will not
  /// cache them. Also, the owning catalog never closes table sessions; implementations
  /// should manage their lifecycle and close them when they are no longer needed.
  ///
  /// \param table The table identifier.
  /// \param properties Properties returned by the table/view endpoint.
  /// \param parent The parent session (typically the catalog session).
  /// \return A session for the table, or an error if session creation fails.
  virtual Result<std::shared_ptr<AuthSession>> TableSession(
      const TableIdentifier& table,
      const std::unordered_map<std::string, std::string>& properties,
      std::shared_ptr<AuthSession> parent);

  /// \brief Close the manager and release any resources.
  ///
  /// This method is called when the owning catalog is closed. Implementations
  /// should release any resources held by the manager, such as cached sessions
  /// or background threads.
  virtual void Close() {}
};

}  // namespace iceberg::rest::auth
