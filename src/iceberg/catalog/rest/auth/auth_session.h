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
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/auth/auth_session.h
/// \brief Authentication session interface for REST catalog.

namespace iceberg::rest::auth {

/// \brief An authentication session that can authenticate outgoing HTTP requests.
class ICEBERG_REST_EXPORT AuthSession {
 public:
  virtual ~AuthSession() = default;

  /// \brief Authenticate the given request headers.
  ///
  /// This method adds authentication information (e.g., Authorization header)
  /// to the provided headers map. The implementation should be idempotent.
  ///
  /// \param[in,out] headers The headers map to add authentication information to.
  /// \return Status indicating success or one of the following errors:
  ///         - AuthenticationFailed: General authentication failure (invalid credentials,
  ///         etc.)
  ///         - TokenExpired: Authentication token has expired and needs refresh
  ///         - NotAuthorized: Not authenticated (401)
  ///         - IOError: Network or connection errors when reaching auth server
  ///         - RestError: HTTP errors from authentication service
  virtual Status Authenticate(std::unordered_map<std::string, std::string>& headers) = 0;

  /// \brief Close the session and release any resources.
  ///
  /// This method is called when the session is no longer needed. For stateful
  /// sessions (e.g., OAuth2 with token refresh), this should stop any background
  /// threads and release resources.
  ///
  /// \return Status indicating success or failure of closing the session.
  virtual Status Close() { return {}; }

  /// \brief Create a default session with static headers.
  ///
  /// This factory method creates a session that adds a fixed set of headers to each
  /// request. It is suitable for authentication methods that use static credentials,
  /// such as Basic auth or static bearer tokens.
  ///
  /// \param headers The headers to add to each request for authentication.
  /// \return A new session that adds the given headers to requests.
  static std::shared_ptr<AuthSession> MakeDefault(
      std::unordered_map<std::string, std::string> headers);
};

}  // namespace iceberg::rest::auth
