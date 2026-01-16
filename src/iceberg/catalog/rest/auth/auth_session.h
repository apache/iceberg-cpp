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
///
/// Authentication sessions are typically immutable, but may hold resources that need
/// to be released when the session is no longer needed (e.g., token refresh threads).
/// Implementations should override Close() to release any such resources.
///
class ICEBERG_REST_EXPORT AuthSession {
 public:
  virtual ~AuthSession() = default;

  /// \brief Authenticate the given request headers.
  ///
  /// This method adds authentication information (e.g., Authorization header)
  /// to the provided headers map. The implementation should be idempotent.
  ///
  /// \param[in,out] headers The headers map to add authentication information to.
  /// \return Status indicating success or failure of authentication.
  ///         - Success: Returns Status::OK
  ///         - Failure: Returns one of the following errors:
  ///           - AuthenticationFailed: General authentication failure (invalid
  ///           credentials, etc.)
  ///           - TokenExpired: Authentication token has expired and needs refresh
  ///           - NotAuthorized: Not authenticated (401)
  ///           - IOError: Network or connection errors when reaching auth server
  ///           - RestError: HTTP errors from authentication service
  virtual Status Authenticate(std::unordered_map<std::string, std::string>& headers) = 0;

  /// \brief Close the session and release any resources.
  ///
  /// This method is called when the session is no longer needed. For stateful
  /// sessions (e.g., OAuth2 with token refresh), this should stop any background
  /// threads and release resources.
  ///
  /// Note: Since sessions may be cached, this method may not be called immediately
  /// after the session is no longer needed, but rather when the session is evicted
  /// from the cache or the cache itself is closed.
  virtual void Close() {}
};

/// \brief A default authentication session that adds static headers to requests.
///
/// This implementation authenticates requests by adding a fixed set of headers.
/// It is suitable for authentication methods that use static credentials,
/// such as Basic auth or static bearer tokens.
class ICEBERG_REST_EXPORT DefaultAuthSession : public AuthSession {
 public:
  /// \brief Construct a DefaultAuthSession with the given headers.
  ///
  /// \param headers The headers to add to each request for authentication.
  explicit DefaultAuthSession(std::unordered_map<std::string, std::string> headers);

  ~DefaultAuthSession() override = default;

  Status Authenticate(std::unordered_map<std::string, std::string>& headers) override;

  static std::unique_ptr<DefaultAuthSession> Make(
      std::unordered_map<std::string, std::string> headers);

 private:
  std::unordered_map<std::string, std::string> headers_;
};

}  // namespace iceberg::rest::auth
