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

/// \file iceberg/catalog/rest/auth/auth_session.h
/// \brief Authentication session interface for REST catalog.

namespace iceberg::rest::auth {

/// \brief An authentication session that can authenticate outgoing HTTP requests.
///
/// Authentication sessions are typically immutable, but may hold resources that need
/// to be released when the session is no longer needed (e.g., token refresh threads).
/// Implementations should override Close() to release any such resources.
///
/// This interface is modeled after Java Iceberg's AuthSession interface.
class ICEBERG_REST_EXPORT AuthSession {
 public:
  virtual ~AuthSession() = default;

  /// \brief Authenticate the given request headers.
  ///
  /// This method adds authentication information (e.g., Authorization header)
  /// to the provided headers map. The implementation should be idempotent.
  ///
  /// \param headers The headers map to add authentication information to.
  virtual void Authenticate(std::unordered_map<std::string, std::string>& headers) = 0;

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

  /// \brief Get a shared pointer to an empty session that does nothing.
  ///
  /// The empty session is a singleton that simply returns the request unchanged.
  /// It is useful as a default or placeholder session.
  ///
  /// \return A shared pointer to the empty session singleton.
  static std::shared_ptr<AuthSession> Empty();
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

  /// \brief Add the configured headers to the request.
  ///
  /// Headers are added only if they don't already exist in the request
  /// (i.e., request headers take precedence).
  ///
  /// \param headers The headers map to add authentication information to.
  void Authenticate(std::unordered_map<std::string, std::string>& headers) override;

  /// \brief Create a DefaultAuthSession with the given headers.
  ///
  /// \param headers The headers to add to each request.
  /// \return A shared pointer to the new session.
  static std::shared_ptr<DefaultAuthSession> Of(
      std::unordered_map<std::string, std::string> headers);

 private:
  std::unordered_map<std::string, std::string> headers_;
};

}  // namespace iceberg::rest::auth
