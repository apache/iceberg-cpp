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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "iceberg/catalog/rest/auth/auth_manager.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/auth/auth_managers.h
/// \brief Factory for creating authentication managers.

namespace iceberg::rest::auth {

/// \brief Factory function type for creating AuthManager instances.
///
/// \param name The name of the manager (used for logging).
/// \param properties Configuration properties.
/// \return A unique pointer to the created AuthManager.
using AuthManagerFactory = std::function<std::unique_ptr<AuthManager>(
    const std::string& name,
    const std::unordered_map<std::string, std::string>& properties)>;

/// \brief Factory class for loading authentication managers.
///
/// This class provides a registry-based approach to create AuthManager instances
/// based on the configured authentication type. It supports built-in types
/// (none, basic, oauth2) and allows registration of custom types.
///
/// This class is modeled after Java Iceberg's AuthManagers class.
class ICEBERG_REST_EXPORT AuthManagers {
 public:
  /// \brief Load an authentication manager based on configuration.
  ///
  /// This method reads the "rest.auth.type" property to determine which
  /// AuthManager implementation to create. Supported types include:
  /// - "none": NoopAuthManager (no authentication)
  /// - "basic": BasicAuthManager (HTTP Basic authentication)
  /// - "oauth2": OAuth2AuthManager (OAuth2 authentication)
  /// - "sigv4": SigV4AuthManager (AWS Signature V4)
  ///
  /// If no auth type is specified, the method will infer the type based on
  /// other properties (e.g., presence of "credential" or "token" implies oauth2).
  /// If no auth-related properties are found, it defaults to "none".
  ///
  /// \param name A name for the manager (used for logging).
  /// \param properties Configuration properties.
  /// \return A unique pointer to the created AuthManager, or an error.
  static Result<std::unique_ptr<AuthManager>> Load(
      const std::string& name,
      const std::unordered_map<std::string, std::string>& properties);

  /// \brief Register a custom authentication manager factory.
  ///
  /// This allows users to extend the supported authentication types by
  /// registering their own AuthManager implementations.
  ///
  /// \param auth_type The authentication type name (e.g., "custom").
  /// \param factory The factory function to create the AuthManager.
  static void Register(const std::string& auth_type, AuthManagerFactory factory);

 private:
  /// \brief Get the global registry of auth manager factories.
  static std::unordered_map<std::string, AuthManagerFactory>& GetRegistry();
};

}  // namespace iceberg::rest::auth
