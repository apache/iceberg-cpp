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
#include <string_view>
#include <unordered_map>

#include "iceberg/catalog/rest/auth/auth_manager.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/auth/auth_managers.h
/// \brief Factory for creating authentication managers.

namespace iceberg::rest::auth {

/// \brief Function that creates an AuthManager from its name.
///
/// \param name Name of the auth manager.
/// \param properties Properties required by the auth manager.
/// \return Newly created manager instance or an error if creation fails.
using AuthManagerFactory = std::function<Result<std::unique_ptr<AuthManager>>(
    std::string_view name,
    const std::unordered_map<std::string, std::string>& properties)>;

/// \brief Registry-backed factory for AuthManager implementations.
class ICEBERG_REST_EXPORT AuthManagers {
 public:
  /// \brief Load a manager by consulting the "rest.auth.type" configuration.
  ///
  /// \param name Name of the auth manager.
  /// \param properties Catalog properties used to determine auth type.
  /// \return Manager instance or an error if no factory matches.
  static Result<std::unique_ptr<AuthManager>> Load(
      std::string_view name,
      const std::unordered_map<std::string, std::string>& properties);

  /// \brief Register or override the factory for a given auth type.
  ///
  /// This method is not thread-safe. All registrations should be done during
  /// application startup before any concurrent access to Load().
  ///
  /// \param auth_type Case-insensitive type identifier (e.g., "basic").
  /// \param factory Factory function that produces the manager.
  static void Register(std::string_view auth_type, AuthManagerFactory factory);
};

}  // namespace iceberg::rest::auth
