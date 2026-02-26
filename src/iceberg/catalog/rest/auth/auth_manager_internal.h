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

#include "iceberg/catalog/rest/auth/auth_manager.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/auth/auth_manager_internal.h
/// \brief Internal factory functions for built-in AuthManager implementations.

namespace iceberg::rest::auth {

/// \brief Create a no-op authentication manager (no authentication).
Result<std::unique_ptr<AuthManager>> MakeNoopAuthManager(
    std::string_view name,
    const std::unordered_map<std::string, std::string>& properties);

/// \brief Create a basic authentication manager.
Result<std::unique_ptr<AuthManager>> MakeBasicAuthManager(
    std::string_view name,
    const std::unordered_map<std::string, std::string>& properties);

}  // namespace iceberg::rest::auth
