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

#include <string>
#include <unordered_map>

#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Session state used to bind catalog operations to a caller context.
///
/// Session context is the isolation boundary for session-aware catalogs. REST
/// catalogs use `session_id` to derive contextual authentication sessions while
/// keeping user-visible catalog operations on the normal `Catalog` interface.
///
/// The type is intentionally an aggregate so callers can construct it with
/// designated initializers. `properties` and `credentials` are kept separate so
/// authentication implementations can detect conflicting keys instead of
/// silently overriding credentials.
struct ICEBERG_EXPORT SessionContext {
  /// Unique session identifier. Explicit contexts must provide a non-empty ID.
  std::string session_id;

  /// Caller identity. Empty means no identity was supplied. This value is
  /// descriptive and is not implicitly inserted into authentication properties.
  std::string identity;

  /// Sensitive context-specific credentials, such as delegated auth material.
  std::unordered_map<std::string, std::string> credentials;

  /// Non-secret context-specific properties, such as tenant or auth options.
  std::unordered_map<std::string, std::string> properties;

  /// \brief Create an empty caller context with a generated non-empty session ID.
  ///
  /// Each call returns a distinct session ID.
  static SessionContext Empty();
};

}  // namespace iceberg
