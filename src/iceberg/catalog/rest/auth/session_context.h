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

#include "iceberg/catalog/rest/iceberg_rest_export.h"

namespace iceberg::rest::auth {

/// \brief Per-session context passed to AuthManager::ContextualSession.
///
/// Mirrors Java's `SessionCatalog.SessionContext`. Separate `properties` and
/// `credentials` so per-context credential overrides don't silently collapse
/// into properties.
struct ICEBERG_REST_EXPORT SessionContext {
  std::unordered_map<std::string, std::string> properties;
  std::unordered_map<std::string, std::string> credentials;
};

}  // namespace iceberg::rest::auth
