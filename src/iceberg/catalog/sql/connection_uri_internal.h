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

/// \file iceberg/catalog/sql/connection_uri_internal.h
/// Minimal parser for `scheme://[user[:password]@]host[:port][/database]`
/// connection URIs, shared by the PostgreSQL and MySQL built-in stores.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "iceberg/result.h"

namespace iceberg::sql {

struct ConnectionUri {
  std::string user;
  std::string password;
  std::string host;
  std::optional<uint32_t> port;
  std::string database;
};

/// \brief Parse a `scheme://[user[:password]@]host[:port][/database]` URI.
///
/// All components are optional; missing pieces are returned empty/unset. A
/// leading `scheme://` is stripped when present. Malformed syntax that cannot
/// be passed unambiguously to a SQL driver is returned as `InvalidArgument`.
Result<ConnectionUri> ParseConnectionUri(std::string_view uri);

/// \brief Render a connection URI with any password replaced by "***".
///
/// Connection URIs may embed credentials (`scheme://user:password@host`).
/// Use this rendering whenever a URI is interpolated into an error message
/// or log line so the password never reaches an audience broader than the
/// credential's. The input is returned unchanged when no password is present.
std::string RedactConnectionUri(std::string_view uri);

}  // namespace iceberg::sql
