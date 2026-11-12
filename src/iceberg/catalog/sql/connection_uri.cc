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

#include <charconv>
#include <string>
#include <system_error>

#include "iceberg/catalog/sql/connection_uri_internal.h"
#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg::sql {

namespace {

Result<uint32_t> ParsePort(std::string_view port_str, std::string_view uri) {
  if (port_str.empty()) {
    return InvalidArgument("Invalid SQL connection URI '{}': port is empty", uri);
  }

  uint32_t port = 0;
  const auto* begin = port_str.data();
  const auto* end = begin + port_str.size();
  const auto [ptr, error] = std::from_chars(begin, end, port);
  if (error == std::errc::result_out_of_range) {
    return InvalidArgument("Invalid SQL connection URI '{}': port is out of range", uri);
  }
  if (error != std::errc{} || ptr != end) {
    return InvalidArgument("Invalid SQL connection URI '{}': port is not numeric", uri);
  }

  return port;
}

}  // namespace

std::string RedactConnectionUri(std::string_view uri) {
  // Locate the authority section, mirroring ParseConnectionUri: after
  // "scheme://" when present, up to the first '/'.
  size_t authority_begin = 0;
  if (const auto scheme = uri.find("://"); scheme != std::string_view::npos) {
    authority_begin = scheme + 3;
  }
  std::string_view authority = uri.substr(authority_begin);
  if (const auto slash = authority.find('/'); slash != std::string_view::npos) {
    authority = authority.substr(0, slash);
  }
  const auto at = authority.rfind('@');
  if (at == std::string_view::npos) {
    return std::string(uri);
  }
  const std::string_view userinfo = authority.substr(0, at);
  const auto colon = userinfo.find(':');
  if (colon == std::string_view::npos) {
    return std::string(uri);  // no password present
  }
  const size_t password_begin = authority_begin + colon + 1;
  const size_t password_end = authority_begin + at;
  std::string redacted(uri);
  redacted.replace(password_begin, password_end - password_begin, "***");
  return redacted;
}

Result<ConnectionUri> ParseConnectionUri(std::string_view uri) {
  ConnectionUri result;
  // Error messages must never echo an embedded password.
  const std::string redacted = RedactConnectionUri(uri);

  const bool has_scheme = uri.find("://") != std::string_view::npos;
  if (const auto scheme = uri.find("://"); scheme != std::string_view::npos) {
    uri.remove_prefix(scheme + 3);
  }

  // Split off the path (database) after the first '/'.
  std::string_view authority = uri;
  if (const auto slash = uri.find('/'); slash != std::string_view::npos) {
    authority = uri.substr(0, slash);
    result.database = std::string(uri.substr(slash + 1));
  }
  if (has_scheme && authority.empty()) {
    return InvalidArgument("Invalid SQL connection URI '{}': authority is empty",
                           redacted);
  }

  // Split userinfo from host at the last '@'.
  std::string_view host_port = authority;
  if (const auto at = authority.rfind('@'); at != std::string_view::npos) {
    std::string_view userinfo = authority.substr(0, at);
    host_port = authority.substr(at + 1);
    if (const auto colon = userinfo.find(':'); colon != std::string_view::npos) {
      result.user = std::string(userinfo.substr(0, colon));
      result.password = std::string(userinfo.substr(colon + 1));
    } else {
      result.user = std::string(userinfo);
    }
  }

  // Split host from port at the last ':'.
  if (const auto colon = host_port.rfind(':'); colon != std::string_view::npos) {
    result.host = std::string(host_port.substr(0, colon));
    ICEBERG_ASSIGN_OR_RAISE(result.port,
                            ParsePort(host_port.substr(colon + 1), redacted));
  } else {
    result.host = std::string(host_port);
  }

  return result;
}

}  // namespace iceberg::sql
