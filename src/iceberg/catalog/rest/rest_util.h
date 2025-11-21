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

#include <cctype>
#include <format>
#include <string>
#include <string_view>
#include <unordered_map>

#include "iceberg/table_identifier.h"

namespace iceberg::rest {

/// \brief Trim a single trailing slash from a URI string_view.
/// \details If \p uri_sv ends with '/', remove that last character; otherwise the input
/// is returned unchanged.
/// \param uri_sv The URI string to trim.
/// \return The (possibly) trimmed URI string.
inline std::string TrimTrailingSlash(std::string uri_sv) {
  if (!uri_sv.empty() && uri_sv.back() == '/') {
    uri_sv.pop_back();
  }
  return uri_sv;
}

/// \brief Percent-encode a string as a URI component (RFC 3986).
/// \details Leaves unreserved characters [A–Z a–z 0–9 - _ . ~] as-is; all others are
/// percent-encoded using uppercase hexadecimal (e.g., space -> "%20").
/// \param value The string to encode.
/// \return The encoded string.
inline std::string EncodeUriComponent(std::string_view value) {
  std::string escaped;
  escaped.reserve(value.length());
  for (const unsigned char c : value) {
    if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      escaped += c;
    } else {
      std::format_to(std::back_inserter(escaped), "%{:02X}", c);
    }
  }
  return escaped;
}

/// \brief Encode a Namespace into a URL-safe component.
/// \details Joins \p ns.levels with the ASCII Unit Separator (0x1F), then percent-encodes
/// the result via EncodeUriComponent. Returns an empty string if there are no levels.
/// \param ns The namespace (sequence of path-like levels) to encode.
/// \return The percent-encoded namespace string suitable for URLs.
inline std::string EncodeNamespaceForUrl(const Namespace& ns) {
  if (ns.levels.empty()) {
    return "";
  }

  std::string joined_string;
  joined_string.append(ns.levels.front());
  for (size_t i = 1; i < ns.levels.size(); ++i) {
    joined_string.append("\x1F");
    joined_string.append(ns.levels[i]);
  }

  return EncodeUriComponent(joined_string);
}

/// \brief Merge catalog configuration properties.
/// \details Merges three sets of configuration properties following the precedence order:
/// server overrides > client configs > server defaults.
/// \param server_defaults Default properties provided by the server.
/// \param client_configs Configuration properties from the client.
/// \param server_overrides Override properties enforced by the server.
/// \return A merged map containing all properties with correct precedence.
inline std::unordered_map<std::string, std::string> MergeConfigs(
    const std::unordered_map<std::string, std::string>& server_defaults,
    const std::unordered_map<std::string, std::string>& client_configs,
    const std::unordered_map<std::string, std::string>& server_overrides) {
  auto merged = server_defaults;
  for (const auto& kv : client_configs) {
    merged.insert_or_assign(kv.first, kv.second);
  }
  for (const auto& kv : server_overrides) {
    merged.insert_or_assign(kv.first, kv.second);
  }
  return merged;
}

}  // namespace iceberg::rest
