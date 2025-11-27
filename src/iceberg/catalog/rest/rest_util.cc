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

#include "iceberg/catalog/rest/rest_util.h"

#include <cpr/util.h>

#include "iceberg/table_identifier.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {
const std::string kNamespaceEscapeSeparator = "%1F";
}

std::string_view TrimTrailingSlash(std::string_view str) {
  while (!str.empty() && str.back() == '/') {
    str.remove_suffix(1);
  }
  return str;
}

Result<std::string> EncodeString(std::string_view str_to_encode) {
  if (str_to_encode.empty()) {
    return "";
  }

  // Use CPR's urlEncode which internally calls libcurl's curl_easy_escape()
  cpr::util::SecureString encoded = cpr::util::urlEncode(str_to_encode);
  if (encoded.empty()) {
    return InvalidArgument("Failed to encode string '{}'", str_to_encode);
  }

  return std::string{encoded.data(), encoded.size()};
}

Result<std::string> DecodeString(std::string_view str_to_decode) {
  if (str_to_decode.empty()) {
    return "";
  }

  // Use CPR's urlDecode which internally calls libcurl's curl_easy_unescape()
  cpr::util::SecureString decoded = cpr::util::urlDecode(str_to_decode);
  if (decoded.empty()) {
    return InvalidArgument("Failed to decode string '{}'", str_to_decode);
  }

  return std::string{decoded.data(), decoded.size()};
}

Result<std::string> EncodeNamespace(const Namespace& ns_to_encode) {
  if (ns_to_encode.levels.empty()) {
    return "";
  }

  ICEBERG_ASSIGN_OR_RAISE(std::string result, EncodeString(ns_to_encode.levels.front()));

  for (size_t i = 1; i < ns_to_encode.levels.size(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(std::string encoded_level,
                            EncodeString(ns_to_encode.levels[i]));
    result.append(kNamespaceEscapeSeparator);
    result.append(std::move(encoded_level));
  }

  return result;
}

Result<Namespace> DecodeNamespace(std::string_view str_to_decode) {
  if (str_to_decode.empty()) {
    return Namespace{.levels = {}};
  }

  Namespace ns{};
  std::string::size_type start = 0;
  std::string::size_type end = str_to_decode.find(kNamespaceEscapeSeparator);

  while (end != std::string::npos) {
    ICEBERG_ASSIGN_OR_RAISE(std::string decoded_level,
                            DecodeString(str_to_decode.substr(start, end - start)));
    ns.levels.push_back(std::move(decoded_level));
    start = end + kNamespaceEscapeSeparator.size();
    end = str_to_decode.find(kNamespaceEscapeSeparator, start);
  }

  ICEBERG_ASSIGN_OR_RAISE(std::string decoded_level,
                          DecodeString(str_to_decode.substr(start)));
  ns.levels.push_back(std::move(decoded_level));
  return ns;
}

std::unordered_map<std::string, std::string> MergeConfigs(
    const std::unordered_map<std::string, std::string>& server_defaults,
    const std::unordered_map<std::string, std::string>& client_configs,
    const std::unordered_map<std::string, std::string>& server_overrides) {
  // Merge with precedence: server_overrides > client_configs > server_defaults
  auto merged = server_defaults;
  for (const auto& [key, value] : client_configs) {
    merged.insert_or_assign(key, value);
  }
  for (const auto& [key, value] : server_overrides) {
    merged.insert_or_assign(key, value);
  }
  return merged;
}

}  // namespace iceberg::rest
