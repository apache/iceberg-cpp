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

#include <string>
#include <string_view>
#include <unordered_map>

#include <cpr/util.h>

namespace iceberg::rest {

std::string_view TrimTrailingSlash(std::string_view str) {
  while (!str.empty() && str.back() == '/') {
    str.remove_suffix(1);
  }
  return str;
}

std::string EncodeString(std::string_view toEncode) {
  // Use CPR's urlEncode which internally calls libcurl's curl_easy_escape()
  cpr::util::SecureString encoded = cpr::util::urlEncode(toEncode);
  return {encoded.data(), encoded.size()};
}

std::string DecodeString(std::string_view encoded) {
  // Use CPR's urlDecode which internally calls libcurl's curl_easy_unescape()
  cpr::util::SecureString decoded = cpr::util::urlDecode(encoded);
  return {decoded.data(), decoded.size()};
}

std::string EncodeNamespaceForUrl(const Namespace& ns) {
  if (ns.levels.empty()) {
    return "";
  }
  std::string result = EncodeString(ns.levels.front());

  // Join encoded levels with "%1F"
  for (size_t i = 1; i < ns.levels.size(); ++i) {
    result.append("%1F");
    result.append(EncodeString(ns.levels[i]));
  }

  return result;
}

Namespace DecodeNamespaceFromUrl(std::string_view encoded) {
  if (encoded.empty()) {
    return Namespace{.levels = {}};
  }

  // Split by "%1F" first
  Namespace ns;
  std::string::size_type start = 0;
  std::string::size_type end = encoded.find("%1F");

  while (end != std::string::npos) {
    ns.levels.push_back(DecodeString(encoded.substr(start, end - start)));
    // Skip the 3-character "%1F" separator
    start = end + 3;
    end = encoded.find("%1F", start);
  }
  ns.levels.push_back(DecodeString(encoded.substr(start)));
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
