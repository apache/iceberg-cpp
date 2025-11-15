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

#include "iceberg/catalog/rest/config.h"

#include "iceberg/catalog/rest/constant.h"

namespace iceberg::rest {

std::unique_ptr<RestCatalogConfig> RestCatalogConfig::default_properties() {
  return std::make_unique<RestCatalogConfig>();
}

std::unique_ptr<RestCatalogConfig> RestCatalogConfig::FromMap(
    const std::unordered_map<std::string, std::string>& properties) {
  auto rest_catalog_config = std::make_unique<RestCatalogConfig>();
  rest_catalog_config->configs_ = properties;
  return rest_catalog_config;
}

Result<cpr::Header> RestCatalogConfig::GetExtraHeaders() const {
  cpr::Header headers;

  headers[std::string(kHeaderContentType)] = std::string(kMimeTypeApplicationJson);
  headers[std::string(kHeaderUserAgent)] = std::string(kUserAgent);
  headers[std::string(kHeaderXClientVersion)] = std::string(kClientVersion);

  constexpr std::string_view prefix = "header.";
  for (const auto& [key, value] : configs_) {
    if (key.starts_with(prefix)) {
      std::string_view header_name = std::string_view(key).substr(prefix.length());

      if (header_name.empty()) {
        return InvalidArgument("Header name cannot be empty after '{}' prefix", prefix);
      }

      if (value.empty()) {
        return InvalidArgument("Header value for '{}' cannot be empty", header_name);
      }
      headers[std::string(header_name)] = value;
    }
  }
  return headers;
}

}  // namespace iceberg::rest
