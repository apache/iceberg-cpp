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

#include "iceberg/catalog/rest/auth/auth_properties.h"

#include <utility>

#include "iceberg/catalog/rest/catalog_properties.h"

namespace iceberg::rest::auth {

namespace {

std::pair<std::string, std::string> ParseCredential(const std::string& credential) {
  auto colon_pos = credential.find(':');
  if (colon_pos == std::string::npos) {
    return {"", credential};
  }
  return {credential.substr(0, colon_pos), credential.substr(colon_pos + 1)};
}

}  // namespace

std::unordered_map<std::string, std::string> AuthProperties::optional_oauth_params()
    const {
  std::unordered_map<std::string, std::string> params;
  if (auto audience = Get(kAudience); !audience.empty()) {
    params.emplace(kAudience.key(), std::move(audience));
  }
  if (auto resource = Get(kResource); !resource.empty()) {
    params.emplace(kResource.key(), std::move(resource));
  }
  return params;
}

Result<AuthProperties> AuthProperties::FromProperties(
    const std::unordered_map<std::string, std::string>& properties) {
  AuthProperties config;
  config.configs_ = properties;

  // Parse client_id/client_secret from credential
  if (auto cred = config.credential(); !cred.empty()) {
    auto [id, secret] = ParseCredential(cred);
    config.client_id_ = std::move(id);
    config.client_secret_ = std::move(secret);
  }

  // Resolve token endpoint: if not explicitly set, derive from catalog URI
  if (properties.find(kOAuth2ServerUri.key()) == properties.end() ||
      properties.at(kOAuth2ServerUri.key()).empty()) {
    auto uri_it = properties.find(RestCatalogProperties::kUri.key());
    if (uri_it != properties.end() && !uri_it->second.empty()) {
      std::string_view base = uri_it->second;
      while (!base.empty() && base.back() == '/') {
        base.remove_suffix(1);
      }
      config.Set(kOAuth2ServerUri,
                 std::string(base) + "/" + std::string(kOAuth2ServerUri.value()));
    }
  }

  // TODO(lishuxu): Parse JWT exp claim from token to set expires_at_millis_.

  return config;
}

}  // namespace iceberg::rest::auth
