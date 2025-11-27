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

#include "iceberg/catalog/rest/catalog_properties.h"

#include <string_view>

namespace iceberg::rest {

std::unique_ptr<RestCatalogProperties> RestCatalogProperties::default_properties() {
  return std::unique_ptr<RestCatalogProperties>(new RestCatalogProperties());
}

std::unique_ptr<RestCatalogProperties> RestCatalogProperties::FromMap(
    const std::unordered_map<std::string, std::string>& properties) {
  auto rest_catalog_config =
      std::unique_ptr<RestCatalogProperties>(new RestCatalogProperties());
  rest_catalog_config->configs_ = properties;
  return rest_catalog_config;
}

std::unordered_map<std::string, std::string> RestCatalogProperties::ExtractHeaders()
    const {
  return Extract(kHeaderPrefix);
}

Result<std::string_view> RestCatalogProperties::Uri() const {
  auto it = configs_.find(kUri.key());
  if (it == configs_.end() || it->second.empty()) {
    return InvalidArgument("Rest catalog configuration property 'uri' is required.");
  }
  return it->second;
}

}  // namespace iceberg::rest
