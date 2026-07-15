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

#include "iceberg/catalog/hive/hive_catalog_properties.h"

#include <string>
#include <string_view>

#include "iceberg/util/string_util.h"

namespace iceberg::hive {

HiveCatalogProperties HiveCatalogProperties::default_properties() { return {}; }

HiveCatalogProperties HiveCatalogProperties::FromMap(
    std::unordered_map<std::string, std::string> properties) {
  HiveCatalogProperties hive_catalog_config;
  hive_catalog_config.configs_ = std::move(properties);
  return hive_catalog_config;
}

Result<std::string_view> HiveCatalogProperties::Uri() const {
  auto it = configs_.find(kUri.key());
  if (it == configs_.end() || it->second.empty()) {
    return InvalidArgument("Hive catalog configuration property 'uri' is required.");
  }
  return it->second;
}

Result<HiveThriftTransport> HiveCatalogProperties::ThriftTransport() const {
  const std::string upper = StringUtils::ToUpper(Get(kThriftTransport));
  if (upper == "BUFFERED") {
    return HiveThriftTransport::kBuffered;
  }
  if (upper == "FRAMED") {
    return HiveThriftTransport::kFramed;
  }
  return InvalidArgument("Invalid Hive thrift transport: '{}'.", Get(kThriftTransport));
}

}  // namespace iceberg::hive
