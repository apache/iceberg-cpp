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

#include "iceberg/table_properties.h"

namespace iceberg {

const std::unordered_set<std::string>& TableProperties::reserved_properties() {
  static const std::unordered_set<std::string> kReservedProperties = {
      kFormatVersion.key(),          kUuid.key(),
      kSnapshotCount.key(),          kCurrentSnapshotId.key(),
      kCurrentSnapshotSummary.key(), kCurrentSnapshotTimestamp.key(),
      kCurrentSchema.key(),          kDefaultPartitionSpec.key(),
      kDefaultSortOrder.key()};
  return kReservedProperties;
}

std::unique_ptr<TableProperties> TableProperties::default_properties() {
  return std::make_unique<TableProperties>();
}

std::unique_ptr<TableProperties> TableProperties::FromMap(
    const std::unordered_map<std::string, std::string>& properties) {
  auto table_properties = std::make_unique<TableProperties>();
  for (const auto& [key, value] : properties) {  // NOLINT(modernize-type-traits)
    table_properties->configs_[key] = value;
  }
  return table_properties;
}

}  // namespace iceberg
