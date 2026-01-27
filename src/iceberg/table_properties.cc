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

const std::unordered_set<std::string>& TableProperties::commit_properties() {
  static const std::unordered_set<std::string> kCommitProperties = {
      kCommitNumRetries.key(), kCommitMinRetryWaitMs.key(), kCommitMaxRetryWaitMs.key(),
      kCommitTotalRetryTimeMs.key()};
  return kCommitProperties;
}

TableProperties TableProperties::FromMap(
    std::unordered_map<std::string, std::string> properties) {
  TableProperties table_properties;
  table_properties.configs_ = std::move(properties);
  return table_properties;
}

}  // namespace iceberg
