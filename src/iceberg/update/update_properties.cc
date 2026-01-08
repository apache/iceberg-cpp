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

#include "iceberg/update/update_properties.h"

#include <cstdint>
#include <memory>

#include "iceberg/metrics_config.h"
#include "iceberg/result.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

Result<std::shared_ptr<UpdateProperties>> UpdateProperties::Make(
    std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(transaction != nullptr,
                   "Cannot create UpdateProperties without a transaction");
  return std::shared_ptr<UpdateProperties>(new UpdateProperties(std::move(transaction)));
}

UpdateProperties::UpdateProperties(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {}

UpdateProperties::~UpdateProperties() = default;

UpdateProperties& UpdateProperties::Set(const std::string& key,
                                        const std::string& value) {
  ICEBERG_BUILDER_CHECK(!removals_.contains(key),
                        "Cannot set property '{}' that is already marked for removal",
                        key);

  if (!TableProperties::reserved_properties().contains(key) ||
      key == TableProperties::kFormatVersion.key()) {
    updates_.insert_or_assign(key, value);
  }

  return *this;
}

UpdateProperties& UpdateProperties::Remove(const std::string& key) {
  ICEBERG_BUILDER_CHECK(!updates_.contains(key),
                        "Cannot remove property '{}' that is already marked for update",
                        key);
  removals_.insert(key);
  return *this;
}

Result<UpdateProperties::ApplyResult> UpdateProperties::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  const auto& current_props = base().properties.configs();
  std::unordered_map<std::string, std::string> new_properties;
  std::vector<std::string> removals;
  for (const auto& [key, value] : current_props) {
    if (!removals_.contains(key)) {
      new_properties[key] = value;
    }
  }

  for (const auto& [key, value] : updates_) {
    new_properties[key] = value;
  }

  auto iter = new_properties.find(TableProperties::kFormatVersion.key());
  if (iter != new_properties.end()) {
    ICEBERG_ASSIGN_OR_RAISE(auto parsed_version,
                            StringUtils::ParseInt<int32_t>(iter->second));

    if (parsed_version > TableMetadata::kSupportedTableFormatVersion) {
      return InvalidArgument(
          "Cannot upgrade table to unsupported format version: v{} (supported: v{})",
          parsed_version, TableMetadata::kSupportedTableFormatVersion);
    }
    format_version_ = static_cast<int8_t>(parsed_version);

    updates_.erase(TableProperties::kFormatVersion.key());
  }

  if (auto schema = base().Schema(); schema.has_value()) {
    ICEBERG_RETURN_UNEXPECTED(
        MetricsConfig::VerifyReferencedColumns(new_properties, *schema.value()));
  }
  return ApplyResult{
      .updates = updates_, .removals = removals_, .format_version = format_version_};
}

}  // namespace iceberg
