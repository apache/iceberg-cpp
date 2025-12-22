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

#include <charconv>
#include <cstdint>
#include <memory>
#include <system_error>

#include "iceberg/metrics_config.h"
#include "iceberg/result.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_update.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<UpdateProperties>> UpdateProperties::Make(
    std::shared_ptr<Transaction> transaction) {
  if (!transaction) [[unlikely]] {
    return InvalidArgument("Cannot create UpdateProperties without a transaction");
  }
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

Result<PendingUpdate::ApplyResult> UpdateProperties::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  const auto& current_props = transaction_->current().properties.configs();
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
    int parsed_version = 0;
    const auto& val = iter->second;
    auto [ptr, ec] = std::from_chars(val.data(), val.data() + val.size(), parsed_version);

    if (ec == std::errc::invalid_argument) {
      return InvalidArgument("Invalid format version '{}': not a valid integer", val);
    } else if (ec == std::errc::result_out_of_range) {
      return InvalidArgument("Format version '{}' is out of range", val);
    }

    if (parsed_version > TableMetadata::kSupportedTableFormatVersion) {
      return InvalidArgument(
          "Cannot upgrade table to unsupported format version: v{} (supported: v{})",
          parsed_version, TableMetadata::kSupportedTableFormatVersion);
    }
    format_version_ = static_cast<int8_t>(parsed_version);

    updates_.erase(TableProperties::kFormatVersion.key());
  }

  if (auto schema = transaction_->current().Schema(); schema.has_value()) {
    ICEBERG_RETURN_UNEXPECTED(
        MetricsConfig::VerifyReferencedColumns(new_properties, *schema.value()));
  }

  ApplyResult result;
  if (!updates_.empty()) {
    result.updates.emplace_back(std::make_unique<table::SetProperties>(updates_));
  }
  if (!removals_.empty()) {
    for (const auto& key : removals_) {
      if (current_props.contains(key)) {
        removals.push_back(key);
      }
    }
    if (!removals.empty()) {
      result.updates.emplace_back(std::make_unique<table::RemoveProperties>(removals));
    }
  }
  if (format_version_.has_value()) {
    result.updates.emplace_back(
        std::make_unique<table::UpgradeFormatVersion>(format_version_.value()));
  };

  return result;
}

}  // namespace iceberg
