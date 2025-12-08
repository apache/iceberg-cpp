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

#include "iceberg/catalog.h"
#include "iceberg/metrics_config.h"
#include "iceberg/result.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/util/macros.h"

namespace iceberg {

UpdateProperties::UpdateProperties(TableIdentifier identifier,
                                   std::shared_ptr<Catalog> catalog,
                                   std::shared_ptr<TableMetadata> base)
    : identifier_(std::move(identifier)),
      catalog_(std::move(catalog)),
      base_metadata_(std::move(base)) {}

UpdateProperties& UpdateProperties::Set(const std::string& key,
                                        const std::string& value) {
  if (removals_.contains(key)) {
    return AddError(
        ErrorKind::kInvalidArgument,
        std::format("Cannot set property '{}' that is already marked for removal", key));
  }

  if (!TableProperties::reserved_properties().contains(key) ||
      key == TableProperties::kFormatVersion.key()) {
    updates_.emplace(key, value);
  }

  return *this;
}

UpdateProperties& UpdateProperties::Remove(const std::string& key) {
  if (updates_.contains(key)) {
    return AddError(
        ErrorKind::kInvalidArgument,
        std::format("Cannot remove property '{}' that is already marked for update",
                    key));
  }

  removals_.insert(key);
  return *this;
}

Status UpdateProperties::Apply() {
  if (!catalog_) {
    return InvalidArgument("Catalog is required to apply property updates");
  }
  if (!base_metadata_) {
    return InvalidArgument("Base table metadata is required to apply property updates");
  }

  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  auto iter = updates_.find(TableProperties::kFormatVersion.key());
  if (iter != updates_.end()) {
    try {
      int parsed_version = std::stoi(iter->second);
      if (parsed_version > TableMetadata::kSupportedTableFormatVersion) {
        return InvalidArgument(
            "Cannot upgrade table to unsupported format version: v{} (supported: v{})",
            parsed_version, TableMetadata::kSupportedTableFormatVersion);
      }
      format_version_ = static_cast<int8_t>(parsed_version);
    } catch (const std::invalid_argument& e) {
      return InvalidArgument("Invalid format version '{}': not a valid integer",
                             iter->second);
    } catch (const std::out_of_range& e) {
      return InvalidArgument("Format version '{}' is out of range", iter->second);
    }

    updates_.erase(iter);
  }

  if (auto schema = base_metadata_->Schema(); schema.has_value()) {
    ICEBERG_RETURN_UNEXPECTED(
        MetricsConfig::VerifyReferencedColumns(updates_, *schema.value()));
  }
  return {};
}

Status UpdateProperties::Commit() {
  ICEBERG_RETURN_UNEXPECTED(Apply());

  std::vector<std::unique_ptr<TableUpdate>> updates;
  if (!updates_.empty()) {
    updates.emplace_back(std::make_unique<table::SetProperties>(std::move(updates_)));
  }
  if (!removals_.empty()) {
    updates.emplace_back(std::make_unique<table::RemoveProperties>(
        std::vector<std::string>{removals_.begin(), removals_.end()}));
  }
  if (format_version_.has_value()) {
    updates.emplace_back(
        std::make_unique<table::UpgradeFormatVersion>(format_version_.value()));
  };

  if (!updates.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto requirements,
                            TableRequirements::ForUpdateTable(*base_metadata_, updates));
    ICEBERG_RETURN_UNEXPECTED(catalog_->UpdateTable(identifier_, requirements, updates));
  }
  return {};
}

}  // namespace iceberg
