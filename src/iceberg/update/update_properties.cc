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

#include <format>
#include <vector>

#include "iceberg/catalog.h"
#include "iceberg/exception.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/util/macros.h"

namespace iceberg {

PropertiesUpdate::PropertiesUpdate(Table* table) : table_(table) {}

PropertiesUpdate& PropertiesUpdate::Set(std::string key, std::string value) {
  if (key.empty()) {
    throw IcebergError("Property key cannot be empty");
  }
  if (removals_.contains(key)) {
    throw IcebergError(std::format("Cannot remove and update the same key: {}", key));
  }

  updates_[std::move(key)] = std::move(value);
  return *this;
}

PropertiesUpdate& PropertiesUpdate::Remove(std::string key) {
  if (key.empty()) {
    throw IcebergError("Property key cannot be empty");
  }
  if (updates_.contains(key)) {
    throw IcebergError(std::format("Cannot remove and update the same key: {}", key));
  }

  removals_.insert(std::move(key));
  return *this;
}

PropertiesUpdate& PropertiesUpdate::DefaultFormat(FileFormatType format) {
  return Set(std::string(TableProperties::kDefaultFileFormat.key()),
             std::string(ToString(format)));
}

Result<std::unordered_map<std::string, std::string>> PropertiesUpdate::Apply() {
  if (table_ == nullptr) {
    return InvalidArgument("Cannot apply updates on a null table");
  }

  if (table_->catalog_) {
    if (auto status = table_->Refresh(); !status) {
      return std::unexpected(status.error());
    }
  }

  std::unordered_map<std::string, std::string> new_properties =
      table_->properties().configs();

  for (const auto& key : removals_) {
    new_properties.erase(key);
  }
  for (const auto& [key, value] : updates_) {
    new_properties[key] = value;
  }

  return new_properties;
}

Status PropertiesUpdate::Commit() {
  if (table_ == nullptr) {
    return InvalidArgument("Cannot commit updates on a null table");
  }

  if (updates_.empty() && removals_.empty()) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto applied, Apply());
  (void)applied;  // apply for validation

  if (!table_->catalog_) {
    return NotSupported("Commit requires a catalog-backed table");
  }

  std::vector<std::unique_ptr<TableRequirement>> requirements;
  std::vector<std::unique_ptr<TableUpdate>> updates;

  if (!updates_.empty()) {
    updates.push_back(std::make_unique<table::SetProperties>(updates_));
  }
  if (!removals_.empty()) {
    std::vector<std::string> removed(removals_.begin(), removals_.end());
    updates.push_back(std::make_unique<table::RemoveProperties>(std::move(removed)));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto updated_table, table_->catalog_->UpdateTable(
                                                  table_->name(), requirements, updates));

  table_->metadata_ = std::move(updated_table->metadata_);
  table_->metadata_location_ = std::move(updated_table->metadata_location_);
  table_->io_ = std::move(updated_table->io_);
  table_->properties_ = std::move(updated_table->properties_);
  table_->metadata_cache_ = std::make_unique<TableMetadataCache>(table_->metadata_.get());

  return {};
}

}  // namespace iceberg
