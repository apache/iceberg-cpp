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

#include "iceberg/table_requirements.h"

#include <memory>

#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/util/macros.h"

namespace iceberg {

void TableUpdateContext::AddRequirement(std::unique_ptr<TableRequirement> requirement) {
  requirements_.emplace_back(std::move(requirement));
}

Result<std::vector<std::unique_ptr<TableRequirement>>> TableUpdateContext::Build() {
  return std::move(requirements_);
}

Result<std::vector<std::unique_ptr<TableRequirement>>> TableRequirements::ForCreateTable(
    const std::vector<std::unique_ptr<TableUpdate>>& table_updates) {
  TableUpdateContext context(nullptr, false);
  context.AddRequirement(std::make_unique<table::AssertDoesNotExist>());
  for (const auto& update : table_updates) {
    ICEBERG_RETURN_UNEXPECTED(update->GenerateRequirements(context));
  }
  return context.Build();
}

Result<std::vector<std::unique_ptr<TableRequirement>>> TableRequirements::ForReplaceTable(
    const TableMetadata& base,
    const std::vector<std::unique_ptr<TableUpdate>>& table_updates) {
  TableUpdateContext context(&base, true);
  context.AddRequirement(std::make_unique<table::AssertUUID>(base.table_uuid));
  for (const auto& update : table_updates) {
    ICEBERG_RETURN_UNEXPECTED(update->GenerateRequirements(context));
  }
  return context.Build();
}

Result<std::vector<std::unique_ptr<TableRequirement>>> TableRequirements::ForUpdateTable(
    const TableMetadata& base,
    const std::vector<std::unique_ptr<TableUpdate>>& table_updates) {
  TableUpdateContext context(&base, false);
  context.AddRequirement(std::make_unique<table::AssertUUID>(base.table_uuid));
  for (const auto& update : table_updates) {
    ICEBERG_RETURN_UNEXPECTED(update->GenerateRequirements(context));
  }
  return context.Build();
}

}  // namespace iceberg
