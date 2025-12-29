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

#include <algorithm>
#include <memory>
#include <ranges>

#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"

namespace iceberg {

void TableUpdateContext::AddRequirement(std::unique_ptr<TableRequirement> requirement) {
  requirements_.emplace_back(std::move(requirement));
}

Result<std::vector<std::unique_ptr<TableRequirement>>> TableUpdateContext::Build() {
  return std::move(requirements_);
}

void TableUpdateContext::RequireLastAssignedFieldIdUnchanged() {
  if (!added_last_assigned_field_id_) {
    if (base_ != nullptr) {
      AddRequirement(
          std::make_unique<table::AssertLastAssignedFieldId>(base_->last_column_id));
    }
    added_last_assigned_field_id_ = true;
  }
}

void TableUpdateContext::RequireCurrentSchemaIdUnchanged() {
  if (!added_current_schema_id_) {
    if (base_ != nullptr && !is_replace_) {
      AddRequirement(
          std::make_unique<table::AssertCurrentSchemaID>(base_->current_schema_id));
    }
    added_current_schema_id_ = true;
  }
}

void TableUpdateContext::RequireLastAssignedPartitionIdUnchanged() {
  if (!added_last_assigned_partition_id_) {
    if (base_ != nullptr) {
      AddRequirement(std::make_unique<table::AssertLastAssignedPartitionId>(
          base_->last_partition_id));
    }
    added_last_assigned_partition_id_ = true;
  }
}

void TableUpdateContext::RequireDefaultSpecIdUnchanged() {
  if (!added_default_spec_id_) {
    if (base_ != nullptr && !is_replace_) {
      AddRequirement(
          std::make_unique<table::AssertDefaultSpecID>(base_->default_spec_id));
    }
    added_default_spec_id_ = true;
  }
}

void TableUpdateContext::RequireDefaultSortOrderIdUnchanged() {
  if (!added_default_sort_order_id_) {
    if (base_ != nullptr && !is_replace_) {
      AddRequirement(std::make_unique<table::AssertDefaultSortOrderID>(
          base_->default_sort_order_id));
    }
    added_default_sort_order_id_ = true;
  }
}

void TableUpdateContext::RequireNoBranchesChanged() {
  if (base_ != nullptr && !is_replace_) {
    for (const auto& [name, ref] : base_->refs) {
      if (ref->type() == SnapshotRefType::kBranch && name != SnapshotRef::kMainBranch) {
        AddRequirement(
            std::make_unique<table::AssertRefSnapshotID>(name, ref->snapshot_id));
      }
    }
  }
}

bool TableUpdateContext::AddChangedRef(const std::string& ref_name) {
  auto [_, inserted] = changed_refs_.insert(ref_name);
  return inserted;
}

Result<std::vector<std::unique_ptr<TableRequirement>>> TableRequirements::ForCreateTable(
    const std::vector<std::unique_ptr<TableUpdate>>& table_updates) {
  TableUpdateContext context(nullptr, false);
  context.AddRequirement(std::make_unique<table::AssertDoesNotExist>());
  for (const auto& update : table_updates) {
    update->GenerateRequirements(context);
  }
  return context.Build();
}

Result<std::vector<std::unique_ptr<TableRequirement>>> TableRequirements::ForReplaceTable(
    const TableMetadata& base,
    const std::vector<std::unique_ptr<TableUpdate>>& table_updates) {
  TableUpdateContext context(&base, true);
  context.AddRequirement(std::make_unique<table::AssertUUID>(base.table_uuid));
  for (const auto& update : table_updates) {
    update->GenerateRequirements(context);
  }
  return context.Build();
}

Result<std::vector<std::unique_ptr<TableRequirement>>> TableRequirements::ForUpdateTable(
    const TableMetadata& base,
    const std::vector<std::unique_ptr<TableUpdate>>& table_updates) {
  TableUpdateContext context(&base, false);
  context.AddRequirement(std::make_unique<table::AssertUUID>(base.table_uuid));
  for (const auto& update : table_updates) {
    update->GenerateRequirements(context);
  }
  return context.Build();
}

Result<bool> TableRequirements::IsCreate(
    const std::vector<std::unique_ptr<TableRequirement>>& requirements) {
  bool is_create = std::ranges::any_of(requirements, [](const auto& req) {
    return req->kind() == TableRequirement::Kind::kAssertDoesNotExist;
  });

  if (is_create) {
    ICEBERG_PRECHECK(
        requirements.size() == 1,
        "Cannot have other requirements than AssertDoesNotExist in a table creation");
  }

  return is_create;
}

}  // namespace iceberg
