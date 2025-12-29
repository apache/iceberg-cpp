
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
#include "iceberg/transaction.h"

#include <memory>

#include "iceberg/catalog.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/update/pending_update.h"
#include "iceberg/update/update_partition_spec.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/update/update_sort_order.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Transaction::Transaction(std::shared_ptr<Table> table, Kind kind, bool auto_commit,
                         std::unique_ptr<TableMetadataBuilder> metadata_builder)
    : table_(std::move(table)),
      kind_(kind),
      auto_commit_(auto_commit),
      metadata_builder_(std::move(metadata_builder)) {}

Transaction::~Transaction() = default;

Result<std::shared_ptr<Transaction>> Transaction::Make(std::shared_ptr<Table> table,
                                                       Kind kind, bool auto_commit) {
  if (!table || !table->catalog()) [[unlikely]] {
    return InvalidArgument("Table and catalog cannot be null");
  }

  std::unique_ptr<TableMetadataBuilder> metadata_builder;
  if (kind == Kind::kCreate) {
    metadata_builder = TableMetadataBuilder::BuildFromEmpty();
    std::ignore = metadata_builder->ApplyChangesForCreate(*table->metadata());
  } else {
    metadata_builder = TableMetadataBuilder::BuildFrom(table->metadata().get());
  }

  return std::shared_ptr<Transaction>(
      new Transaction(std::move(table), kind, auto_commit, std::move(metadata_builder)));
}

const TableMetadata* Transaction::base() const { return metadata_builder_->base(); }

const TableMetadata& Transaction::current() const { return metadata_builder_->current(); }

Status Transaction::AddUpdate(const std::shared_ptr<PendingUpdate>& update) {
  if (!last_update_committed_) {
    return InvalidArgument("Cannot add update when previous update is not committed");
  }
  pending_updates_.emplace_back(std::weak_ptr<PendingUpdate>(update));
  last_update_committed_ = false;
  return {};
}

Status Transaction::Apply(PendingUpdate& update) {
  switch (update.kind()) {
    case PendingUpdate::Kind::kUpdateProperties: {
      auto& update_properties = internal::checked_cast<UpdateProperties&>(update);
      ICEBERG_ASSIGN_OR_RAISE(auto result, update_properties.Apply());
      if (!result.updates.empty()) {
        metadata_builder_->SetProperties(std::move(result.updates));
      }
      if (!result.removals.empty()) {
        metadata_builder_->RemoveProperties(std::move(result.removals));
      }
      if (result.format_version.has_value()) {
        metadata_builder_->UpgradeFormatVersion(result.format_version.value());
      }
    } break;
    case PendingUpdate::Kind::kUpdateSortOrder: {
      auto& update_sort_order = internal::checked_cast<UpdateSortOrder&>(update);
      ICEBERG_ASSIGN_OR_RAISE(auto sort_order, update_sort_order.Apply());
      metadata_builder_->SetDefaultSortOrder(std::move(sort_order));
    } break;
    case PendingUpdate::Kind::kUpdatePartitionSpec: {
      auto& update_partition_spec = internal::checked_cast<UpdatePartitionSpec&>(update);
      ICEBERG_ASSIGN_OR_RAISE(auto result, update_partition_spec.Apply());
      if (result.set_as_default) {
        metadata_builder_->SetDefaultPartitionSpec(std::move(result.spec));
      } else {
        metadata_builder_->AddPartitionSpec(std::move(result.spec));
      }
    } break;
    default:
      return NotSupported("Unsupported pending update: {}",
                          static_cast<int>(update.kind()));
  }

  last_update_committed_ = true;

  if (auto_commit_) {
    ICEBERG_RETURN_UNEXPECTED(Commit());
  }

  return {};
}

Result<std::shared_ptr<Table>> Transaction::Commit() {
  if (committed_) {
    return Invalid("Transaction already committed");
  }
  if (!last_update_committed_) {
    return InvalidArgument(
        "Cannot commit transaction when previous update is not committed");
  }

  const auto& updates = metadata_builder_->changes();
  if (updates.empty()) {
    committed_ = true;
    return table_;
  }

  std::vector<std::unique_ptr<TableRequirement>> requirements;
  switch (kind_) {
    case Kind::kCreate: {
      ICEBERG_ASSIGN_OR_RAISE(requirements, TableRequirements::ForCreateTable(updates));
    } break;
    case Kind::kUpdate: {
      ICEBERG_ASSIGN_OR_RAISE(requirements, TableRequirements::ForUpdateTable(
                                                *metadata_builder_->base(), updates));

    } break;
  }

  // XXX: we should handle commit failure and retry here.
  ICEBERG_ASSIGN_OR_RAISE(auto updated_table, table_->catalog()->UpdateTable(
                                                  table_->name(), requirements, updates));

  // Mark as committed and update table reference
  committed_ = true;
  table_ = std::move(updated_table);

  return table_;
}

Result<std::shared_ptr<UpdatePartitionSpec>> Transaction::NewUpdatePartitionSpec() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdatePartitionSpec> update_spec,
                          UpdatePartitionSpec::Make(shared_from_this()));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_spec));
  return update_spec;
}

Result<std::shared_ptr<UpdateProperties>> Transaction::NewUpdateProperties() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateProperties> update_properties,
                          UpdateProperties::Make(shared_from_this()));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_properties));
  return update_properties;
}

Result<std::shared_ptr<UpdateSortOrder>> Transaction::NewUpdateSortOrder() {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<UpdateSortOrder> update_sort_order,
                          UpdateSortOrder::Make(shared_from_this()));
  ICEBERG_RETURN_UNEXPECTED(AddUpdate(update_sort_order));
  return update_sort_order;
}

}  // namespace iceberg
