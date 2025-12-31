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

#include "iceberg/update/update_schema.h"

#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/type.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<UpdateSchema>> UpdateSchema::Make(
    std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(transaction != nullptr,
                   "Cannot create UpdateSchema without transaction");
  return std::shared_ptr<UpdateSchema>(new UpdateSchema(std::move(transaction)));
}

UpdateSchema::UpdateSchema(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {
  const TableMetadata& base_metadata = transaction_->current();

  // Get the current schema
  auto schema_result = base_metadata.Schema();
  if (!schema_result.has_value()) {
    AddError(schema_result.error());
    return;
  }
  schema_ = std::move(schema_result.value());

  // Initialize last_column_id from base metadata
  last_column_id_ = base_metadata.last_column_id;

  // Initialize identifier field names from the current schema
  auto identifier_names_result = schema_->IdentifierFieldNames();
  if (!identifier_names_result.has_value()) {
    AddError(identifier_names_result.error());
    return;
  }
  identifier_field_names_ = identifier_names_result.value() |
                            std::ranges::to<std::unordered_set<std::string>>();
}

UpdateSchema::~UpdateSchema() = default;

UpdateSchema& UpdateSchema::AllowIncompatibleChanges() {
  allow_incompatible_changes_ = true;
  return *this;
}

UpdateSchema& UpdateSchema::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  return *this;
}

UpdateSchema& UpdateSchema::AddColumn(std::string_view name, std::shared_ptr<Type> type,
                                      std::string_view doc) {
  // Check for "." in top-level name
  ICEBERG_BUILDER_CHECK(!name.contains('.'),
                        "Cannot add column with ambiguous name: {}, use "
                        "AddColumn(parent, name, type, doc)",
                        name);
  return AddColumnInternal(std::nullopt, name, /*is_optional=*/true, std::move(type),
                           doc);
}

UpdateSchema& UpdateSchema::AddColumn(std::optional<std::string_view> parent,
                                      std::string_view name, std::shared_ptr<Type> type,
                                      std::string_view doc) {
  return AddColumnInternal(std::move(parent), name, /*is_optional=*/true, std::move(type),
                           doc);
}

UpdateSchema& UpdateSchema::AddRequiredColumn(std::string_view name,
                                              std::shared_ptr<Type> type,
                                              std::string_view doc) {
  // Check for "." in top-level name
  ICEBERG_BUILDER_CHECK(!name.contains('.'),
                        "Cannot add column with ambiguous name: {}, use "
                        "AddRequiredColumn(parent, name, type, doc)",
                        name);
  return AddColumnInternal(std::nullopt, name, /*is_optional=*/false, std::move(type),
                           doc);
}

UpdateSchema& UpdateSchema::AddRequiredColumn(std::optional<std::string_view> parent,
                                              std::string_view name,
                                              std::shared_ptr<Type> type,
                                              std::string_view doc) {
  return AddColumnInternal(std::move(parent), name, /*is_optional=*/false,
                           std::move(type), doc);
}

UpdateSchema& UpdateSchema::UpdateColumn(std::string_view name,
                                         std::shared_ptr<PrimitiveType> new_type) {
  // TODO(Guotao Yu): Implement UpdateColumn
  AddError(NotImplemented("UpdateSchema::UpdateColumn not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::UpdateColumnDoc(std::string_view name,
                                            std::string_view new_doc) {
  // TODO(Guotao Yu): Implement UpdateColumnDoc
  AddError(NotImplemented("UpdateSchema::UpdateColumnDoc not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::AddColumnInternal(std::optional<std::string_view> parent,
                                              std::string_view name, bool is_optional,
                                              std::shared_ptr<Type> type,
                                              std::string_view doc) {
  // TODO(Guotao Yu): Implement AddColumnInternal logic
  // This is where the real work happens - finding parent, validating, etc.
  AddError(NotImplemented("UpdateSchema::AddColumnInternal not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::RenameColumn(std::string_view name,
                                         std::string_view new_name) {
  // TODO(Guotao Yu): Implement RenameColumn
  AddError(NotImplemented("UpdateSchema::RenameColumn not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::MakeColumnOptional(std::string_view name) {
  // TODO(Guotao Yu): Implement MakeColumnOptional
  AddError(NotImplemented("UpdateSchema::MakeColumnOptional not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::RequireColumn(std::string_view name) {
  // TODO(Guotao Yu): Implement RequireColumn
  AddError(NotImplemented("UpdateSchema::RequireColumn not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::DeleteColumn(std::string_view name) {
  // TODO(Guotao Yu): Implement DeleteColumn
  AddError(NotImplemented("UpdateSchema::DeleteColumn not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::MoveFirst(std::string_view name) {
  // TODO(Guotao Yu): Implement MoveFirst
  AddError(NotImplemented("UpdateSchema::MoveFirst not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::MoveBefore(std::string_view name,
                                       std::string_view before_name) {
  // TODO(Guotao Yu): Implement MoveBefore
  AddError(NotImplemented("UpdateSchema::MoveBefore not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::MoveAfter(std::string_view name,
                                      std::string_view after_name) {
  // TODO(Guotao Yu): Implement MoveAfter
  AddError(NotImplemented("UpdateSchema::MoveAfter not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::UnionByNameWith(std::shared_ptr<Schema> new_schema) {
  // TODO(Guotao Yu): Implement UnionByNameWith
  AddError(NotImplemented("UpdateSchema::UnionByNameWith not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::SetIdentifierFields(
    const std::span<std::string_view>& names) {
  identifier_field_names_ = names | std::ranges::to<std::unordered_set<std::string>>();
  return *this;
}

Result<UpdateSchema::ApplyResult> UpdateSchema::Apply() {
  // TODO(Guotao Yu): Implement Apply
  return NotImplemented("UpdateSchema::Apply not implemented");
}

}  // namespace iceberg
