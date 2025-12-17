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

#include "iceberg/update/replace_sort_order.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "iceberg/catalog.h"
#include "iceberg/expression/term.h"
#include "iceberg/result.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirements.h"
#include "iceberg/transform.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"

namespace iceberg {

ReplaceSortOrder::ReplaceSortOrder(TableIdentifier identifier,
                                   std::shared_ptr<Catalog> catalog,
                                   std::shared_ptr<TableMetadata> base)
    : identifier_(std::move(identifier)),
      catalog_(std::move(catalog)),
      base_metadata_(std::move(base)) {}

ReplaceSortOrder& ReplaceSortOrder::AddSortField(std::shared_ptr<Term> term,
                                                 SortDirection direction,
                                                 NullOrder null_order) {
  if (!term) {
    return AddError(ErrorKind::kInvalidArgument, "Term cannot be null");
  }
  if (term->kind() != Term::Kind::kTransform) {
    return AddError(ErrorKind::kInvalidArgument, "Term must be a transform term");
  }
  if (!term->is_unbound()) {
    return AddError(ErrorKind::kInvalidArgument, "Term must be unbound");
  }
  // use checked-cast to get UnboundTransform
  auto unbound_transform = internal::checked_pointer_cast<UnboundTransform>(term);

  BUILDER_ASSIGN_OR_RETURN(auto schema, GetSchema());
  BUILDER_ASSIGN_OR_RETURN(auto bound_term,
                           unbound_transform->Bind(*schema, case_sensitive_));

  int32_t source_id = bound_term->reference()->field_id();
  sort_fields_.emplace_back(source_id, unbound_transform->transform(), direction,
                            null_order);
  return *this;
}

ReplaceSortOrder& ReplaceSortOrder::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  return *this;
}

Status ReplaceSortOrder::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  // Note: We use kInitialSortOrderId (1) here like the Java implementation.
  // The actual sort order ID will be assigned by TableMetadataBuilder when
  // the AddSortOrder update is applied.
  ICEBERG_ASSIGN_OR_RAISE(auto order,
                          SortOrder::Make(SortOrder::kInitialSortOrderId, sort_fields_));
  ICEBERG_ASSIGN_OR_RAISE(auto schema, GetSchema());
  ICEBERG_RETURN_UNEXPECTED(order->Validate(*schema));
  built_sort_order_ = std::move(order);
  return {};
}

Status ReplaceSortOrder::Commit() {
  ICEBERG_RETURN_UNEXPECTED(Apply());

  // Use TableMetadataBuilder to generate the changes
  auto builder = TableMetadataBuilder::BuildFrom(base_metadata_.get());
  builder->SetDefaultSortOrder(built_sort_order_);

  // Call Build() for error checks and validate metadata consistency. We simply ignore the
  // returned Tablemetadata here, this may need to be optimized in the future.
  ICEBERG_RETURN_UNEXPECTED(builder->Build());
  const auto& updates = builder->Changes();
  if (!updates.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto requirements,
                            TableRequirements::ForUpdateTable(*base_metadata_, updates));
    ICEBERG_RETURN_UNEXPECTED(catalog_->UpdateTable(identifier_, requirements, updates));
  }

  return {};
}

std::shared_ptr<SortOrder> ReplaceSortOrder::GetBuiltSortOrder() const {
  return built_sort_order_;
}

Result<std::shared_ptr<Schema>> ReplaceSortOrder::GetSchema() {
  if (!cached_schema_) {
    ICEBERG_ASSIGN_OR_RAISE(cached_schema_, base_metadata_->Schema());
  }
  return cached_schema_;
}

}  // namespace iceberg
