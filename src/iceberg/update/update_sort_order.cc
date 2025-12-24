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

#include "iceberg/update/update_sort_order.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "iceberg/expression/term.h"
#include "iceberg/result.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<UpdateSortOrder>> UpdateSortOrder::Make(
    std::shared_ptr<Transaction> transaction) {
  if (!transaction) [[unlikely]] {
    return InvalidArgument("Cannot create UpdateSortOrder without a transaction");
  }
  return std::shared_ptr<UpdateSortOrder>(new UpdateSortOrder(std::move(transaction)));
}

UpdateSortOrder::UpdateSortOrder(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {}

UpdateSortOrder::~UpdateSortOrder() = default;

UpdateSortOrder& UpdateSortOrder::AddSortField(std::shared_ptr<Term> term,
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
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto schema, transaction_->current().Schema());
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto bound_term,
                                   unbound_transform->Bind(*schema, case_sensitive_));

  int32_t source_id = bound_term->reference()->field_id();
  sort_fields_.emplace_back(source_id, unbound_transform->transform(), direction,
                            null_order);
  return *this;
}

UpdateSortOrder& UpdateSortOrder::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  return *this;
}

Result<UpdateSortOrder::ApplyResult> UpdateSortOrder::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  // If no sort fields are specified, return an unsorted order (ID = 0).
  std::shared_ptr<SortOrder> order;
  if (sort_fields_.empty()) {
    order = SortOrder::Unsorted();
  } else {
    // Use kInitialSortOrderId (1) as a placeholder for non-empty sort orders.
    // The actual sort order ID will be assigned by TableMetadataBuilder when
    // the AddSortOrder update is applied.
    ICEBERG_ASSIGN_OR_RAISE(
        order, SortOrder::Make(SortOrder::kInitialSortOrderId, sort_fields_));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto schema, transaction_->current().Schema());
  ICEBERG_RETURN_UNEXPECTED(order->Validate(*schema));
  return ApplyResult{std::move(order)};
}

}  // namespace iceberg
