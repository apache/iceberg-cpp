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
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/transform.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<UpdateSortOrder>> UpdateSortOrder::Make(
    std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(transaction != nullptr,
                   "Cannot create UpdateSortOrder without a transaction");
  return std::shared_ptr<UpdateSortOrder>(new UpdateSortOrder(std::move(transaction)));
}

UpdateSortOrder::UpdateSortOrder(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {}

UpdateSortOrder::~UpdateSortOrder() = default;

UpdateSortOrder& UpdateSortOrder::AddSortField(const std::shared_ptr<Term>& term,
                                               SortDirection direction,
                                               NullOrder null_order) {
  ICEBERG_BUILDER_CHECK(term != nullptr, "Term cannot be null");
  ICEBERG_BUILDER_CHECK(term->is_unbound(), "Term must be unbound");

  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto schema, transaction_->current().Schema());
  if (term->kind() == Term::Kind::kReference) {
    // kReference is treated as identity transform
    auto named_ref = internal::checked_pointer_cast<NamedReference>(term);
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto bound_ref,
                                     named_ref->Bind(*schema, case_sensitive_));
    sort_fields_.emplace_back(bound_ref->field_id(), Transform::Identity(), direction,
                              null_order);
  } else if (term->kind() == Term::Kind::kTransform) {
    auto unbound_transform = internal::checked_pointer_cast<UnboundTransform>(term);
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto bound_term,
                                     unbound_transform->Bind(*schema, case_sensitive_));
    sort_fields_.emplace_back(bound_term->reference()->field_id(),
                              unbound_transform->transform(), direction, null_order);
  } else {
    return AddError(ErrorKind::kNotSupported, "Not supported unbound term: {}",
                    static_cast<int>(term->kind()));
  }

  return *this;
}

UpdateSortOrder& UpdateSortOrder::AddSortFieldByName(std::string_view name,
                                                     SortDirection direction,
                                                     NullOrder null_order) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto named_ref,
                                   NamedReference::Make(std::string(name)));
  return AddSortField(std::move(named_ref), direction, null_order);
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
    // Use -1 as a placeholder for non-empty sort orders.
    // The actual sort order ID will be assigned by TableMetadataBuilder when
    // the AddSortOrder update is applied.
    ICEBERG_ASSIGN_OR_RAISE(order, SortOrder::Make(/*sort_id=*/-1, sort_fields_));
    ICEBERG_ASSIGN_OR_RAISE(auto schema, transaction_->current().Schema());
    ICEBERG_RETURN_UNEXPECTED(order->Validate(*schema));
  }
  return ApplyResult{std::move(order)};
}

}  // namespace iceberg
