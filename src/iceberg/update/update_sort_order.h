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

#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "iceberg/expression/term.h"
#include "iceberg/sort_field.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"

/// \file iceberg/update/update_sort_order.h
/// \brief Updates the table sort order.

namespace iceberg {

/// \brief Updating table sort order with a newly created order.
class ICEBERG_EXPORT UpdateSortOrder : public PendingUpdate {
 public:
  static Result<std::shared_ptr<UpdateSortOrder>> Make(
      std::shared_ptr<Transaction> transaction);

  ~UpdateSortOrder() override;

  struct ApplyResult {
    std::shared_ptr<SortOrder> sort_order;
  };

  /// \brief Add a sort field to the sort order.
  ///
  /// \param term A transform term referencing the field
  /// \param direction The sort direction (ascending or descending)
  /// \param null_order The null order (first or last)
  /// \return Reference to this UpdateSortOrder for chaining
  UpdateSortOrder& AddSortField(const std::shared_ptr<Term>& term,
                                SortDirection direction, NullOrder null_order);

  /// \brief Add a sort field by field name with identity transform.
  ///
  /// \param name The name of the field to sort by
  /// \param direction The sort direction (ascending or descending)
  /// \param null_order The null order (first or last)
  /// \return Reference to this UpdateSortOrder for chaining
  UpdateSortOrder& AddSortFieldByName(std::string_view name, SortDirection direction,
                                      NullOrder null_order);

  /// \brief Set case sensitivity of sort column name resolution.
  ///
  /// \param case_sensitive When true, column name resolution is case-sensitive
  /// \return Reference to this UpdateSortOrder for chaining
  UpdateSortOrder& CaseSensitive(bool case_sensitive);

  Kind kind() const final { return Kind::kUpdateSortOrder; }

  /// \brief Apply the pending changes and return the new SortOrder.
  Result<ApplyResult> Apply();

 private:
  explicit UpdateSortOrder(std::shared_ptr<Transaction> transaction);

  std::vector<SortField> sort_fields_;
  bool case_sensitive_ = true;
};

}  // namespace iceberg
