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

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/sort_field.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

/// \brief A sort order for a table
///
/// A sort order is defined by a sort order id and a list of sort fields.
/// The order of the sort fields within the list defines the order in which the sort is
/// applied to the data.
class ICEBERG_EXPORT SortOrder : public util::Formattable {
 public:
  static constexpr int32_t kUnsortedOrderId = 0;
  static constexpr int32_t kInitialSortOrderId = 1;

  /// \brief Get an unsorted sort order singleton.
  static const std::shared_ptr<SortOrder>& Unsorted();

  /// \brief Get the sort order id.
  int32_t order_id() const;

  /// \brief Get the list of sort fields.
  std::span<const SortField> fields() const;

  /// \brief Returns true if the sort order is sorted
  bool is_sorted() const { return !fields_.empty(); }

  /// \brief Returns true if the sort order is unsorted
  /// A SortOrder is unsorted if it has no sort fields.
  bool is_unsorted() const { return fields_.empty(); }

  /// \brief Checks whether this order satisfies another order.
  bool Satisfies(const SortOrder& other) const;

  /// \brief Checks whether this order is equivalent to another order while ignoring the
  /// order id.
  bool SameOrder(const SortOrder& other) const;

  std::string ToString() const override;

  friend bool operator==(const SortOrder& lhs, const SortOrder& rhs) {
    return lhs.Equals(rhs);
  }

  /// \brief Validates the sort order against a schema.
  /// \param schema The schema to validate against.
  /// \return Error status if the sort order has any invalid transform.
  Status Validate(const Schema& schema) const;

  /// \brief Create a SortOrder.
  /// \param schema The schema to bind the sort order to.
  /// \param sort_id The sort order id.
  /// \param fields The sort fields.
  /// \return A Result containing the SortOrder or an error.
  static Result<std::unique_ptr<SortOrder>> Make(const Schema& schema, int32_t sort_id,
                                                 std::vector<SortField> fields);

  /// \brief Create a SortOrder without binding to a schema.
  /// \param sort_id The sort order id.
  /// \param fields The sort fields.
  /// \return A Result containing the SortOrder or an error.
  /// \note This method does not check whether the sort fields are valid for any schema.
  /// Use IsBoundToSchema to check if the sort order is valid for a given schema.
  static Result<std::unique_ptr<SortOrder>> Make(int32_t sort_id,
                                                 std::vector<SortField> fields);

 private:
  /// \brief Constructs a SortOrder instance.
  /// \param order_id The sort order id.
  /// \param fields The sort fields.
  /// \note Use the static Make methods to create SortOrder instances.
  SortOrder(int32_t order_id, std::vector<SortField> fields);

  /// \brief Compare two sort orders for equality.
  bool Equals(const SortOrder& other) const;

  int32_t order_id_;
  std::vector<SortField> fields_;
};

}  // namespace iceberg
