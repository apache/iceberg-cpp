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
#include <span>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/sort_field.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

/// \brief A sort order for a table
///
/// A sort order is defined by a sort order id and a list of sort fields.
/// The order of the sort fields within the list defines the order in which the sort is
/// applied to the data.
class ICEBERG_EXPORT SortOrder : public util::Formattable {
 public:
  static constexpr int32_t kInitialSortOrderId = 1;

  SortOrder(int32_t order_id, std::vector<SortField> fields);

  /// \brief Get an unsorted sort order singleton.
  static const std::shared_ptr<SortOrder>& Unsorted();

  /// \brief Get the sort order id.
  int32_t order_id() const;

  /// \brief Get the list of sort fields.
  std::span<const SortField> fields() const;

  std::string ToString() const override;

  friend bool operator==(const SortOrder& lhs, const SortOrder& rhs) {
    return lhs.Equals(rhs);
  }

  friend bool operator!=(const SortOrder& lhs, const SortOrder& rhs) {
    return !(lhs == rhs);
  }

 private:
  /// \brief Compare two sort orders for equality.
  bool Equals(const SortOrder& other) const;

  int32_t order_id_;
  std::vector<SortField> fields_;
};

}  // namespace iceberg
