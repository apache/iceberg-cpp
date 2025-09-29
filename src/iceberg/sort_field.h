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

/// \file iceberg/sort_field.h
/// A sort field in a sort order

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

/// \brief Sort direction in a partition, either ascending or descending
enum class SortDirection {
  /// Ascending
  kAscending,
  /// Descending
  kDescending,
};
/// \brief Get the relative sort direction name
ICEBERG_EXPORT constexpr std::string_view ToString(SortDirection direction) {
  switch (direction) {
    case SortDirection::kAscending:
      return "asc";
    case SortDirection::kDescending:
      return "desc";
    default:
      return "invalid";
  }
}
/// \brief Get the relative sort direction from name
ICEBERG_EXPORT constexpr Result<SortDirection> SortDirectionFromString(
    std::string_view str) {
  if (str == "asc") return SortDirection::kAscending;
  if (str == "desc") return SortDirection::kDescending;
  return InvalidArgument("Invalid SortDirection string: {}", str);
}

enum class NullOrder {
  /// Nulls are sorted first
  kFirst,
  /// Nulls are sorted last
  kLast,
};
/// \brief Get the relative null order name
ICEBERG_EXPORT constexpr std::string_view ToString(NullOrder null_order) {
  switch (null_order) {
    case NullOrder::kFirst:
      return "nulls-first";
    case NullOrder::kLast:
      return "nulls-last";
    default:
      return "invalid";
  }
}
/// \brief Get the relative null order from name
ICEBERG_EXPORT constexpr Result<NullOrder> NullOrderFromString(std::string_view str) {
  if (str == "nulls-first") return NullOrder::kFirst;
  if (str == "nulls-last") return NullOrder::kLast;
  return InvalidArgument("Invalid NullOrder string: {}", str);
}

/// \brief a field with its transform.
class ICEBERG_EXPORT SortField : public util::Formattable {
 public:
  /// \brief Construct a field.
  /// \param[in] source_id The source field ID.
  /// \param[in] transform The transform function.
  /// \param[in] direction The sort direction.
  /// \param[in] null_order The null order.
  SortField(int32_t source_id, std::shared_ptr<Transform> transform,
            SortDirection direction, NullOrder null_order);

  /// \brief Get the source field ID.
  int32_t source_id() const;

  /// \brief Get the transform type.
  const std::shared_ptr<Transform>& transform() const;

  /// \brief Get the sort direction.
  SortDirection direction() const;

  /// \brief Get the null order.
  NullOrder null_order() const;

  std::string ToString() const override;

  friend bool operator==(const SortField& lhs, const SortField& rhs) {
    return lhs.Equals(rhs);
  }

 private:
  /// \brief Compare two fields for equality.
  [[nodiscard]] bool Equals(const SortField& other) const;

  int32_t source_id_;
  std::shared_ptr<Transform> transform_;
  SortDirection direction_;
  NullOrder null_order_;
};

}  // namespace iceberg
