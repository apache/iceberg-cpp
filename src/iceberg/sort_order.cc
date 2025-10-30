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

#include "iceberg/sort_order.h"

#include <format>
#include <ranges>

#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

SortOrder::SortOrder(int32_t order_id, std::vector<SortField> fields)
    : order_id_(order_id), fields_(std::move(fields)) {}

const std::shared_ptr<SortOrder>& SortOrder::Unsorted() {
  static const std::shared_ptr<SortOrder> unsorted =
      std::make_shared<SortOrder>(/*order_id=*/0, std::vector<SortField>{});
  return unsorted;
}

int32_t SortOrder::order_id() const { return order_id_; }

std::span<const SortField> SortOrder::fields() const { return fields_; }

bool SortOrder::Satisfies(const SortOrder& other) const {
  // any ordering satisfies an unsorted ordering
  if (other.IsUnsorted()) {
    return true;
  }

  // this ordering cannot satisfy an ordering with more sort fields
  if (fields_.size() < other.fields().size()) {
    return false;
  }

  // this ordering has either more or the same number of sort fields
  for (const auto& [field, other_field] : std::views::zip(fields_, other.fields_)) {
    if (!field.Satisfies(other_field)) {
      return false;
    }
  }

  return true;
}

bool SortOrder::SameOrder(const SortOrder& other) const {
  if (fields_.size() != other.fields().size()) {
    return false;
  }

  for (const auto& [field, other_field] : std::views::zip(fields_, other.fields())) {
    if (!(field == other_field)) {
      return false;
    }
  }

  return true;
}

std::string SortOrder::ToString() const {
  std::string repr = "[";
  for (const auto& field : fields_) {
    std::format_to(std::back_inserter(repr), "\n  {}", field);
  }
  if (!fields_.empty()) {
    repr.push_back('\n');
  }
  repr += "]";
  return repr;
}

bool SortOrder::Equals(const SortOrder& other) const {
  return order_id_ == other.order_id_ && fields_ == other.fields_;
}

}  // namespace iceberg
