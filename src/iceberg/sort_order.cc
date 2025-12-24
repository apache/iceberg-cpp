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
#include <memory>
#include <optional>
#include <ranges>

#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/sort_field.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"

namespace iceberg {

SortOrder::SortOrder(int32_t order_id, std::vector<SortField> fields)
    : order_id_(order_id), fields_(std::move(fields)) {}

const std::shared_ptr<SortOrder>& SortOrder::Unsorted() {
  static const std::shared_ptr<SortOrder> unsorted = std::shared_ptr<SortOrder>(
      new SortOrder(kUnsortedOrderId, std::vector<SortField>{}));
  return unsorted;
}

int32_t SortOrder::order_id() const { return order_id_; }

std::span<const SortField> SortOrder::fields() const { return fields_; }

bool SortOrder::Satisfies(const SortOrder& other) const {
  // any ordering satisfies an unsorted ordering
  if (other.is_unsorted()) {
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
  return fields_ == other.fields_;
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

Status SortOrder::Validate(const Schema& schema) const {
  for (const auto& field : fields_) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema_field, schema.FindFieldById(field.source_id()));
    if (!schema_field.has_value()) {
      return InvalidArgument("Cannot find source column for sort field: {}", field);
    }

    const auto& source_type = schema_field.value().get().type();

    if (!field.transform()->CanTransform(*source_type)) {
      return InvalidArgument("Invalid source type {} for transform {}",
                             source_type->ToString(), field.transform()->ToString());
    }
  }
  return {};
}

Result<std::unique_ptr<SortOrder>> SortOrder::Make(const Schema& schema, int32_t sort_id,
                                                   std::vector<SortField> fields) {
  if (!fields.empty() && sort_id == kUnsortedOrderId) [[unlikely]] {
    return InvalidArgument("{} is reserved for unsorted sort order", kUnsortedOrderId);
  }

  if (fields.empty() && sort_id != kUnsortedOrderId) [[unlikely]] {
    return InvalidArgument("Sort order must have at least one sort field.");
  }

  auto sort_order = std::unique_ptr<SortOrder>(new SortOrder(sort_id, std::move(fields)));
  ICEBERG_RETURN_UNEXPECTED(sort_order->Validate(schema));
  return sort_order;
}

Result<std::unique_ptr<SortOrder>> SortOrder::Make(int32_t sort_id,
                                                   std::vector<SortField> fields) {
  if (!fields.empty() && sort_id == kUnsortedOrderId) [[unlikely]] {
    return InvalidArgument("{} is reserved for unsorted sort order", kUnsortedOrderId);
  }

  if (fields.empty() && sort_id != kUnsortedOrderId) [[unlikely]] {
    return InvalidArgument("Sort order must have at least one sort field");
  }

  return std::unique_ptr<SortOrder>(new SortOrder(sort_id, std::move(fields)));
}

}  // namespace iceberg
