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

#include "iceberg/sort_field.h"

#include <format>

#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/formatter.h"

namespace iceberg {

SortField::SortField(int32_t source_id, std::shared_ptr<TransformFunction> transform,
                     SortDirection direction, NullOrder null_order)
    : source_id_(source_id),
      transform_(std::move(transform)),
      direction_(direction),
      null_order_(null_order) {}

int32_t SortField::source_id() const { return source_id_; }

std::shared_ptr<TransformFunction> const& SortField::transform() const {
  return transform_;
}

SortDirection SortField::direction() const { return direction_; }

NullOrder SortField::null_order() const { return null_order_; }

std::string SortField::ToString() const {
  return std::format(
      "sort_field(source_id={}, transform={}, direction={}, null_order={})", source_id_,
      *transform_, SortDirectionToString(direction_), NullOrderToString(null_order_));
}

bool SortField::Equals(const SortField& other) const {
  return source_id_ == other.source_id_ && *transform_ == *other.transform_ &&
         direction_ == other.direction_ && null_order_ == other.null_order_;
}

}  // namespace iceberg
