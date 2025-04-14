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

#include "iceberg/partition_field.h"

#include <format>

#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

PartitionField::PartitionField(int32_t source_id, int32_t field_id, std::string name,
                               std::shared_ptr<Transform> transform)
    : source_id_(source_id),
      field_id_(field_id),
      name_(std::move(name)),
      transform_(std::move(transform)) {}

int32_t PartitionField::source_id() const { return source_id_; }

int32_t PartitionField::field_id() const { return field_id_; }

std::string_view PartitionField::name() const { return name_; }

std::shared_ptr<Transform> const& PartitionField::transform() const { return transform_; }

std::string PartitionField::ToString() const {
  return std::format("{} ({} {}({}))", name_, field_id_, *transform_, source_id_);
}

bool PartitionField::Equals(const PartitionField& other) const {
  return source_id_ == other.source_id_ && field_id_ == other.field_id_ &&
         name_ == other.name_ && *transform_ == *other.transform_;
}

}  // namespace iceberg
