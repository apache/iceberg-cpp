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

#include <memory>

#include "iceberg/expression/literal.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/partition_summary_internal.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"

namespace iceberg {

Status PartitionFieldStats::Update(const Literal& value) {
  if (type_->type_id() != value.type()->type_id()) [[unlikely]] {
    return InvalidArgument("value type {} is not compatible with expected type {}.",
                           *value.type(), *type_);
  }

  if (value.IsNull()) {
    contains_null_ = true;
    return {};
  }

  if (value.IsNaN()) {
    contains_nan_ = true;
    return {};
  }

  if (!lower_bound_ || value < *lower_bound_) {
    lower_bound_ = value;
  }
  if (!upper_bound_ || value > *upper_bound_) {
    upper_bound_ = value;
  }
  return {};
}

Result<PartitionFieldSummary> PartitionFieldStats::Finish() const {
  PartitionFieldSummary summary;
  summary.contains_null = contains_null_;
  summary.contains_nan = contains_nan_;
  if (lower_bound_) {
    ICEBERG_ASSIGN_OR_RAISE(summary.lower_bound, lower_bound_->Serialize());
  }
  if (upper_bound_) {
    ICEBERG_ASSIGN_OR_RAISE(summary.upper_bound, upper_bound_->Serialize());
  }
  return summary;
}

PartitionSummary::PartitionSummary(const StructType& partition_type) {
  field_stats_.reserve(partition_type.fields().size());
  for (const auto& field : partition_type.fields()) {
    field_stats_.emplace_back(field.type());
  }
}

Status PartitionSummary::Update(const PartitionValues& partition_values) {
  if (partition_values.num_fields() != field_stats_.size()) [[unlikely]] {
    return InvalidArgument("partition values size {} does not match field stats size {}",
                           partition_values.num_fields(), field_stats_.size());
  }

  for (size_t i = 0; i < partition_values.num_fields(); i++) {
    ICEBERG_ASSIGN_OR_RAISE(auto val, partition_values.ValueAt(i));
    ICEBERG_ASSIGN_OR_RAISE(
        auto lit, val.get().CastTo(internal::checked_pointer_cast<PrimitiveType>(
                      field_stats_[i].type())));
    ICEBERG_RETURN_UNEXPECTED(field_stats_[i].Update(lit));
  }
  return {};
}

Result<std::vector<PartitionFieldSummary>> PartitionSummary::Summaries() const {
  std::vector<PartitionFieldSummary> summaries;
  summaries.reserve(field_stats_.size());
  for (const auto& field_stat : field_stats_) {
    ICEBERG_ASSIGN_OR_RAISE(auto summary, field_stat.Finish());
    summaries.push_back(std::move(summary));
  }
  return summaries;
}

}  // namespace iceberg
