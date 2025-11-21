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

#include "iceberg/partition_summary_internal.h"

#include <memory>

#include "iceberg/expression/literal.h"
#include "iceberg/manifest_list.h"
#include "iceberg/result.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Status PartitionFieldStats::Update(const Literal& value) {
  if (type_->type_id() != value.type()->type_id()) {
    return InvalidArgument("value is not compatible with type");
  }

  if (value.IsNull()) {
    contains_null_ = true;
    return {};
  }

  if (value.IsNan()) {
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
    ICEBERG_ASSIGN_OR_RAISE(auto serialized_lower, lower_bound_->Serialize())
    summary.lower_bound = std::move(serialized_lower);
  }
  if (upper_bound_) {
    ICEBERG_ASSIGN_OR_RAISE(auto serialized_upper, upper_bound_->Serialize())
    summary.upper_bound = std::move(serialized_upper);
  }
  return summary;
}

PartitionSummary::PartitionSummary(const StructType& partition_type) {
  std::vector<PartitionFieldStats> field_stats;
  for (const auto& field : partition_type.fields()) {
    field_stats.emplace_back(field.type());
  }
  field_stats_ = std::move(field_stats);
}

Status PartitionSummary::Update(const std::vector<Literal>& partition_values) {
  if (partition_values.size() != field_stats_.size()) {
    return InvalidArgument("partition values size {} does not match field stats size {}",
                           partition_values.size(), field_stats_.size());
  }

  for (size_t i = 0; i < partition_values.size(); i++) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto literal,
        partition_values[i].CastTo(
            internal::checked_pointer_cast<PrimitiveType>(field_stats_[i].type())));
    ICEBERG_RETURN_UNEXPECTED(field_stats_[i].Update(literal));
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
