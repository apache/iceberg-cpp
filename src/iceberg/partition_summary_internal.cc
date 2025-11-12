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

PartitionFieldSummary PartitionFieldStats::Finish() const {
  PartitionFieldSummary summary;
  summary.contains_null = contains_null_;
  summary.contains_nan = contains_nan_;
  if (lower_bound_) {
    summary.lower_bound = lower_bound_->Serialize().value();
  }
  if (upper_bound_) {
    summary.upper_bound = upper_bound_->Serialize().value();
  }
  return summary;
}

Status PartitionSummary::Update(const std::vector<Literal>& partition) {
  if (partition.size() != field_stats_.size()) {
    return InvalidArgument("partition size does not match field stats size");
  }

  for (size_t i = 0; i < partition.size(); i++) {
    ICEBERG_RETURN_UNEXPECTED(field_stats_[i].Update(partition[i]));
  }
  return {};
}

std::vector<PartitionFieldSummary> PartitionSummary::Summaries() const {
  std::vector<PartitionFieldSummary> summaries;
  for (const auto& field_stat : field_stats_) {
    summaries.push_back(field_stat.Finish());
  }
  return summaries;
}

Result<std::unique_ptr<PartitionSummary>> PartitionSummary::Make(
    const StructType& partition_type) {
  std::vector<PartitionFieldStats> field_stats;
  for (const auto& field : partition_type.fields()) {
    field_stats.emplace_back(field.type());
  }
  return std::unique_ptr<PartitionSummary>(new PartitionSummary(std::move(field_stats)));
}

}  // namespace iceberg
