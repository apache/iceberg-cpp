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

#include "iceberg/expression/literal.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

class PartitionFieldStats {
 public:
  explicit PartitionFieldStats(const std::shared_ptr<Type>& type) : type_(type) {}

  Status Update(const Literal& value);

  PartitionFieldSummary Finish() const;

 private:
  std::shared_ptr<Type> type_{nullptr};
  bool contains_null_{false};
  bool contains_nan_{false};
  std::optional<Literal> lower_bound_;
  std::optional<Literal> upper_bound_;
};

class PartitionSummary {
 public:
  /// \brief Update the partition summary with partition.
  Status Update(const std::vector<Literal>& partition);

  /// \brief Get the list of partition field summaries.
  std::vector<PartitionFieldSummary> Summaries() const;

  /// \brief Create a PartitionSummary from the partition type.
  /// \param partition_type The partition type.
  /// \return A Result containing a unique pointer to the PartitionSummary.
  static Result<std::unique_ptr<PartitionSummary>> Make(const StructType& partition_type);

 private:
  /// \brief Create a PartitionSummary with the given field stats.
  explicit PartitionSummary(std::vector<PartitionFieldStats> field_stats)
      : field_stats_(std::move(field_stats)) {}

  std::vector<PartitionFieldStats> field_stats_;
};

}  // namespace iceberg
