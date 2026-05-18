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

/// \file iceberg/metrics.h
/// Iceberg file format metrics

#include <optional>
#include <unordered_map>

#include "iceberg/expression/literal.h"
#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Field-level metrics for a single column.
///
/// This structure captures value counts, null counts, NaN counts, and optional
/// lower/upper bounds for a specific field identified by its field_id.
struct ICEBERG_EXPORT FieldMetrics {
  /// \brief The field ID this metrics belongs to.
  int32_t field_id;

  /// \brief The total number of values (including nulls) for this field.
  /// A negative value indicates the count is unknown.
  int64_t value_count = -1;

  /// \brief The number of null values for this field.
  /// A negative value indicates the count is unknown.
  int64_t null_value_count = -1;

  /// \brief The number of NaN values for this field.
  /// A negative value indicates the count is unknown.
  int64_t nan_value_count = -1;

  /// \brief The lower bound value as a Literal.
  /// Empty if no lower bound is available.
  std::optional<Literal> lower_bound = std::nullopt;

  /// \brief The upper bound value as a Literal.
  /// Empty if no upper bound is available.
  std::optional<Literal> upper_bound = std::nullopt;
};

/// \brief Iceberg file format metrics
struct ICEBERG_EXPORT Metrics {
  std::optional<int64_t> row_count;
  std::unordered_map<int32_t, int64_t> column_sizes;
  std::unordered_map<int32_t, int64_t> value_counts;
  std::unordered_map<int32_t, int64_t> null_value_counts;
  std::unordered_map<int32_t, int64_t> nan_value_counts;
  std::unordered_map<int32_t, Literal> lower_bounds;
  std::unordered_map<int32_t, Literal> upper_bounds;
};

}  // namespace iceberg
