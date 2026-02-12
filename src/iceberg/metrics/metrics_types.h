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

/// \file iceberg/metrics/metrics_types.h
/// \brief Serialisable snapshot types shared across scan and commit metrics.
///
/// CounterResult, TimerResult, and DurationNs are primitive result types used by
/// ScanMetricsResult, CommitMetricsResult, and the JSON serde layer.  Defining
/// them here keeps ScanReport and CommitReport headers independent of each other.

#include <chrono>
#include <cstdint>
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/metrics/counter.h"  // CounterUnit

namespace iceberg {

/// \brief Duration type for metrics reporting (nanosecond precision).
using DurationNs = std::chrono::nanoseconds;

/// \brief Serialisable snapshot of a single Counter measurement.
///
/// Carries both the unit and value.
struct ICEBERG_EXPORT CounterResult {
  CounterUnit unit = CounterUnit::kCount;
  int64_t value = 0;

  bool operator==(const CounterResult&) const = default;
};

/// \brief Serialisable snapshot of a single Timer measurement.
///
/// Carries the unit name, recording count, and total accumulated duration.
struct ICEBERG_EXPORT TimerResult {
  // Time unit name; always "nanoseconds" for built-in timers.
  std::string unit{"nanoseconds"};
  int64_t count = 0;
  std::chrono::nanoseconds total_duration{0};

  bool operator==(const TimerResult&) const = default;
};

}  // namespace iceberg
