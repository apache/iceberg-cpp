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

/// \file iceberg/metrics/metrics_context.h
/// \brief Factory interface for creating named Counter and Timer instances.

#include <memory>
#include <string_view>
#include <unordered_map>

#include "iceberg/iceberg_export.h"
#include "iceberg/metrics/counter.h"
#include "iceberg/metrics/timer.h"

namespace iceberg {

/// \brief Factory for creating named Counter and Timer instances.
///
/// A MetricsContext owns all metrics it creates. Asking for the same name
/// twice returns the same object (identity by name). The null context returns
/// noop singletons without allocating.
///
/// Mirrors org.apache.iceberg.metrics.MetricsContext.
class ICEBERG_EXPORT MetricsContext {
 public:
  virtual ~MetricsContext() = default;

  /// \brief Get or create a named Counter.
  virtual std::shared_ptr<Counter> GetCounter(std::string_view name,
                                              CounterUnit unit) = 0;

  /// \brief Get or create a named Timer (nanosecond precision).
  virtual std::shared_ptr<Timer> GetTimer(std::string_view name) = 0;

  /// \brief Return the null (no-op) MetricsContext singleton.
  ///
  /// All metrics returned by the null context are noop; nothing is allocated.
  static MetricsContext& Null();

  /// \brief Create a new DefaultMetricsContext.
  static std::unique_ptr<MetricsContext> Default();
};

/// \brief MetricsContext backed by DefaultCounter and DefaultTimer instances.
///
/// Thread-safe for metric access; not thread-safe for concurrent GetCounter/GetTimer
/// calls with the same name from multiple threads (registration should be done
/// during single-threaded setup).
class ICEBERG_EXPORT DefaultMetricsContext : public MetricsContext {
 public:
  std::shared_ptr<Counter> GetCounter(std::string_view name, CounterUnit unit) override;

  std::shared_ptr<Timer> GetTimer(std::string_view name) override;

 private:
  std::unordered_map<std::string, std::shared_ptr<Counter>> counters_;
  std::unordered_map<std::string, std::shared_ptr<Timer>> timers_;
};

}  // namespace iceberg
