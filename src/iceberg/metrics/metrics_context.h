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
#include <shared_mutex>
#include <string_view>
#include <unordered_map>

#include "iceberg/iceberg_export.h"
#include "iceberg/metrics/counter.h"
#include "iceberg/metrics/timer.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

/// \brief Factory for creating named Counter and Timer instances.
///
/// A MetricsContext owns all metrics it creates. Asking for the same name
/// twice returns the same object (identity by name). The null context returns
/// noop singletons without allocating.
class ICEBERG_EXPORT MetricsContext {
 public:
  virtual ~MetricsContext() = default;

  /// \brief Get or create a named Counter with an explicit unit.
  virtual std::shared_ptr<Counter> GetCounter(std::string_view name,
                                              CounterUnit unit) = 0;

  /// \brief Get or create a count-unit Counter by name.
  ///
  /// Convenience overload defaulting to CounterUnit::kCount.
  std::shared_ptr<Counter> GetCounter(std::string_view name) {
    return GetCounter(name, CounterUnit::kCount);
  }

  /// \brief Get or create a named Timer (nanosecond precision).
  virtual std::shared_ptr<Timer> GetTimer(std::string_view name) = 0;

  /// \brief Return the null (no-op) MetricsContext singleton.
  ///
  /// All metrics returned by the null context are noop; nothing is allocated.
  static std::shared_ptr<MetricsContext> Null();

  /// \brief Create a new DefaultMetricsContext.
  static std::unique_ptr<MetricsContext> Default();
};

/// \brief MetricsContext backed by DefaultCounter and DefaultTimer instances.
///
/// Fully thread-safe. Concurrent GetCounter/GetTimer calls are protected by a
/// shared mutex: lookups take a shared lock, and inserts upgrade to an exclusive
/// lock with a re-check to preserve identity-by-name semantics.
/// Counter/Timer increments are independently thread-safe via std::atomic.
class ICEBERG_EXPORT DefaultMetricsContext : public MetricsContext {
 public:
  using MetricsContext::GetCounter;  // expose the one-arg base overload
  std::shared_ptr<Counter> GetCounter(std::string_view name, CounterUnit unit) override;

  std::shared_ptr<Timer> GetTimer(std::string_view name) override;

 private:
  mutable std::shared_mutex mtx_;
  std::unordered_map<std::string, std::shared_ptr<Counter>, StringHash, StringEqual>
      counters_;
  std::unordered_map<std::string, std::shared_ptr<Timer>, StringHash, StringEqual>
      timers_;
};

}  // namespace iceberg
