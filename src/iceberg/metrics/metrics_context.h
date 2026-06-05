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

#include "iceberg/iceberg_export.h"
#include "iceberg/metrics/counter.h"
#include "iceberg/metrics/timer.h"

namespace iceberg {

/// \brief Factory for creating named Counter and Timer instances.
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

  /// \brief Get or create a named Timer with an explicit unit.
  virtual std::shared_ptr<Timer> GetTimer(std::string_view name, TimerUnit unit) = 0;

  /// \brief Get or create a nanosecond-precision Timer by name.
  ///
  /// Convenience overload defaulting to TimerUnit::kNanoseconds.
  std::shared_ptr<Timer> GetTimer(std::string_view name) {
    return GetTimer(name, TimerUnit::kNanoseconds);
  }

  /// \brief Return the no-op MetricsContext singleton.
  ///
  /// All metrics returned by the no-op context are noop; nothing is allocated.
  static const std::shared_ptr<MetricsContext>& Noop();

  /// \brief Create a new DefaultMetricsContext.
  static std::unique_ptr<MetricsContext> Default();
};

/// \brief MetricsContext backed by DefaultCounter and DefaultTimer instances.
class ICEBERG_EXPORT DefaultMetricsContext : public MetricsContext {
 public:
  using MetricsContext::GetCounter;  // expose the one-arg base overload
  using MetricsContext::GetTimer;    // expose the one-arg base overload

  std::shared_ptr<Counter> GetCounter(std::string_view name, CounterUnit unit) override;

  std::shared_ptr<Timer> GetTimer(std::string_view name, TimerUnit unit) override;
};

}  // namespace iceberg
