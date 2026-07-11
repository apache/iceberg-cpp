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

/// \file iceberg/metrics/counter.h
/// \brief Define metrics counters.

#include <atomic>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

/// \brief Unit for a Counter metric.
enum class CounterUnit {
  kCount,
  kBytes,
  kUndefined,
};

/// \brief String representation of a CounterUnit.
ICEBERG_EXPORT constexpr std::string_view ToString(CounterUnit unit) noexcept {
  switch (unit) {
    case CounterUnit::kCount:
      return "count";
    case CounterUnit::kBytes:
      return "bytes";
    case CounterUnit::kUndefined:
      return "undefined";
  }
  std::unreachable();
}

/// \brief Parse a CounterUnit from a string.
///
/// \param s The string to parse ("count", "bytes", or "undefined").
/// \return The CounterUnit, or an InvalidArgument error if unrecognized.
ICEBERG_EXPORT inline Result<CounterUnit> CounterUnitFromString(std::string_view s) {
  auto unit = StringUtils::ToLower(s);
  if (unit == "count") return CounterUnit::kCount;
  if (unit == "bytes") return CounterUnit::kBytes;
  if (unit == "undefined") return CounterUnit::kUndefined;
  return InvalidArgument("Invalid unit: {}", s);
}

/// \brief Abstract counter for tracking event totals.
class ICEBERG_EXPORT Counter {
 public:
  virtual ~Counter() = default;

  /// \brief Increment the counter by 1.
  virtual void Increment() { Increment(1); }

  /// \brief Increment the counter by the given amount.
  virtual void Increment(int64_t amount) = 0;

  /// \brief Return the current count.
  virtual int64_t value() const = 0;

  /// \brief Return the unit for this counter.
  virtual CounterUnit unit() const { return CounterUnit::kCount; }

  /// \brief Return true if this counter is a no-op.
  virtual bool IsNoop() const { return false; }

  /// \brief Return a shared no-op counter singleton.
  static std::shared_ptr<Counter> Noop();
};

/// \brief Thread-safe counter backed by std::atomic<int64_t>.
class ICEBERG_EXPORT DefaultCounter : public Counter {
 public:
  explicit DefaultCounter(CounterUnit unit = CounterUnit::kCount);

  using Counter::Increment;
  void Increment(int64_t amount) override;
  int64_t value() const override;
  CounterUnit unit() const override { return unit_; }

 private:
  std::atomic<int64_t> count_{0};
  CounterUnit unit_;
};

}  // namespace iceberg
