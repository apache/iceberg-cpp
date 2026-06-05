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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>

#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Time units supported by Timer.
enum class TimerUnit {
  kNanoseconds,
  kMicroseconds,
  kMilliseconds,
  kSeconds,
  kMinutes,
  kHours,
  kDays,
};

/// \brief String representation of a TimerUnit.
ICEBERG_EXPORT constexpr std::string_view ToString(TimerUnit unit) noexcept {
  switch (unit) {
    case TimerUnit::kNanoseconds:
      return "nanoseconds";
    case TimerUnit::kMicroseconds:
      return "microseconds";
    case TimerUnit::kMilliseconds:
      return "milliseconds";
    case TimerUnit::kSeconds:
      return "seconds";
    case TimerUnit::kMinutes:
      return "minutes";
    case TimerUnit::kHours:
      return "hours";
    case TimerUnit::kDays:
      return "days";
  }
  std::unreachable();
}

/// \brief Abstract timer for measuring operation durations.
///
/// Use Start() to obtain a Timed RAII guard that records the elapsed duration when it
/// goes out of scope.
class ICEBERG_EXPORT Timer {
 public:
  /// \brief RAII guard that records elapsed time into a non-owning Timer on destruction.
  class ICEBERG_EXPORT Timed {
   public:
    explicit Timed(Timer& timer);
    ~Timed();

    Timed(const Timed&) = delete;
    Timed& operator=(const Timed&) = delete;
    Timed(Timed&& other) noexcept;
    Timed& operator=(Timed&& other) noexcept;

    /// \brief Explicitly stop timing and record the duration.
    ///
    /// Subsequent calls (including the destructor) are no-ops.
    void Stop();

   private:
    Timer* timer_;
    std::chrono::steady_clock::time_point start_;
    bool stopped_ = false;
  };

  virtual ~Timer() = default;

  /// \brief Number of timing recordings made so far.
  virtual int64_t Count() const = 0;

  /// \brief Total accumulated duration across all recordings.
  virtual std::chrono::nanoseconds TotalDuration() const = 0;

  /// \brief Record a nanosecond duration directly.
  ///
  /// Use the template overload below to record
  /// any std::chrono duration type with automatic unit conversion.
  virtual void Record(std::chrono::nanoseconds duration) = 0;

  /// \brief Record a duration of any chrono type, converting to nanoseconds.
  template <typename Rep, typename Period>
  void Record(std::chrono::duration<Rep, Period> duration) {
    Record(std::chrono::duration_cast<std::chrono::nanoseconds>(duration));
  }

  /// \brief Return the time unit used by this timer.
  virtual std::string_view Unit() const { return "nanoseconds"; }

  /// \brief Return true if this timer is a no-op.
  virtual bool IsNoop() const { return false; }

  /// \brief Start timing and return a RAII Timed guard.
  ///
  /// The elapsed duration is recorded into this timer when the Timed guard is
  /// destroyed or Stop() is called. The caller must ensure this timer outlives
  /// the returned guard.
  Timed Start();

  /// \brief Execute a callable, record its wall-clock duration, and return its result.
  template <typename Callable>
  decltype(auto) Time(Callable&& fn) {
    auto timed = Start();
    using ResultType = std::invoke_result_t<Callable&&>;
    if constexpr (std::is_void_v<ResultType>) {
      std::invoke(std::forward<Callable>(fn));
    } else if constexpr (std::is_reference_v<ResultType>) {
      return std::invoke(std::forward<Callable>(fn));
    } else {
      return std::invoke(std::forward<Callable>(fn));
    }
  }

  /// \brief Return a shared no-op timer singleton.
  static std::shared_ptr<Timer> Noop();
};

/// \brief Thread-safe timer backed by std::atomic<int64_t>.
class ICEBERG_EXPORT DefaultTimer : public Timer {
 public:
  explicit DefaultTimer(TimerUnit unit = TimerUnit::kNanoseconds);

  int64_t Count() const override;
  std::chrono::nanoseconds TotalDuration() const override;
  void Record(std::chrono::nanoseconds duration) override;
  std::string_view Unit() const override { return ToString(unit_); }

 private:
  std::atomic<int64_t> count_{0};
  std::atomic<int64_t> total_nanos_{0};
  TimerUnit unit_;
};

}  // namespace iceberg
