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

#include <algorithm>
#include <chrono>
#include <cmath>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Configuration for retry behavior
struct ICEBERG_EXPORT RetryConfig {
  /// Maximum number of retry attempts (not including the first attempt)
  int32_t num_retries = 4;
  /// Minimum wait time between retries in milliseconds
  int32_t min_wait_ms = 100;
  /// Maximum wait time between retries in milliseconds
  int32_t max_wait_ms = 60 * 1000;  // 1 minute
  /// Total wall-clock time budget for retries, including backoff sleeps.
  int32_t total_timeout_ms = 30 * 60 * 1000;  // 30 minutes
  /// Exponential backoff scale factor
  double scale_factor = 2.0;
};

/// \brief Utility class for running tasks with retry logic
class ICEBERG_EXPORT RetryRunner {
 public:
  /// \brief Construct a RetryRunner with the given configuration
  explicit RetryRunner(RetryConfig config = {}) : config_(std::move(config)) {}

  /// \brief Specify error types that should trigger a retry.
  ///
  /// When set, only errors matching one of these kinds will be retried.
  /// All other errors will stop retries immediately.
  ///
  /// \note OnlyRetryOn takes priority over StopRetryOn. If OnlyRetryOn is set,
  /// StopRetryOn is ignored.
  RetryRunner& OnlyRetryOn(std::initializer_list<ErrorKind> error_kinds) {
    only_retry_on_ = std::vector<ErrorKind>(error_kinds);
    return *this;
  }

  /// \brief Specify a single error type that should trigger a retry.
  ///
  /// \note OnlyRetryOn takes priority over StopRetryOn. If OnlyRetryOn is set,
  /// StopRetryOn is ignored.
  RetryRunner& OnlyRetryOn(ErrorKind error_kind) {
    only_retry_on_ = std::vector<ErrorKind>{error_kind};
    return *this;
  }

  /// \brief Specify error types that should stop retries immediately.
  ///
  /// When set, errors matching one of these kinds will not be retried.
  /// All other errors will be retried.
  ///
  /// \note OnlyRetryOn takes priority over StopRetryOn. If OnlyRetryOn is set,
  /// StopRetryOn is ignored.
  RetryRunner& StopRetryOn(std::initializer_list<ErrorKind> error_kinds) {
    stop_retry_on_ = std::vector<ErrorKind>(error_kinds);
    return *this;
  }

  /// \brief Run a task that returns a Result<T>
  ///
  /// TODO: Replace attempt_counter with a metrics reporter once it is available.
  template <typename F, typename T = typename std::invoke_result_t<F>::value_type>
  Result<T> Run(F&& task, int32_t* attempt_counter = nullptr) {
    if (config_.num_retries < 0) {
      return InvalidArgument("num_retries must be non-negative, got {}",
                             config_.num_retries);
    }

    const auto deadline = ComputeDeadline();
    int32_t attempt = 0;
    const int32_t max_attempts = config_.num_retries + 1;

    while (true) {
      ++attempt;
      if (attempt_counter != nullptr) {
        *attempt_counter = attempt;
      }

      auto result = task();
      if (result.has_value()) {
        return result;
      }

      if (!CanRetry(result.error().kind, attempt, max_attempts, deadline)) {
        return result;
      }

      if (!WaitForNextAttempt(attempt, deadline)) {
        return result;
      }
    }
  }

 private:
  using Clock = std::chrono::steady_clock;
  using Duration = std::chrono::milliseconds;
  using TimePoint = Clock::time_point;

  std::optional<TimePoint> ComputeDeadline() const {
    if (config_.total_timeout_ms <= 0) {
      return std::nullopt;
    }
    return Clock::now() + Duration(config_.total_timeout_ms);
  }

  bool HasTimedOut(const std::optional<TimePoint>& deadline) const {
    return deadline.has_value() && Clock::now() >= *deadline;
  }

  /// \brief Check if the given error kind should trigger a retry.
  bool ShouldRetry(ErrorKind kind) const {
    if (!only_retry_on_.empty()) {
      return std::ranges::any_of(only_retry_on_,
                                 [kind](ErrorKind k) { return kind == k; });
    }

    if (!stop_retry_on_.empty()) {
      return !std::ranges::any_of(stop_retry_on_,
                                  [kind](ErrorKind k) { return kind == k; });
    }

    return true;
  }

  bool CanRetry(ErrorKind kind, int32_t attempt, int32_t max_attempts,
                const std::optional<TimePoint>& deadline) const {
    return attempt < max_attempts && !HasTimedOut(deadline) && ShouldRetry(kind);
  }

  std::optional<Duration> RetryDelayWithinBudget(
      int32_t attempt, const std::optional<TimePoint>& deadline) const {
    const auto delay = Duration(CalculateDelay(attempt));
    if (!deadline.has_value()) {
      return delay;
    }

    const auto now = Clock::now();
    if (now >= *deadline) {
      return std::nullopt;
    }

    const auto remaining = std::chrono::duration_cast<Duration>(*deadline - now);
    if (remaining <= Duration::zero() || delay >= remaining) {
      return std::nullopt;
    }

    return delay;
  }

  bool WaitForNextAttempt(int32_t attempt,
                          const std::optional<TimePoint>& deadline) const {
    const auto delay = RetryDelayWithinBudget(attempt, deadline);
    if (!delay.has_value()) {
      return false;
    }

    std::this_thread::sleep_for(*delay);
    return !HasTimedOut(deadline);
  }

  /// \brief Calculate delay with exponential backoff and jitter
  int32_t CalculateDelay(int32_t attempt) const {
    // Calculate base delay with exponential backoff
    double base_delay = config_.min_wait_ms * std::pow(config_.scale_factor, attempt - 1);
    int32_t delay_ms = static_cast<int32_t>(
        std::min(base_delay, static_cast<double>(config_.max_wait_ms)));

    static thread_local std::mt19937 gen(std::random_device{}());
    int32_t jitter_range = std::max(1, delay_ms / 10);
    std::uniform_int_distribution<> dis(0, jitter_range - 1);
    delay_ms += dis(gen);
    return std::max(1, delay_ms);
  }

  RetryConfig config_;
  std::vector<ErrorKind> only_retry_on_;
  std::vector<ErrorKind> stop_retry_on_;
};

/// \brief Helper function to create a RetryRunner with table commit configuration
ICEBERG_EXPORT inline RetryRunner MakeCommitRetryRunner(int32_t num_retries,
                                                        int32_t min_wait_ms,
                                                        int32_t max_wait_ms,
                                                        int32_t total_timeout_ms) {
  return RetryRunner(RetryConfig{.num_retries = num_retries,
                                 .min_wait_ms = min_wait_ms,
                                 .max_wait_ms = max_wait_ms,
                                 .total_timeout_ms = total_timeout_ms})
      .OnlyRetryOn(ErrorKind::kCommitFailed);
}

}  // namespace iceberg
