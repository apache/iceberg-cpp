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

#include <chrono>
#include <concepts>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

namespace detail {

template <typename T>
struct IsResult : std::false_type {};

template <typename T>
struct IsResult<Result<T>> : std::true_type {};

template <typename T>
concept ResultType = IsResult<std::remove_cvref_t<T>>::value;

template <typename F>
concept RetryTask = requires(F& f) {
  { std::invoke(f) } -> ResultType;
};

template <typename F>
using RetryTaskResult = std::remove_cvref_t<std::invoke_result_t<F&>>;

}  // namespace detail

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
///
/// When retries are enabled (`num_retries > 0`), callers must explicitly configure
/// retry policy with `OnlyRetryOn(...)` or `StopRetryOn(...)`.
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
    retry_policy_mode_ = RetryPolicyMode::kOnlyRetryOn;
    retry_error_kinds_ = std::vector<ErrorKind>(error_kinds);
    return *this;
  }

  /// \brief Specify a single error type that should trigger a retry.
  ///
  /// \note OnlyRetryOn takes priority over StopRetryOn. If OnlyRetryOn is set,
  /// StopRetryOn is ignored.
  RetryRunner& OnlyRetryOn(ErrorKind error_kind) { return OnlyRetryOn({error_kind}); }

  /// \brief Specify error types that should stop retries immediately.
  ///
  /// When set, errors matching one of these kinds will not be retried.
  /// All other errors will be retried.
  ///
  /// \note OnlyRetryOn takes priority over StopRetryOn. If OnlyRetryOn is set,
  /// StopRetryOn is ignored.
  RetryRunner& StopRetryOn(std::initializer_list<ErrorKind> error_kinds) {
    if (retry_policy_mode_ == RetryPolicyMode::kOnlyRetryOn) {
      return *this;
    }

    retry_policy_mode_ = RetryPolicyMode::kStopRetryOn;
    retry_error_kinds_ = std::vector<ErrorKind>(error_kinds);
    return *this;
  }

  /// \brief Specify a single error type that should stop retries immediately.
  ///
  /// \note OnlyRetryOn takes priority over StopRetryOn. If OnlyRetryOn is set,
  /// StopRetryOn is ignored.
  RetryRunner& StopRetryOn(ErrorKind error_kind) { return StopRetryOn({error_kind}); }

  /// \brief Run a task that returns a Result<T>
  ///
  /// When `num_retries > 0`, the retry policy must be configured explicitly via
  /// `OnlyRetryOn(...)` or `StopRetryOn(...)`.
  ///
  /// TODO: Replace attempt_counter with a metrics reporter once it is available.
  template <typename F>
    requires detail::RetryTask<F>
  auto Run(F&& task, int32_t* attempt_counter = nullptr) -> detail::RetryTaskResult<F> {
    using TaskResult = detail::RetryTaskResult<F>;

    const auto validation = ValidateConfig();
    if (!validation.has_value()) {
      return TaskResult(std::unexpected(validation.error()));
    }

    const auto deadline = ComputeDeadline();
    int32_t attempt = 0;
    const int32_t max_attempts = config_.num_retries + 1;

    while (true) {
      ++attempt;
      if (attempt_counter != nullptr) {
        *attempt_counter = attempt;
      }

      auto result = std::invoke(task);
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
  enum class RetryPolicyMode {
    // No retry policy was selected; invalid when retries are enabled.
    kUnset,
    // Retry only errors listed in retry_error_kinds_.
    kOnlyRetryOn,
    // Retry all errors except those listed in retry_error_kinds_.
    kStopRetryOn,
  };

  using Clock = std::chrono::steady_clock;
  using Duration = std::chrono::milliseconds;
  using TimePoint = Clock::time_point;

  /// \brief Validate retry counts, timing bounds, and the selected retry policy.
  Status ValidateConfig() const;
  std::optional<TimePoint> ComputeDeadline() const;
  bool HasTimedOut(const std::optional<TimePoint>& deadline) const;

  /// \brief Check if the given error kind should trigger a retry.
  bool ShouldRetry(ErrorKind kind) const;
  bool CanRetry(ErrorKind kind, int32_t attempt, int32_t max_attempts,
                const std::optional<TimePoint>& deadline) const;
  std::optional<Duration> RetryDelayWithinBudget(
      int32_t attempt, const std::optional<TimePoint>& deadline) const;
  bool WaitForNextAttempt(int32_t attempt,
                          const std::optional<TimePoint>& deadline) const;
  /// \brief Calculate delay with exponential backoff and jitter
  int32_t CalculateDelay(int32_t attempt) const;

  RetryConfig config_;
  RetryPolicyMode retry_policy_mode_ = RetryPolicyMode::kUnset;
  std::vector<ErrorKind> retry_error_kinds_;
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
