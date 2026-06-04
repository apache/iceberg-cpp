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
#include <cstdint>
#include <functional>
#include <optional>
#include <type_traits>
#include <utility>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace detail {

template <typename F>
concept RetryTask = AsResult<std::invoke_result_t<F&>>;

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

namespace detail {

class ICEBERG_EXPORT RetryRunnerBase {
 protected:
  explicit RetryRunnerBase(RetryConfig config) : config_(std::move(config)) {}

  using Clock = std::chrono::steady_clock;
  using Duration = std::chrono::milliseconds;
  using TimePoint = Clock::time_point;

  /// \brief Validate retry counts and timing bounds.
  Status ValidateConfig() const;
  std::optional<TimePoint> ComputeDeadline() const;
  bool HasTimedOut(const std::optional<TimePoint>& deadline) const;
  std::optional<Duration> RetryDelayWithinBudget(
      int32_t attempt, const std::optional<TimePoint>& deadline) const;
  bool WaitForNextAttempt(int32_t attempt,
                          const std::optional<TimePoint>& deadline) const;
  /// \brief Calculate delay with exponential backoff and jitter
  int32_t CalculateDelay(int32_t attempt) const;

  RetryConfig config_;
};

}  // namespace detail

namespace retry {

enum class RetryPolicyMode {
  kNoRetry,
  kOnlyRetryOn,
  kStopRetryOn,
};

template <RetryPolicyMode Mode, ErrorKind... Kinds>
struct RetryPolicy {
  static_assert(Mode != RetryPolicyMode::kNoRetry || sizeof...(Kinds) == 0,
                "NoRetry must not include error kinds");
  static_assert(Mode == RetryPolicyMode::kNoRetry || sizeof...(Kinds) > 0,
                "RetryPolicy must include at least one error kind");

  static constexpr RetryPolicyMode kMode = Mode;
  static constexpr bool kEnabled = Mode != RetryPolicyMode::kNoRetry;

  static constexpr bool ShouldRetry(ErrorKind kind) {
    if constexpr (Mode == RetryPolicyMode::kNoRetry) {
      return false;
    } else if constexpr (Mode == RetryPolicyMode::kOnlyRetryOn) {
      return ((kind == Kinds) || ...);
    } else {
      return !((kind == Kinds) || ...);
    }
  }
};

using NoRetry = RetryPolicy<RetryPolicyMode::kNoRetry>;

template <ErrorKind... Kinds>
using OnlyRetryOn = RetryPolicy<RetryPolicyMode::kOnlyRetryOn, Kinds...>;

template <ErrorKind... Kinds>
using StopRetryOn = RetryPolicy<RetryPolicyMode::kStopRetryOn, Kinds...>;

template <typename T>
inline constexpr bool kIsRetryPolicy = false;

template <RetryPolicyMode Mode, ErrorKind... Kinds>
inline constexpr bool kIsRetryPolicy<RetryPolicy<Mode, Kinds...>> = true;

template <typename T>
concept Policy = kIsRetryPolicy<std::remove_cvref_t<T>>;

}  // namespace retry

/// \brief Utility class for running tasks with retry logic
///
/// When retries are enabled (`num_retries > 0`), RetryPolicy must be an enabled
/// policy such as `retry::OnlyRetryOn<...>` or `retry::StopRetryOn<...>`.
template <retry::Policy RetryPolicy>
class RetryRunner : private detail::RetryRunnerBase {
 public:
  /// \brief Construct a RetryRunner with the given configuration
  explicit RetryRunner(RetryConfig config = {})
      : detail::RetryRunnerBase(std::move(config)) {}

  /// \brief Run a task that returns a Result<T>
  ///
  /// When `num_retries > 0`, RetryPolicy must allow retrying matching errors.
  ///
  /// TODO: Replace attempt_counter with a metrics reporter once it is available.
  template <detail::RetryTask F>
  auto Run(F&& task, int32_t* attempt_counter = nullptr)
      -> std::remove_cvref_t<std::invoke_result_t<F&>> {
    ICEBERG_RETURN_UNEXPECTED(ValidatePolicyConfig());

    const auto deadline = this->ComputeDeadline();
    int32_t attempt = 0;
    const int32_t max_attempts = this->config_.num_retries + 1;

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

      if (!this->WaitForNextAttempt(attempt, deadline)) {
        return result;
      }
    }
  }

 private:
  using TimePoint = detail::RetryRunnerBase::TimePoint;

  Status ValidatePolicyConfig() const {
    auto validation = this->ValidateConfig();
    if (!validation.has_value()) {
      return validation;
    }
    if (this->config_.num_retries > 0 && !RetryPolicy::kEnabled) {
      return InvalidArgument("Retry policy must be enabled when num_retries > 0");
    }
    return {};
  }

  bool CanRetry(ErrorKind kind, int32_t attempt, int32_t max_attempts,
                const std::optional<TimePoint>& deadline) const {
    return attempt < max_attempts && !this->HasTimedOut(deadline) &&
           RetryPolicy::ShouldRetry(kind);
  }
};

/// \brief Helper function to create a RetryRunner with table commit configuration
ICEBERG_EXPORT inline auto MakeCommitRetryRunner(int32_t num_retries, int32_t min_wait_ms,
                                                 int32_t max_wait_ms,
                                                 int32_t total_timeout_ms) {
  return RetryRunner<retry::OnlyRetryOn<ErrorKind::kCommitFailed>>(
      RetryConfig{.num_retries = num_retries,
                  .min_wait_ms = min_wait_ms,
                  .max_wait_ms = max_wait_ms,
                  .total_timeout_ms = total_timeout_ms});
}

}  // namespace iceberg
