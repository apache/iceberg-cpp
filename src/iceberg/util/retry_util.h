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
#include <functional>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "iceberg/result.h"

namespace iceberg {

/// \brief Configuration for retry behavior
struct RetryConfig {
  /// Maximum number of retry attempts (not including the first attempt)
  int32_t num_retries = 4;
  /// Minimum wait time between retries in milliseconds
  int32_t min_wait_ms = 100;
  /// Maximum wait time between retries in milliseconds
  int32_t max_wait_ms = 60 * 1000;  // 1 minute
  /// Total maximum time for all retries in milliseconds
  int32_t total_timeout_ms = 30 * 60 * 1000;  // 30 minutes
  /// Exponential backoff scale factor
  double scale_factor = 2.0;
};

/// \brief Utility class for running tasks with retry logic
class RetryRunner {
 public:
  RetryRunner() = default;

  RetryRunner& WithRetries(int32_t num_retries) {
    config_.num_retries = num_retries;
    return *this;
  }

  RetryRunner& WithExponentialBackoff(int32_t min_wait_ms, int32_t max_wait_ms,
                                      int32_t total_timeout_ms, double scale_factor) {
    config_.min_wait_ms = min_wait_ms;
    config_.max_wait_ms = max_wait_ms;
    config_.total_timeout_ms = total_timeout_ms;
    config_.scale_factor = scale_factor;
    return *this;
  }

  /// \brief Specify error types that should trigger a retry
  RetryRunner& OnlyRetryOn(std::initializer_list<ErrorKind> error_kinds) {
    only_retry_on_ = std::vector<ErrorKind>(error_kinds);
    return *this;
  }

  /// \brief Specify error types that should trigger a retry
  RetryRunner& OnlyRetryOn(ErrorKind error_kind) {
    only_retry_on_ = std::vector<ErrorKind>{error_kind};
    return *this;
  }

  /// \brief Specify error types that should stop retries immediately
  RetryRunner& StopRetryOn(std::initializer_list<ErrorKind> error_kinds) {
    stop_retry_on_ = std::vector<ErrorKind>(error_kinds);
    return *this;
  }

  /// \brief Run a task that returns a Result<T>
  template <typename F, typename T = typename std::invoke_result_t<F>::value_type>
  Result<T> Run(F&& task, int32_t* attempt_counter = nullptr) {
    auto start_time = std::chrono::steady_clock::now();
    int32_t attempt = 0;
    int32_t max_attempts = config_.num_retries + 1;

    while (true) {
      ++attempt;
      if (attempt_counter != nullptr) {
        *attempt_counter = attempt;
      }

      auto result = task();
      if (result.has_value()) {
        return result;
      }

      const auto& error = result.error();

      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - start_time)
                         .count();

      // total_timeout_ms <= 0 means no total timeout limit
      bool timed_out = config_.total_timeout_ms > 0 &&
                       elapsed > config_.total_timeout_ms && attempt > 1;
      if (attempt >= max_attempts || timed_out) {
        return result;
      }

      if (!ShouldRetry(error.kind)) {
        return result;
      }

      int32_t delay_ms = CalculateDelay(attempt);
      Sleep(delay_ms);
    }
  }

 private:
  /// \brief Check if the given error kind should trigger a retry
  bool ShouldRetry(ErrorKind kind) const {
    if (only_retry_on_.has_value()) {
      for (const auto& retry_kind : only_retry_on_.value()) {
        if (kind == retry_kind) {
          return true;
        }
      }
      return false;
    }

    if (stop_retry_on_.has_value()) {
      for (const auto& stop_kind : stop_retry_on_.value()) {
        if (kind == stop_kind) {
          return false;
        }
      }
    }

    return true;
  }

  /// \brief Calculate delay with exponential backoff and jitter
  int32_t CalculateDelay(int32_t attempt) const {
    // Calculate base delay with exponential backoff
    double base_delay = config_.min_wait_ms * std::pow(config_.scale_factor, attempt - 1);
    int32_t delay_ms = static_cast<int32_t>(
        std::min(base_delay, static_cast<double>(config_.max_wait_ms)));

    static thread_local std::mt19937 gen(std::random_device{}());
    int32_t jitter_range = std::max(1, delay_ms / 10);
    std::uniform_int_distribution<> dis(-jitter_range, jitter_range);
    delay_ms += dis(gen);
    return std::max(1, delay_ms);
  }

  /// \brief Sleep for the specified duration
  void Sleep(int32_t ms) const {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
  }

  RetryConfig config_;
  std::optional<std::vector<ErrorKind>> only_retry_on_;
  std::optional<std::vector<ErrorKind>> stop_retry_on_;
};

/// \brief Helper function to create a RetryRunner with table commit configuration
inline RetryRunner MakeCommitRetryRunner(int32_t num_retries, int32_t min_wait_ms,
                                         int32_t max_wait_ms, int32_t total_timeout_ms) {
  return RetryRunner()
      .WithRetries(num_retries)
      .WithExponentialBackoff(min_wait_ms, max_wait_ms, total_timeout_ms, 2.0)
      .OnlyRetryOn(ErrorKind::kCommitFailed);
}

}  // namespace iceberg
