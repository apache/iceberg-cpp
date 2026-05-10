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

#include "iceberg/util/retry_util.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <random>
#include <thread>

#include "iceberg/util/retry_util_internal.h"

namespace iceberg {
namespace {

const RetryTestHooks*& ActiveRetryTestHooks() {
  // Keep test hooks thread-local so fake retry timing in one test thread does not
  // leak into unrelated retry work or require synchronization around a global pointer.
  static thread_local const RetryTestHooks* active_retry_test_hooks = nullptr;
  return active_retry_test_hooks;
}

RetryTestHooks::TimePoint RetryNow() {
  const auto* hooks = GetActiveRetryTestHooks();
  if (hooks != nullptr && hooks->now) {
    return hooks->now();
  }
  return RetryTestHooks::Clock::now();
}

void RetrySleepFor(RetryTestHooks::Duration duration) {
  const auto* hooks = GetActiveRetryTestHooks();
  if (hooks != nullptr && hooks->sleep_for) {
    hooks->sleep_for(duration);
    return;
  }
  std::this_thread::sleep_for(duration);
}

int32_t ApplyRetryJitter(int32_t base_delay_ms) {
  const auto* hooks = GetActiveRetryTestHooks();
  if (hooks != nullptr && hooks->jitter) {
    return hooks->jitter(base_delay_ms);
  }

  static thread_local std::mt19937 gen(std::random_device{}());
  const int32_t jitter_range = std::max(1, base_delay_ms / 10);
  std::uniform_int_distribution<> dis(0, jitter_range - 1);
  const int64_t jittered_delay_ms = static_cast<int64_t>(base_delay_ms) + dis(gen);
  return static_cast<int32_t>(
      std::min<int64_t>(jittered_delay_ms, std::numeric_limits<int32_t>::max()));
}

}  // namespace

const RetryTestHooks* GetActiveRetryTestHooks() { return ActiveRetryTestHooks(); }

void SetActiveRetryTestHooks(const RetryTestHooks* hooks) {
  ActiveRetryTestHooks() = hooks;
}

Status RetryRunner::ValidateConfig() const {
  if (config_.num_retries < 0) {
    return InvalidArgument("num_retries must be non-negative, got {}",
                           config_.num_retries);
  }
  if (config_.num_retries == 0) {
    return {};
  }
  if (config_.num_retries == std::numeric_limits<int32_t>::max()) {
    return InvalidArgument("num_retries is too large, got {}", config_.num_retries);
  }
  if (config_.min_wait_ms <= 0) {
    return InvalidArgument("min_wait_ms must be positive, got {}", config_.min_wait_ms);
  }
  if (config_.max_wait_ms <= 0) {
    return InvalidArgument("max_wait_ms must be positive, got {}", config_.max_wait_ms);
  }
  if (config_.max_wait_ms < config_.min_wait_ms) {
    return InvalidArgument("max_wait_ms must be greater than or equal to min_wait_ms");
  }
  if (!std::isfinite(config_.scale_factor) || config_.scale_factor < 1.0) {
    return InvalidArgument("scale_factor must be finite and at least 1.0, got {}",
                           config_.scale_factor);
  }
  if (retry_policy_mode_ == RetryPolicyMode::kUnset) {
    return InvalidArgument(
        "Retry policy must be explicitly configured with OnlyRetryOn(...) or "
        "StopRetryOn(...) when num_retries > 0");
  }
  if (retry_error_kinds_.empty()) {
    return InvalidArgument("Retry policy must include at least one error kind");
  }

  return {};
}

std::optional<RetryRunner::TimePoint> RetryRunner::ComputeDeadline() const {
  if (config_.total_timeout_ms <= 0) {
    return std::nullopt;
  }
  return RetryNow() + Duration(config_.total_timeout_ms);
}

bool RetryRunner::HasTimedOut(const std::optional<TimePoint>& deadline) const {
  return deadline.has_value() && RetryNow() >= *deadline;
}

bool RetryRunner::ShouldRetry(ErrorKind kind) const {
  const bool policy_contains_kind = std::ranges::contains(retry_error_kinds_, kind);
  switch (retry_policy_mode_) {
    case RetryPolicyMode::kOnlyRetryOn:
      return policy_contains_kind;
    case RetryPolicyMode::kStopRetryOn:
      return !policy_contains_kind;
    case RetryPolicyMode::kUnset:
      return false;
  }
  return false;
}

bool RetryRunner::CanRetry(ErrorKind kind, int32_t attempt, int32_t max_attempts,
                           const std::optional<TimePoint>& deadline) const {
  return attempt < max_attempts && !HasTimedOut(deadline) && ShouldRetry(kind);
}

std::optional<RetryRunner::Duration> RetryRunner::RetryDelayWithinBudget(
    int32_t attempt, const std::optional<TimePoint>& deadline) const {
  const auto delay = Duration(CalculateDelay(attempt));
  if (!deadline.has_value()) {
    return delay;
  }

  const auto now = RetryNow();
  if (now >= *deadline) {
    return std::nullopt;
  }

  const auto remaining = std::chrono::duration_cast<Duration>(*deadline - now);
  if (remaining <= Duration::zero() || delay >= remaining) {
    return std::nullopt;
  }

  return delay;
}

bool RetryRunner::WaitForNextAttempt(int32_t attempt,
                                     const std::optional<TimePoint>& deadline) const {
  const auto delay = RetryDelayWithinBudget(attempt, deadline);
  if (!delay.has_value()) {
    return false;
  }

  RetrySleepFor(*delay);
  return !HasTimedOut(deadline);
}

int32_t RetryRunner::CalculateDelay(int32_t attempt) const {
  const double base_delay =
      config_.min_wait_ms * std::pow(config_.scale_factor, attempt - 1);
  const int32_t delay_ms = static_cast<int32_t>(
      std::min(base_delay, static_cast<double>(config_.max_wait_ms)));
  return std::clamp(ApplyRetryJitter(delay_ms), 1, config_.max_wait_ms);
}

}  // namespace iceberg
