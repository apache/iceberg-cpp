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

#include "iceberg/iceberg_export.h"

namespace iceberg {

struct RetryTestHooks {
  using Clock = std::chrono::steady_clock;
  using Duration = std::chrono::milliseconds;
  using TimePoint = Clock::time_point;

  std::function<TimePoint()> now;
  std::function<void(Duration)> sleep_for;
  std::function<int32_t(int32_t)> jitter;
};

ICEBERG_EXPORT const RetryTestHooks* GetActiveRetryTestHooks();
ICEBERG_EXPORT void SetActiveRetryTestHooks(const RetryTestHooks* hooks);

class ScopedRetryTestHooks {
 public:
  explicit ScopedRetryTestHooks(const RetryTestHooks& hooks)
      : previous_hooks_(GetActiveRetryTestHooks()) {
    SetActiveRetryTestHooks(&hooks);
  }

  ScopedRetryTestHooks(const ScopedRetryTestHooks&) = delete;
  ScopedRetryTestHooks& operator=(const ScopedRetryTestHooks&) = delete;
  ScopedRetryTestHooks(ScopedRetryTestHooks&&) = delete;
  ScopedRetryTestHooks& operator=(ScopedRetryTestHooks&&) = delete;

  ~ScopedRetryTestHooks() { SetActiveRetryTestHooks(previous_hooks_); }

 private:
  const RetryTestHooks* previous_hooks_;
};

}  // namespace iceberg
