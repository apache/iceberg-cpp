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

#include <cstdint>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/util/retry_util.h"
#include "iceberg/util/retry_util_internal.h"

namespace iceberg::test {

using CommitFailedRetry = retry::OnlyRetryOn<ErrorKind::kCommitFailed>;

using TransientIORetry =
    retry::OnlyRetryOn<ErrorKind::kIOError, ErrorKind::kServiceUnavailable,
                       ErrorKind::kInternalServerError>;

class FakeRetryEnvironment {
 public:
  using Duration = RetryTestHooks::Duration;
  using TimePoint = RetryTestHooks::TimePoint;

  FakeRetryEnvironment() {
    hooks_.now = [this]() { return now_; };
    hooks_.sleep_for = [this](Duration duration) {
      sleep_durations_.push_back(duration);
      now_ += duration;
    };
    hooks_.jitter = [this](int32_t base_delay_ms) {
      observed_base_delays_ms_.push_back(base_delay_ms);
      return base_delay_ms + jitter_offset_ms_;
    };
  }

  void Advance(Duration duration) { now_ += duration; }

  void SetJitterOffsetMs(int32_t jitter_offset_ms) {
    jitter_offset_ms_ = jitter_offset_ms;
  }

  const RetryTestHooks& hooks() const { return hooks_; }

  const std::vector<Duration>& sleep_durations() const { return sleep_durations_; }

  const std::vector<int32_t>& observed_base_delays_ms() const {
    return observed_base_delays_ms_;
  }

 private:
  RetryTestHooks hooks_;
  TimePoint now_{};
  int32_t jitter_offset_ms_ = 0;
  std::vector<Duration> sleep_durations_;
  std::vector<int32_t> observed_base_delays_ms_;
};

}  // namespace iceberg::test
