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

#include <concepts>
#include <limits>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/test/matchers.h"
#include "iceberg/util/retry_util_internal.h"

namespace iceberg {
namespace {

struct ResultReturningTask {
  Result<int> operator()() const { return 1; }
};

struct NonResultReturningTask {
  int operator()() const { return 1; }
};

static_assert(detail::RetryTask<ResultReturningTask>);
static_assert(!detail::RetryTask<NonResultReturningTask>);
static_assert(requires(RetryRunner runner, ResultReturningTask task) {
  { runner.Run(task) } -> std::same_as<Result<int>>;
});

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

}  // namespace

TEST(RetryRunnerTest, SuccessOnFirstAttempt) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 3,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .OnlyRetryOn(ErrorKind::kCommitFailed)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          return 42;
                        },
                        &attempts);

  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(*result, 42);
  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(attempts, 1);
}

TEST(RetryRunnerTest, RetryOnceThenSucceed) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 3,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .OnlyRetryOn(ErrorKind::kCommitFailed)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          if (call_count == 1) {
                            return CommitFailed("transient failure");
                          }
                          return 42;
                        },
                        &attempts);

  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(*result, 42);
  EXPECT_EQ(call_count, 2);
  EXPECT_EQ(attempts, 2);
}

TEST(RetryRunnerTest, MaxAttemptsExhausted) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 2,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .OnlyRetryOn(ErrorKind::kCommitFailed)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          return CommitFailed("always fails");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_EQ(call_count, 3);
  EXPECT_EQ(attempts, 3);
}

TEST(RetryRunnerTest, OnlyRetryOnFilter) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 3,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .OnlyRetryOn(ErrorKind::kCommitFailed)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          return ValidationFailed("schema conflict");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(attempts, 1);
}

TEST(RetryRunnerTest, OnlyRetryOnMatchingError) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 2,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .OnlyRetryOn(ErrorKind::kCommitFailed)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          if (call_count <= 2) {
                            return CommitFailed("transient");
                          }
                          return 100;
                        },
                        &attempts);

  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(*result, 100);
  EXPECT_EQ(call_count, 3);
  EXPECT_EQ(attempts, 3);
}

TEST(RetryRunnerTest, OnlyRetryOnTakesPrecedenceOverStopRetryOn) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 2,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .OnlyRetryOn(ErrorKind::kCommitFailed)
                    .StopRetryOn(ErrorKind::kCommitFailed)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          if (call_count == 1) {
                            return CommitFailed("transient");
                          }
                          return 100;
                        },
                        &attempts);

  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(*result, 100);
  EXPECT_EQ(call_count, 2);
  EXPECT_EQ(attempts, 2);
}

TEST(RetryRunnerTest, StopRetryOnMatchingError) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 5,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .StopRetryOn(ErrorKind::kCommitStateUnknown)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          return CommitStateUnknown("datacenter on fire");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kCommitStateUnknown));
  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(attempts, 1);
}

TEST(RetryRunnerTest, StopRetryOnNonMatchingErrorAllowsRetry) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 2,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .StopRetryOn({ErrorKind::kCommitStateUnknown})
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          if (call_count == 1) {
                            return CommitFailed("retryable");
                          }
                          return 88;
                        },
                        &attempts);

  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(*result, 88);
  EXPECT_EQ(call_count, 2);
  EXPECT_EQ(attempts, 2);
}

TEST(RetryRunnerTest, ZeroRetriesAllowsUnsetPolicyAndSkipsBackoffValidation) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 0,
                                        .min_wait_ms = 0,
                                        .max_wait_ms = 0,
                                        .total_timeout_ms = 5000,
                                        .scale_factor = 0.5})
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          return CommitFailed("fail");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(attempts, 1);
}

TEST(RetryRunnerTest, NegativeRetriesFailsBeforeTaskRuns) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = -1,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          return 1;
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("num_retries must be non-negative"));
  EXPECT_EQ(call_count, 0);
  EXPECT_EQ(attempts, 0);
}

TEST(RetryRunnerTest, InvalidBackoffConfigFailsBeforeTaskRuns) {
  struct InvalidConfigCase {
    RetryConfig config;
    const char* expected_message;
  };

  const std::vector<InvalidConfigCase> test_cases = {
      {.config = RetryConfig{.num_retries = std::numeric_limits<int32_t>::max(),
                             .min_wait_ms = 1,
                             .max_wait_ms = 10,
                             .total_timeout_ms = 5000},
       .expected_message = "num_retries is too large"},
      {.config = RetryConfig{.num_retries = 1,
                             .min_wait_ms = 0,
                             .max_wait_ms = 10,
                             .total_timeout_ms = 5000},
       .expected_message = "min_wait_ms must be positive"},
      {.config = RetryConfig{.num_retries = 1,
                             .min_wait_ms = 1,
                             .max_wait_ms = 0,
                             .total_timeout_ms = 5000},
       .expected_message = "max_wait_ms must be positive"},
      {.config = RetryConfig{.num_retries = 1,
                             .min_wait_ms = 20,
                             .max_wait_ms = 10,
                             .total_timeout_ms = 5000},
       .expected_message = "max_wait_ms must be greater than or equal to min_wait_ms"},
      {.config = RetryConfig{.num_retries = 1,
                             .min_wait_ms = 1,
                             .max_wait_ms = 10,
                             .total_timeout_ms = 5000,
                             .scale_factor = 0.5},
       .expected_message = "scale_factor must be finite and at least 1.0"},
      {.config = RetryConfig{.num_retries = 1,
                             .min_wait_ms = 1,
                             .max_wait_ms = 10,
                             .total_timeout_ms = 5000,
                             .scale_factor = std::numeric_limits<double>::infinity()},
       .expected_message = "scale_factor must be finite and at least 1.0"},
  };

  for (const auto& test_case : test_cases) {
    int call_count = 0;
    int32_t attempts = 0;

    auto result = RetryRunner(test_case.config)
                      .OnlyRetryOn(ErrorKind::kCommitFailed)
                      .Run(
                          [&]() -> Result<int> {
                            ++call_count;
                            return 1;
                          },
                          &attempts);

    EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument))
        << test_case.expected_message;
    EXPECT_THAT(result, HasErrorMessage(test_case.expected_message));
    EXPECT_EQ(call_count, 0);
    EXPECT_EQ(attempts, 0);
  }
}

TEST(RetryRunnerTest, UnsetRetryPolicyFailsBeforeTaskRuns) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 1,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          return CommitFailed("fail");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Retry policy must be explicitly configured"));
  EXPECT_EQ(call_count, 0);
  EXPECT_EQ(attempts, 0);
}

TEST(RetryRunnerTest, EmptyOnlyRetryOnPolicyFailsBeforeTaskRuns) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 1,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .OnlyRetryOn(std::initializer_list<ErrorKind>{})
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          return CommitFailed("fail");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result,
              HasErrorMessage("Retry policy must include at least one error kind"));
  EXPECT_EQ(call_count, 0);
  EXPECT_EQ(attempts, 0);
}

TEST(RetryRunnerTest, EmptyStopRetryOnPolicyFailsBeforeTaskRuns) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 1,
                                        .min_wait_ms = 1,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 5000})
                    .StopRetryOn({})
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          return CommitFailed("fail");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result,
              HasErrorMessage("Retry policy must include at least one error kind"));
  EXPECT_EQ(call_count, 0);
  EXPECT_EQ(attempts, 0);
}

TEST(RetryRunnerTest, TotalTimeoutStopsBeforeStartingAnotherAttempt) {
  FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks scoped_hooks(fake_retry.hooks());
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 3,
                                        .min_wait_ms = 20,
                                        .max_wait_ms = 20,
                                        .total_timeout_ms = 15})
                    .OnlyRetryOn(ErrorKind::kCommitFailed)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          if (call_count == 1) {
                            fake_retry.Advance(FakeRetryEnvironment::Duration(10));
                          }
                          return CommitFailed("retry budget exhausted");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(attempts, 1);
  EXPECT_TRUE(fake_retry.sleep_durations().empty());
  EXPECT_EQ(fake_retry.observed_base_delays_ms(), std::vector<int32_t>({20}));
}

TEST(RetryRunnerTest, TotalTimeoutStopsWhenDelayEqualsRemainingBudget) {
  FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks scoped_hooks(fake_retry.hooks());
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 3,
                                        .min_wait_ms = 10,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 20})
                    .OnlyRetryOn(ErrorKind::kCommitFailed)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          fake_retry.Advance(FakeRetryEnvironment::Duration(10));
                          return CommitFailed("retry budget exhausted");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(attempts, 1);
  EXPECT_TRUE(fake_retry.sleep_durations().empty());
  EXPECT_EQ(fake_retry.observed_base_delays_ms(), std::vector<int32_t>({10}));
}

TEST(RetryRunnerTest, NonPositiveTotalTimeoutDisablesDeadline) {
  FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks scoped_hooks(fake_retry.hooks());
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 2,
                                        .min_wait_ms = 10,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 0})
                    .OnlyRetryOn(ErrorKind::kCommitFailed)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          fake_retry.Advance(FakeRetryEnvironment::Duration(100));
                          if (call_count <= 2) {
                            return CommitFailed("transient");
                          }
                          return 123;
                        },
                        &attempts);

  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(*result, 123);
  EXPECT_EQ(call_count, 3);
  EXPECT_EQ(attempts, 3);
  EXPECT_EQ(fake_retry.sleep_durations(), std::vector<FakeRetryEnvironment::Duration>(
                                              {FakeRetryEnvironment::Duration(10),
                                               FakeRetryEnvironment::Duration(10)}));
  EXPECT_EQ(fake_retry.observed_base_delays_ms(), std::vector<int32_t>({10, 10}));
}

TEST(RetryRunnerTest, RetryDelayDoesNotExceedMaxWaitAfterJitter) {
  FakeRetryEnvironment fake_retry;
  fake_retry.SetJitterOffsetMs(100);
  ScopedRetryTestHooks scoped_hooks(fake_retry.hooks());
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner(RetryConfig{.num_retries = 1,
                                        .min_wait_ms = 10,
                                        .max_wait_ms = 10,
                                        .total_timeout_ms = 0})
                    .OnlyRetryOn(ErrorKind::kCommitFailed)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          if (call_count == 1) {
                            return CommitFailed("transient");
                          }
                          return 321;
                        },
                        &attempts);

  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(*result, 321);
  EXPECT_EQ(call_count, 2);
  EXPECT_EQ(attempts, 2);
  EXPECT_EQ(fake_retry.sleep_durations(), std::vector<FakeRetryEnvironment::Duration>(
                                              {FakeRetryEnvironment::Duration(10)}));
  EXPECT_EQ(fake_retry.observed_base_delays_ms(), std::vector<int32_t>({10}));
}

TEST(RetryRunnerTest, MakeCommitRetryRunnerConfig) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = MakeCommitRetryRunner(2, 1, 10, 5000)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          return ValidationFailed("not retryable");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(attempts, 1);
}

TEST(RetryRunnerTest, MakeCommitRetryRunnerRetriesCommitFailed) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = MakeCommitRetryRunner(3, 1, 10, 5000)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          if (call_count <= 2) {
                            return CommitFailed("transient");
                          }
                          return 99;
                        },
                        &attempts);

  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(*result, 99);
  EXPECT_EQ(call_count, 3);
  EXPECT_EQ(attempts, 3);
}

TEST(RetryRunnerTest, OnlyRetryOnMultipleErrorKinds) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result =
      RetryRunner(RetryConfig{.num_retries = 5,
                              .min_wait_ms = 1,
                              .max_wait_ms = 10,
                              .total_timeout_ms = 5000})
          .OnlyRetryOn({ErrorKind::kCommitFailed, ErrorKind::kServiceUnavailable})
          .Run(
              [&]() -> Result<int> {
                ++call_count;
                if (call_count == 1) {
                  return CommitFailed("conflict");
                }
                if (call_count == 2) {
                  return ServiceUnavailable("server busy");
                }
                return 77;
              },
              &attempts);

  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(*result, 77);
  EXPECT_EQ(call_count, 3);
  EXPECT_EQ(attempts, 3);
}

}  // namespace iceberg
