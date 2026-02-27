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

#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// --------------------------------------------------------------------------
// Test: Successful on first attempt — no retries
// --------------------------------------------------------------------------
TEST(RetryRunnerTest, SuccessOnFirstAttempt) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner()
                    .WithRetries(3)
                    .WithExponentialBackoff(1, 10, 5000, 2.0)
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

// --------------------------------------------------------------------------
// Test: Retry once then succeed
// --------------------------------------------------------------------------
TEST(RetryRunnerTest, RetryOnceThenSucceed) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner()
                    .WithRetries(3)
                    .WithExponentialBackoff(1, 10, 5000, 2.0)
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

// --------------------------------------------------------------------------
// Test: Max attempts exhausted
// --------------------------------------------------------------------------
TEST(RetryRunnerTest, MaxAttemptsExhausted) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner()
                    .WithRetries(2)
                    .WithExponentialBackoff(1, 10, 5000, 2.0)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          return CommitFailed("always fails");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_EQ(call_count, 3);  // 1 initial + 2 retries
  EXPECT_EQ(attempts, 3);
}

// --------------------------------------------------------------------------
// Test: OnlyRetryOn filters correctly
// --------------------------------------------------------------------------
TEST(RetryRunnerTest, OnlyRetryOnFilter) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner()
                    .WithRetries(3)
                    .WithExponentialBackoff(1, 10, 5000, 2.0)
                    .OnlyRetryOn(ErrorKind::kCommitFailed)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          // Return a non-retryable error
                          return ValidationFailed("schema conflict");
                        },
                        &attempts);

  // Should NOT retry because ValidationFailed is not in the retry list
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(attempts, 1);
}

// --------------------------------------------------------------------------
// Test: OnlyRetryOn retries matching error
// --------------------------------------------------------------------------
TEST(RetryRunnerTest, OnlyRetryOnMatchingError) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner()
                    .WithRetries(2)
                    .WithExponentialBackoff(1, 10, 5000, 2.0)
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
  EXPECT_EQ(call_count, 3);  // 2 failures + 1 success
  EXPECT_EQ(attempts, 3);
}

// --------------------------------------------------------------------------
// Test: StopRetryOn stops on matching error
// --------------------------------------------------------------------------
TEST(RetryRunnerTest, StopRetryOnMatchingError) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner()
                    .WithRetries(5)
                    .WithExponentialBackoff(1, 10, 5000, 2.0)
                    .StopRetryOn({ErrorKind::kCommitStateUnknown})
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

// --------------------------------------------------------------------------
// Test: Zero retries means only one attempt
// --------------------------------------------------------------------------
TEST(RetryRunnerTest, ZeroRetries) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner()
                    .WithRetries(0)
                    .WithExponentialBackoff(1, 10, 5000, 2.0)
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

// --------------------------------------------------------------------------
// Test: MakeCommitRetryRunner has correct configuration
// --------------------------------------------------------------------------
TEST(RetryRunnerTest, MakeCommitRetryRunnerConfig) {
  int call_count = 0;
  int32_t attempts = 0;

  // MakeCommitRetryRunner should only retry on kCommitFailed
  auto result = MakeCommitRetryRunner(2, 1, 10, 5000)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          // ValidationFailed should not be retried
                          return ValidationFailed("not retryable");
                        },
                        &attempts);

  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(attempts, 1);
}

// --------------------------------------------------------------------------
// Test: MakeCommitRetryRunner retries CommitFailed
// --------------------------------------------------------------------------
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

// --------------------------------------------------------------------------
// Test: OnlyRetryOn with multiple error kinds
// --------------------------------------------------------------------------
TEST(RetryRunnerTest, OnlyRetryOnMultipleErrorKinds) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result =
      RetryRunner()
          .WithRetries(5)
          .WithExponentialBackoff(1, 10, 5000, 2.0)
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

// --------------------------------------------------------------------------
// Test: Default retry (no filter) retries all errors
// --------------------------------------------------------------------------
TEST(RetryRunnerTest, DefaultRetryAllErrors) {
  int call_count = 0;
  int32_t attempts = 0;

  auto result = RetryRunner()
                    .WithRetries(3)
                    .WithExponentialBackoff(1, 10, 5000, 2.0)
                    .Run(
                        [&]() -> Result<int> {
                          ++call_count;
                          if (call_count == 1) {
                            return IOError("disk full");
                          }
                          if (call_count == 2) {
                            return ValidationFailed("bad schema");
                          }
                          return 55;
                        },
                        &attempts);

  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(*result, 55);
  EXPECT_EQ(call_count, 3);
  EXPECT_EQ(attempts, 3);
}

}  // namespace iceberg
