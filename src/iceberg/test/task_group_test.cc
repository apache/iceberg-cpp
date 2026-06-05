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

#include "iceberg/util/task_group.h"

#include <atomic>
#include <concepts>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/retry.h"
#include "iceberg/util/functional.h"

namespace iceberg {

namespace {

RetryConfig FastRetryConfig(int32_t num_retries = 2) {
  return RetryConfig{.num_retries = num_retries,
                     .min_wait_ms = 1,
                     .max_wait_ms = 1,
                     .total_timeout_ms = 0};
}

}  // namespace

TEST(TaskGroupCompileTest, TaskConcepts) {
  auto move_only_mutable_lambda = [value = std::make_unique<int>(1)]() mutable -> Status {
    return *value == 1 ? Status{} : IOError("unexpected value");
  };
  using MoveOnlyMutableLambda = decltype(move_only_mutable_lambda);

  auto move_only_const_lambda = [value = std::make_unique<int>(1)]() -> Status {
    return *value == 1 ? Status{} : IOError("unexpected value");
  };
  using MoveOnlyConstLambda = decltype(move_only_const_lambda);

  auto copyable_mutable_lambda = [attempt = 0]() mutable -> Status {
    return ++attempt > 0 ? Status{} : Status{};
  };
  using CopyableMutableLambda = decltype(copyable_mutable_lambda);

  static_assert(!std::copy_constructible<FnOnce<void()>>);
  static_assert(!std::default_initializable<FnOnce<void()>>);
  static_assert(std::move_constructible<FnOnce<void()>>);

  static_assert(internal::OnceStatusTask<MoveOnlyMutableLambda>);
  static_assert(!internal::OnceStatusTask<MoveOnlyMutableLambda&>);
  static_assert(internal::OnceStatusTask<MoveOnlyConstLambda>);
  static_assert(!internal::OnceStatusTask<MoveOnlyConstLambda&>);
  static_assert(internal::OnceStatusTask<CopyableMutableLambda>);
  static_assert(internal::OnceStatusTask<CopyableMutableLambda&>);

  static_assert(!internal::RepeatableStatusTask<MoveOnlyMutableLambda>);
  static_assert(internal::RepeatableStatusTask<MoveOnlyConstLambda>);
  static_assert(internal::RepeatableStatusTask<CopyableMutableLambda>);

  static_assert(!internal::RetryableStatusTask<MoveOnlyMutableLambda>);
  static_assert(!internal::RetryableStatusTask<MoveOnlyMutableLambda&>);
  static_assert(internal::RetryableStatusTask<MoveOnlyConstLambda>);
  static_assert(!internal::RetryableStatusTask<MoveOnlyConstLambda&>);
  static_assert(internal::RetryableStatusTask<CopyableMutableLambda>);
  static_assert(internal::RetryableStatusTask<CopyableMutableLambda&>);
}

TEST(FnOnceTest, SupportsMoveOnlyCapture) {
  auto value = std::make_unique<int>(41);
  FnOnce<int()> task([value = std::move(value)]() { return *value + 1; });

  EXPECT_EQ(std::move(task)(), 42);
}

TEST(TaskGroupTest, UsesExecutor) {
  test::ThreadExecutor executor;
  TaskGroup group;
  bool ran = false;

  group.SetExecutor(std::ref(executor));
  group.Submit([&]() -> Status {
    ran = true;
    return {};
  });

  EXPECT_THAT(std::move(group).Run(), IsOk());
  EXPECT_TRUE(ran);
  EXPECT_EQ(executor.submit_count(), 1);
}

TEST(TaskGroupTest, ReturnsSubmitError) {
  test::ThreadExecutor executor(ServiceUnavailable("executor busy"));
  TaskGroup group;

  group.SetExecutor(std::ref(executor));
  group.Submit([]() -> Status { return {}; });

  EXPECT_THAT(std::move(group).Run(), IsError(ErrorKind::kServiceUnavailable));
  EXPECT_EQ(executor.submit_count(), 1);
}

TEST(TaskGroupTest, DirectMoveOnlyTask) {
  TaskGroup group;
  auto value = std::make_unique<int>(7);
  int observed = 0;

  group.Submit([value = std::move(value), &observed]() mutable -> Status {
    observed = *value;
    return {};
  });

  EXPECT_THAT(std::move(group).Run(), IsOk());
  EXPECT_EQ(observed, 7);
}

TEST(TaskGroupTest, ClearsExecutor) {
  test::ThreadExecutor executor;
  TaskGroup group;
  int call_count = 0;

  group.SetExecutor(std::ref(executor));
  group.SetExecutor(std::nullopt);
  group.Submit([&]() -> Status {
    ++call_count;
    return {};
  });

  EXPECT_THAT(std::move(group).Run(), IsOk());
  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(executor.submit_count(), 0);
}

TEST(TaskGroupTest, FluentSubmit) {
  test::ThreadExecutor executor;
  int call_count = 0;

  auto status = TaskGroup()
                    .SetExecutor(std::ref(executor))
                    .Submit([&]() -> Status {
                      ++call_count;
                      return {};
                    })
                    .Submit([&]() -> Status {
                      ++call_count;
                      return {};
                    })
                    .Run();

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(call_count, 2);
}

TEST(TaskGroupTest, DirectAggregatesErrors) {
  TaskGroup group;
  int call_count = 0;

  group.Submit([&]() -> Status {
    ++call_count;
    return IOError("first failure");
  });
  group.Submit([&]() -> Status {
    ++call_count;
    return ValidationFailed("second failure");
  });

  auto status = std::move(group).Run();
  EXPECT_THAT(status, IsError(ErrorKind::kIOError));
  EXPECT_THAT(status, HasErrorMessage("Task group failed with 2 errors"));
  EXPECT_THAT(status, HasErrorMessage("first failure"));
  EXPECT_THAT(status, HasErrorMessage("second failure"));
  EXPECT_EQ(call_count, 2);
}

TEST(TaskGroupTest, ParallelSubmitsAll) {
  test::ThreadExecutor executor;
  TaskGroup group;
  std::atomic<int> call_count = 0;

  group.SetExecutor(std::ref(executor));
  group.Submit([&]() -> Status {
    call_count.fetch_add(1, std::memory_order_relaxed);
    return {};
  });
  group.Submit([&]() -> Status {
    call_count.fetch_add(1, std::memory_order_relaxed);
    return {};
  });

  EXPECT_THAT(std::move(group).Run(), IsOk());
  EXPECT_EQ(call_count.load(std::memory_order_relaxed), 2);
  EXPECT_EQ(executor.submit_count(), 2);
}

TEST(TaskGroupTest, ParallelAggregatesErrors) {
  test::ThreadExecutor executor;
  TaskGroup group;
  std::atomic<int> call_count = 0;

  group.SetExecutor(std::ref(executor));
  group.Submit([&]() -> Status {
    call_count.fetch_add(1, std::memory_order_relaxed);
    return IOError("first failure");
  });
  group.Submit([&]() -> Status {
    call_count.fetch_add(1, std::memory_order_relaxed);
    return ValidationFailed("second failure");
  });

  auto status = std::move(group).Run();
  EXPECT_THAT(status, IsError(ErrorKind::kIOError));
  EXPECT_THAT(status, HasErrorMessage("Task group failed with 2 errors"));
  EXPECT_THAT(status, HasErrorMessage("first failure"));
  EXPECT_THAT(status, HasErrorMessage("second failure"));
  EXPECT_EQ(call_count.load(std::memory_order_relaxed), 2);
  EXPECT_EQ(executor.submit_count(), 2);
}

TEST(TaskGroupTest, ParallelSubmitErrors) {
  test::ThreadExecutor executor(ServiceUnavailable("executor busy"));
  TaskGroup group;
  int call_count = 0;

  group.SetExecutor(std::ref(executor));
  group.Submit([&]() -> Status {
    ++call_count;
    return {};
  });
  group.Submit([&]() -> Status {
    ++call_count;
    return {};
  });

  auto status = std::move(group).Run();
  EXPECT_THAT(status, IsError(ErrorKind::kServiceUnavailable));
  EXPECT_THAT(status, HasErrorMessage("Task group failed with 2 errors"));
  EXPECT_THAT(status, HasErrorMessage("executor busy"));
  EXPECT_EQ(call_count, 0);
  EXPECT_EQ(executor.submit_count(), 2);
}

TEST(TaskGroupTest, RetriesTasks) {
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks retry_hooks(fake_retry.hooks());
  TaskGroup<test::TransientIORetry> group{FastRetryConfig()};
  int call_count = 0;

  group.Submit([&]() -> Status {
    ++call_count;
    if (call_count == 1) {
      return IOError("transient read failure");
    }
    return {};
  });

  EXPECT_THAT(std::move(group).Run(), IsOk());
  EXPECT_EQ(call_count, 2);
  EXPECT_EQ(fake_retry.sleep_durations(),
            std::vector<test::FakeRetryEnvironment::Duration>(
                {test::FakeRetryEnvironment::Duration(1)}));
}

TEST(TaskGroupTest, RetryReusesTaskState) {
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks retry_hooks(fake_retry.hooks());
  TaskGroup<test::TransientIORetry> group{FastRetryConfig()};

  group.Submit([attempt = 0]() mutable -> Status {
    ++attempt;
    if (attempt == 1) {
      return IOError("transient read failure");
    }
    return {};
  });

  EXPECT_THAT(std::move(group).Run(), IsOk());
  EXPECT_EQ(fake_retry.sleep_durations(),
            std::vector<test::FakeRetryEnvironment::Duration>(
                {test::FakeRetryEnvironment::Duration(1)}));
}

TEST(TaskGroupTest, RetryAcceptsMoveOnlyRepeatableTask) {
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks retry_hooks(fake_retry.hooks());
  TaskGroup<test::TransientIORetry> group{FastRetryConfig()};
  int call_count = 0;
  auto value = std::make_unique<int>(7);

  group.Submit([value = std::move(value), &call_count]() -> Status {
    ++call_count;
    if (call_count == 1) {
      return IOError("transient read failure");
    }
    return *value == 7 ? Status{} : IOError("unexpected value");
  });

  EXPECT_THAT(std::move(group).Run(), IsOk());
  EXPECT_EQ(call_count, 2);
  EXPECT_EQ(fake_retry.sleep_durations(),
            std::vector<test::FakeRetryEnvironment::Duration>(
                {test::FakeRetryEnvironment::Duration(1)}));
}

TEST(TaskGroupTest, DefaultRetryConfig) {
  TaskGroup<test::TransientIORetry> group;
  int call_count = 0;

  group.Submit([&]() -> Status {
    ++call_count;
    return {};
  });

  EXPECT_THAT(std::move(group).Run(), IsOk());
  EXPECT_EQ(call_count, 1);
}

TEST(TaskGroupTest, DoesNotRetryNotFound) {
  TaskGroup<test::TransientIORetry> group{FastRetryConfig()};
  int call_count = 0;

  group.Submit([&]() -> Status {
    ++call_count;
    return NotFound("missing manifest");
  });

  EXPECT_THAT(std::move(group).Run(), IsError(ErrorKind::kNotFound));
  EXPECT_EQ(call_count, 1);
}

TEST(TaskGroupTest, RetryUsesExecutor) {
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks retry_hooks(fake_retry.hooks());
  test::ThreadExecutor executor;
  TaskGroup<test::TransientIORetry> group{FastRetryConfig()};
  std::atomic<int> first_task_calls = 0;
  std::atomic<int> second_task_calls = 0;

  group.SetExecutor(std::ref(executor));
  group.Submit([&]() -> Status {
    auto call_count = first_task_calls.fetch_add(1, std::memory_order_relaxed) + 1;
    if (call_count == 1) {
      return ServiceUnavailable("server busy");
    }
    return {};
  });
  group.Submit([&]() -> Status {
    second_task_calls.fetch_add(1, std::memory_order_relaxed);
    return {};
  });

  EXPECT_THAT(std::move(group).Run(), IsOk());
  EXPECT_EQ(first_task_calls.load(std::memory_order_relaxed), 2);
  EXPECT_EQ(second_task_calls.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(executor.submit_count(), 2);
}

}  // namespace iceberg
