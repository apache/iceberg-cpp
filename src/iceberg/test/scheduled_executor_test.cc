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

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"
#include "iceberg/util/scheduled_executor_internal.h"

namespace iceberg::internal {

// Inline execution lets the tests observe the timer's submitting thread.
using test::InlineExecutor;

TEST(ScheduledExecutorTest, SharesTimerAndHonorsDeadlinesAcrossExecutors) {
  InlineExecutor first_executor;
  InlineExecutor second_executor;
  std::promise<void> entered;
  std::promise<void> release;
  auto entered_done = entered.get_future();
  auto release_done = release.get_future().share();
  std::promise<std::thread::id> earlier;
  std::promise<std::thread::id> later;
  auto earlier_done = earlier.get_future();
  auto later_done = later.get_future();
  std::atomic<int> order{0};
  ScheduledExecutor first(first_executor);
  ScheduledExecutor second(second_executor);
  // Install both deadlines while the shared timer is still in the first callback.
  first.Schedule(std::chrono::milliseconds::zero(), ExecutorTask([&] {
                   entered.set_value();
                   release_done.wait();
                 }));
  auto entered_status = entered_done.wait_for(std::chrono::seconds(5));
  if (entered_status != std::future_status::ready) {
    release.set_value();
  }
  ASSERT_EQ(entered_status, std::future_status::ready);
  const auto now = std::chrono::steady_clock::now();
  first.Schedule(std::chrono::milliseconds(150), ExecutorTask([&] {
                   EXPECT_GE(std::chrono::steady_clock::now(),
                             now + std::chrono::milliseconds(150));
                   EXPECT_EQ(order++, 1);
                   later.set_value(std::this_thread::get_id());
                 }));
  second.Schedule(std::chrono::milliseconds(10), ExecutorTask([&] {
                    EXPECT_GE(std::chrono::steady_clock::now(),
                              now + std::chrono::milliseconds(10));
                    EXPECT_EQ(order++, 0);
                    earlier.set_value(std::this_thread::get_id());
                  }));
  release.set_value();
  ASSERT_EQ(earlier_done.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  ASSERT_EQ(later_done.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  auto timer_thread = earlier_done.get();
  EXPECT_NE(timer_thread, std::this_thread::get_id());
  EXPECT_EQ(timer_thread, later_done.get());
  EXPECT_EQ(first_executor.submit_count(), 2);
  EXPECT_EQ(second_executor.submit_count(), 1);
}

TEST(ScheduledExecutorTest, ExceptionsDoNotEscapeTimerThread) {
  // Simulates a user executor that throws, e.g. when it cannot start a thread.
  class ThrowingExecutor : public Executor {
   public:
    Status Submit(ExecutorTask) override { throw std::runtime_error("no threads"); }
  } executor;
  std::promise<Status> submitted;
  auto done = submitted.get_future();
  ScheduledExecutor scheduler(executor);
  scheduler.Schedule(std::chrono::milliseconds::zero(),
                     ExecutorTask([] { ADD_FAILURE(); }),
                     TaskSubmissionCallback([&](Status status) {
                       submitted.set_value(std::move(status));
                       throw std::runtime_error("callback failed");
                     }));
  ASSERT_EQ(done.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  auto status = done.get();
  EXPECT_THAT(status, IsError(ErrorKind::kUnknownError));
  EXPECT_THAT(status, HasErrorMessage("no threads"));
  // The shared timer survived both exceptions.
  EXPECT_TRUE(test::WaitForSharedTimer());
}

TEST(ScheduledExecutorTest, ShutdownCancelsOnlyItsOwnTasks) {
  test::ThreadExecutor first_executor(ServiceUnavailable("rejected"));
  InlineExecutor second_executor;
  std::promise<void> dispatched;
  auto done = dispatched.get_future();
  std::atomic<int> notifications{0};
  ScheduledExecutor first(first_executor);
  ScheduledExecutor second(second_executor);
  auto capture = std::make_shared<int>(42);
  std::weak_ptr<int> weak_capture = capture;
  first.Schedule(
      std::chrono::milliseconds(3'600'000),
      ExecutorTask([capture = std::move(capture)] { ADD_FAILURE() << *capture; }),
      TaskSubmissionCallback([&](Status) { ++notifications; }));
  first.Shutdown();
  first.Shutdown();
  EXPECT_TRUE(weak_capture.expired());
  // Scheduling after Shutdown is refused and drops the task without a callback.
  auto late_capture = std::make_shared<int>(7);
  std::weak_ptr<int> weak_late_capture = late_capture;
  first.Schedule(
      std::chrono::milliseconds::zero(),
      ExecutorTask([late_capture = std::move(late_capture)] { ADD_FAILURE(); }),
      TaskSubmissionCallback([&](Status) { ++notifications; }));
  EXPECT_TRUE(weak_late_capture.expired());

  second.Schedule(std::chrono::milliseconds::zero(),
                  ExecutorTask([&] { dispatched.set_value(); }));
  ASSERT_EQ(done.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(first_executor.submit_count(), 0);
  EXPECT_EQ(notifications, 0);
  EXPECT_EQ(second_executor.submit_count(), 1);
}

TEST(ScheduledExecutorTest, SubmissionFailureCanDestroySchedulerOnTimerThread) {
  test::ThreadExecutor executor(ServiceUnavailable("rejected"));
  struct Context {
    std::unique_ptr<ScheduledExecutor> scheduler;
    std::promise<void> rejected;
  };
  auto context = std::make_shared<Context>();
  context->scheduler = std::make_unique<ScheduledExecutor>(executor);
  auto rejected = context->rejected.get_future();
  context->scheduler->Schedule(std::chrono::milliseconds::zero(),
                               ExecutorTask([] { ADD_FAILURE(); }),
                               TaskSubmissionCallback([context](Status status) {
                                 EXPECT_FALSE(status.has_value());
                                 context->scheduler.reset();
                                 context->rejected.set_value();
                               }));
  ASSERT_EQ(rejected.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(context->scheduler, nullptr);
  EXPECT_EQ(executor.submit_count(), 1);
}

TEST(ScheduledExecutorTest, ShutdownWaitsForActiveSubmit) {
  InlineExecutor executor;
  std::promise<void> release;
  std::promise<void> entered;
  auto release_done = release.get_future().share();
  auto entered_done = entered.get_future();
  ScheduledExecutor scheduler(executor);
  scheduler.Schedule(std::chrono::milliseconds::zero(), ExecutorTask([&] {
                       entered.set_value();
                       release_done.wait();
                     }));
  auto entered_status = entered_done.wait_for(std::chrono::seconds(5));
  if (entered_status != std::future_status::ready) {
    release.set_value();
  }
  ASSERT_EQ(entered_status, std::future_status::ready);

  auto shutdown = std::async(std::launch::async, [&] { scheduler.Shutdown(); });
  EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);
  release.set_value();
  ASSERT_EQ(shutdown.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  shutdown.get();
}

}  // namespace iceberg::internal
