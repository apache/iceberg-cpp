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
#include <cstdint>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/util/thread_pool_internal.h"

namespace iceberg {

TEST(ThreadPoolTest, ZeroWorkersThrows) {
  EXPECT_THROW(ThreadPool(0), std::invalid_argument);
}

TEST(ThreadPoolTest, SubmitRunsTask) {
  ThreadPool pool(2);
  std::atomic<int> n{0};
  auto fut = pool.Submit([&] { n.fetch_add(1, std::memory_order_relaxed); });
  fut.wait();
  EXPECT_EQ(n.load(), 1);
}

TEST(ThreadPoolTest, RunAndWaitProcessesEveryItem) {
  ThreadPool pool(4);
  std::vector<int> items(100);
  for (int i = 0; i < 100; ++i) items[i] = i;

  std::atomic<std::int64_t> sum{0};
  pool.RunAndWait<int>(std::span<const int>(items),
                       [&](int v) { sum.fetch_add(v, std::memory_order_relaxed); });

  // 0 + 1 + ... + 99 = 4950
  EXPECT_EQ(sum.load(), 4950);
}

TEST(ThreadPoolTest, RunAndWaitEmptyIsNoOp) {
  ThreadPool pool(2);
  std::vector<int> items;
  std::atomic<int> seen{0};
  pool.RunAndWait<int>(std::span<const int>(items),
                       [&](int) { seen.fetch_add(1, std::memory_order_relaxed); });
  EXPECT_EQ(seen.load(), 0);
}

TEST(ThreadPoolTest, RunAndWaitExecutesConcurrently) {
  // Use a barrier-style check: each task increments an in-flight counter, sleeps
  // briefly, decrements, and records the peak. With multiple workers the peak
  // should exceed 1.
  constexpr int kWorkers = 4;
  constexpr int kItems = 8;
  ThreadPool pool(kWorkers);

  std::vector<int> items(kItems, 0);
  std::atomic<int> in_flight{0};
  std::atomic<int> peak{0};

  pool.RunAndWait<int>(std::span<const int>(items), [&](int) {
    int now = in_flight.fetch_add(1, std::memory_order_acq_rel) + 1;
    int prev = peak.load(std::memory_order_relaxed);
    while (now > prev &&
           !peak.compare_exchange_weak(prev, now, std::memory_order_relaxed)) {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    in_flight.fetch_sub(1, std::memory_order_acq_rel);
  });

  EXPECT_GT(peak.load(), 1) << "expected concurrent execution across workers";
}

TEST(ThreadPoolTest, ExceptionInTaskDoesNotKillWorker) {
  ThreadPool pool(1);
  // packaged_task captures the exception into the future; the worker loop must
  // continue and process the next submission.
  auto bad = pool.Submit([] { throw std::runtime_error("boom"); });
  EXPECT_THROW(bad.get(), std::runtime_error);

  std::atomic<int> ok{0};
  auto good = pool.Submit([&] { ok.store(1, std::memory_order_relaxed); });
  good.wait();
  EXPECT_EQ(ok.load(), 1);
}

TEST(ThreadPoolTest, DestructorJoinsAllPendingWork) {
  std::atomic<int> done{0};
  {
    ThreadPool pool(2);
    for (int i = 0; i < 16; ++i) {
      // Discard futures: we rely on the destructor to drain queued work.
      (void)pool.Submit([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        done.fetch_add(1, std::memory_order_relaxed);
      });
    }
  }
  EXPECT_EQ(done.load(), 16);
}

}  // namespace iceberg
