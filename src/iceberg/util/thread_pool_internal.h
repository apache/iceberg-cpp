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

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Fixed-size worker pool for fire-and-wait task batches.
///
/// Internal API: not part of the public iceberg surface. Used today by
/// FileCleanupStrategy in update/expire_snapshots.cc to parallelize bulk deletes
/// without spawning a fresh std::thread per call. Workers are created eagerly in
/// the constructor and joined in the destructor.
///
/// All public methods are thread-safe.
class ICEBERG_EXPORT ThreadPool {
 public:
  /// \brief Construct a pool with `num_workers` worker threads. Must be > 0.
  explicit ThreadPool(std::size_t num_workers);

  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ThreadPool(ThreadPool&&) = delete;
  ThreadPool& operator=(ThreadPool&&) = delete;

  std::size_t num_workers() const { return workers_.size(); }

  /// \brief Submit a task. The returned future becomes ready after `task` runs.
  ///
  /// If `task` throws, the exception is captured into the returned future
  /// (standard std::packaged_task semantics) and the worker thread continues.
  std::future<void> Submit(std::function<void()> task);

  /// \brief Submit one task per item and block until every task has completed.
  ///
  /// Exceptions thrown by `work` are caught and discarded. Callers that need to
  /// observe per-item failures should record them inside `work` (e.g. via an
  /// atomic counter or a thread-safe collection).
  ///
  /// `items` and `work` must outlive this call. Both are captured by reference
  /// into the queued tasks; this is safe because RunAndWait blocks until every
  /// task has finished.
  template <typename Item, typename Fn>
  void RunAndWait(std::span<const Item> items, Fn&& work) {
    if (items.empty()) return;
    std::vector<std::future<void>> futures;
    futures.reserve(items.size());
    for (const auto& item : items) {
      futures.emplace_back(Submit([&item, &work]() {
        try {
          work(item);
        } catch (...) {
          // best-effort: see RunAndWait doc
        }
      }));
    }
    for (auto& f : futures) {
      f.wait();
    }
  }

 private:
  void WorkerLoop();

  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> queue_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool stop_ = false;
};

}  // namespace iceberg
