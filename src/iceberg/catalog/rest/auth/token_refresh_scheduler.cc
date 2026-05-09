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

#include "iceberg/catalog/rest/auth/token_refresh_scheduler.h"

#include <algorithm>

namespace iceberg::rest::auth {

TokenRefreshScheduler& TokenRefreshScheduler::Instance() {
  static TokenRefreshScheduler instance;
  return instance;
}

TokenRefreshScheduler::TokenRefreshScheduler() : worker_([this] { Run(); }) {}

TokenRefreshScheduler::~TokenRefreshScheduler() { Shutdown(); }

uint64_t TokenRefreshScheduler::Schedule(std::chrono::milliseconds delay,
                                         std::function<void()> callback) {
  std::lock_guard lock(mutex_);
  if (shutdown_) {
    return 0;
  }
  uint64_t id = next_id_++;
  tasks_.push_back(Task{.id = id,
                        .fire_at = std::chrono::steady_clock::now() + delay,
                        .callback = std::move(callback)});
  cv_.notify_one();
  return id;
}

void TokenRefreshScheduler::Cancel(uint64_t handle) {
  if (handle == 0) return;
  std::lock_guard lock(mutex_);
  std::erase_if(tasks_, [handle](const Task& t) { return t.id == handle; });
}

void TokenRefreshScheduler::Shutdown() {
  {
    std::lock_guard lock(mutex_);
    if (shutdown_) return;
    shutdown_ = true;
    tasks_.clear();
  }
  cv_.notify_one();
  if (worker_.joinable()) {
    worker_.join();
  }
}

void TokenRefreshScheduler::Run() {
  while (true) {
    std::function<void()> callback;

    {
      std::unique_lock lock(mutex_);

      if (tasks_.empty() && !shutdown_) {
        // Wait until a task is added or shutdown is requested
        cv_.wait(lock, [this] { return !tasks_.empty() || shutdown_; });
      }

      if (shutdown_) break;
      if (tasks_.empty()) continue;

      // Find the task with the earliest fire_at
      auto earliest_it = std::min_element(
          tasks_.begin(), tasks_.end(),
          [](const Task& a, const Task& b) { return a.fire_at < b.fire_at; });

      auto fire_at = earliest_it->fire_at;
      auto target_id = earliest_it->id;

      // Wait until fire_at or until woken (new task, cancel, or shutdown).
      // Note: The predicate does O(n) scan on each spurious wakeup. This is
      // acceptable for the expected task count (< 10). If task count grows
      // significantly, consider replacing vector with a priority queue.
      cv_.wait_until(lock, fire_at, [&] {
        // Wake up if: shutdown, task list changed, or time is up
        if (shutdown_) return true;
        if (tasks_.empty()) return true;
        // Check if the earliest task has changed (new task added or cancelled)
        auto new_earliest = std::min_element(
            tasks_.begin(), tasks_.end(),
            [](const Task& a, const Task& b) { return a.fire_at < b.fire_at; });
        return new_earliest->id != target_id;
      });

      if (shutdown_) break;

      // If we were woken because the earliest task changed, loop again
      auto now = std::chrono::steady_clock::now();
      auto due_it = std::find_if(tasks_.begin(), tasks_.end(),
                                 [now](const Task& t) { return t.fire_at <= now; });
      if (due_it == tasks_.end()) continue;

      callback = std::move(due_it->callback);
      tasks_.erase(due_it);
    }

    // Execute callback outside the lock
    if (callback) {
      callback();
    }
  }
}

}  // namespace iceberg::rest::auth
