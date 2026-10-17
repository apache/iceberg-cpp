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
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_set>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/util/executor.h"

namespace iceberg::internal {

class ScheduledExecutor;

class ScheduledTask {
 public:
  ScheduledTask() = default;
  ScheduledTask(ScheduledExecutor* executor, uint64_t id)
      : executor_(executor), id_(id) {}

  void Cancel();
  bool valid() const { return executor_ != nullptr && id_ != 0; }

 private:
  ScheduledExecutor* executor_ = nullptr;
  uint64_t id_ = 0;
};

/// A delay queue that dispatches due tasks to an existing Executor.
///
/// The executor is borrowed and must outlive this object and all scheduled tasks.
class ScheduledExecutor {
 public:
  explicit ScheduledExecutor(Executor& executor);
  ~ScheduledExecutor();

  ScheduledExecutor(const ScheduledExecutor&) = delete;
  ScheduledExecutor& operator=(const ScheduledExecutor&) = delete;

  ScheduledTask Schedule(std::chrono::steady_clock::time_point deadline,
                         ExecutorTask task);
  ScheduledTask Schedule(std::chrono::milliseconds delay, ExecutorTask task);
  void Cancel(uint64_t id);
  void Shutdown();

 private:
  struct Task {
    uint64_t id;
    std::chrono::steady_clock::time_point deadline;
    ExecutorTask task;
  };

  struct Earlier {
    bool operator()(const Task& lhs, const Task& rhs) const {
      return lhs.deadline > rhs.deadline;
    }
  };

  void Run();

  Executor& executor_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::priority_queue<Task, std::vector<Task>, Earlier> tasks_;
  std::unordered_set<uint64_t> cancelled_;
  std::unordered_set<uint64_t> scheduled_;
  uint64_t next_id_ = 1;
  bool shutdown_ = false;
  std::thread timer_thread_;
};

}  // namespace iceberg::internal
