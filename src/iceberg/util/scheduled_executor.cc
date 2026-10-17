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

#include <optional>
#include <utility>

#include "iceberg/util/scheduled_executor_internal.h"

namespace iceberg::internal {

void ScheduledTask::Cancel() {
  if (valid()) {
    executor_->Cancel(id_);
    executor_ = nullptr;
    id_ = 0;
  }
}

ScheduledExecutor::ScheduledExecutor(Executor& executor)
    : executor_(executor), timer_thread_([this] { Run(); }) {}

ScheduledExecutor::~ScheduledExecutor() { Shutdown(); }

ScheduledTask ScheduledExecutor::Schedule(std::chrono::steady_clock::time_point deadline,
                                          ExecutorTask task) {
  std::lock_guard lock(mutex_);
  if (shutdown_) return {};
  auto id = next_id_++;
  tasks_.push(Task{.id = id, .deadline = deadline, .task = std::move(task)});
  scheduled_.insert(id);
  cv_.notify_one();
  return ScheduledTask(this, id);
}

ScheduledTask ScheduledExecutor::Schedule(std::chrono::milliseconds delay,
                                          ExecutorTask task) {
  return Schedule(std::chrono::steady_clock::now() + delay, std::move(task));
}

void ScheduledExecutor::Cancel(uint64_t id) {
  if (id == 0) return;
  {
    std::lock_guard lock(mutex_);
    if (scheduled_.erase(id) != 0) cancelled_.insert(id);
  }
  cv_.notify_one();
}

void ScheduledExecutor::Shutdown() {
  {
    std::lock_guard lock(mutex_);
    if (shutdown_) return;
    shutdown_ = true;
    while (!tasks_.empty()) tasks_.pop();
    cancelled_.clear();
    scheduled_.clear();
  }
  cv_.notify_one();
  if (timer_thread_.joinable()) timer_thread_.join();
}

void ScheduledExecutor::Run() {
  while (true) {
    std::optional<Task> task;
    {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [this] { return shutdown_ || !tasks_.empty(); });
      if (shutdown_) return;

      while (!tasks_.empty() && cancelled_.contains(tasks_.top().id)) {
        cancelled_.erase(tasks_.top().id);
        tasks_.pop();
      }
      if (tasks_.empty()) continue;

      auto deadline = tasks_.top().deadline;
      if (cv_.wait_until(lock, deadline, [this, deadline] {
            return shutdown_ || tasks_.empty() || tasks_.top().deadline != deadline ||
                   cancelled_.contains(tasks_.top().id);
          })) {
        continue;
      }

      task.emplace(std::move(const_cast<Task&>(tasks_.top())));
      tasks_.pop();
      scheduled_.erase(task->id);
      if (cancelled_.erase(task->id) != 0) continue;
    }
    std::ignore = executor_.Submit(std::move(task->task));
  }
}

}  // namespace iceberg::internal
