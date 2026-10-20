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

#include <condition_variable>
#include <exception>
#include <iterator>
#include <map>
#include <mutex>
#include <thread>
#include <utility>

#include "iceberg/util/scheduled_executor_internal.h"

namespace iceberg::internal {

// Kept alive so static destructors can still schedule tasks.
class SharedTimer {
 public:
  static SharedTimer& Instance() {
    static auto* timer = new SharedTimer();
    return *timer;
  }

  void Schedule(std::weak_ptr<ScheduledExecutorState> owner,
                std::chrono::steady_clock::time_point deadline, ExecutorTask task,
                std::optional<TaskSubmissionCallback> on_submission) {
    std::lock_guard lock(mutex_);
    // Equal deadlines keep insertion order.
    auto it = tasks_.emplace(deadline, Task{.owner = std::move(owner),
                                            .task = std::move(task),
                                            .on_submission = std::move(on_submission)});
    // Wake the timer only if its next deadline changed.
    if (it == tasks_.begin()) {
      cv_.notify_one();
    }
  }

  void CancelAll(const std::weak_ptr<ScheduledExecutorState>& owner) {
    Queue cancelled;  // Release task captures outside the queue lock.
    {
      std::lock_guard lock(mutex_);
      for (auto it = tasks_.begin(); it != tasks_.end();) {
        auto next = std::next(it);
        if (SameOwner(it->second.owner, owner)) {
          cancelled.insert(tasks_.extract(it));
        }
        it = next;
      }
    }
    if (!cancelled.empty()) {
      cv_.notify_one();
    }
  }

  bool IsTimerThread() const {
    return timer_thread_.get_id() == std::this_thread::get_id();
  }

 private:
  SharedTimer() : timer_thread_([this] { Run(); }) {}

  struct Task {
    std::weak_ptr<ScheduledExecutorState> owner;
    ExecutorTask task;
    std::optional<TaskSubmissionCallback> on_submission;
  };

  using Queue = std::multimap<std::chrono::steady_clock::time_point, Task>;

  static bool SameOwner(const std::weak_ptr<ScheduledExecutorState>& a,
                        const std::weak_ptr<ScheduledExecutorState>& b) {
    return !a.owner_before(b) && !b.owner_before(a);
  }

  void Run();

  std::mutex mutex_;
  std::condition_variable cv_;
  Queue tasks_;
  std::thread timer_thread_;
};

class ScheduledExecutorState
    : public std::enable_shared_from_this<ScheduledExecutorState> {
 public:
  explicit ScheduledExecutorState(Executor& executor)
      : executor_(executor), timer_(SharedTimer::Instance()) {}

  ScheduledExecutorState(const ScheduledExecutorState&) = delete;
  ScheduledExecutorState& operator=(const ScheduledExecutorState&) = delete;

  void Schedule(std::chrono::steady_clock::time_point deadline, ExecutorTask task,
                std::optional<TaskSubmissionCallback> on_submission) {
    std::lock_guard lock(mutex_);
    if (shutdown_) {
      return;
    }
    timer_.Schedule(weak_from_this(), deadline, std::move(task),
                    std::move(on_submission));
  }

  void Shutdown() {
    {
      std::lock_guard lock(mutex_);
      shutdown_ = true;
    }
    timer_.CancelAll(weak_from_this());
    if (!timer_.IsTimerThread()) {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [this] { return active_dispatches_ == 0; });
    }
  }

 private:
  friend class SharedTimer;

  void Dispatch(ExecutorTask task, std::optional<TaskSubmissionCallback> on_submission) {
    {
      std::lock_guard lock(mutex_);
      // Shutdown may race with popping a task.
      if (shutdown_) {
        return;
      }
      ++active_dispatches_;
    }
    struct DispatchGuard {
      ScheduledExecutorState& state;
      ~DispatchGuard() {
        std::lock_guard lock(state.mutex_);
        if (--state.active_dispatches_ == 0) {
          state.cv_.notify_all();
        }
      }
    } guard{*this};

    Status status;
    try {
      status = executor_.Submit(std::move(task));
    } catch (const std::exception& e) {
      status = UnknownError("Executor::Submit threw: {}", e.what());
    } catch (...) {
      status = UnknownError("Executor::Submit threw a non-standard exception");
    }
    if (!on_submission.has_value()) {
      return;
    }
    try {
      std::move (*on_submission)(std::move(status));
    } catch (...) {
      // Keep the shared timer running.
    }
  }

  Executor& executor_;
  SharedTimer& timer_;
  std::mutex mutex_;
  std::condition_variable cv_;
  size_t active_dispatches_ = 0;
  bool shutdown_ = false;
};

void SharedTimer::Run() {
  while (true) {
    Queue::node_type node;
    {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [this] { return !tasks_.empty(); });

      auto deadline = tasks_.begin()->first;
      if (cv_.wait_until(lock, deadline, [this, deadline] {
            return tasks_.empty() || tasks_.begin()->first != deadline;
          })) {
        continue;
      }
      node = tasks_.extract(tasks_.begin());
    }
    auto& task = node.mapped();
    if (auto owner = task.owner.lock()) {
      owner->Dispatch(std::move(task.task), std::move(task.on_submission));
    }
  }
}

ScheduledExecutor::ScheduledExecutor(Executor& executor)
    : state_(std::make_shared<ScheduledExecutorState>(executor)) {}

ScheduledExecutor::~ScheduledExecutor() { Shutdown(); }

void ScheduledExecutor::Schedule(std::chrono::milliseconds delay, ExecutorTask task,
                                 std::optional<TaskSubmissionCallback> on_submission) {
  state_->Schedule(std::chrono::steady_clock::now() + delay, std::move(task),
                   std::move(on_submission));
}

void ScheduledExecutor::Shutdown() { state_->Shutdown(); }

}  // namespace iceberg::internal
