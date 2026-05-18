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

#include "iceberg/util/thread_pool_internal.h"

#include <memory>
#include <stdexcept>

namespace iceberg {

ThreadPool::ThreadPool(std::size_t num_workers) {
  if (num_workers == 0) {
    throw std::invalid_argument("ThreadPool num_workers must be > 0");
  }
  workers_.reserve(num_workers);
  for (std::size_t i = 0; i < num_workers; ++i) {
    workers_.emplace_back([this] { WorkerLoop(); });
  }
}

ThreadPool::~ThreadPool() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    stop_ = true;
  }
  cv_.notify_all();
  for (auto& w : workers_) {
    if (w.joinable()) w.join();
  }
}

std::future<void> ThreadPool::Submit(std::function<void()> task) {
  auto pkg = std::make_shared<std::packaged_task<void()>>(std::move(task));
  std::future<void> fut = pkg->get_future();
  {
    std::unique_lock<std::mutex> lock(mu_);
    if (stop_) {
      // Pool is shutting down. Run the task on the calling thread so the future
      // becomes ready and callers don't deadlock waiting on it.
      lock.unlock();
      (*pkg)();
      return fut;
    }
    queue_.emplace([pkg]() { (*pkg)(); });
  }
  cv_.notify_one();
  return fut;
}

void ThreadPool::WorkerLoop() {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mu_);
      cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
      if (queue_.empty()) {
        // Drain mode: only exit once all queued work has been pulled.
        return;
      }
      task = std::move(queue_.front());
      queue_.pop();
    }
    // packaged_task captures exceptions into the future, so this won't throw.
    task();
  }
}

}  // namespace iceberg
