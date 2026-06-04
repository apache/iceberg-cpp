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

#include <atomic>
#include <thread>
#include <utility>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/util/executor.h"

namespace iceberg::test {

class ThreadExecutor final : public Executor {
 public:
  explicit ThreadExecutor(Status submit_status = {})
      : submit_status_(std::move(submit_status)) {}

  ~ThreadExecutor() override {
    for (auto& thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

  Status Submit(ExecutorTask task) override {
    submit_count_.fetch_add(1, std::memory_order_relaxed);
    if (!submit_status_.has_value()) {
      return std::unexpected(submit_status_.error());
    }
    threads_.emplace_back(std::move(task));
    return {};
  }

  int submit_count() const { return submit_count_.load(std::memory_order_relaxed); }

 private:
  Status submit_status_;
  std::atomic<int> submit_count_{0};
  std::vector<std::thread> threads_;
};

}  // namespace iceberg::test
