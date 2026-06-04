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

#include <format>
#include <future>
#include <string>
#include <utility>

#include "iceberg/util/macros.h"

namespace iceberg::internal {

namespace {

Status AggregateTaskErrors(std::vector<Error> errors) {
  if (errors.empty()) {
    return {};
  }
  if (errors.size() == 1) {
    return std::unexpected(std::move(errors.front()));
  }

  ErrorKind kind = errors.front().kind;
  std::string message = std::format("Task group failed with {} errors:", errors.size());
  for (const auto& error : errors) {
    message += std::format("\n  - {}", error.message);
  }
  return std::unexpected(Error{.kind = kind, .message = std::move(message)});
}

Result<std::future<Status>> SubmitTask(Executor& executor, FnOnce<Status()> task) {
  std::promise<Status> promise;
  auto future = promise.get_future();

  ExecutorTask executor_task(
      [promise = std::move(promise), task = std::move(task)]() mutable {
        promise.set_value(std::move(task)());
      });

  ICEBERG_RETURN_UNEXPECTED(executor.Submit(std::move(executor_task)));

  return future;
}

}  // namespace

Status RunTasksSingleThreaded(std::vector<FnOnce<Status()>> tasks) {
  std::vector<Error> errors;
  for (auto& task : tasks) {
    auto status = std::move(task)();
    if (!status.has_value()) {
      errors.push_back(std::move(status.error()));
    }
  }
  return AggregateTaskErrors(std::move(errors));
}

Status RunTasksParallel(Executor& executor, std::vector<FnOnce<Status()>> tasks) {
  std::vector<std::future<Status>> futures;
  futures.reserve(tasks.size());

  std::vector<Error> errors;
  for (auto& task : tasks) {
    auto future = SubmitTask(executor, std::move(task));
    if (!future.has_value()) {
      errors.push_back(std::move(future.error()));
      continue;
    }
    futures.push_back(std::move(future.value()));
  }

  for (auto& future : futures) {
    auto status = future.get();
    if (!status.has_value()) {
      errors.push_back(std::move(status.error()));
    }
  }

  return AggregateTaskErrors(std::move(errors));
}

}  // namespace iceberg::internal
