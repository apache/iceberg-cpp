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

#include <atomic>
#include <format>
#include <future>
#include <string>
#include <utility>

#include "iceberg/util/error_util_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/task_group_internal.h"

namespace iceberg::internal {

namespace {

Status AggregateTaskErrors(std::vector<Error> errors) {
  if (errors.empty()) {
    return {};
  }
  if (errors.size() == 1) {
    return ::iceberg::unexpected(std::move(errors.front()));
  }

  ErrorKind kind = errors.front().kind;
  std::string message = std::format("Task group failed with {} errors:", errors.size());
  for (const auto& error : errors) {
    message += std::format("\n  - {}", error.message);
  }
  return ::iceberg::unexpected(Error{.kind = kind, .message = std::move(message)});
}

template <typename Task>
Result<std::future<Status>> SubmitTask(Executor& executor, Task task) {
  std::promise<Status> promise;
  auto future = promise.get_future();

  ExecutorTask executor_task(
      [promise = std::move(promise), task = std::move(task)]() mutable {
        promise.set_value(std::move(task)());
      });

  ICEBERG_RETURN_UNEXPECTED(CatchExceptions(
      "Submit task", [&] { return executor.Submit(std::move(executor_task)); }));

  return future;
}

template <typename Task>
Status RunTasks(size_t size, OptionalExecutor executor, const Task& task,
                bool stop_on_failure) {
  std::vector<std::future<Status>> futures;
  if (executor) futures.reserve(size);
  std::vector<Error> errors;
  std::atomic<bool> stop{false};
  for (size_t i = 0; i < size && !stop.load(); ++i) {
    auto run = [&, i]() -> Status {
      if (stop.load()) return {};
      auto status = CatchExceptions("Run task", [&] { return task(i); });
      if (stop_on_failure && !status) stop.store(true);
      return status;
    };
    if (executor) {
      auto future = SubmitTask(executor->get(), std::move(run));
      if (future) {
        futures.push_back(std::move(*future));
      } else {
        errors.push_back(std::move(future.error()));
        if (stop_on_failure) stop.store(true);
      }
    } else if (auto status = run(); !status) {
      errors.push_back(std::move(status.error()));
    }
  }

  for (auto& future : futures) {
    auto status = CatchExceptions("Wait for task", [&] { return future.get(); });
    if (!status.has_value()) {
      errors.push_back(std::move(status.error()));
    }
  }

  return AggregateTaskErrors(std::move(errors));
}

}  // namespace

Status RunTasksSingleThreaded(std::vector<FnOnce<Status()>> tasks) {
  return RunTasks(
      tasks.size(), std::nullopt, [&](size_t i) { return std::move(tasks[i])(); }, false);
}

Status RunTasksParallel(Executor& executor, std::vector<FnOnce<Status()>> tasks) {
  return RunTasks(
      tasks.size(), std::ref(executor), [&](size_t i) { return std::move(tasks[i])(); },
      false);
}

Status RunTasksWithStopOnFailure(size_t size, OptionalExecutor executor,
                                 const std::function<Status(size_t)>& task) {
  return RunTasks(size, executor, task, true);
}

}  // namespace iceberg::internal
