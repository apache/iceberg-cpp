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

/// \file iceberg/util/task_group.h
/// \brief Provide grouped task execution utilities.

#include <concepts>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/util/executor.h"
#include "iceberg/util/functional.h"
#include "iceberg/util/retry_util.h"

namespace iceberg {

namespace internal {

template <typename F>
concept OnceStatusTask = RvalueInvocable<Status, F>;

template <typename T>
concept RepeatableStatusTask =
    std::is_invocable_r_v<Status, const T&> ||
    (std::copy_constructible<T> && std::is_invocable_r_v<Status, T&>);

template <typename F>
concept RetryableStatusTask = std::constructible_from<std::remove_cvref_t<F>, F> &&
                              RepeatableStatusTask<std::remove_cvref_t<F>>;

ICEBERG_EXPORT Status RunTasksSingleThreaded(std::vector<FnOnce<Status()>> tasks);

ICEBERG_EXPORT Status RunTasksParallel(Executor& executor,
                                       std::vector<FnOnce<Status()>> tasks);

}  // namespace internal

template <retry::Policy RetryPolicy = retry::NoRetry>
class ICEBERG_TEMPLATE_CLASS_EXPORT TaskGroup {
 private:
  static constexpr bool kRetryEnabled = !std::same_as<RetryPolicy, retry::NoRetry>;

  struct Empty {};

  using RetryConfigStorage = std::conditional_t<kRetryEnabled, RetryConfig, Empty>;

 public:
  TaskGroup() = default;

  explicit TaskGroup(RetryConfig retry_config)
    requires(kRetryEnabled)
      : retry_config_(std::move(retry_config)) {}

  auto&& SetExecutor(this auto&& self, OptionalExecutor executor) {
    self.executor_ = std::move(executor);
    return std::forward<decltype(self)>(self);
  }

  template <typename F>
    requires((!kRetryEnabled && internal::OnceStatusTask<F>) ||
             (kRetryEnabled && internal::RetryableStatusTask<F>))
  auto&& Submit(this auto&& self, F&& task) {
    self.tasks_.emplace_back([&] {
      if constexpr (!kRetryEnabled) {
        return std::forward<F>(task);
      } else {
        return [retry_config = self.retry_config_,
                task = std::forward<F>(task)]() mutable -> Status {
          return RetryRunner<RetryPolicy>(retry_config).Run(task);
        };
      }
    }());
    return std::forward<decltype(self)>(self);
  }

  Status Run() && {
    if (!executor_.has_value()) {
      return internal::RunTasksSingleThreaded(std::move(tasks_));
    }
    return internal::RunTasksParallel(executor_->get(), std::move(tasks_));
  }

 private:
  std::vector<FnOnce<Status()>> tasks_;
  OptionalExecutor executor_;
  [[no_unique_address]] RetryConfigStorage retry_config_;
};

}  // namespace iceberg
