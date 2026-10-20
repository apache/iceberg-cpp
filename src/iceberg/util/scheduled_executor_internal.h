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
#include <memory>
#include <optional>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/util/executor.h"

namespace iceberg::internal {

class ScheduledExecutorState;

/// Called once after Executor::Submit returns or throws, to report whether the task
/// was submitted. This does not report the task's result or wait for it to finish.
///
/// Submit exceptions are reported as an error Status. Under Executor's contract,
/// an error means the task will not run, so the caller can safely retry.
/// Runs on the shared timer thread and must not block. Cancelled tasks skip the callback.
using TaskSubmissionCallback = FnOnce<void(Status)>;

/// Adds delayed scheduling to an Executor. One timer thread shared by all instances
/// waits for deadlines, then submits each due task to that instance's executor.
///
/// The executor is not owned and must outlive this object and its submitted tasks.
/// Submit must queue work promptly: running tasks inline would block the timer and
/// delay scheduled work for every instance. Destruction calls Shutdown().
class ICEBERG_EXPORT ScheduledExecutor {
 public:
  explicit ScheduledExecutor(Executor& executor);
  ~ScheduledExecutor();

  ScheduledExecutor(const ScheduledExecutor&) = delete;
  ScheduledExecutor& operator=(const ScheduledExecutor&) = delete;

  /// Queues `task` for submission once `delay` has passed. The executor decides when
  /// it actually runs. Equal deadlines are submitted in scheduling order.
  /// After Shutdown, drops the task without calling on_submission.
  void Schedule(std::chrono::milliseconds delay, ExecutorTask task,
                std::optional<TaskSubmissionCallback> on_submission = std::nullopt);

  /// Stops accepting tasks and cancels this instance's tasks still waiting on the timer.
  /// Already submitted tasks are not cancelled or waited for.
  ///
  /// Waits for active Submit calls and their callbacks to finish, unless called on the
  /// timer thread itself. Safe to call more than once; other instances are unaffected.
  void Shutdown();

 private:
  std::shared_ptr<ScheduledExecutorState> state_;
};

}  // namespace iceberg::internal
