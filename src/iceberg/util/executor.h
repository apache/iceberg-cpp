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

/// \file iceberg/util/executor.h
/// \brief Define task executor interfaces.

#include <functional>
#include <optional>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/util/functional.h"

namespace iceberg {

using ExecutorTask = FnOnce<void()>;

/// \brief Schedules iceberg-cpp internal planning tasks.
///
/// Public APIs that accept an executor remain synchronous: the calling thread may block
/// while waiting for submitted tasks to finish. Callers must ensure the executor can
/// continue making progress while the caller is blocked. Calling those APIs from one of
/// the same bounded executor's worker threads can deadlock unless the executor supports
/// nested blocking work.
///
/// When an executor is configured, planning callbacks may be called concurrently. Any
/// shared mutable state captured by those callbacks must be synchronized by the caller.
class ICEBERG_EXPORT Executor {
 public:
  virtual ~Executor() = default;

  /// \brief Schedule a task for execution.
  virtual Status Submit(ExecutorTask task) = 0;
};

using OptionalExecutor = std::optional<std::reference_wrapper<Executor>>;

}  // namespace iceberg
