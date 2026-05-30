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
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "iceberg/catalog/rest/iceberg_rest_export.h"

/// \file iceberg/catalog/rest/auth/token_refresh_scheduler.h
/// \brief Global scheduler for OAuth2 token refresh tasks.

namespace iceberg::rest::auth {

/// \brief A process-global scheduler for delayed token refresh tasks.
///
/// Uses a single background thread that sleeps until the next task is due.
/// All OAuth2AuthSession instances share this scheduler. Tasks are lightweight
/// (a single HTTP POST to refresh a token), so one thread is sufficient.
///
/// Thread safety: All public methods are thread-safe.
///
/// TODO(lishuxu): Migrate to the shared thread pool abstraction once available
/// (see https://github.com/apache/iceberg-cpp/pull/646#discussion_r3304315308).
class ICEBERG_REST_EXPORT TokenRefreshScheduler {
 public:
  /// \brief Get the global singleton instance.
  ///
  /// Lazily created on first access and intentionally leaked: the worker
  /// thread is reclaimed by the OS at process exit. Tests needing
  /// deterministic shutdown should use a local instance.
  static TokenRefreshScheduler& Instance();

  /// \brief Schedule a callback to run after a delay.
  ///
  /// \param delay Time to wait before executing the callback.
  /// \param callback Function to execute when the delay expires.
  /// \return A unique handle that can be used to cancel the task.
  uint64_t Schedule(std::chrono::milliseconds delay, std::function<void()> callback);

  /// \brief Cancel a previously scheduled task.
  ///
  /// If the task has already fired or does not exist, this is a no-op.
  ///
  /// \param handle The handle returned by Schedule().
  void Cancel(uint64_t handle);

  /// \brief Shutdown the scheduler, cancelling all pending tasks.
  ///
  /// After shutdown, Schedule() calls are no-ops (return 0).
  /// This is called automatically on destruction.
  ///
  /// WARNING: Do not call this on the global Instance() unless you intend to
  /// permanently stop all token refresh for the entire process. This is mainly
  /// useful for testing with locally-constructed scheduler instances.
  void Shutdown();

  ~TokenRefreshScheduler();

  // Non-copyable, non-movable
  TokenRefreshScheduler(const TokenRefreshScheduler&) = delete;
  TokenRefreshScheduler& operator=(const TokenRefreshScheduler&) = delete;
  TokenRefreshScheduler(TokenRefreshScheduler&&) = delete;
  TokenRefreshScheduler& operator=(TokenRefreshScheduler&&) = delete;

  /// \brief Construct a scheduler (prefer Instance() for production use).
  ///
  /// This constructor is public to allow testing with isolated instances.
  /// In production code, use Instance() to get the global singleton.
  TokenRefreshScheduler();

 private:
  /// \brief Worker loop that processes tasks.
  void Run();

  struct Task {
    uint64_t id;
    std::chrono::steady_clock::time_point fire_at;
    std::function<void()> callback;
  };

  std::mutex mutex_;
  std::condition_variable cv_;
  std::vector<Task> tasks_;
  uint64_t next_id_ = 1;  // 0 is reserved as "invalid handle"
  bool shutdown_ = false;
  std::thread worker_;
};

}  // namespace iceberg::rest::auth
