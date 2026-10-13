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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/auth/auth_session_cache_internal.h
/// \brief Cache of child authentication sessions, keyed by caller identity.

namespace iceberg::rest::auth {
class TokenRefreshScheduler;
}  // namespace iceberg::rest::auth

namespace iceberg::rest::auth::internal {

/// \brief Caches child auth sessions and expires them after a period of inactivity.
///
/// Entries expire after `session_timeout` without access, and each session that
/// leaves the cache is handed to the removal listener exactly once. Idle entries
/// are reclaimed when accessed or when Sweep() runs; StartPeriodicSweep() can
/// schedule Sweep() to run periodically.
///
/// Thread safety: all public methods are thread-safe. Loaders and the removal
/// listener are always invoked without holding the cache lock.
class ICEBERG_REST_EXPORT AuthSessionCache
    : public std::enable_shared_from_this<AuthSessionCache> {
 public:
  using Clock = std::function<std::chrono::steady_clock::time_point()>;
  using Loader = std::function<Result<std::shared_ptr<AuthSession>>()>;
  /// Called once for every session that leaves the cache. Must not block.
  using RemovalListener = std::function<void(const std::shared_ptr<AuthSession>&)>;

  /// \brief Create a cache.
  ///
  /// \param session_timeout Idle time after which an entry expires; must not be
  ///        negative. Zero disables caching in effect.
  /// \param on_removal Listener invoked for each session that leaves the cache.
  /// \param clock Time source; tests can inject a fake clock.
  static Result<std::shared_ptr<AuthSessionCache>> Make(
      std::chrono::milliseconds session_timeout, RemovalListener on_removal,
      Clock clock = std::chrono::steady_clock::now);

  ~AuthSessionCache();

  AuthSessionCache(const AuthSessionCache&) = delete;
  AuthSessionCache& operator=(const AuthSessionCache&) = delete;

  /// \brief Return the cached session for `key`, loading it on a miss.
  ///
  /// Concurrent calls for the same key run `loader` at most once; the others wait
  /// for its result. Failed loads are not cached. Returns an error once the cache
  /// is closed; a session loaded after Close() is handed to the removal listener.
  Result<std::shared_ptr<AuthSession>> Get(const std::string& key, const Loader& loader);

  /// \brief Remove all expired entries and notify the removal listener.
  void Sweep();

  /// \brief Run Sweep() every `interval` on `scheduler` until Close().
  ///
  /// `scheduler` must outlive this cache or the call to Close(). `interval` must
  /// be positive. Calling this more than once, or after Close(), has no effect.
  Status StartPeriodicSweep(TokenRefreshScheduler& scheduler,
                            std::chrono::milliseconds interval);

  /// \brief Stop periodic sweeping and remove every entry. Idempotent.
  void Close();

  /// \brief Number of entries, including loads in progress.
  size_t size() const;

 private:
  struct Slot {
    bool loading = true;
    std::shared_ptr<AuthSession> session;
    std::chrono::steady_clock::time_point last_access;
  };

  AuthSessionCache(std::chrono::milliseconds session_timeout, RemovalListener on_removal,
                   Clock clock);

  bool IsExpired(const Slot& slot, std::chrono::steady_clock::time_point now) const;
  void ScheduleSweepLocked();
  void RunScheduledSweep();
  void NotifyRemoved(std::vector<std::shared_ptr<AuthSession>>& sessions) const;

  const std::chrono::milliseconds session_timeout_;
  const RemovalListener on_removal_;
  const Clock clock_;

  mutable std::mutex mutex_;
  std::condition_variable load_finished_;
  std::unordered_map<std::string, std::shared_ptr<Slot>> slots_;
  bool closed_ = false;
  TokenRefreshScheduler* sweep_scheduler_ = nullptr;
  std::chrono::milliseconds sweep_interval_{0};
  uint64_t sweep_task_id_ = 0;
};

}  // namespace iceberg::rest::auth::internal
