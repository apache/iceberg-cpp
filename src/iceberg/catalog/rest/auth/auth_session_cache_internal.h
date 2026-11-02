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
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/result.h"
#include "iceberg/util/executor.h"
#include "iceberg/util/scheduled_executor_internal.h"

/// \file iceberg/catalog/rest/auth/auth_session_cache_internal.h
/// \brief Cache of child authentication sessions, keyed by caller identity.

namespace iceberg::rest::auth::internal {

/// \brief Caches child auth sessions and expires them after a period of inactivity.
///
/// Entries expire after `session_timeout` without access. OAuth2 sessions that
/// will not refresh also expire once their token does; such an entry is replaced
/// when its key is next accessed. Each session that leaves the cache is handed to
/// the removal listener exactly once.
///
/// Idle entries are reclaimed in access order, least recently used first, so the
/// cost is proportional to the number of entries removed. Sweep() does this, and
/// StartPeriodicSweep() runs it in the background; without a periodic sweep, Get()
/// does it before adding an entry.
///
/// Thread safety: all public methods are thread-safe. Loaders and the removal
/// listener are always invoked without holding the cache lock.
class ICEBERG_REST_EXPORT AuthSessionCache
    : public std::enable_shared_from_this<AuthSessionCache> {
 public:
  using Clock = std::function<std::chrono::steady_clock::time_point()>;
  using Loader = std::function<Result<std::shared_ptr<AuthSession>>()>;
  /// Called once for every session that leaves the cache. Must not block or throw.
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

  /// \brief Suggested StartPeriodicSweep() interval for `session_timeout`: the
  /// timeout clamped to [1s, 60s].
  static std::chrono::milliseconds DefaultSweepInterval(
      std::chrono::milliseconds session_timeout);

  /// \brief Return the cached session for `key`, loading it on a miss.
  ///
  /// Concurrent calls for the same key run `loader` at most once; the others wait
  /// for its result. Failed loads are not cached; an exception thrown by `loader`
  /// is returned as an UnknownError. Returns an error once the cache
  /// is closed; a session loaded after Close() is handed to the removal listener.
  Result<std::shared_ptr<AuthSession>> Get(const std::string& key, const Loader& loader);

  /// \brief Remove entries idle for at least the session timeout and notify the
  /// removal listener. Entries whose token expired are replaced on their next
  /// access instead.
  void Sweep();

  /// \brief Run Sweep() on `executor` every `interval` until Close().
  ///
  /// `executor` is borrowed and must outlive this cache and its submitted tasks.
  /// `interval` must be positive. Calling this more than once, or after Close(),
  /// has no effect.
  Status StartPeriodicSweep(Executor& executor, std::chrono::milliseconds interval);

  /// \brief Stop periodic sweeping and remove every entry. Idempotent.
  void Close();

  /// \brief Number of entries, including loads in progress.
  size_t size() const;

 private:
  struct Slot;

  /// One element per loaded slot. `key` points at the key stored in slots_, which
  /// stays valid until that element is erased, even when the map rehashes.
  struct AccessEntry {
    const std::string* key;
    Slot* slot;
  };
  using AccessOrder = std::list<AccessEntry>;

  struct Slot {
    bool loading = true;
    std::shared_ptr<AuthSession> session;
    std::chrono::steady_clock::time_point last_access;
    /// Position in access_order_; set once the load succeeds.
    std::optional<AccessOrder::iterator> access_pos;
  };
  using SlotMap = std::unordered_map<std::string, std::shared_ptr<Slot>>;

  AuthSessionCache(std::chrono::milliseconds session_timeout, RemovalListener on_removal,
                   Clock clock);

  bool IsExpired(const Slot& slot, std::chrono::steady_clock::time_point now) const;
  void TouchLocked(SlotMap::iterator it, std::chrono::steady_clock::time_point now);
  void EraseLocked(SlotMap::iterator it,
                   std::vector<std::shared_ptr<AuthSession>>& removed);
  void EvictIdleLocked(std::chrono::steady_clock::time_point now,
                       std::vector<std::shared_ptr<AuthSession>>& removed);
  void ScheduleSweepLocked();
  void ScheduleNextSweep();
  void RunScheduledSweep();
  void NotifyRemoved(std::vector<std::shared_ptr<AuthSession>>& sessions) const;

  const std::chrono::milliseconds session_timeout_;
  const RemovalListener on_removal_;
  const Clock clock_;

  mutable std::mutex mutex_;
  std::condition_variable load_finished_;
  SlotMap slots_;
  /// Loaded slots ordered by last access, least recent first.
  AccessOrder access_order_;
  bool closed_ = false;
  std::optional<::iceberg::internal::ScheduledExecutor> sweep_scheduler_;
  std::chrono::milliseconds sweep_interval_{0};
};

}  // namespace iceberg::rest::auth::internal
