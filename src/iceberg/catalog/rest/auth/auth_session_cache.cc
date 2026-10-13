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

#include <utility>

#include "iceberg/catalog/rest/auth/auth_session_cache_internal.h"
#include "iceberg/catalog/rest/auth/auth_session_internal.h"
#include "iceberg/catalog/rest/auth/token_refresh_scheduler.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest::auth::internal {

Result<std::shared_ptr<AuthSessionCache>> AuthSessionCache::Make(
    std::chrono::milliseconds session_timeout, RemovalListener on_removal, Clock clock) {
  ICEBERG_PRECHECK(session_timeout >= std::chrono::milliseconds::zero(),
                   "Auth session timeout must not be negative: {} ms",
                   session_timeout.count());
  ICEBERG_PRECHECK(clock != nullptr, "Auth session cache clock must not be null");
  return std::shared_ptr<AuthSessionCache>(
      new AuthSessionCache(session_timeout, std::move(on_removal), std::move(clock)));
}

AuthSessionCache::AuthSessionCache(std::chrono::milliseconds session_timeout,
                                   RemovalListener on_removal, Clock clock)
    : session_timeout_(session_timeout),
      on_removal_(std::move(on_removal)),
      clock_(std::move(clock)) {}

AuthSessionCache::~AuthSessionCache() { Close(); }

Result<std::shared_ptr<AuthSession>> AuthSessionCache::Get(const std::string& key,
                                                           const Loader& loader) {
  // Phase 1: Look up cache (under lock)
  std::shared_ptr<Slot> slot;
  std::vector<std::shared_ptr<AuthSession>> removed;
  {
    std::unique_lock lock(mutex_);
    while (true) {
      if (closed_) {
        return Invalid("Auth session cache is closed");
      }
      auto it = slots_.find(key);
      if (it == slots_.end()) {
        break;
      }
      auto existing = it->second;
      if (existing->loading) {
        // Wait for another thread to finish loading. Loop again after waking
        // because a failed load removes the slot.
        load_finished_.wait(lock, [&] { return closed_ || !existing->loading; });
        continue;
      }
      auto now = clock_();
      if (!IsExpired(*existing, now)) {
        existing->last_access = now;
        return existing->session;
      }
      removed.push_back(std::move(existing->session));
      slots_.erase(it);
      break;
    }
    // Create loading slot. Other threads calling Get(key) will wait.
    slot = std::make_shared<Slot>();
    slots_.emplace(key, slot);
  }

  // Phase 2: Call loader (lock released)
  NotifyRemoved(removed);
  auto result = loader();
  if (result.has_value() && result.value() == nullptr) {
    result = Invalid("Auth session loader returned a null session");
  }

  // Phase 3: Update cache (re-acquire lock)
  std::shared_ptr<AuthSession> orphan;
  {
    std::lock_guard lock(mutex_);
    slot->loading = false;
    auto it = slots_.find(key);
    bool current = it != slots_.end() && it->second == slot;

    if (!result.has_value()) {
      // Failed loads are not cached. Remove the slot so next Get() can retry.
      if (current) {
        slots_.erase(it);
      }
    } else if (closed_ || !current) {
      // Cache closed during load, or slot replaced. Mark as orphan.
      orphan = result.value();
    } else {
      slot->session = result.value();
      slot->last_access = clock_();
    }
  }
  load_finished_.notify_all();

  if (orphan != nullptr) {
    if (on_removal_) {
      on_removal_(orphan);
    }
    return Invalid("Auth session cache is closed");
  }
  return result;
}

void AuthSessionCache::Sweep() {
  std::vector<std::shared_ptr<AuthSession>> removed;
  {
    std::lock_guard lock(mutex_);
    auto now = clock_();
    for (auto it = slots_.begin(); it != slots_.end();) {
      if (!it->second->loading && IsExpired(*it->second, now)) {
        removed.push_back(std::move(it->second->session));
        it = slots_.erase(it);
      } else {
        ++it;
      }
    }
  }
  NotifyRemoved(removed);
}

Status AuthSessionCache::StartPeriodicSweep(TokenRefreshScheduler& scheduler,
                                            std::chrono::milliseconds interval) {
  ICEBERG_PRECHECK(interval > std::chrono::milliseconds::zero(),
                   "Auth session sweep interval must be positive: {} ms",
                   interval.count());
  std::lock_guard lock(mutex_);
  if (closed_ || sweep_scheduler_ != nullptr) {
    return {};
  }
  sweep_scheduler_ = &scheduler;
  sweep_interval_ = interval;
  ScheduleSweepLocked();
  return {};
}

void AuthSessionCache::Close() {
  std::vector<std::shared_ptr<AuthSession>> removed;
  TokenRefreshScheduler* scheduler = nullptr;
  uint64_t sweep_task_id = 0;
  {
    std::lock_guard lock(mutex_);
    if (closed_) {
      return;
    }
    closed_ = true;
    for (auto& [key, slot] : slots_) {
      // Sessions still loading are handed to the listener when their load ends.
      if (!slot->loading) {
        removed.push_back(std::move(slot->session));
      }
    }
    slots_.clear();
    scheduler = sweep_scheduler_;
    sweep_task_id = std::exchange(sweep_task_id_, 0);
  }
  load_finished_.notify_all();
  if (scheduler != nullptr) {
    scheduler->Cancel(sweep_task_id);
  }
  NotifyRemoved(removed);
}

size_t AuthSessionCache::size() const {
  std::lock_guard lock(mutex_);
  return slots_.size();
}

bool AuthSessionCache::IsExpired(const Slot& slot,
                                 std::chrono::steady_clock::time_point now) const {
  // Check 1: Idle timeout (expireAfterAccess semantics)
  if (now - slot.last_access >= session_timeout_) {
    return true;
  }

  // Check 2: Token expiration
  // Even for refreshable sessions, check token expiration to handle cases where:
  // - Refresh has permanently failed (after max retries)
  // - Refresh is delayed or not scheduled
  // - Token was obtained from external source (table token)
  auto oauth2 = std::dynamic_pointer_cast<internal::OAuth2Session>(slot.session);
  if (oauth2) {
    auto expires_at = oauth2->ExpiresAt();
    if (expires_at.has_value() && now >= *expires_at) {
      return true;
    }
  }

  return false;
}

void AuthSessionCache::ScheduleSweepLocked() {
  // Scheduling under the cache lock ensures Close() either sees this task's id
  // or prevents it from being scheduled.
  sweep_task_id_ =
      sweep_scheduler_->Schedule(sweep_interval_, [weak_self = weak_from_this()] {
        if (auto self = weak_self.lock()) {
          self->RunScheduledSweep();
        }
      });
}

void AuthSessionCache::RunScheduledSweep() {
  Sweep();
  std::lock_guard lock(mutex_);
  if (!closed_) {
    ScheduleSweepLocked();
  }
}

void AuthSessionCache::NotifyRemoved(
    std::vector<std::shared_ptr<AuthSession>>& sessions) const {
  if (!on_removal_) {
    return;
  }
  for (auto& session : sessions) {
    if (session != nullptr) {
      on_removal_(session);
    }
  }
}

}  // namespace iceberg::rest::auth::internal
