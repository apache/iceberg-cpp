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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/auth/auth_session_cache_internal.h"
#include "iceberg/catalog/rest/auth/auth_session_internal.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest::auth {

namespace {

using internal::AuthSessionCache;
using namespace std::chrono_literals;

constexpr auto kTimeout = std::chrono::milliseconds(1000);
constexpr auto kTokenLifetime = std::chrono::seconds(60);

/// Starts at the real steady clock so it is comparable with OAuth2Session::ExpiresAt().
class FakeClock {
 public:
  std::chrono::steady_clock::time_point Now() const {
    return std::chrono::steady_clock::time_point(std::chrono::milliseconds(now_ms_));
  }

  AuthSessionCache::Clock AsClock() {
    return [this] { return Now(); };
  }

  void Advance(std::chrono::milliseconds delta) { now_ms_ += delta.count(); }

 private:
  std::atomic<int64_t> now_ms_{std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count()};
};

class RemovalRecorder {
 public:
  AuthSessionCache::RemovalListener Listener() {
    return [this](const std::shared_ptr<AuthSession>& session) {
      std::lock_guard lock(mutex_);
      removed_.push_back(session);
    };
  }

  std::vector<std::shared_ptr<AuthSession>> removed() const {
    std::lock_guard lock(mutex_);
    return removed_;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<std::shared_ptr<AuthSession>> removed_;
};

/// One-shot latch used to hold a loader inside Get().
class Gate {
 public:
  void Open() {
    {
      std::lock_guard lock(mutex_);
      open_ = true;
    }
    cv_.notify_all();
  }

  void Wait() {
    std::unique_lock lock(mutex_);
    cv_.wait(lock, [this] { return open_; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool open_ = false;
};

AuthSessionCache::Loader CountingLoader(std::atomic<int>& calls) {
  return [&calls]() -> Result<std::shared_ptr<AuthSession>> {
    ++calls;
    return AuthSession::MakeDefault({});
  };
}

/// Loads an OAuth2 session whose token expires kTokenLifetime after `clock` reads.
/// The session refreshes on `refresh_executor` when it is not null.
AuthSessionCache::Loader OAuth2Loader(std::atomic<int>& calls, const FakeClock& clock,
                                      Executor* refresh_executor) {
  return [&calls, &clock, refresh_executor]() -> Result<std::shared_ptr<AuthSession>> {
    ++calls;
    OAuthTokenResponse token_response{
        .access_token = "opaque-token",
        .token_type = "bearer",
        .expires_in_secs = kTokenLifetime.count(),
    };
    ICEBERG_ASSIGN_OR_RAISE(
        auto session,
        internal::OAuth2Session::Make(token_response,
                                      {.token_endpoint = "http://localhost/oauth/tokens",
                                       .refresh_executor = refresh_executor},
                                      std::make_shared<HttpClient>(), clock.Now()));
    return session;
  };
}

}  // namespace

class AuthSessionCacheTest : public ::testing::Test {
 protected:
  std::shared_ptr<AuthSessionCache> MakeCache(
      std::chrono::milliseconds timeout = kTimeout) {
    auto cache = AuthSessionCache::Make(timeout, recorder_.Listener(), clock_.AsClock());
    EXPECT_THAT(cache, IsOk());
    return cache.value();
  }

  // Declared before any cache so they outlive it.
  FakeClock clock_;
  RemovalRecorder recorder_;
};

TEST_F(AuthSessionCacheTest, RejectsNegativeTimeout) {
  EXPECT_THAT(AuthSessionCache::Make(-1ms, recorder_.Listener()),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(AuthSessionCacheTest, ReturnsCachedSessionOnHit) {
  auto cache = MakeCache();
  std::atomic<int> calls = 0;

  ICEBERG_UNWRAP_OR_FAIL(auto first, cache->Get("tenant-a", CountingLoader(calls)));
  ICEBERG_UNWRAP_OR_FAIL(auto second, cache->Get("tenant-a", CountingLoader(calls)));

  EXPECT_EQ(first, second);
  EXPECT_EQ(calls, 1);
  EXPECT_EQ(cache->size(), 1);
}

TEST_F(AuthSessionCacheTest, KeysAreIsolated) {
  auto cache = MakeCache();
  std::atomic<int> calls = 0;

  ICEBERG_UNWRAP_OR_FAIL(auto a, cache->Get("tenant-a", CountingLoader(calls)));
  ICEBERG_UNWRAP_OR_FAIL(auto b, cache->Get("tenant-b", CountingLoader(calls)));

  EXPECT_NE(a, b);
  EXPECT_EQ(calls, 2);
}

TEST_F(AuthSessionCacheTest, FailedLoadIsNotCached) {
  auto cache = MakeCache();
  auto failing = []() -> Result<std::shared_ptr<AuthSession>> {
    return AuthenticationFailed("token endpoint unavailable");
  };
  std::atomic<int> calls = 0;

  EXPECT_THAT(cache->Get("tenant-a", failing), IsError(ErrorKind::kAuthenticationFailed));
  EXPECT_EQ(cache->size(), 0);
  EXPECT_THAT(cache->Get("tenant-a", CountingLoader(calls)), IsOk());
  EXPECT_EQ(calls, 1);
}

TEST_F(AuthSessionCacheTest, NullSessionIsRejected) {
  auto cache = MakeCache();
  auto null_loader = []() -> Result<std::shared_ptr<AuthSession>> { return nullptr; };

  EXPECT_THAT(cache->Get("tenant-a", null_loader), IsError(ErrorKind::kInvalid));
  EXPECT_EQ(cache->size(), 0);
}

TEST_F(AuthSessionCacheTest, ExpiresAfterIdleTimeout) {
  auto cache = MakeCache();
  std::atomic<int> calls = 0;
  ICEBERG_UNWRAP_OR_FAIL(auto first, cache->Get("tenant-a", CountingLoader(calls)));

  // Each access resets the idle timer, so the entry outlives the timeout in total.
  clock_.Advance(kTimeout - 1ms);
  ICEBERG_UNWRAP_OR_FAIL(auto hit, cache->Get("tenant-a", CountingLoader(calls)));
  clock_.Advance(kTimeout - 1ms);
  ICEBERG_UNWRAP_OR_FAIL(auto still_hit, cache->Get("tenant-a", CountingLoader(calls)));
  EXPECT_EQ(hit, first);
  EXPECT_EQ(still_hit, first);
  EXPECT_EQ(calls, 1);

  clock_.Advance(kTimeout);
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, cache->Get("tenant-a", CountingLoader(calls)));
  EXPECT_NE(reloaded, first);
  EXPECT_EQ(calls, 2);
  EXPECT_THAT(recorder_.removed(), ::testing::ElementsAre(first));
}

TEST_F(AuthSessionCacheTest, SweepRemovesOnlyExpiredEntries) {
  auto cache = MakeCache();
  std::atomic<int> calls = 0;
  ICEBERG_UNWRAP_OR_FAIL(auto idle, cache->Get("idle", CountingLoader(calls)));
  ICEBERG_UNWRAP_OR_FAIL(auto active, cache->Get("active", CountingLoader(calls)));

  clock_.Advance(kTimeout - 1ms);
  ASSERT_THAT(cache->Get("active", CountingLoader(calls)), IsOk());
  clock_.Advance(1ms);
  cache->Sweep();

  EXPECT_EQ(cache->size(), 1);
  EXPECT_THAT(recorder_.removed(), ::testing::ElementsAre(idle));
  ICEBERG_UNWRAP_OR_FAIL(auto still_active, cache->Get("active", CountingLoader(calls)));
  EXPECT_EQ(still_active, active);
}

TEST_F(AuthSessionCacheTest, NonRefreshableSessionExpiresWithToken) {
  auto cache = MakeCache(std::chrono::hours(1));
  std::atomic<int> calls = 0;
  auto loader = OAuth2Loader(calls, clock_, /*refresh_executor=*/nullptr);
  ICEBERG_UNWRAP_OR_FAIL(auto first, cache->Get("tenant-a", loader));

  clock_.Advance(kTokenLifetime - 1ms);
  ICEBERG_UNWRAP_OR_FAIL(auto hit, cache->Get("tenant-a", loader));
  EXPECT_EQ(hit, first);

  clock_.Advance(1ms);
  cache->Sweep();
  EXPECT_EQ(cache->size(), 0);
  EXPECT_THAT(recorder_.removed(), ::testing::ElementsAre(first));

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, cache->Get("tenant-a", loader));
  EXPECT_NE(reloaded, first);
  EXPECT_EQ(calls, 2);
}

TEST_F(AuthSessionCacheTest, RefreshableSessionOutlivesToken) {
  // Must outlive the cache. The refresh is due long after this test ends, so
  // nothing is submitted.
  test::MockExecutor executor;
  EXPECT_CALL(executor, Submit).Times(0);
  auto cache = MakeCache(std::chrono::hours(1));
  std::atomic<int> calls = 0;
  auto loader = OAuth2Loader(calls, clock_, &executor);
  ICEBERG_UNWRAP_OR_FAIL(auto first, cache->Get("tenant-a", loader));

  // Its refresh may still be in flight; eviction would stop it for other holders.
  clock_.Advance(kTokenLifetime + 1s);
  cache->Sweep();
  ICEBERG_UNWRAP_OR_FAIL(auto hit, cache->Get("tenant-a", loader));

  EXPECT_EQ(hit, first);
  EXPECT_EQ(calls, 1);
  EXPECT_TRUE(recorder_.removed().empty());
}

TEST_F(AuthSessionCacheTest, ConcurrentGetsForSameKeyLoadOnce) {
  auto cache = MakeCache();
  std::atomic<int> calls = 0;
  Gate entered;
  Gate release;
  auto blocking_loader = [&]() -> Result<std::shared_ptr<AuthSession>> {
    ++calls;
    entered.Open();
    release.Wait();
    return AuthSession::MakeDefault({});
  };

  constexpr int kThreads = 8;
  std::vector<std::shared_ptr<AuthSession>> results(kThreads);
  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      auto session = cache->Get("tenant-a", blocking_loader);
      if (session.has_value()) {
        results[i] = session.value();
      }
    });
  }
  entered.Wait();
  std::this_thread::sleep_for(20ms);  // let the other callers queue up
  release.Open();
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(calls, 1);
  ASSERT_NE(results[0], nullptr);
  for (const auto& session : results) {
    EXPECT_EQ(session, results[0]);
  }
}

TEST_F(AuthSessionCacheTest, WaitersRetryAfterFailedLoad) {
  auto cache = MakeCache();
  std::atomic<int> calls = 0;
  Gate entered;
  Gate release;
  auto loader = [&]() -> Result<std::shared_ptr<AuthSession>> {
    if (++calls == 1) {
      entered.Open();
      release.Wait();
      return AuthenticationFailed("first attempt fails");
    }
    return AuthSession::MakeDefault({});
  };

  Result<std::shared_ptr<AuthSession>> first_result = nullptr;
  std::thread first([&] { first_result = cache->Get("tenant-a", loader); });
  entered.Wait();
  Result<std::shared_ptr<AuthSession>> waiter_result = nullptr;
  std::thread waiter([&] { waiter_result = cache->Get("tenant-a", loader); });
  std::this_thread::sleep_for(20ms);
  release.Open();
  first.join();
  waiter.join();

  EXPECT_THAT(first_result, IsError(ErrorKind::kAuthenticationFailed));
  EXPECT_THAT(waiter_result, IsOk());
  EXPECT_EQ(calls, 2);
  EXPECT_EQ(cache->size(), 1);
}

TEST_F(AuthSessionCacheTest, CloseRemovesAllEntriesOnce) {
  auto cache = MakeCache();
  std::atomic<int> calls = 0;
  ICEBERG_UNWRAP_OR_FAIL(auto a, cache->Get("tenant-a", CountingLoader(calls)));
  ICEBERG_UNWRAP_OR_FAIL(auto b, cache->Get("tenant-b", CountingLoader(calls)));

  cache->Close();
  cache->Close();

  EXPECT_EQ(cache->size(), 0);
  EXPECT_THAT(recorder_.removed(), ::testing::UnorderedElementsAre(a, b));
  EXPECT_THAT(cache->Get("tenant-a", CountingLoader(calls)),
              IsError(ErrorKind::kInvalid));
  EXPECT_EQ(calls, 2);
}

TEST_F(AuthSessionCacheTest, LoadFinishingAfterCloseIsRemovedAndWaitersFail) {
  auto cache = MakeCache();
  Gate entered;
  Gate release;
  std::shared_ptr<AuthSession> loaded;
  auto loader = [&]() -> Result<std::shared_ptr<AuthSession>> {
    entered.Open();
    release.Wait();
    loaded = AuthSession::MakeDefault({});
    return loaded;
  };

  Result<std::shared_ptr<AuthSession>> loader_result = nullptr;
  std::thread loading([&] { loader_result = cache->Get("tenant-a", loader); });
  entered.Wait();
  Result<std::shared_ptr<AuthSession>> waiter_result = nullptr;
  std::thread waiter([&] { waiter_result = cache->Get("tenant-a", loader); });
  std::this_thread::sleep_for(20ms);

  cache->Close();
  waiter.join();  // Close() wakes waiters before the load finishes.
  EXPECT_THAT(waiter_result, IsError(ErrorKind::kInvalid));

  release.Open();
  loading.join();
  EXPECT_THAT(loader_result, IsError(ErrorKind::kInvalid));
  EXPECT_EQ(cache->size(), 0);
  EXPECT_THAT(recorder_.removed(), ::testing::ElementsAre(loaded));
}

TEST_F(AuthSessionCacheTest, EverySessionIsRemovedExactlyOnceUnderContention) {
  auto cache = MakeCache();
  std::mutex loaded_mutex;
  std::set<std::shared_ptr<AuthSession>> loaded;
  auto loader = [&]() -> Result<std::shared_ptr<AuthSession>> {
    auto session = AuthSession::MakeDefault({});
    std::lock_guard lock(loaded_mutex);
    loaded.insert(session);
    return session;
  };

  std::atomic<bool> stop = false;
  std::thread sweeper([&] {
    while (!stop) {
      cache->Sweep();
      std::this_thread::yield();
    }
  });
  std::vector<std::thread> workers;
  for (int t = 0; t < 4; ++t) {
    workers.emplace_back([&, t] {
      for (int i = 0; i < 500; ++i) {
        EXPECT_THAT(cache->Get("key-" + std::to_string((i + t) % 5), loader), IsOk());
        if (i % 10 == 0) {
          clock_.Advance(kTimeout / 3);
        }
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  stop = true;
  sweeper.join();
  cache->Close();

  auto removed = recorder_.removed();
  std::set<std::shared_ptr<AuthSession>> unique_removed(removed.begin(), removed.end());
  EXPECT_EQ(unique_removed.size(), removed.size()) << "a session was removed twice";
  EXPECT_EQ(unique_removed, loaded);
}

TEST_F(AuthSessionCacheTest, PeriodicSweepRemovesExpiredEntries) {
  test::ThreadExecutor executor;  // must outlive the cache
  auto cache = MakeCache();
  std::atomic<int> calls = 0;
  ICEBERG_UNWRAP_OR_FAIL(auto idle, cache->Get("idle", CountingLoader(calls)));
  clock_.Advance(kTimeout);

  ASSERT_THAT(cache->StartPeriodicSweep(executor, 10ms), IsOk());

  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (cache->size() != 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(5ms);
  }
  EXPECT_EQ(cache->size(), 0);
  EXPECT_THAT(recorder_.removed(), ::testing::ElementsAre(idle));
}

TEST_F(AuthSessionCacheTest, PeriodicSweepContinuesAfterSubmissionFailure) {
  test::ThreadExecutor executor(IOError("executor rejected the task"));
  auto cache = MakeCache();
  ASSERT_THAT(cache->StartPeriodicSweep(executor, 10ms), IsOk());

  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (executor.submit_count() < 3 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(5ms);
  }
  EXPECT_GE(executor.submit_count(), 3);
  cache->Close();
}

TEST_F(AuthSessionCacheTest, PeriodicSweepArgumentsAndLifecycle) {
  test::ThreadExecutor executor;
  auto cache = MakeCache();

  EXPECT_THAT(cache->StartPeriodicSweep(executor, 0ms),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(cache->StartPeriodicSweep(executor, 10ms), IsOk());
  EXPECT_THAT(cache->StartPeriodicSweep(executor, 10ms), IsOk());  // no-op

  // Destroying the cache cancels the pending sweep.
  cache.reset();
  int submitted = executor.submit_count();
  ASSERT_TRUE(test::WaitForSharedTimer(30ms));
  EXPECT_EQ(executor.submit_count(), submitted);

  auto closed = MakeCache();
  closed->Close();
  EXPECT_THAT(closed->StartPeriodicSweep(executor, 10ms), IsOk());  // no-op
  ASSERT_TRUE(test::WaitForSharedTimer(30ms));
  EXPECT_EQ(executor.submit_count(), submitted);
}

}  // namespace iceberg::rest::auth
