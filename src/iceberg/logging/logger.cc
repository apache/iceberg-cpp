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

#include "iceberg/logging/logger.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <tuple>
#include <utility>

namespace iceberg {

namespace {

/// \brief Logger that drops every record.
class NoopLogger final : public Logger {
 public:
  bool ShouldLog(LogLevel /*level*/) const noexcept override { return false; }
  void Log(LogMessage&& /*message*/) noexcept override {}
  void SetLevel(LogLevel /*level*/) noexcept override {}
  LogLevel level() const noexcept override { return LogLevel::kOff; }
  bool IsNoop() const override { return true; }
};

/// \brief Construct the process default logger for this build configuration.
///
/// This block ships only the interface and the no-op logger; the concrete
/// std::cerr and spdlog sinks (and the build-config selection between them)
/// arrive in later blocks, which update this factory.
std::shared_ptr<Logger> MakeDefaultLogger() { return std::make_shared<NoopLogger>(); }

/// \brief The process-global default-logger slot.
struct DefaultSlot {
  std::mutex mtx;
  std::shared_ptr<Logger> logger;
  // Seeded to 1 so a fresh thread (tls_gen == 0) always refreshes on first use.
  std::atomic<uint64_t> gen{1};

  DefaultSlot() : logger(MakeDefaultLogger()) {}
};

/// \brief Immortal (leaked, hence reachable -> LSan-clean) accessor for the slot.
DefaultSlot& Slot() {
  static auto* slot = new DefaultSlot();
  return *slot;
}

/// \brief A thread's cached view of the default logger and the generation it was
/// cached at. Heap-allocated per thread and freed at thread exit (see
/// AccessThreadCache). `override_` is the active ScopedLogger binding for this
/// thread (empty when none); when set it supersedes the cached default.
struct ThreadCache {
  std::shared_ptr<Logger> logger;
  uint64_t gen = 0;  // 0 != Slot().gen (seeded to 1) -> first use always refreshes
  std::shared_ptr<Logger> override_;  // active ScopedLogger binding, empty if none
};

}  // namespace

std::shared_ptr<Logger> Logger::Noop() {
  // Intentionally leaked: reachable via the function-local static (LSan-clean)
  // and never destroyed, so logging during static teardown stays safe.
  static auto* instance = new std::shared_ptr<Logger>(std::make_shared<NoopLogger>());
  return *instance;
}

std::shared_ptr<Logger> GetDefaultLogger() {
  DefaultSlot& slot = Slot();
  std::lock_guard<std::mutex> lock(slot.mtx);
  return slot.logger;
}

void SetDefaultLogger(std::shared_ptr<Logger> logger) {
  if (!logger) {
    logger = Logger::Noop();
  }
  DefaultSlot& slot = Slot();
  std::lock_guard<std::mutex> lock(slot.mtx);
  slot.logger = std::move(logger);
  // Publish the swap; the mutex provides the happens-before, gen is a detector.
  slot.gen.fetch_add(1, std::memory_order_relaxed);
}

void SetDefaultLevel(LogLevel level) {
  DefaultSlot& slot = Slot();
  std::lock_guard<std::mutex> lock(slot.mtx);
  slot.logger->SetLevel(level);
}

namespace internal {

namespace {

/// \brief The one place the per-thread cache's lifetime is managed; shared by
/// CurrentLogger, GetCurrentLogger, and the ScopedLogger helpers.
///
/// Safe to call from any thread_local destructor during teardown: `dead`/`cache`
/// are trivially destructible so their storage lives the whole thread, and `guard`
/// (the only one with a destructor) sets `dead` before freeing the cache -- so a
/// late caller gets nullptr instead of touching freed memory, in any order.
///
/// \param create allocate the cache if absent (writers pass true; read-only
///   callers pass false so a query never allocates). Returns nullptr while tearing
///   down, or when !create and no cache exists -- caller falls back to global/noop.
ThreadCache* AccessThreadCache(bool create) noexcept {
  static thread_local bool dead = false;
  static thread_local ThreadCache* cache = nullptr;
  static thread_local struct Guard {
    ~Guard() {
      dead = true;  // mark BEFORE freeing, so a re-entrant log hits the fallback
      delete cache;
      cache = nullptr;
    }
  } guard;
  std::ignore = guard;  // mark the thread_local as intentionally used (its dtor is
                        // registered by reaching the declaration above)

  if (dead) return nullptr;
  if (cache == nullptr && create) cache = new ThreadCache();
  return cache;
}

}  // namespace

const std::shared_ptr<Logger>& CurrentLogger() noexcept {
  ThreadCache* cache = AccessThreadCache(/*create=*/true);
  if (cache == nullptr) {
    // Thread teardown after the cache was freed: serve an immortal no-op so a log
    // from a later thread_local destructor is safe. Such teardown logs are dropped.
    static auto* fallback = new std::shared_ptr<Logger>(Logger::Noop());
    return *fallback;
  }

  // A scoped override wins -- no lock, no gen load. After the cache check (deref is
  // teardown-safe), before the refresh (keeps the override path cheapest).
  if (cache->override_) return cache->override_;

  DefaultSlot& slot = Slot();
  uint64_t current = slot.gen.load(std::memory_order_relaxed);
  if (current != cache->gen) {
    std::lock_guard<std::mutex> lock(slot.mtx);
    cache->logger = slot.logger;
    cache->gen = current;
  }
  return cache->logger;
}

std::shared_ptr<Logger> ExchangeThreadOverride(std::shared_ptr<Logger> next) noexcept {
  ThreadCache* cache = AccessThreadCache(/*create=*/true);
  if (cache == nullptr) return {};  // tearing down -> binding a scope is a no-op
  std::shared_ptr<Logger> prev = std::move(cache->override_);
  cache->override_ = std::move(next);
  return prev;  // empty == no override was active
}

void RestoreThreadOverride(std::shared_ptr<Logger> prev) noexcept {
  ThreadCache* cache = AccessThreadCache(/*create=*/false);  // never allocate to restore
  if (cache == nullptr) return;  // dead or never created -> nothing to restore
  cache->override_ = std::move(prev);
}

void Emit(Logger& logger, LogLevel level, const std::source_location& location,
          std::string&& message) {
  logger.Log(LogMessage{.level = level,
                        .message = std::move(message),
                        .location = location,
                        .attributes = {}});
}

void EmitFormatError(Logger& logger, LogLevel level,
                     const std::source_location& location) noexcept {
  // Fixed short literal (<= 15 bytes, fits SSO on libstdc++/libc++/MSVC -> no heap
  // allocation), no std::format, no retry. Cannot throw or recurse.
  logger.Log(LogMessage{.level = level,
                        .message = std::string("<fmt error>"),
                        .location = location,
                        .attributes = {}});
}

}  // namespace internal

ScopedLogger::ScopedLogger(std::shared_ptr<Logger> logger) noexcept
    : previous_(internal::ExchangeThreadOverride(std::move(logger))) {}

ScopedLogger::~ScopedLogger() { internal::RestoreThreadOverride(std::move(previous_)); }

std::shared_ptr<Logger> GetCurrentLogger() {
  ThreadCache* cache = internal::AccessThreadCache(/*create=*/false);
  if (cache == nullptr) {
    // Teardown, or no per-thread cache ever created -> no override possible.
    // GetDefaultLogger() touches only the immortal slot (no thread_local), so it
    // stays valid during teardown, and is never null.
    return GetDefaultLogger();
  }
  if (cache->override_) return cache->override_;
  return GetDefaultLogger();
}

}  // namespace iceberg
