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
/// cached at. Heap-allocated per thread and freed at thread exit (see CurrentLogger).
struct ThreadCache {
  std::shared_ptr<Logger> logger;
  uint64_t gen = 0;  // 0 != Slot().gen (seeded to 1) -> first use always refreshes
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

const std::shared_ptr<Logger>& CurrentLogger() noexcept {
  // Per-thread cache, freed at thread exit, yet safe to read from ANY other
  // thread_local's destructor during teardown, in any destruction order. The
  // three thread_locals are split by lifetime on purpose:
  //
  //   * `dead` and `cache` are trivially destructible, so their storage stays
  //     valid for the WHOLE thread (their lifetime is not ended by a destructor);
  //     they remain readable throughout the teardown of every other thread_local.
  //   * `guard` is the only one with a destructor. At thread exit it sets `dead`
  //     and frees the cache. A thread_local destroyed AFTER `guard` reads
  //     `dead == true` and falls back instead of touching freed memory; one
  //     destroyed BEFORE `guard` still sees a live cache. Either order is safe.
  //
  // (The earlier AliveFlag was wrong because the flag itself had a destructor, so
  // reading it after that destructor ran was UB -- the very bug it tried to avoid.)
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

  if (dead) {
    // Thread teardown after the cache was freed: serve an immortal no-op so a log
    // from a later thread_local destructor is safe. Such teardown logs are dropped.
    static auto* fallback = new std::shared_ptr<Logger>(Logger::Noop());
    return *fallback;
  }
  if (cache == nullptr) {
    cache = new ThreadCache();
  }

  DefaultSlot& slot = Slot();
  uint64_t current = slot.gen.load(std::memory_order_relaxed);
  if (current != cache->gen) {
    std::lock_guard<std::mutex> lock(slot.mtx);
    cache->logger = slot.logger;
    cache->gen = current;
  }
  return cache->logger;
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

}  // namespace iceberg
