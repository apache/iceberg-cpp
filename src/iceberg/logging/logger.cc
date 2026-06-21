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
#include <utility>
#include <vector>

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
/// cached at. Heap-allocated and intentionally never freed (see CurrentLogger).
struct ThreadCache {
  std::shared_ptr<Logger> logger;
  uint64_t gen = 0;  // 0 != Slot().gen (seeded to 1) -> first use always refreshes
};

/// \brief Keeps every leaked ThreadCache reachable so LeakSanitizer does not flag
/// the intentional per-thread leak. Immortal (leaked) itself, like the slot.
struct CacheRegistry {
  std::mutex mtx;
  std::vector<ThreadCache*> caches;
};
CacheRegistry& Registry() {
  static auto* registry = new CacheRegistry();
  return *registry;
}

/// \brief Allocate a per-thread cache that is never destroyed and register it so
/// it stays reachable for LSan.
ThreadCache* NewThreadCache() {
  auto* cache = new ThreadCache();  // intentionally leaked; see CurrentLogger
  CacheRegistry& registry = Registry();
  std::lock_guard<std::mutex> lock(registry.mtx);
  registry.caches.push_back(cache);
  return cache;
}

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
  // The per-thread cache is reached through a trivially-destructible thread_local
  // raw pointer to a heap object that is intentionally NEVER destroyed. This makes
  // the accessor safe to call during thread teardown -- including from another
  // thread_local's destructor, in any destruction order: the pointer has no
  // destructor (so it stays readable throughout teardown), and the cached
  // shared_ptr outlives every thread_local, so there is no use-after-destruction.
  // The intentional per-thread leak is kept LSan-reachable via the registry.
  static thread_local ThreadCache* cache = NewThreadCache();
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
