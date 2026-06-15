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

namespace iceberg {

namespace {

/// \brief Logger that drops every record.
class NoopLogger final : public Logger {
 public:
  bool ShouldLog(LogLevel /*level*/) const override { return false; }
  void Log(LogMessage&& /*message*/) noexcept override {}
  void SetLevel(LogLevel /*level*/) override {}
  LogLevel level() const override { return LogLevel::kOff; }
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

namespace detail {

const std::shared_ptr<Logger>& CurrentLogger() noexcept {
  static thread_local std::shared_ptr<Logger> tls;
  static thread_local uint64_t tls_gen = 0;
  // Sentinel whose destructor marks the cache dead at thread exit. It is
  // declared after tls/tls_gen, so it is destroyed FIRST (reverse order); once
  // dead, a log from any later-destroyed thread_local destructor must not touch
  // the (about-to-be / already) destroyed tls slot.
  static thread_local struct AliveFlag {
    bool value = true;
    ~AliveFlag() { value = false; }
  } alive;
  if (!alive.value) {
    // Thread teardown: the TLS cache is unsafe. Fall back to an immortal logger
    // (leaked, never destroyed) so logging during teardown stays safe.
    static const std::shared_ptr<Logger> kFallback = Logger::Noop();
    return kFallback;
  }
  DefaultSlot& slot = Slot();
  uint64_t current = slot.gen.load(std::memory_order_relaxed);
  if (current != tls_gen) {
    std::lock_guard<std::mutex> lock(slot.mtx);
    tls = slot.logger;
    tls_gen = current;
  }
  return tls;
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

}  // namespace detail

}  // namespace iceberg
