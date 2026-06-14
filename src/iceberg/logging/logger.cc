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

// Build-generated, .cc-only (never from a public header). Defines
// ICEBERG_HAS_SPDLOG when built with -DICEBERG_SPDLOG=ON; tested with #ifdef.
#include "iceberg/logging/cerr_logger.h"
#include "iceberg/logging/config.h"
#ifdef ICEBERG_HAS_SPDLOG
#  include "iceberg/logging/internal/spdlog_logger.h"
#endif

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
/// Prefers the spdlog backend when compiled in; otherwise the always-available
/// std::cerr logger.
std::shared_ptr<Logger> MakeDefaultLogger() {
#ifdef ICEBERG_HAS_SPDLOG
  return std::make_shared<internal::SpdLogger>();
#else
  return std::make_shared<CerrLogger>();
#endif
}

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

/// \brief Cached effective minimum level (lock-free fast-path gate).
///
/// Constant-initialized permissive (kTrace); seeded to the default logger's
/// level() on first slot use and on every Set*. As a lower bound it may only
/// admit extra calls through to the authoritative Logger::ShouldLog.
std::atomic<LogLevel> g_effective_level{LogLevel::kTrace};

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
  g_effective_level.store(logger->level(), std::memory_order_relaxed);
  slot.logger = std::move(logger);
  // Publish the swap; the mutex provides the happens-before, gen is a detector.
  slot.gen.fetch_add(1, std::memory_order_relaxed);
}

void SetDefaultLevel(LogLevel level) {
  DefaultSlot& slot = Slot();
  std::lock_guard<std::mutex> lock(slot.mtx);
  slot.logger->SetLevel(level);
  g_effective_level.store(level, std::memory_order_relaxed);
}

namespace detail {

LogLevel EffectiveLevel() noexcept {
  return g_effective_level.load(std::memory_order_relaxed);
}

const std::shared_ptr<Logger>& CurrentLogger() noexcept {
  static thread_local std::shared_ptr<Logger> tls;
  static thread_local uint64_t tls_gen = 0;
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
  // Fixed short literal (SSO -> no heap allocation), no std::format, no retry.
  logger.Log(LogMessage{.level = level,
                        .message = std::string("<log format error>"),
                        .location = location,
                        .attributes = {}});
}

}  // namespace detail

}  // namespace iceberg
