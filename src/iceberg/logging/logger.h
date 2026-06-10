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

/// \file iceberg/logging/logger.h
/// \brief Pluggable logging interface and the process-global default logger.
///
/// This header is backend-agnostic: it never includes the build-generated
/// backend configuration header and never references the spdlog feature macro,
/// so consumers see one stable API regardless of how the backend was configured.

#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/logging/log_level.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief A structured key/value attribute attached to a log record.
///
/// Both key and value are owned so a sink may retain the record safely.
/// Unused in v1; reserved so structured logging can be added without an ABI
/// break to LogMessage.
struct LogAttribute {
  std::string key;
  std::string value;
};

/// \brief A single log record handed to a Logger.
///
/// The formatted message is owned (moved in by the logging macros), so a sink
/// may safely retain the record beyond the Log() call. The member set must not
/// depend on the build's logging backend (the spdlog backend never appears here).
struct LogMessage {
  LogLevel level;
  std::string message;
  std::source_location location;
  std::vector<LogAttribute> attributes;
};

/// \brief Pluggable logging sink.
///
/// Implementations must be thread-safe and must not throw. They must also obey:
///   - No reentrancy: Log()/Flush() must not call the logging macros or
///     GetDefaultLogger() (UB -- deadlock with mutex-based sinks).
///   - Lower-bound level: ShouldLog(l) must imply l >= level(), i.e. level()
///     summarizes ShouldLog(), because the global fast-path gate reflects only
///     level(). A logger needing finer logic must report its most permissive
///     threshold from level().
class ICEBERG_EXPORT Logger {
 public:
  virtual ~Logger() = default;

  /// \brief Property-based setup, called by Loggers::Load() before first use.
  ///
  /// The default is a no-op. Recognized properties include "level" (parsed via
  /// LogLevelFromString) and, for formatting sinks, "pattern".
  virtual Status Initialize(
      [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
    return {};
  }

  /// \brief Cheap check whether a record at \p level would be emitted.
  virtual bool ShouldLog(LogLevel level) const = 0;

  /// \brief Emit one (already-formatted) record, taking ownership. Must not throw.
  virtual void Log(LogMessage&& message) noexcept = 0;

  /// \brief Set the minimum level this logger emits.
  virtual void SetLevel(LogLevel level) = 0;

  /// \brief Return the minimum level this logger emits.
  virtual LogLevel level() const = 0;

  /// \brief Flush any buffered output. Must not throw; best-effort on the fatal path.
  virtual void Flush() noexcept {}

  /// \brief Return true if this logger is a no-op.
  virtual bool IsNoop() const { return false; }

  /// \brief Return a shared, immortal no-op logger singleton.
  static std::shared_ptr<Logger> Noop();
};

/// \brief Return the process-global default logger (never null).
///
/// Off the hot path -- acquires the slot lock and returns an owning copy. The
/// logging macros use the cheaper internal hot-path accessor instead.
ICEBERG_EXPORT std::shared_ptr<Logger> GetDefaultLogger();

/// \brief Install a new process-global default logger.
///
/// A null argument installs the no-op logger. Thread-safe; intended for
/// occasional (configuration-time) use rather than the hot path.
ICEBERG_EXPORT void SetDefaultLogger(std::shared_ptr<Logger> logger);

/// \brief Set the minimum level of the current default logger.
///
/// Updates both the logger and the lock-free fast-path gate atomically.
ICEBERG_EXPORT void SetDefaultLevel(LogLevel level);

namespace detail {

/// \brief Lock-free read of the cached effective minimum level.
///
/// This is a non-authoritative lower-bound early-out: it may admit extra calls
/// through to the authoritative Logger::ShouldLog, but never wrongly rejects.
ICEBERG_EXPORT LogLevel EffectiveLevel() noexcept;

/// \brief Hot-path accessor for the default logger.
///
/// Returns a reference to a thread-local cached shared_ptr that is refreshed
/// only when the default logger has changed (no lock / no refcount churn in
/// steady state). The reference is valid for the duration of the calling
/// statement.
ICEBERG_EXPORT const std::shared_ptr<Logger>& CurrentLogger() noexcept;

}  // namespace detail

}  // namespace iceberg
