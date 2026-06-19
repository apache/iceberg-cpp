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

#include <cstdlib>
#include <format>
#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
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
struct ICEBERG_EXPORT LogAttribute {
  std::string key;
  std::string value;
};

/// \brief A single log record handed to a Logger.
///
/// The formatted message is owned (moved in by the logging macros), so a sink
/// may safely retain the record beyond the Log() call. The member set must not
/// depend on the build's logging backend (the spdlog backend never appears here).
struct ICEBERG_EXPORT LogMessage {
  LogLevel level = LogLevel::kOff;
  std::string message;
  std::source_location location = std::source_location::current();
  std::vector<LogAttribute> attributes;
};

/// \brief Well-known Logger::Initialize() property keys.
///
/// `level` is honored by the base Logger::Initialize (parsed via
/// LogLevelFromString). `pattern` is honored by the formatting sinks
/// (CerrLogger, SpdLogger).
inline constexpr std::string_view kLevelProperty = "level";
inline constexpr std::string_view kPatternProperty = "pattern";

/// \brief Pluggable logging sink.
///
/// ShouldLog() is the single authority for runtime filtering -- the macros call
/// it on every (compile-time-enabled) statement, so level changes by any path
/// take effect immediately. Implementations must be thread-safe and must not
/// throw. They must also obey:
///   - No reentrancy: Log()/Flush() must not call the logging macros or
///     GetDefaultLogger() (UB -- deadlock with mutex-based sinks).
///   - level() is an accessor consistent with ShouldLog (used by SetDefaultLevel
///     and introspection); ShouldLog may implement finer logic than a level compare.
class ICEBERG_EXPORT Logger {
 public:
  virtual ~Logger() = default;

  /// \brief Property-based setup, called by Loggers::Load() before first use.
  ///
  /// The base implementation applies the "level" property (parsed via
  /// LogLevelFromString); an unrecognized value is an InvalidArgument error.
  /// Formatting sinks override this to also apply "pattern" and then delegate
  /// to this base for "level".
  virtual Status Initialize(
      const std::unordered_map<std::string, std::string>& properties) {
    if (auto it = properties.find(std::string(kLevelProperty)); it != properties.end()) {
      auto parsed = LogLevelFromString(it->second);
      if (!parsed) return std::unexpected(parsed.error());
      SetLevel(*parsed);
    }
    return {};
  }

  /// \brief Cheap check whether a record at \p level would be emitted.
  virtual bool ShouldLog(LogLevel level) const noexcept = 0;

  /// \brief Emit one (already-formatted) record, taking ownership. Must not throw.
  virtual void Log(LogMessage&& message) noexcept = 0;

  /// \brief Set the minimum level this logger emits.
  virtual void SetLevel(LogLevel level) noexcept = 0;

  /// \brief Return the minimum level this logger emits.
  virtual LogLevel level() const noexcept = 0;

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
/// Convenience for `GetDefaultLogger()->SetLevel(level)`. Filtering is always
/// decided by the logger's own ShouldLog(), so changing a logger's level by any
/// means (this, SetLevel on a held handle, or Initialize) takes effect immediately.
ICEBERG_EXPORT void SetDefaultLevel(LogLevel level);

// ---------------------------------------------------------------------------
// Using the API directly (the LOG_* macros that wrap this are added later in
// the stack). Example: a custom sink, installed as the process default.
//
//   class MySink : public Logger {
//    public:
//     bool ShouldLog(LogLevel level) const noexcept override { return level >= level_; }
//     void Log(LogMessage&& m) noexcept override { write_line(m.message); }
//     void SetLevel(LogLevel level) noexcept override { level_ = level; }
//     LogLevel level() const noexcept override { return level_; }
//    private:
//     std::atomic<LogLevel> level_{LogLevel::kInfo};
//   };
//
//   SetDefaultLogger(std::make_shared<MySink>());   // install process-wide
//   SetDefaultLevel(LogLevel::kDebug);              // adjust the threshold
//
//   auto logger = GetDefaultLogger();               // borrow the current default
//   if (logger->ShouldLog(LogLevel::kInfo)) {
//     logger->Log(LogMessage{.level = LogLevel::kInfo, .message = "scan ready"});
//   }
//
//   // Or configure from catalog-style properties (applies the "level" key):
//   auto sink = std::make_shared<MySink>();
//   auto status = sink->Initialize({{std::string(kLevelProperty), "warn"}});  // -> kWarn
// ---------------------------------------------------------------------------

namespace detail {

/// \brief Hot-path accessor for the default logger.
///
/// Returns a reference to a thread-local cached shared_ptr that is refreshed
/// only when the default logger has changed (no lock / no refcount churn in
/// steady state). The reference is valid for the duration of the calling
/// statement.
ICEBERG_EXPORT const std::shared_ptr<Logger>& CurrentLogger() noexcept;

/// \brief Build a LogMessage from the already-formatted text and dispatch it.
///
/// Declared ICEBERG_EXPORT because the logging macros expand into this call in
/// consumer translation units.
ICEBERG_EXPORT void Emit(Logger& logger, LogLevel level,
                         const std::source_location& location, std::string&& message);

/// \brief Emit a fixed fallback record when formatting threw.
///
/// noexcept, allocation-light (small/SSO literal), performs no std::format, and
/// does not recurse -- so the macro's "logging never throws" guarantee holds
/// even when a format argument throws.
ICEBERG_EXPORT void EmitFormatError(Logger& logger, LogLevel level,
                                    const std::source_location& location) noexcept;

/// \brief Runtime (non-literal) format-string helper.
///
/// std::format requires a compile-time format string; this routes a runtime
/// string through std::vformat. Args are bound as named lvalues and the
/// arg-store is held in a named variable so it outlives the vformat call
/// (C++23 make_format_args rejects rvalues -- P2905 / LWG3631).
template <typename... Args>
std::string VFormat(std::string_view fmt, Args&&... args) {
  auto store = std::make_format_args(args...);
  return std::vformat(fmt, store);
}

}  // namespace detail

}  // namespace iceberg
