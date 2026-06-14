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
/// \brief Pluggable logging interface, the process-global default logger, and
/// the logging macros.
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

/// \brief Compile-time severity floor: statements below this level are removed
/// entirely from the build (their format call sites and source_location literals
/// are never emitted). Defaults to keeping everything. ICEBERG_LOG_FATAL is never
/// gated by this floor -- its abort is always compiled in.
#ifndef ICEBERG_LOG_ACTIVE_LEVEL
#  define ICEBERG_LOG_ACTIVE_LEVEL ::iceberg::LogLevel::kTrace
#endif

// Internal: fixed-severity emit with compile-time floor + lock-free gate +
// authoritative ShouldLog, formatting only on the taken path, never throwing.
#define ICEBERG_INTERNAL_LOG(level_, FMT_, ...)                                      \
  do {                                                                               \
    if constexpr ((level_) >= ICEBERG_LOG_ACTIVE_LEVEL) {                            \
      if ((level_) >= ::iceberg::detail::EffectiveLevel()) {                         \
        const auto& _ib_logger = ::iceberg::detail::CurrentLogger();                 \
        if (_ib_logger && _ib_logger->ShouldLog(level_)) {                           \
          try {                                                                      \
            ::iceberg::detail::Emit(*_ib_logger, (level_),                           \
                                    ::std::source_location::current(),               \
                                    ::std::format(FMT_ __VA_OPT__(, ) __VA_ARGS__)); \
          } catch (...) {                                                            \
            ::iceberg::detail::EmitFormatError(*_ib_logger, (level_),                \
                                               ::std::source_location::current());   \
          }                                                                          \
        }                                                                            \
      }                                                                              \
    }                                                                                \
  } while (0)

#define ICEBERG_LOG_TRACE(...) \
  ICEBERG_INTERNAL_LOG(::iceberg::LogLevel::kTrace, __VA_ARGS__)
#define ICEBERG_LOG_DEBUG(...) \
  ICEBERG_INTERNAL_LOG(::iceberg::LogLevel::kDebug, __VA_ARGS__)
#define ICEBERG_LOG_INFO(...) \
  ICEBERG_INTERNAL_LOG(::iceberg::LogLevel::kInfo, __VA_ARGS__)
#define ICEBERG_LOG_WARN(...) \
  ICEBERG_INTERNAL_LOG(::iceberg::LogLevel::kWarn, __VA_ARGS__)
#define ICEBERG_LOG_ERROR(...) \
  ICEBERG_INTERNAL_LOG(::iceberg::LogLevel::kError, __VA_ARGS__)
#define ICEBERG_LOG_CRITICAL(...) \
  ICEBERG_INTERNAL_LOG(::iceberg::LogLevel::kCritical, __VA_ARGS__)

// FATAL: emit if enabled (never compile-stripped), then ALWAYS flush + abort.
#define ICEBERG_LOG_FATAL(FMT_, ...)                                                   \
  do {                                                                                 \
    if (::iceberg::LogLevel::kFatal >= ::iceberg::detail::EffectiveLevel()) {          \
      const auto& _ib_logger = ::iceberg::detail::CurrentLogger();                     \
      if (_ib_logger && _ib_logger->ShouldLog(::iceberg::LogLevel::kFatal)) {          \
        try {                                                                          \
          ::iceberg::detail::Emit(*_ib_logger, ::iceberg::LogLevel::kFatal,            \
                                  ::std::source_location::current(),                   \
                                  ::std::format(FMT_ __VA_OPT__(, ) __VA_ARGS__));     \
        } catch (...) {                                                                \
          ::iceberg::detail::EmitFormatError(*_ib_logger, ::iceberg::LogLevel::kFatal, \
                                             ::std::source_location::current());       \
        }                                                                              \
      }                                                                                \
    }                                                                                  \
    if (auto _ib_fatal = ::iceberg::GetDefaultLogger()) _ib_fatal->Flush();            \
    ::std::abort();                                                                    \
  } while (0)

// Generic, runtime-level form against the default logger. No compile-time floor
// (the level is not a constant). Aborts when level == kFatal.
#define ICEBERG_LOG(level_, FMT_, ...)                                             \
  do {                                                                             \
    const ::iceberg::LogLevel _ib_lvl = (level_);                                  \
    if (_ib_lvl >= ::iceberg::detail::EffectiveLevel()) {                          \
      const auto& _ib_logger = ::iceberg::detail::CurrentLogger();                 \
      if (_ib_logger && _ib_logger->ShouldLog(_ib_lvl)) {                          \
        try {                                                                      \
          ::iceberg::detail::Emit(*_ib_logger, _ib_lvl,                            \
                                  ::std::source_location::current(),               \
                                  ::std::format(FMT_ __VA_OPT__(, ) __VA_ARGS__)); \
        } catch (...) {                                                            \
          ::iceberg::detail::EmitFormatError(*_ib_logger, _ib_lvl,                 \
                                             ::std::source_location::current());   \
        }                                                                          \
      }                                                                            \
    }                                                                              \
    if (_ib_lvl == ::iceberg::LogLevel::kFatal) {                                  \
      if (auto _ib_fatal = ::iceberg::GetDefaultLogger()) _ib_fatal->Flush();      \
      ::std::abort();                                                              \
    }                                                                              \
  } while (0)

// Generic form targeting an EXPLICIT logger. Honors only that logger's
// ShouldLog -- never the default logger's global gate. Aborts when level == kFatal.
#define ICEBERG_LOG_TO(logger_, level_, FMT_, ...)                                      \
  do {                                                                                  \
    ::iceberg::Logger& _ib_logger = (logger_);                                          \
    const ::iceberg::LogLevel _ib_lvl = (level_);                                       \
    if (_ib_logger.ShouldLog(_ib_lvl)) {                                                \
      try {                                                                             \
        ::iceberg::detail::Emit(_ib_logger, _ib_lvl, ::std::source_location::current(), \
                                ::std::format(FMT_ __VA_OPT__(, ) __VA_ARGS__));        \
      } catch (...) {                                                                   \
        ::iceberg::detail::EmitFormatError(_ib_logger, _ib_lvl,                         \
                                           ::std::source_location::current());          \
      }                                                                                 \
    }                                                                                   \
    if (_ib_lvl == ::iceberg::LogLevel::kFatal) {                                       \
      _ib_logger.Flush();                                                               \
      ::std::abort();                                                                   \
    }                                                                                   \
  } while (0)

// Runtime (non-literal) format string against the default logger.
#define ICEBERG_LOG_RUNTIME_FMT(level_, FMT_, ...)                               \
  do {                                                                           \
    const ::iceberg::LogLevel _ib_lvl = (level_);                                \
    if (_ib_lvl >= ::iceberg::detail::EffectiveLevel()) {                        \
      const auto& _ib_logger = ::iceberg::detail::CurrentLogger();               \
      if (_ib_logger && _ib_logger->ShouldLog(_ib_lvl)) {                        \
        try {                                                                    \
          ::iceberg::detail::Emit(                                               \
              *_ib_logger, _ib_lvl, ::std::source_location::current(),           \
              ::iceberg::detail::VFormat((FMT_)__VA_OPT__(, ) __VA_ARGS__));     \
        } catch (...) {                                                          \
          ::iceberg::detail::EmitFormatError(*_ib_logger, _ib_lvl,               \
                                             ::std::source_location::current()); \
        }                                                                        \
      }                                                                          \
    }                                                                            \
    if (_ib_lvl == ::iceberg::LogLevel::kFatal) {                                \
      if (auto _ib_fatal = ::iceberg::GetDefaultLogger()) _ib_fatal->Flush();    \
      ::std::abort();                                                            \
    }                                                                            \
  } while (0)

// Bare, Java-style aliases. Opt-IN only (define ICEBERG_LOG_SHORT_MACROS before
// including this header) to avoid colliding with glog/abseil/windows.h in
// consumer translation units. No bare LOG(level) is provided.
#ifdef ICEBERG_LOG_SHORT_MACROS
#  define LOG_TRACE(...) ICEBERG_LOG_TRACE(__VA_ARGS__)
#  define LOG_DEBUG(...) ICEBERG_LOG_DEBUG(__VA_ARGS__)
#  define LOG_INFO(...) ICEBERG_LOG_INFO(__VA_ARGS__)
#  define LOG_WARN(...) ICEBERG_LOG_WARN(__VA_ARGS__)
#  define LOG_ERROR(...) ICEBERG_LOG_ERROR(__VA_ARGS__)
#  define LOG_CRITICAL(...) ICEBERG_LOG_CRITICAL(__VA_ARGS__)
#  define LOG_FATAL(...) ICEBERG_LOG_FATAL(__VA_ARGS__)
#endif  // ICEBERG_LOG_SHORT_MACROS
