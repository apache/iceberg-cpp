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

#include <concepts>
#include <cstdlib>
#include <format>
#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/logging/log_level.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief A structured key/value attribute attached to a log record.
///
/// Both key and value are owned so a sink may retain the record safely. Engine
/// loggers can surface these as discrete fields (query id, task id, table name,
/// snapshot id, file path, ...); see LogMessage::Builder to populate them.
struct ICEBERG_EXPORT LogAttribute {
  std::string key;
  std::string value;
};

/// \brief A single log record handed to a Logger.
///
/// The formatted message is owned (moved in by the logging macros), so a sink
/// may safely retain the record beyond the Log() call. The member set must not
/// depend on the build's logging backend (the spdlog backend never appears here).
/// Use LogMessage::Builder for a readable way to assemble one, especially with
/// structured attributes.
struct ICEBERG_EXPORT LogMessage {
  LogLevel level = LogLevel::kOff;
  std::string message;
  std::source_location location = std::source_location::current();
  std::vector<LogAttribute> attributes;

  class Builder;
};

/// \brief Fluent builder for LogMessage, the easy path to attach structured
/// attributes.
///
/// Example:
///   auto record = LogMessage::Builder(LogLevel::kInfo)
///                     .Message("scan finished")
///                     .Attribute("table", table_name)
///                     .Attribute("snapshot_id", std::to_string(id))
///                     .Build();
///   logger->Log(std::move(record));
///
/// The location defaults to the caller's construction site (captured via the
/// constructor's default argument); override it with Location() (e.g. to forward
/// a caller's std::source_location).
class ICEBERG_EXPORT LogMessage::Builder {
 public:
  explicit Builder(LogLevel level,
                   std::source_location location = std::source_location::current())
      : level_(level), location_(location) {}

  /// \brief Set the already-formatted message text.
  Builder& Message(std::string message) {
    message_ = std::move(message);
    return *this;
  }

  /// \brief Append a structured key/value attribute.
  Builder& Attribute(std::string key, std::string value) {
    attributes_.push_back(LogAttribute{.key = std::move(key), .value = std::move(value)});
    return *this;
  }

  /// \brief Override the record's source location (defaults to the build site).
  Builder& Location(std::source_location location) {
    location_ = location;
    return *this;
  }

  /// \brief Materialize the LogMessage, moving the accumulated state out.
  LogMessage Build() {
    return LogMessage{.level = level_,
                      .message = std::move(message_),
                      .location = location_,
                      .attributes = std::move(attributes_)};
  }

 private:
  LogLevel level_;
  std::string message_;
  // `location_` is a trivially copyable members no need to move.
  std::source_location location_;
  std::vector<LogAttribute> attributes_;
};

/// \brief Well-known Logger::Initialize() property keys.
///
/// `level` is honored by the base Logger::Initialize (parsed via
/// LogLevelFromString) on every backend. `pattern` is honored only by the
/// spdlog backend; CerrLogger uses a fixed layout and ignores it.
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
  /// The spdlog backend overrides this to also apply "pattern" and then delegates
  /// to this base for "level"; CerrLogger uses the base as-is (fixed layout).
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

/// \brief Return the effective logger for this thread (never null): the active
/// ScopedLogger binding if any, else the global default.
///
/// Off the hot path -- returns an owning copy, e.g. to capture the current logger
/// and re-bind it on a worker thread (see ScopedLogger). During teardown, prefer
/// the Log(...) overloads over emitting through this handle.
ICEBERG_EXPORT std::shared_ptr<Logger> GetCurrentLogger();

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

/// \brief Bind a logger for the current thread until this object leaves scope.
///
/// The default logging path on this thread -- CurrentLogger(), Log(level, ...),
/// and the LOG_* macros -- routes to \p logger instead of the global default;
/// explicit Log(logger, ...) is unaffected. Bindings nest and restore on exit, and
/// nullptr masks any enclosing binding back to the global default. Lets an engine
/// route Iceberg's own logs into a per catalog/session/query/task context with no
/// call-site changes.
///
/// \code
///   auto query_log = std::make_shared<MySink>();
///   iceberg::ScopedLogger bind(query_log);            // this thread, this scope
///   iceberg::Log(LogLevel::kInfo, "scan {}", id);     // -> query_log
/// \endcode
///
/// Stack-only and same-thread (non-copyable, non-movable). For thread pools,
/// capture on the submitting thread and re-bind on the worker:
/// \code
///   auto captured = iceberg::GetCurrentLogger();
///   pool.submit([captured, work] { iceberg::ScopedLogger bind(captured); work(); });
/// \endcode
class ICEBERG_EXPORT ScopedLogger {
 public:
  explicit ScopedLogger(std::shared_ptr<Logger> logger) noexcept;
  ~ScopedLogger();

  ScopedLogger(const ScopedLogger&) = delete;
  ScopedLogger& operator=(const ScopedLogger&) = delete;
  ScopedLogger(ScopedLogger&&) = delete;
  ScopedLogger& operator=(ScopedLogger&&) = delete;

 private:
  std::shared_ptr<Logger> previous_;
};

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

namespace internal {

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

/// \brief A checked format string bundled with the caller's source_location.
///
/// The consteval constructor preserves std::format's compile-time format-string
/// checking while capturing the call site (the std::print/println technique),
/// so the function-style Log() can record an accurate file:line without a macro.
/// Used as a non-deduced parameter so the trailing args drive deduction.
template <typename... Args>
struct FmtWithLoc {
  std::format_string<Args...> fmt;
  std::source_location loc;

  template <typename T>
    requires std::convertible_to<const T&, std::format_string<Args...>>
  consteval FmtWithLoc(  // NOLINT(google-explicit-constructor): mirrors
                         // std::format_string
      const T& s, std::source_location loc = std::source_location::current())
      : fmt(s), loc(loc) {}
};

/// \brief Shared gate -> format -> emit body for the function-style Log() API.
///
/// Formats only when the logger is enabled for \p level, and never throws (a
/// formatting failure routes to EmitFormatError, matching the macros).
template <typename... Args>
void FormatAndEmit(Logger& logger, LogLevel level, const std::source_location& loc,
                   std::format_string<Args...> fmt, Args&&... args) noexcept {
  if (!logger.ShouldLog(level)) return;
  try {
    Emit(logger, level, loc, std::format(fmt, std::forward<Args>(args)...));
  } catch (...) {
    EmitFormatError(logger, level, loc);
  }
}

}  // namespace internal

/// \brief Log to the process-default logger, std::format style. Formats only if
/// the level is enabled; never throws.
///
/// Example: `iceberg::Log(LogLevel::kInfo, "loaded {} files", n);`
template <typename... Args>
void Log(LogLevel level, internal::FmtWithLoc<std::type_identity_t<Args>...> fmt,
         Args&&... args) noexcept {
  const std::shared_ptr<Logger>& logger = internal::CurrentLogger();
  if (logger) {
    internal::FormatAndEmit(*logger, level, fmt.loc, fmt.fmt,
                            std::forward<Args>(args)...);
  }
}

/// \brief Log to an explicit logger, std::format style. Formats only if enabled.
///
/// Example: `iceberg::Log(logger, LogLevel::kWarn, "retry {}", attempt);`
template <typename... Args>
void Log(Logger& logger, LogLevel level,
         internal::FmtWithLoc<std::type_identity_t<Args>...> fmt,
         Args&&... args) noexcept {
  internal::FormatAndEmit(logger, level, fmt.loc, fmt.fmt, std::forward<Args>(args)...);
}

}  // namespace iceberg

// ---------------------------------------------------------------------------
// Logging macros.
//
// Every macro takes a std::format string followed by its arguments. The
// rendered line depends on the active backend (see cerr_logger.h for the
// std::cerr layout, or the spdlog pattern); the examples below show the call
// site and, for the default CerrLogger, the line it produces.
//
//   ICEBERG_LOG_TRACE("entering scan for {}", table);
//     2026-06-16T10:59:41.186Z trace [12345] table_scan.cc:88] entering scan for db.t
//   ICEBERG_LOG_DEBUG("cache miss key={}", key);
//     2026-06-16T10:59:41.186Z debug [12345] cache.cc:42] cache miss key=manifest-7
//   ICEBERG_LOG_INFO("loaded {} manifests in {} ms", n, ms);
//     2026-06-16T10:59:41.186Z info [12345] table_scan.cc:91] loaded 5 manifests in 12 ms
//   ICEBERG_LOG_WARN("retry {} after {}", attempt, err);
//     2026-06-16T10:59:41.186Z warn [12345] io.cc:51] retry 2 after timeout
//   ICEBERG_LOG_ERROR("commit failed: {}", status);
//     2026-06-16T10:59:41.186Z error [12345] txn.cc:77] commit failed: conflict
//   ICEBERG_LOG_CRITICAL("metadata unreadable at {}", path);
//     2026-06-16T10:59:41.186Z critical [12345] meta.cc:30] metadata unreadable at
//     s3://b/m.json
//   ICEBERG_LOG_FATAL("unrecoverable: {}", reason);   // emits, flushes, then
//   std::abort()
//     2026-06-16T10:59:41.186Z fatal [12345] boot.cc:19] unrecoverable: bad config
//
// Less common forms:
//   ICEBERG_LOG(level, "level chosen at runtime: {}", x);     // runtime severity
//   ICEBERG_LOG_TO(logger, level, "to an explicit logger {}", y);
//   ICEBERG_LOG_RUNTIME_FMT(level, fmt_string, args...);       // non-literal format
//
// With ICEBERG_LOG_SHORT_MACROS defined, bare aliases (LOG_INFO, ...) are also
// available. A format string is mandatory; zero extra args is fine
// (ICEBERG_LOG_INFO("done")).
// ---------------------------------------------------------------------------

/// \brief Compile-time severity floor: statements below this level are removed
/// entirely from the build (their format call sites and source_location literals
/// are never emitted). Defaults to keeping everything. ICEBERG_LOG_FATAL is never
/// gated by this floor -- its abort is always compiled in.
#ifndef ICEBERG_LOG_ACTIVE_LEVEL
#  define ICEBERG_LOG_ACTIVE_LEVEL ::iceberg::LogLevel::kTrace
#endif

// Internal: fixed-severity emit with compile-time floor then the authoritative
// Logger::ShouldLog (the single source of truth for runtime filtering), with
// formatting only on the taken path, never throwing.
#define ICEBERG_INTERNAL_LOG(level_, FMT_, ...)                                      \
  do {                                                                               \
    if constexpr ((level_) >= ICEBERG_LOG_ACTIVE_LEVEL) {                            \
      const auto& _ib_logger = ::iceberg::internal::CurrentLogger();                 \
      if (_ib_logger && _ib_logger->ShouldLog(level_)) {                             \
        try {                                                                        \
          ::iceberg::internal::Emit(*_ib_logger, (level_),                           \
                                    ::std::source_location::current(),               \
                                    ::std::format(FMT_ __VA_OPT__(, ) __VA_ARGS__)); \
        } catch (...) {                                                              \
          ::iceberg::internal::EmitFormatError(*_ib_logger, (level_),                \
                                               ::std::source_location::current());   \
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
// Acquires the default logger ONCE and uses the same instance for emit and flush
// so a concurrent SetDefaultLogger cannot flush a different logger than it emitted to.
#define ICEBERG_LOG_FATAL(FMT_, ...)                                                   \
  do {                                                                                 \
    auto _ib_logger = ::iceberg::GetDefaultLogger();                                   \
    if (_ib_logger && _ib_logger->ShouldLog(::iceberg::LogLevel::kFatal)) {            \
      try {                                                                            \
        ::iceberg::internal::Emit(*_ib_logger, ::iceberg::LogLevel::kFatal,            \
                                  ::std::source_location::current(),                   \
                                  ::std::format(FMT_ __VA_OPT__(, ) __VA_ARGS__));     \
      } catch (...) {                                                                  \
        ::iceberg::internal::EmitFormatError(*_ib_logger, ::iceberg::LogLevel::kFatal, \
                                             ::std::source_location::current());       \
      }                                                                                \
    }                                                                                  \
    if (_ib_logger) _ib_logger->Flush();                                               \
    ::std::abort();                                                                    \
  } while (0)

// Generic, runtime-level form against the default logger. No compile-time floor
// (the level is not a constant). Acquires the logger once; aborts when level == kFatal
// (flushing that same logger first).
#define ICEBERG_LOG(level_, FMT_, ...)                                             \
  do {                                                                             \
    const ::iceberg::LogLevel _ib_lvl = (level_);                                  \
    const auto& _ib_logger = ::iceberg::internal::CurrentLogger();                 \
    if (_ib_logger && _ib_logger->ShouldLog(_ib_lvl)) {                            \
      try {                                                                        \
        ::iceberg::internal::Emit(*_ib_logger, _ib_lvl,                            \
                                  ::std::source_location::current(),               \
                                  ::std::format(FMT_ __VA_OPT__(, ) __VA_ARGS__)); \
      } catch (...) {                                                              \
        ::iceberg::internal::EmitFormatError(*_ib_logger, _ib_lvl,                 \
                                             ::std::source_location::current());   \
      }                                                                            \
    }                                                                              \
    if (_ib_lvl == ::iceberg::LogLevel::kFatal) {                                  \
      if (_ib_logger) _ib_logger->Flush();                                         \
      ::std::abort();                                                              \
    }                                                                              \
  } while (0)

// Generic form targeting an EXPLICIT logger (must be an lvalue Logger&). Honors
// only that logger's ShouldLog. Aborts when level == kFatal.
#define ICEBERG_LOG_TO(logger_, level_, FMT_, ...)                                 \
  do {                                                                             \
    ::iceberg::Logger& _ib_logger = (logger_);                                     \
    const ::iceberg::LogLevel _ib_lvl = (level_);                                  \
    if (_ib_logger.ShouldLog(_ib_lvl)) {                                           \
      try {                                                                        \
        ::iceberg::internal::Emit(_ib_logger, _ib_lvl,                             \
                                  ::std::source_location::current(),               \
                                  ::std::format(FMT_ __VA_OPT__(, ) __VA_ARGS__)); \
      } catch (...) {                                                              \
        ::iceberg::internal::EmitFormatError(_ib_logger, _ib_lvl,                  \
                                             ::std::source_location::current());   \
      }                                                                            \
    }                                                                              \
    if (_ib_lvl == ::iceberg::LogLevel::kFatal) {                                  \
      _ib_logger.Flush();                                                          \
      ::std::abort();                                                              \
    }                                                                              \
  } while (0)

// Runtime (non-literal) format string against the default logger. Acquires the
// logger once; aborts when level == kFatal (flushing that same logger first).
#define ICEBERG_LOG_RUNTIME_FMT(level_, FMT_, ...)                               \
  do {                                                                           \
    const ::iceberg::LogLevel _ib_lvl = (level_);                                \
    const auto& _ib_logger = ::iceberg::internal::CurrentLogger();               \
    if (_ib_logger && _ib_logger->ShouldLog(_ib_lvl)) {                          \
      try {                                                                      \
        ::iceberg::internal::Emit(                                               \
            *_ib_logger, _ib_lvl, ::std::source_location::current(),             \
            ::iceberg::internal::VFormat((FMT_)__VA_OPT__(, ) __VA_ARGS__));     \
      } catch (...) {                                                            \
        ::iceberg::internal::EmitFormatError(*_ib_logger, _ib_lvl,               \
                                             ::std::source_location::current()); \
      }                                                                          \
    }                                                                            \
    if (_ib_lvl == ::iceberg::LogLevel::kFatal) {                                \
      if (_ib_logger) _ib_logger->Flush();                                       \
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
