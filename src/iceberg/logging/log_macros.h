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

/// \file iceberg/logging/log_macros.h
/// \brief Iceberg-prefixed logging macros (ICEBERG_LOG_*).
///
/// Kept out of logger.h so consumers of the C++ logging API (Logger, Log(),
/// ScopedLogger) are not forced to pull in these macros or the conforming
/// preprocessor they require. Include this header to use the macros; include
/// short_log_macros.h for the bare LOG_* aliases.

#include <cstdlib>
#include <format>
#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <utility>

#include "iceberg/logging/log_level.h"
#include "iceberg/logging/logger.h"

namespace iceberg::internal {

/// \brief Runtime (non-literal) format-string helper for ICEBERG_LOG_RUNTIME_FMT.
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

/// \brief Gate on \p logger.ShouldLog, then format (via \p make_message) and emit.
///
/// \p make_message is a callable returning the formatted std::string; it is
/// invoked only after the level passes ShouldLog, so a disabled log never
/// evaluates its format arguments. Never throws: a std::formatter that throws
/// (any type) routes to EmitFormatError, so the noexcept logging contract holds.
template <typename MakeMessage>
void EmitIfEnabled(Logger& logger, LogLevel level, const std::source_location& location,
                   MakeMessage&& make_message) noexcept {
  if (!logger.ShouldLog(level)) return;
  try {
    Emit(logger, level, location, std::forward<MakeMessage>(make_message)());
  } catch (...) {
    EmitFormatError(logger, level, location);
  }
}

/// \brief Emit to the current (scoped-or-default) logger if enabled.
template <typename MakeMessage>
void LogToCurrent(LogLevel level, const std::source_location& location,
                  MakeMessage&& make_message) noexcept {
  const std::shared_ptr<Logger>& logger = CurrentLogger();
  if (logger) {
    EmitIfEnabled(*logger, level, location, std::forward<MakeMessage>(make_message));
  }
}

/// \brief Runtime-level variant against the current logger: emit if enabled, then
/// flush + abort when level == kFatal (using the same acquired logger).
template <typename MakeMessage>
void LogToCurrentRuntime(LogLevel level, const std::source_location& location,
                         MakeMessage&& make_message) noexcept {
  const std::shared_ptr<Logger>& logger = CurrentLogger();
  if (logger) {
    EmitIfEnabled(*logger, level, location, std::forward<MakeMessage>(make_message));
  }
  if (level == LogLevel::kFatal) {
    if (logger) logger->Flush();
    std::abort();
  }
}

/// \brief Runtime-level variant against an explicit logger: emit if enabled, then
/// flush + abort when level == kFatal.
template <typename MakeMessage>
void LogToExplicitRuntime(Logger& logger, LogLevel level,
                          const std::source_location& location,
                          MakeMessage&& make_message) noexcept {
  EmitIfEnabled(logger, level, location, std::forward<MakeMessage>(make_message));
  if (level == LogLevel::kFatal) {
    logger.Flush();
    std::abort();
  }
}

/// \brief Fatal path: acquire the effective (scoped-or-default) logger ONCE, emit
/// if enabled, flush that same logger, run any registered FatalHandler, then
/// abort. Never returns.
///
/// The message is always formatted here (independent of ShouldLog) so the handler
/// receives it even when the fatal record itself is filtered out. The handler runs
/// after emit+flush and before abort; if it does not itself terminate the process,
/// std::abort() still runs.
template <typename MakeMessage>
[[noreturn]] void LogFatal(const std::source_location& location,
                           MakeMessage&& make_message) noexcept {
  std::string message;
  try {
    message = std::forward<MakeMessage>(make_message)();
  } catch (...) {
    message = "<fmt error>";
  }
  auto logger = GetCurrentLogger();
  if (logger) {
    if (logger->ShouldLog(LogLevel::kFatal)) {
      Emit(*logger, LogLevel::kFatal, location, std::string(message));
    }
    logger->Flush();
  }
  if (auto handler = GetFatalHandler()) {
    try {
      handler(location, message);
    } catch (...) {  // a throwing handler must not prevent the abort
    }
  }
  std::abort();
}

}  // namespace iceberg::internal

// ---------------------------------------------------------------------------
// Logging macros.
//
// Every macro takes a std::format string followed by its arguments. The
// rendered line depends on the active backend (see cerr_logger.h for the
// std::cerr layout, or the spdlog pattern); the examples below show the call
// site and, for the default CerrLogger, the line it produces.
//
//   ICEBERG_LOG_TRACE("entering scan for {}", table);
//     2026-06-16T10:59:41.186Z trace [12345] [table_scan.cc:88] entering scan for db.t
//   ICEBERG_LOG_DEBUG("cache miss key={}", key);
//     2026-06-16T10:59:41.186Z debug [12345] [cache.cc:42] cache miss key=manifest-7
//   ICEBERG_LOG_INFO("loaded {} manifests in {} ms", n, ms);
//     2026-06-16T10:59:41.186Z info [12345] [table_scan.cc:91] loaded 5 manifests in 12
//     ms
//   ICEBERG_LOG_WARN("retry {} after {}", attempt, err);
//     2026-06-16T10:59:41.186Z warn [12345] [io.cc:51] retry 2 after timeout
//   ICEBERG_LOG_ERROR("commit failed: {}", status);
//     2026-06-16T10:59:41.186Z error [12345] [txn.cc:77] commit failed: conflict
//   ICEBERG_LOG_CRITICAL("metadata unreadable at {}", path);
//     2026-06-16T10:59:41.186Z critical [12345] [meta.cc:30] metadata unreadable at
//     s3://b/m.json
//   ICEBERG_LOG_FATAL("unrecoverable: {}", reason);   // emits, flushes, then
//   std::abort()
//     2026-06-16T10:59:41.186Z fatal [12345] [boot.cc:19] unrecoverable: bad config
//
// Less common forms:
//   ICEBERG_LOG(level, "level chosen at runtime: {}", x);     // runtime severity
//   ICEBERG_LOG_TO(logger, level, "to an explicit logger {}", y);
//   ICEBERG_LOG_RUNTIME_FMT(level, fmt_string, args...);       // non-literal format
//
// Include short_log_macros.h for bare aliases (LOG_INFO, ...). A format string is
// mandatory; zero extra args is fine (ICEBERG_LOG_INFO("done")).
// ---------------------------------------------------------------------------

/// \brief Compile-time severity floor: statements below this level are discarded
/// via `if constexpr`, so no emit code runs and no format call / source_location
/// is generated for them (the compiler is free to optimize the dead branch away).
/// The statement must still be well-formed -- a bad format string or a
/// non-formattable argument is a compile error even when the branch is discarded.
/// Defaults to keeping everything. ICEBERG_LOG_FATAL is never gated by this floor
/// -- its abort is always compiled in.
#ifndef ICEBERG_LOG_ACTIVE_LEVEL
#  define ICEBERG_LOG_ACTIVE_LEVEL ::iceberg::LogLevel::kTrace
#endif

// A message-builder lambda that formats lazily (only invoked past ShouldLog by the
// EmitIfEnabled helpers), so disabled logs never evaluate their arguments.
#define ICEBERG_INTERNAL_LOG_MESSAGE(FMT_, ...) \
  [&]() -> ::std::string { return ::std::format((FMT_)__VA_OPT__(, ) __VA_ARGS__); }

// Fixed-severity emit with a compile-time floor (`if constexpr`) then the shared
// current-logger path. Formatting happens only on the taken path and never throws.
#define ICEBERG_INTERNAL_LOG(level_, FMT_, ...)                           \
  do {                                                                    \
    if constexpr ((level_) >= ICEBERG_LOG_ACTIVE_LEVEL) {                 \
      ::iceberg::internal::LogToCurrent(                                  \
          (level_), ::std::source_location::current(),                    \
          ICEBERG_INTERNAL_LOG_MESSAGE(FMT_ __VA_OPT__(, ) __VA_ARGS__)); \
    }                                                                     \
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
// Acquires the effective (scoped-or-default) logger ONCE so a concurrent
// SetDefaultLogger cannot flush a different logger than it emitted to.
#define ICEBERG_LOG_FATAL(FMT_, ...)     \
  ::iceberg::internal::LogFatal(         \
      ::std::source_location::current(), \
      ICEBERG_INTERNAL_LOG_MESSAGE(FMT_ __VA_OPT__(, ) __VA_ARGS__))

// Generic, runtime-level form against the default logger. No compile-time floor
// (the level is not a constant). Aborts when level == kFatal.
#define ICEBERG_LOG(level_, FMT_, ...)             \
  ::iceberg::internal::LogToCurrentRuntime(        \
      (level_), ::std::source_location::current(), \
      ICEBERG_INTERNAL_LOG_MESSAGE(FMT_ __VA_OPT__(, ) __VA_ARGS__))

// Generic form targeting an EXPLICIT logger (must be an lvalue Logger&). Honors
// only that logger's ShouldLog. Aborts when level == kFatal.
#define ICEBERG_LOG_TO(logger_, level_, FMT_, ...)            \
  ::iceberg::internal::LogToExplicitRuntime(                  \
      (logger_), (level_), ::std::source_location::current(), \
      ICEBERG_INTERNAL_LOG_MESSAGE(FMT_ __VA_OPT__(, ) __VA_ARGS__))

// Runtime (non-literal) format string against the default logger. Aborts when
// level == kFatal.
#define ICEBERG_LOG_RUNTIME_FMT(level_, FMT_, ...)                             \
  ::iceberg::internal::LogToCurrentRuntime(                                    \
      (level_), ::std::source_location::current(), [&]() -> ::std::string {    \
        return ::iceberg::internal::VFormat((FMT_)__VA_OPT__(, ) __VA_ARGS__); \
      })
