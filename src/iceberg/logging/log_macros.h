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
/// \brief Iceberg-prefixed logging macros.

#include <cstdlib>
#include <format>
#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <utility>

#include "iceberg/logging/logger.h"

#define ICEBERG_LOG_LEVEL_TRACE 0
#define ICEBERG_LOG_LEVEL_DEBUG 1
#define ICEBERG_LOG_LEVEL_INFO 2
#define ICEBERG_LOG_LEVEL_WARN 3
#define ICEBERG_LOG_LEVEL_ERROR 4
#define ICEBERG_LOG_LEVEL_CRITICAL 5
#define ICEBERG_LOG_LEVEL_FATAL 6
#define ICEBERG_LOG_LEVEL_OFF 7

#ifndef ICEBERG_LOG_ACTIVE_LEVEL
#  define ICEBERG_LOG_ACTIVE_LEVEL ICEBERG_LOG_LEVEL_TRACE
#endif

static_assert(static_cast<int>(::iceberg::LogLevel::kTrace) == ICEBERG_LOG_LEVEL_TRACE);
static_assert(static_cast<int>(::iceberg::LogLevel::kDebug) == ICEBERG_LOG_LEVEL_DEBUG);
static_assert(static_cast<int>(::iceberg::LogLevel::kInfo) == ICEBERG_LOG_LEVEL_INFO);
static_assert(static_cast<int>(::iceberg::LogLevel::kWarn) == ICEBERG_LOG_LEVEL_WARN);
static_assert(static_cast<int>(::iceberg::LogLevel::kError) == ICEBERG_LOG_LEVEL_ERROR);
static_assert(static_cast<int>(::iceberg::LogLevel::kCritical) ==
              ICEBERG_LOG_LEVEL_CRITICAL);
static_assert(static_cast<int>(::iceberg::LogLevel::kFatal) == ICEBERG_LOG_LEVEL_FATAL);
static_assert(static_cast<int>(::iceberg::LogLevel::kOff) == ICEBERG_LOG_LEVEL_OFF);

namespace iceberg::logging::detail {

template <typename... Args>
std::string VFormat(std::string_view fmt, Args&&... args) {
  auto store = std::make_format_args(args...);
  return std::vformat(fmt, store);
}

template <typename FormatMessage>
void EmitIfEnabled(Logger& logger, LogLevel level, const std::source_location& location,
                   FormatMessage&& format_message) noexcept {
  if (level == LogLevel::kOff || !logger.ShouldLog(level)) return;

  try {
    ::iceberg::internal::Emit(logger, level, location,
                              std::forward<FormatMessage>(format_message)());
  } catch (...) {
    ::iceberg::internal::EmitFormatError(logger, level, location);
  }
}

template <typename FormatMessage>
void LogCurrentIfEnabled(LogLevel level, const std::source_location& location,
                         FormatMessage&& format_message) noexcept {
  const std::shared_ptr<Logger>& logger = ::iceberg::internal::CurrentLogger();
  if (logger) {
    EmitIfEnabled(*logger, level, location, std::forward<FormatMessage>(format_message));
  }
}

template <typename FormatMessage>
void LogCurrentRuntime(LogLevel level, const std::source_location& location,
                       FormatMessage&& format_message) noexcept {
  const std::shared_ptr<Logger>& logger = ::iceberg::internal::CurrentLogger();
  if (logger) {
    EmitIfEnabled(*logger, level, location, std::forward<FormatMessage>(format_message));
  }
  if (level == LogLevel::kFatal) {
    if (logger) logger->Flush();
    std::abort();
  }
}

template <typename FormatMessage>
void LogToRuntime(Logger& logger, LogLevel level, const std::source_location& location,
                  FormatMessage&& format_message) noexcept {
  EmitIfEnabled(logger, level, location, std::forward<FormatMessage>(format_message));
  if (level == LogLevel::kFatal) {
    logger.Flush();
    std::abort();
  }
}

template <typename FormatMessage>
[[noreturn]] void LogCurrentFatal(const std::source_location& location,
                                  FormatMessage&& format_message) noexcept {
  auto logger = ::iceberg::GetCurrentLogger();
  if (logger) {
    EmitIfEnabled(*logger, LogLevel::kFatal, location,
                  std::forward<FormatMessage>(format_message));
    logger->Flush();
  }
  std::abort();
}

}  // namespace iceberg::logging::detail

#define ICEBERG_INTERNAL_LOG_CURRENT(level_, FMT_, ...)                       \
  do {                                                                        \
    ::iceberg::logging::detail::LogCurrentIfEnabled(                          \
        (level_), ::std::source_location::current(), [&]() -> ::std::string { \
          return ::std::format((FMT_)__VA_OPT__(, ) __VA_ARGS__);             \
        });                                                                   \
  } while (false)

#define ICEBERG_INTERNAL_LOG_RUNTIME(level_, FMT_, ...)                       \
  do {                                                                        \
    ::iceberg::logging::detail::LogCurrentRuntime(                            \
        (level_), ::std::source_location::current(), [&]() -> ::std::string { \
          return ::std::format((FMT_)__VA_OPT__(, ) __VA_ARGS__);             \
        });                                                                   \
  } while (false)

#define ICEBERG_INTERNAL_LOG_TO(logger_, level_, FMT_, ...)                              \
  do {                                                                                   \
    ::iceberg::logging::detail::LogToRuntime(                                            \
        (logger_), (level_), ::std::source_location::current(), [&]() -> ::std::string { \
          return ::std::format((FMT_)__VA_OPT__(, ) __VA_ARGS__);                        \
        });                                                                              \
  } while (false)

#define ICEBERG_INTERNAL_LOG_RUNTIME_FMT(level_, FMT_, ...)                             \
  do {                                                                                  \
    ::iceberg::logging::detail::LogCurrentRuntime(                                      \
        (level_), ::std::source_location::current(), [&]() -> ::std::string {           \
          return ::iceberg::logging::detail::VFormat((FMT_)__VA_OPT__(, ) __VA_ARGS__); \
        });                                                                             \
  } while (false)

#define ICEBERG_INTERNAL_LOG_FATAL(FMT_, ...)                       \
  do {                                                              \
    ::iceberg::logging::detail::LogCurrentFatal(                    \
        ::std::source_location::current(), [&]() -> ::std::string { \
          return ::std::format((FMT_)__VA_OPT__(, ) __VA_ARGS__);   \
        });                                                         \
  } while (false)

#if ICEBERG_LOG_ACTIVE_LEVEL <= ICEBERG_LOG_LEVEL_TRACE
#  define ICEBERG_LOG_TRACE(...) \
    ICEBERG_INTERNAL_LOG_CURRENT(::iceberg::LogLevel::kTrace, __VA_ARGS__)
#else
#  define ICEBERG_LOG_TRACE(...) ((void)0)
#endif

#if ICEBERG_LOG_ACTIVE_LEVEL <= ICEBERG_LOG_LEVEL_DEBUG
#  define ICEBERG_LOG_DEBUG(...) \
    ICEBERG_INTERNAL_LOG_CURRENT(::iceberg::LogLevel::kDebug, __VA_ARGS__)
#else
#  define ICEBERG_LOG_DEBUG(...) ((void)0)
#endif

#if ICEBERG_LOG_ACTIVE_LEVEL <= ICEBERG_LOG_LEVEL_INFO
#  define ICEBERG_LOG_INFO(...) \
    ICEBERG_INTERNAL_LOG_CURRENT(::iceberg::LogLevel::kInfo, __VA_ARGS__)
#else
#  define ICEBERG_LOG_INFO(...) ((void)0)
#endif

#if ICEBERG_LOG_ACTIVE_LEVEL <= ICEBERG_LOG_LEVEL_WARN
#  define ICEBERG_LOG_WARN(...) \
    ICEBERG_INTERNAL_LOG_CURRENT(::iceberg::LogLevel::kWarn, __VA_ARGS__)
#else
#  define ICEBERG_LOG_WARN(...) ((void)0)
#endif

#if ICEBERG_LOG_ACTIVE_LEVEL <= ICEBERG_LOG_LEVEL_ERROR
#  define ICEBERG_LOG_ERROR(...) \
    ICEBERG_INTERNAL_LOG_CURRENT(::iceberg::LogLevel::kError, __VA_ARGS__)
#else
#  define ICEBERG_LOG_ERROR(...) ((void)0)
#endif

#if ICEBERG_LOG_ACTIVE_LEVEL <= ICEBERG_LOG_LEVEL_CRITICAL
#  define ICEBERG_LOG_CRITICAL(...) \
    ICEBERG_INTERNAL_LOG_CURRENT(::iceberg::LogLevel::kCritical, __VA_ARGS__)
#else
#  define ICEBERG_LOG_CRITICAL(...) ((void)0)
#endif

#define ICEBERG_LOG_FATAL(...) ICEBERG_INTERNAL_LOG_FATAL(__VA_ARGS__)

#define ICEBERG_LOG(...) ICEBERG_INTERNAL_LOG_RUNTIME(__VA_ARGS__)

#define ICEBERG_LOG_TO(...) ICEBERG_INTERNAL_LOG_TO(__VA_ARGS__)

#define ICEBERG_LOG_RUNTIME_FMT(...) ICEBERG_INTERNAL_LOG_RUNTIME_FMT(__VA_ARGS__)
