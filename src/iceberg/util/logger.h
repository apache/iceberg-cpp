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

#include <format>
#include <iostream>
#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <type_traits>

#include <iceberg/iceberg_export.h>

namespace iceberg {

/// \brief Log levels for the iceberg logger system
enum class LogLevel : int {
  kTrace = 0,
  kDebug = 1,
  kInfo = 2,
  kWarn = 3,
  kError = 4,
  kCritical = 5,
  kOff = 6
};

/// \brief Convert log level to string representation
ICEBERG_EXPORT constexpr std::string_view LogLevelToString(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace:
      return "TRACE";
    case LogLevel::kDebug:
      return "DEBUG";
    case LogLevel::kInfo:
      return "INFO";
    case LogLevel::kWarn:
      return "WARN";
    case LogLevel::kError:
      return "ERROR";
    case LogLevel::kCritical:
      return "CRITICAL";
    case LogLevel::kOff:
      return "OFF";
    default:
      return "UNKNOWN";
  }
}

/// \brief Logger interface that uses CRTP to avoid virtual function overhead
/// \tparam Derived The concrete logger implementation
template <typename Derived>
class ICEBERG_EXPORT LoggerInterface {
 public:
  /// \brief Check if a log level is enabled
  bool ShouldLog(LogLevel level) const noexcept {
    return derived()->ShouldLogImpl(level);
  }

  /// \brief Log a message with the specified level
  template <typename... Args>
  void Log(LogLevel level, const std::source_location& location,
           std::string_view format_str, Args&&... args) const {
    derived()->LogImpl(level, location, format_str, std::forward<Args>(args)...);
  }

  /// \brief Set the minimum log level
  void SetLevel(LogLevel level) { derived()->SetLevelImpl(level); }

  /// \brief Get the current minimum log level
  LogLevel GetLevel() const noexcept { return derived()->GetLevelImpl(); }

 protected:
  LoggerInterface() = default;
  ~LoggerInterface() = default;

 private:
  /// \brief Get const pointer to the derived class instance
  const Derived* derived() const noexcept { return static_cast<const Derived*>(this); }

  /// \brief Get non-const pointer to the derived class instance
  Derived* derived() noexcept { return static_cast<Derived*>(this); }
};

/// \brief Concept to constrain types that implement the Logger interface
template <typename T>
concept Logger = requires(const T& t, T& nt, LogLevel level,
                          const std::source_location& location,
                          std::string_view format_str) {
  { t.ShouldLogImpl(level) } -> std::convertible_to<bool>;
  { t.LogImpl(level, location, format_str) } -> std::same_as<void>;
  { nt.SetLevelImpl(level) } -> std::same_as<void>;
  { t.GetLevelImpl() } -> std::convertible_to<LogLevel>;
} && std::is_base_of_v<LoggerInterface<T>, T>;

/// \brief Global logger registry for managing logger instances
class ICEBERG_EXPORT LoggerRegistry {
 public:
  /// \brief Get the singleton instance of the logger registry
  static LoggerRegistry& Instance();

  /// \brief Set the default logger implementation
  ///
  /// \tparam LoggerImpl The logger implementation type
  /// \param logger The logger instance to set as default
  template <Logger LoggerImpl>
  void SetDefaultLogger(std::shared_ptr<LoggerImpl> logger) {
    default_logger_ = std::static_pointer_cast<void>(logger);
    log_func_ = [](const void* logger_ptr, LogLevel level,
                   const std::source_location& location, std::string_view format_str,
                   std::format_args args) {
      auto* typed_logger = static_cast<const LoggerImpl*>(logger_ptr);
      std::string formatted_message = std::vformat(format_str, args);
      typed_logger->Log(level, location, "{}", formatted_message);
    };
    should_log_func_ = [](const void* logger_ptr, LogLevel level) -> bool {
      auto* typed_logger = static_cast<const LoggerImpl*>(logger_ptr);
      return typed_logger->ShouldLog(level);
    };
  }

  /// \brief Get the default logger
  ///
  /// \tparam LoggerImpl The expected logger implementation type
  /// \return Shared pointer to the logger, or nullptr if type doesn't match
  template <Logger LoggerImpl>
  std::shared_ptr<LoggerImpl> GetDefaultLogger() const {
    return std::static_pointer_cast<LoggerImpl>(default_logger_);
  }

  /// \brief Log using the default logger
  template <typename... Args>
  void Log(LogLevel level, const std::source_location& location,
           std::string_view format_str, Args&&... args) const {
    if (default_logger_ && should_log_func_ && log_func_) {
      if (should_log_func_(default_logger_.get(), level)) {
        try {
          if constexpr (sizeof...(args) > 0) {
            auto args_store = std::make_format_args(args...);
            log_func_(default_logger_.get(), level, location, format_str, args_store);
          } else {
            log_func_(default_logger_.get(), level, location, format_str,
                      std::format_args{});
          }
        } catch (const std::exception& e) {
          std::cerr << "Logging error: " << e.what() << std::endl;
        }
      }
    }
  }

  /// \brief Initialize with default spdlog logger
  void InitializeDefault(std::string_view logger_name = "iceberg");

 private:
  LoggerRegistry() = default;

  std::shared_ptr<void> default_logger_;
  void (*log_func_)(const void*, LogLevel, const std::source_location&, std::string_view,
                    std::format_args) = nullptr;
  bool (*should_log_func_)(const void*, LogLevel) = nullptr;
};

/// \brief Convenience macros for logging with automatic source location
#define ICEBERG_LOG_WITH_LOCATION(level, format_str, ...)                             \
  do {                                                                                \
    ::iceberg::LoggerRegistry::Instance().Log(level, std::source_location::current(), \
                                              format_str __VA_OPT__(, ) __VA_ARGS__); \
  } while (false)

#define ICEBERG_LOG_TRACE(format_str, ...)               \
  ICEBERG_LOG_WITH_LOCATION(::iceberg::LogLevel::kTrace, \
                            format_str __VA_OPT__(, ) __VA_ARGS__)

#define ICEBERG_LOG_DEBUG(format_str, ...)               \
  ICEBERG_LOG_WITH_LOCATION(::iceberg::LogLevel::kDebug, \
                            format_str __VA_OPT__(, ) __VA_ARGS__)

#define ICEBERG_LOG_INFO(format_str, ...)               \
  ICEBERG_LOG_WITH_LOCATION(::iceberg::LogLevel::kInfo, \
                            format_str __VA_OPT__(, ) __VA_ARGS__)

#define ICEBERG_LOG_WARN(format_str, ...)               \
  ICEBERG_LOG_WITH_LOCATION(::iceberg::LogLevel::kWarn, \
                            format_str __VA_OPT__(, ) __VA_ARGS__)

#define ICEBERG_LOG_ERROR(format_str, ...)               \
  ICEBERG_LOG_WITH_LOCATION(::iceberg::LogLevel::kError, \
                            format_str __VA_OPT__(, ) __VA_ARGS__)

#define ICEBERG_LOG_CRITICAL(format_str, ...)               \
  ICEBERG_LOG_WITH_LOCATION(::iceberg::LogLevel::kCritical, \
                            format_str __VA_OPT__(, ) __VA_ARGS__)
}  // namespace iceberg
