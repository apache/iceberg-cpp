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
#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <type_traits>

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
constexpr std::string_view LogLevelToString(LogLevel level) {
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
class LoggerInterface {
 public:
  /// \brief Check if a log level is enabled
  bool ShouldLog(LogLevel level) const noexcept {
    return static_cast<const Derived*>(this)->ShouldLogImpl(level);
  }

  /// \brief Log a message with the specified level
  template <typename... Args>
  void Log(LogLevel level, std::string_view format_str, Args&&... args) const {
    if (ShouldLog(level)) {
      static_cast<const Derived*>(this)->LogImpl(level, format_str,
                                                 std::forward<Args>(args)...);
    }
  }

  /// \brief Log a message with source location information
  template <typename... Args>
  void LogWithLocation(LogLevel level, std::string_view format_str,
                       const std::source_location& location, Args&&... args) const {
    if (ShouldLog(level)) {
      static_cast<const Derived*>(this)->LogWithLocationImpl(level, format_str, location,
                                                             std::forward<Args>(args)...);
    }
  }

  /// \brief Set the minimum log level
  void SetLevel(LogLevel level) { static_cast<Derived*>(this)->SetLevelImpl(level); }

  /// \brief Get the current minimum log level
  LogLevel GetLevel() const noexcept {
    return static_cast<const Derived*>(this)->GetLevelImpl();
  }

  // Convenience methods for different log levels
  template <typename... Args>
  void Trace(std::string_view format_str, Args&&... args) const {
    Log(LogLevel::kTrace, format_str, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void Debug(std::string_view format_str, Args&&... args) const {
    Log(LogLevel::kDebug, format_str, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void Info(std::string_view format_str, Args&&... args) const {
    Log(LogLevel::kInfo, format_str, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void Warn(std::string_view format_str, Args&&... args) const {
    Log(LogLevel::kWarn, format_str, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void Error(std::string_view format_str, Args&&... args) const {
    Log(LogLevel::kError, format_str, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void Critical(std::string_view format_str, Args&&... args) const {
    Log(LogLevel::kCritical, format_str, std::forward<Args>(args)...);
  }

 protected:
  LoggerInterface() = default;
  ~LoggerInterface() = default;
};

/// \brief Concept to constrain types that implement the Logger interface
template <typename T>
concept Logger = std::is_base_of_v<LoggerInterface<T>, T>;

/// \brief Default spdlog-based logger implementation
class SpdlogLogger : public LoggerInterface<SpdlogLogger> {
 public:
  /// \brief Create a new spdlog logger with the given name
  explicit SpdlogLogger(std::string_view logger_name = "iceberg");

  /// \brief Create a spdlog logger from an existing spdlog::logger
  explicit SpdlogLogger(std::shared_ptr<void> spdlog_logger);

  // Implementation methods required by LoggerInterface
  bool ShouldLogImpl(LogLevel level) const noexcept;

  template <typename... Args>
  void LogImpl(LogLevel level, std::string_view format_str, Args&&... args) const {
    if constexpr (sizeof...(args) > 0) {
      std::string formatted_message =
          std::vformat(format_str, std::make_format_args(args...));
      LogRawImpl(level, formatted_message);
    } else {
      LogRawImpl(level, std::string(format_str));
    }
  }

  template <typename... Args>
  void LogWithLocationImpl(LogLevel level, std::string_view format_str,
                           const std::source_location& location, Args&&... args) const {
    std::string message;
    if constexpr (sizeof...(args) > 0) {
      message = std::vformat(format_str, std::make_format_args(args...));
    } else {
      message = std::string(format_str);
    }
    LogWithLocationRawImpl(level, message, location);
  }

  void SetLevelImpl(LogLevel level);
  LogLevel GetLevelImpl() const noexcept;

  /// \brief Get the underlying spdlog logger (for advanced usage)
  std::shared_ptr<void> GetUnderlyingLogger() const { return logger_; }

 private:
  void LogRawImpl(LogLevel level, const std::string& message) const;
  void LogWithLocationRawImpl(LogLevel level, const std::string& message,
                              const std::source_location& location) const;

  std::shared_ptr<void> logger_;  // Type-erased spdlog::logger
  LogLevel current_level_ = LogLevel::kInfo;
};

/// \brief No-op logger implementation for performance-critical scenarios
class NullLogger : public LoggerInterface<NullLogger> {
 public:
  bool ShouldLogImpl(LogLevel) const noexcept { return false; }

  template <typename... Args>
  void LogImpl(LogLevel, std::string_view, Args&&...) const noexcept {}

  template <typename... Args>
  void LogWithLocationImpl(LogLevel, std::string_view, const std::source_location&,
                           Args&&...) const noexcept {}

  void SetLevelImpl(LogLevel) noexcept {}
  LogLevel GetLevelImpl() const noexcept { return LogLevel::kOff; }
};

/// \brief Global logger registry for managing logger instances
class LoggerRegistry {
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
    log_func_ = [](const void* logger_ptr, LogLevel level, const std::string& message) {
      auto* typed_logger = static_cast<const LoggerImpl*>(logger_ptr);
      if (typed_logger->ShouldLog(level)) {
        typed_logger->LogImpl(level, "{}", message);
      }
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
  void Log(LogLevel level, std::string_view format_str, Args&&... args) const {
    if (default_logger_ && log_func_) {
      std::string formatted_message;
      if constexpr (sizeof...(args) > 0) {
        formatted_message = std::vformat(format_str, std::make_format_args(args...));
      } else {
        formatted_message = std::string(format_str);
      }
      log_func_(default_logger_.get(), level, formatted_message);
    }
  }

  /// \brief Initialize with default spdlog logger
  void InitializeDefault(std::string_view logger_name = "iceberg");

 private:
  LoggerRegistry() = default;

  std::shared_ptr<void> default_logger_;
  void (*log_func_)(const void*, LogLevel, const std::string&) = nullptr;
};

/// \brief Convenience macros for logging with automatic source location
#define LOG_WITH_LOCATION(level, format_str, ...)                                     \
  do {                                                                                \
    ::iceberg::LoggerRegistry::Instance().Log(level,                                  \
                                              format_str __VA_OPT__(, ) __VA_ARGS__); \
  } while (0)

#define LOG_TRACE(format_str, ...) \
  LOG_WITH_LOCATION(::iceberg::LogLevel::kTrace, format_str __VA_OPT__(, ) __VA_ARGS__)

#define LOG_DEBUG(format_str, ...) \
  LOG_WITH_LOCATION(::iceberg::LogLevel::kDebug, format_str __VA_OPT__(, ) __VA_ARGS__)

#define LOG_INFO(format_str, ...) \
  LOG_WITH_LOCATION(::iceberg::LogLevel::kInfo, format_str __VA_OPT__(, ) __VA_ARGS__)

#define LOG_WARN(format_str, ...) \
  LOG_WITH_LOCATION(::iceberg::LogLevel::kWarn, format_str __VA_OPT__(, ) __VA_ARGS__)

#define LOG_ERROR(format_str, ...) \
  LOG_WITH_LOCATION(::iceberg::LogLevel::kError, format_str __VA_OPT__(, ) __VA_ARGS__)

#define LOG_CRITICAL(format_str, ...) \
  LOG_WITH_LOCATION(::iceberg::LogLevel::kCritical, format_str __VA_OPT__(, ) __VA_ARGS__)

}  // namespace iceberg
