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

#include "iceberg/util/logger.h"

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "iceberg/util/spdlog_logger.h"

namespace iceberg {

namespace {

/// \brief Convert iceberg LogLevel to spdlog level
spdlog::level::level_enum ToSpdlogLevel(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace:
      return spdlog::level::trace;
    case LogLevel::kDebug:
      return spdlog::level::debug;
    case LogLevel::kInfo:
      return spdlog::level::info;
    case LogLevel::kWarn:
      return spdlog::level::warn;
    case LogLevel::kError:
      return spdlog::level::err;
    case LogLevel::kCritical:
      return spdlog::level::critical;
    case LogLevel::kOff:
      return spdlog::level::off;
    default:
      return spdlog::level::info;
  }
}

/**
 * \brief Convert spdlog level to iceberg LogLevel
 */
LogLevel FromSpdlogLevel(spdlog::level::level_enum level) {
  switch (level) {
    case spdlog::level::trace:
      return LogLevel::kTrace;
    case spdlog::level::debug:
      return LogLevel::kDebug;
    case spdlog::level::info:
      return LogLevel::kInfo;
    case spdlog::level::warn:
      return LogLevel::kWarn;
    case spdlog::level::err:
      return LogLevel::kError;
    case spdlog::level::critical:
      return LogLevel::kCritical;
    case spdlog::level::off:
      return LogLevel::kOff;
    default:
      return LogLevel::kInfo;
  }
}

}  // namespace

// SpdlogLogger implementation
SpdlogLogger::SpdlogLogger(std::string_view logger_name) {
  auto spdlog_logger = spdlog::get(std::string(logger_name));
  if (!spdlog_logger) {
    spdlog_logger = spdlog::stdout_color_mt(std::string(logger_name));
  }
  logger_ = std::static_pointer_cast<void>(spdlog_logger);
  current_level_ = FromSpdlogLevel(spdlog_logger->level());
}

SpdlogLogger::SpdlogLogger(std::shared_ptr<void> spdlog_logger)
    : logger_(std::move(spdlog_logger)) {
  auto typed_logger = std::static_pointer_cast<spdlog::logger>(logger_);
  current_level_ = FromSpdlogLevel(typed_logger->level());
}

bool SpdlogLogger::ShouldLogImpl(LogLevel level) const noexcept {
  return level >= current_level_;
}

void SpdlogLogger::LogRawImpl(LogLevel level, const std::source_location& location,
                              const std::string& message) const {
  auto typed_logger = std::static_pointer_cast<spdlog::logger>(logger_);
  auto spdlog_level = ToSpdlogLevel(level);

  // Add source location information
  std::string full_message =
      std::format("[{}:{}:{}] {}", location.file_name(), location.line(),
                  location.function_name(), message);

  typed_logger->log(spdlog_level, full_message);
}

void SpdlogLogger::SetLevelImpl(LogLevel level) {
  current_level_ = level;
  auto typed_logger = std::static_pointer_cast<spdlog::logger>(logger_);
  typed_logger->set_level(ToSpdlogLevel(level));
}

LogLevel SpdlogLogger::GetLevelImpl() const noexcept { return current_level_; }

// LoggerRegistry implementation
LoggerRegistry& LoggerRegistry::Instance() {
  static LoggerRegistry instance;
  return instance;
}

void LoggerRegistry::InitializeDefault(std::string_view logger_name) {
  auto spdlog_logger = std::make_shared<SpdlogLogger>(logger_name);
  SetDefaultLogger(spdlog_logger);
}

}  // namespace iceberg
