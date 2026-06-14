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

#include "iceberg/logging/internal/spdlog_logger.h"

#ifdef ICEBERG_HAS_SPDLOG

#  include <memory>
#  include <utility>

#  include <spdlog/common.h>
#  include <spdlog/sinks/stdout_sinks.h>

namespace iceberg::internal {

namespace {

spdlog::level::level_enum ToSpdLevel(LogLevel level) noexcept {
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
    case LogLevel::kFatal:
      // spdlog has no "fatal"; the process abort is owned by the macro layer.
      return spdlog::level::critical;
    case LogLevel::kOff:
      return spdlog::level::off;
  }
  return spdlog::level::off;
}

}  // namespace

SpdLogger::SpdLogger(LogLevel level)
    : SpdLogger(std::make_shared<spdlog::logger>(
                    "iceberg", std::make_shared<spdlog::sinks::stderr_sink_mt>()),
                level) {}

SpdLogger::SpdLogger(std::shared_ptr<spdlog::logger> logger, LogLevel level)
    : logger_(std::move(logger)), level_(level) {
  if (logger_) {
    logger_->set_level(spdlog::level::trace);  // filtering is done by ShouldLog
  }
}

void SpdLogger::Log(LogMessage&& message) noexcept {
  try {
    spdlog::source_loc loc{message.location.file_name(),
                           static_cast<int>(message.location.line()),
                           message.location.function_name()};
    // Pass the pre-formatted text as an argument ("{}") so any braces in the
    // message are not re-interpreted as a format string.
    logger_->log(loc, ToSpdLevel(message.level), "{}", message.message);
  } catch (...) {
    // Logging must never throw.
  }
}

void SpdLogger::Flush() noexcept {
  try {
    logger_->flush();
  } catch (...) {
  }
}

}  // namespace iceberg::internal

#endif  // ICEBERG_HAS_SPDLOG
