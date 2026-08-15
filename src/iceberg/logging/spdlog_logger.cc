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

#include "iceberg/logging/spdlog_logger_internal.h"

#ifdef ICEBERG_HAS_SPDLOG

#  include <memory>
#  include <string>
#  include <unordered_map>
#  include <utility>

#  include <spdlog/common.h>
#  include <spdlog/sinks/stdout_color_sinks.h>

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
      // spdlog has no fatal level; the macro layer handles termination.
      return spdlog::level::critical;
    case LogLevel::kOff:
      return spdlog::level::off;
  }
  return spdlog::level::off;
}

}  // namespace

SpdLogger::SpdLogger(LogLevel level)
    : SpdLogger(spdlog::logger("iceberg",
                               std::make_shared<spdlog::sinks::stderr_color_sink_mt>()),
                level) {}

SpdLogger::SpdLogger(spdlog::logger logger, LogLevel level)
    : logger_(std::move(logger)), level_(level) {
  logger_.set_level(spdlog::level::trace);
}

Status SpdLogger::Initialize(
    const std::unordered_map<std::string, std::string>& properties) {
  if (auto it = properties.find(std::string(kPatternProperty)); it != properties.end()) {
    logger_.set_pattern(it->second);
  }
  return Logger::Initialize(properties);
}

void SpdLogger::Log(LogMessage&& message) noexcept {
  try {
    spdlog::source_loc loc{message.location.file_name(),
                           static_cast<int>(message.location.line()),
                           message.location.function_name()};
    // LogMessage is already formatted; use spdlog's raw-message overload.
    logger_.log(loc, ToSpdLevel(message.level),
                spdlog::string_view_t{message.message.data(), message.message.size()});
  } catch (...) {
    // Logging must never throw.
  }
}

void SpdLogger::Flush() noexcept {
  try {
    logger_.flush();
  } catch (...) {
  }
}

}  // namespace iceberg::internal

#endif  // ICEBERG_HAS_SPDLOG
