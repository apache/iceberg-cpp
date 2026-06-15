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
      // spdlog has no "fatal"; the process abort is owned by the macro layer.
      return spdlog::level::critical;
    case LogLevel::kOff:
      return spdlog::level::off;
  }
  return spdlog::level::off;
}

/// \brief The built-in sink: a color stderr spdlog logger.
std::shared_ptr<spdlog::logger> MakeDefaultSpdLogger() {
  return std::make_shared<spdlog::logger>(
      "iceberg", std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
}

}  // namespace

SpdLogger::SpdLogger(LogLevel level) : SpdLogger(MakeDefaultSpdLogger(), level) {}

Status SpdLogger::Initialize(
    const std::unordered_map<std::string, std::string>& properties) {
  if (auto it = properties.find(std::string(kPatternProperty)); it != properties.end()) {
    logger_->set_pattern(it->second);
  }
  // Apply "level" via the base implementation.
  return Logger::Initialize(properties);
}

SpdLogger::SpdLogger(std::shared_ptr<spdlog::logger> logger, LogLevel level)
    : logger_(std::move(logger)), level_(level) {
  // logger_ is non-null for the rest of this object's life, so Initialize/Log/Flush
  // may dereference it unconditionally. Enforced by substitution rather than an
  // assertion, which would vanish under NDEBUG and leave a release-build crash: a
  // null argument falls back to the same stderr-backed logger the default
  // constructor builds, so a caller mistake degrades to the default sink.
  if (!logger_) {
    logger_ = MakeDefaultSpdLogger();
  }
  logger_->set_level(spdlog::level::trace);  // filtering is done by ShouldLog
}

void SpdLogger::Log(LogMessage&& message) noexcept {
  try {
    spdlog::source_loc loc{message.location.file_name(),
                           static_cast<int>(message.location.line()),
                           message.location.function_name()};
    // Raw-message overload: the text is already formatted, so hand spdlog the bytes
    // directly instead of running them back through fmt (which would re-parse and
    // copy the whole message, allocating for long ones). It also means braces in the
    // message can never be interpreted as format placeholders.
    logger_->log(loc, ToSpdLevel(message.level),
                 spdlog::string_view_t{message.message.data(), message.message.size()});
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
