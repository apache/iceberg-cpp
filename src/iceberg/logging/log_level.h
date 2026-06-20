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

/// \file iceberg/logging/log_level.h
/// \brief Severity levels for the logging system.

#include <string_view>
#include <utility>

#include "iceberg/result.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

/// \brief Logging severity level, ordered from most to least verbose.
///
/// Levels are ordered so that `level >= threshold` is the enabled test.
/// `kOff` is the maximum sentinel: as a threshold it disables all emission
/// (it is never the level of an actual message).
enum class LogLevel {
  kTrace,
  kDebug,
  kInfo,
  kWarn,
  kError,
  kCritical,
  kFatal,
  kOff,
};

/// \brief String representation of a LogLevel.
constexpr std::string_view ToString(LogLevel level) noexcept {
  switch (level) {
    case LogLevel::kTrace:
      return "trace";
    case LogLevel::kDebug:
      return "debug";
    case LogLevel::kInfo:
      return "info";
    case LogLevel::kWarn:
      return "warn";
    case LogLevel::kError:
      return "error";
    case LogLevel::kCritical:
      return "critical";
    case LogLevel::kFatal:
      return "fatal";
    case LogLevel::kOff:
      return "off";
  }
  std::unreachable();
}

/// \brief Parse a LogLevel from a string (case-insensitive).
///
/// \param s The string to parse ("trace", "debug", "info", "warn", "error",
///   "critical", "fatal", or "off").
/// \return The LogLevel, or an InvalidArgument error if unrecognized.
inline Result<LogLevel> LogLevelFromString(std::string_view s) {
  auto level = StringUtils::ToLower(s);
  if (level == "trace") return LogLevel::kTrace;
  if (level == "debug") return LogLevel::kDebug;
  if (level == "info") return LogLevel::kInfo;
  if (level == "warn") return LogLevel::kWarn;
  if (level == "error") return LogLevel::kError;
  if (level == "critical") return LogLevel::kCritical;
  if (level == "fatal") return LogLevel::kFatal;
  if (level == "off") return LogLevel::kOff;
  return InvalidArgument("Invalid log level: {}", s);
}

}  // namespace iceberg
