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

#include "iceberg/util/logger.h"

namespace iceberg {

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
  void LogImpl(LogLevel level, const std::source_location& location,
               std::string_view format_str, Args&&... args) const {
    if constexpr (sizeof...(args) > 0) {
      std::string formatted_message =
          std::vformat(format_str, std::make_format_args(args...));
      LogRawImpl(level, location, formatted_message);
    } else {
      LogRawImpl(level, location, std::string(format_str));
    }
  }

  void SetLevelImpl(LogLevel level);
  LogLevel GetLevelImpl() const noexcept;

  /// \brief Get the underlying spdlog logger (for advanced usage)
  std::shared_ptr<void> GetUnderlyingLogger() const { return logger_; }

 private:
  void LogRawImpl(LogLevel level, const std::source_location& location,
                  const std::string& message) const;
  std::shared_ptr<void> logger_;  // Type-erased spdlog::logger
  LogLevel current_level_ = LogLevel::kInfo;
};

}  // namespace iceberg
