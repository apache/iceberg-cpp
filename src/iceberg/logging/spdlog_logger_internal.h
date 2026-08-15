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

/// \file iceberg/logging/spdlog_logger_internal.h
/// \brief Internal spdlog-backed logging sink.

#include "iceberg/logging/config.h"

#ifdef ICEBERG_HAS_SPDLOG

#  include <atomic>

#  include <spdlog/logger.h>

#  include "iceberg/logging/logger.h"

namespace iceberg::internal {

class ICEBERG_EXPORT SpdLogger : public Logger {
 public:
  explicit SpdLogger(LogLevel level = LogLevel::kInfo);

  /// \brief Construct with a synchronous logger using thread-safe sinks.
  explicit SpdLogger(spdlog::logger logger, LogLevel level = LogLevel::kInfo);

  Status Initialize(
      const std::unordered_map<std::string, std::string>& properties) override;

  bool ShouldLog(LogLevel level) const noexcept override {
    return level >= level_.load(std::memory_order_relaxed);
  }
  void Log(LogMessage&& message) noexcept override;
  void SetLevel(LogLevel level) noexcept override {
    level_.store(level, std::memory_order_relaxed);
  }
  LogLevel level() const noexcept override {
    return level_.load(std::memory_order_relaxed);
  }
  void Flush() noexcept override;

 private:
  spdlog::logger logger_;
  std::atomic<LogLevel> level_;
};

}  // namespace iceberg::internal

#endif  // ICEBERG_HAS_SPDLOG
