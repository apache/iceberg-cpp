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

/// \file iceberg/logging/internal/spdlog_logger.h
/// \brief spdlog-backed logging sink.
///
/// INTERNAL, NOT INSTALLED. It is only included from .cc files (logger.cc and
/// spdlog_logger.cc) after config.h, and only when the project is built with
/// ICEBERG_SPDLOG=ON. SpdLogger is not a consumer-constructible public type --
/// applications obtain it via the default logger or the "logger-impl"="spdlog"
/// registry factory.

#include "iceberg/logging/config.h"

#ifdef ICEBERG_HAS_SPDLOG

#  include <atomic>
#  include <memory>

#  include <spdlog/logger.h>

#  include "iceberg/logging/log_level.h"
#  include "iceberg/logging/logger.h"

namespace iceberg::internal {

/// \brief Logger backed by spdlog (synchronous only in v1).
///
/// Synchronous because spdlog::source_loc holds non-owning const char* that are
/// unsafe to forward into an async logger (spdlog #3227).
class SpdLogger : public Logger {
 public:
  /// \brief Construct over a default stderr-backed spdlog logger.
  explicit SpdLogger(LogLevel level = LogLevel::kInfo);

  /// \brief Construct over a caller-provided spdlog logger.
  explicit SpdLogger(std::shared_ptr<spdlog::logger> logger,
                     LogLevel level = LogLevel::kInfo);

  bool ShouldLog(LogLevel level) const override {
    return level >= level_.load(std::memory_order_relaxed);
  }
  void Log(LogMessage&& message) noexcept override;
  void SetLevel(LogLevel level) override {
    level_.store(level, std::memory_order_relaxed);
  }
  LogLevel level() const override { return level_.load(std::memory_order_relaxed); }
  void Flush() noexcept override;

 private:
  std::shared_ptr<spdlog::logger> logger_;
  std::atomic<LogLevel> level_;
};

}  // namespace iceberg::internal

#endif  // ICEBERG_HAS_SPDLOG
