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

/// \file iceberg/logging/cerr_logger.h
/// \brief Always-available std::cerr logging backend.

#include <atomic>
#include <mutex>

#include "iceberg/iceberg_export.h"
#include "iceberg/logging/log_level.h"
#include "iceberg/logging/logger.h"

namespace iceberg {

/// \brief Logger that writes one line per record to std::cerr.
///
/// Line layout: `YYYY-MM-DDThh:mm:ss.mmmZ LEVEL [tid] file:line] message`.
/// The minimum level is held in a lock-free atomic; a mutex serializes the
/// whole-line write so concurrent records never interleave. Pure standard
/// library -- always compiled, regardless of ICEBERG_SPDLOG.
class ICEBERG_EXPORT CerrLogger : public Logger {
 public:
  explicit CerrLogger(LogLevel level = LogLevel::kInfo) : level_(level) {}

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
  std::atomic<LogLevel> level_;
  std::mutex mutex_;
};

}  // namespace iceberg
