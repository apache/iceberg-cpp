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

#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "iceberg/logging/logger.h"

namespace iceberg {

/// \brief Test sink that records every emitted LogMessage under a mutex.
class CapturingLogger : public Logger {
 public:
  bool ShouldLog(LogLevel level) const override { return level >= level_; }

  void Log(LogMessage&& message) noexcept override {
    std::lock_guard<std::mutex> lock(mutex_);
    records_.push_back(std::move(message));
  }

  void SetLevel(LogLevel level) override { level_ = level; }
  LogLevel level() const override { return level_; }

  std::vector<LogMessage> records() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return records_;
  }

  std::size_t count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return records_.size();
  }

 private:
  mutable std::mutex mutex_;
  LogLevel level_ = LogLevel::kTrace;
  std::vector<LogMessage> records_;
};

/// \brief RAII guard that restores the process default logger on scope exit, so
/// tests that swap the global default don't leak state into other tests.
class ScopedDefaultLogger {
 public:
  explicit ScopedDefaultLogger(std::shared_ptr<Logger> logger)
      : previous_(GetDefaultLogger()) {
    SetDefaultLogger(std::move(logger));
  }
  ~ScopedDefaultLogger() { SetDefaultLogger(previous_); }

  ScopedDefaultLogger(const ScopedDefaultLogger&) = delete;
  ScopedDefaultLogger& operator=(const ScopedDefaultLogger&) = delete;

 private:
  std::shared_ptr<Logger> previous_;
};

}  // namespace iceberg
