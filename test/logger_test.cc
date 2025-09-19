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

#include <iostream>

#include <gtest/gtest.h>

#include "iceberg/util/spdlog_logger.h"

namespace iceberg {

/// \brief Example custom logger implementation using std::cout for testing
///
/// This shows how downstream projects can implement their own logger
/// by inheriting from LoggerInterface and implementing the required methods.
class StdoutLogger : public LoggerInterface<StdoutLogger> {
 public:
  explicit StdoutLogger(LogLevel min_level = LogLevel::kInfo) : min_level_(min_level) {}

  // Required implementation methods
  bool ShouldLogImpl(LogLevel level) const noexcept { return level >= min_level_; }

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

  void SetLevelImpl(LogLevel level) noexcept { min_level_ = level; }

  LogLevel GetLevelImpl() const noexcept { return min_level_; }

 private:
  void LogRawImpl(LogLevel level, const std::source_location& location,
                  const std::string& message) const {
    std::cout << "[" << LogLevelToString(level) << "] " << message << std::endl;
  }

  LogLevel min_level_;
};

class LoggerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Each test starts with a fresh logger registry
  }

  void TearDown() override {
    // Reset to default state
    LoggerRegistry::Instance().InitializeDefault("test_logger");
  }
};

TEST_F(LoggerTest, DefaultSpdlogLogger) {
  // Initialize with default spdlog logger
  LoggerRegistry::Instance().InitializeDefault("test_logger");

  // Test basic logging functionality
  ICEBERG_LOG_INFO("This is an info message");
  ICEBERG_LOG_DEBUG("This is a debug message with value: {}", 42);
  ICEBERG_LOG_WARN("This is a warning message");
  ICEBERG_LOG_ERROR("This is an error message");

  // The test passes if no exceptions are thrown
  SUCCEED();
}

TEST_F(LoggerTest, CustomStdoutLogger) {
  // Create and register a custom logger
  auto custom_logger = std::make_shared<StdoutLogger>(LogLevel::kDebug);
  LoggerRegistry::Instance().SetDefaultLogger(custom_logger);

  // Test logging with custom logger
  ICEBERG_LOG_DEBUG("Debug message from custom logger");
  ICEBERG_LOG_INFO("Info message with parameter: {}", "test");
  ICEBERG_LOG_WARN("Warning from custom logger");

  SUCCEED();
}

TEST_F(LoggerTest, LoggerLevels) {
  auto logger = std::make_shared<SpdlogLogger>("test_level_logger");

  // Test level filtering
  logger->SetLevel(LogLevel::kWarn);
  EXPECT_EQ(logger->GetLevel(), LogLevel::kWarn);

  // These should be filtered out
  EXPECT_FALSE(logger->ShouldLog(LogLevel::kTrace));
  EXPECT_FALSE(logger->ShouldLog(LogLevel::kDebug));
  EXPECT_FALSE(logger->ShouldLog(LogLevel::kInfo));

  // These should pass through
  EXPECT_TRUE(logger->ShouldLog(LogLevel::kWarn));
  EXPECT_TRUE(logger->ShouldLog(LogLevel::kError));
  EXPECT_TRUE(logger->ShouldLog(LogLevel::kCritical));
}
}  // namespace iceberg
