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

#include "iceberg/logging/log_macros.h"

#include <format>
#include <iostream>
#include <memory>
#include <stdexcept>

#include <gtest/gtest.h>

#include "iceberg/test/logging_test_helpers.h"

template <>
struct std::formatter<iceberg::LogAttribute, char> {
  constexpr auto parse(std::format_parse_context& context) { return context.begin(); }

  auto format(const iceberg::LogAttribute&, std::format_context& context) const
      -> std::format_context::iterator {
    throw 7;
    return context.out();
  }
};

namespace iceberg {
namespace {

class DeathLogger : public Logger {
 public:
  explicit DeathLogger(std::string name) : name_(std::move(name)) {}

  bool ShouldLog(LogLevel level) const noexcept override { return level >= level_; }

  void Log(LogMessage&& message) noexcept override {
    std::cerr << "log:" << name_ << ":" << message.message << "\n";
  }

  void SetLevel(LogLevel level) noexcept override { level_ = level; }

  LogLevel level() const noexcept override { return level_; }

  void Flush() noexcept override { std::cerr << "flush:" << name_ << "\n"; }

 private:
  std::string name_;
  LogLevel level_ = LogLevel::kTrace;
};

LogLevel NextLevelEvaluationCount(LogLevel* level, int* evaluations) {
  ++(*evaluations);
  return *level;
}

}  // namespace

TEST(LogMacrosTest, FixedLevelMacroLogsThroughCurrentLogger) {
  auto logger = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(logger);

  const auto here = std::source_location::current();
  ICEBERG_LOG_INFO("loaded {}", 3);

  auto records = logger->records();
  ASSERT_EQ(records.size(), 1);
  EXPECT_EQ(records[0].level, LogLevel::kInfo);
  EXPECT_EQ(records[0].message, "loaded 3");
  EXPECT_STREQ(records[0].location.file_name(), here.file_name());
  EXPECT_EQ(records[0].location.line(), here.line() + 1);
}

TEST(LogMacrosTest, DisabledRuntimeLevelDoesNotEvaluateFormatArguments) {
  auto logger = std::make_shared<CapturingLogger>();
  logger->SetLevel(LogLevel::kError);
  ScopedDefaultLogger guard(logger);
  int evaluations = 0;

  ICEBERG_LOG(LogLevel::kInfo, "side effect {}", ++evaluations);

  EXPECT_EQ(evaluations, 0);
  EXPECT_EQ(logger->count(), 0);
}

TEST(LogMacrosTest, RuntimeLevelIsEvaluatedOnce) {
  auto logger = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(logger);
  LogLevel level = LogLevel::kWarn;
  int evaluations = 0;

  ICEBERG_LOG(NextLevelEvaluationCount(&level, &evaluations), "warn {}", 1);

  EXPECT_EQ(evaluations, 1);
  ASSERT_EQ(logger->count(), 1);
  EXPECT_EQ(logger->records()[0].level, LogLevel::kWarn);
}

TEST(LogMacrosTest, ExplicitLoggerIsEvaluatedOnce) {
  CapturingLogger logger;
  CapturingLogger* logger_ptr = &logger;
  int evaluations = 0;

  ICEBERG_LOG_TO(*([&] {
    ++evaluations;
    return logger_ptr;
  }()),
                 LogLevel::kError, "error {}", 5);

  EXPECT_EQ(evaluations, 1);
  ASSERT_EQ(logger.count(), 1);
  EXPECT_EQ(logger.records()[0].message, "error 5");
}

TEST(LogMacrosTest, RuntimeFormatUsesVFormat) {
  auto logger = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(logger);
  std::string fmt = "value {}";

  ICEBERG_LOG_RUNTIME_FMT(LogLevel::kInfo, fmt, 42);

  ASSERT_EQ(logger->count(), 1);
  EXPECT_EQ(logger->records()[0].message, "value 42");
}

TEST(LogMacrosTest, OffRuntimeLevelDoesNotEvaluateFormatArgumentsOrEmit) {
  auto logger = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(logger);
  int evaluations = 0;

  ICEBERG_LOG(LogLevel::kOff, "side effect {}", ++evaluations);

  EXPECT_EQ(evaluations, 0);
  EXPECT_EQ(logger->count(), 0);
}

TEST(LogMacrosTest, FormatterFailuresDoNotEscape) {
  auto logger = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(logger);

  ICEBERG_LOG_INFO("bad {}", LogAttribute{.key = "k", .value = "v"});

  ASSERT_EQ(logger->count(), 1);
  EXPECT_EQ(logger->records()[0].message, "<fmt error>");
}

TEST(LogMacrosTest, ScopedLoggerOverridesDefaultLogger) {
  auto global = std::make_shared<CapturingLogger>();
  auto scoped = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(global);

  {
    ScopedLogger bind(scoped);
    ICEBERG_LOG_INFO("scoped");
  }

  EXPECT_EQ(global->count(), 0);
  ASSERT_EQ(scoped->count(), 1);
  EXPECT_EQ(scoped->records()[0].message, "scoped");
}

TEST(LogMacrosTest, FatalUsesScopedLoggerFlushesThenAborts) {
  auto global = std::make_shared<DeathLogger>("global");
  auto scoped = std::make_shared<DeathLogger>("scoped");
  ScopedDefaultLogger guard(global);

  EXPECT_DEATH(
      {
        ScopedLogger bind(scoped);
        ICEBERG_LOG_FATAL("fatal {}", 9);
      },
      "log:scoped:fatal 9(.|\n)*flush:scoped");
}

}  // namespace iceberg
