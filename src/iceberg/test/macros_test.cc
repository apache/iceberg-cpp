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

#include <memory>

#include <gtest/gtest.h>

#include "iceberg/logging/cerr_logger.h"
#include "iceberg/logging/log_level.h"
#include "iceberg/logging/logger.h"
#include "iceberg/test/logging_test_helpers.h"

namespace iceberg {

namespace {

std::shared_ptr<CapturingLogger> InstallCapturing(LogLevel level = LogLevel::kTrace) {
  auto logger = std::make_shared<CapturingLogger>();
  logger->SetLevel(level);
  return logger;
}

}  // namespace

TEST(MacrosTest, InfoFormatsAndCapturesLocation) {
  auto logger = InstallCapturing();
  ScopedDefaultLogger guard(logger);
  ICEBERG_LOG_INFO("x={}", 42);
  auto records = logger->records();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].level, LogLevel::kInfo);
  EXPECT_EQ(records[0].message, "x=42");
  EXPECT_NE(records[0].location.line(), 0u);
}

TEST(MacrosTest, RuntimeLevelFiltersBelowThreshold) {
  auto logger = InstallCapturing();
  ScopedDefaultLogger guard(logger);
  SetDefaultLevel(LogLevel::kError);
  ICEBERG_LOG_INFO("dropped");
  ICEBERG_LOG_ERROR("kept");
  auto records = logger->records();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].message, "kept");
}

TEST(MacrosTest, DisabledLevelDoesNotEvaluateArguments) {
  auto logger = InstallCapturing();
  ScopedDefaultLogger guard(logger);
  SetDefaultLevel(LogLevel::kError);
  int calls = 0;
  auto counted = [&calls]() {
    ++calls;
    return 1;
  };
  ICEBERG_LOG_INFO("{}", counted());
  EXPECT_EQ(calls, 0);
}

TEST(MacrosTest, DanglingElseBindsCorrectly) {
  auto logger = InstallCapturing();
  ScopedDefaultLogger guard(logger);
  bool took_else = false;
  // Intentionally brace-free: this verifies the macro keeps dangling-else binding
  // correct. Adding braces would defeat the test, so suppress the tidy check.
  // NOLINTBEGIN(google-readability-braces-around-statements)
  if (false)
    ICEBERG_LOG_INFO("if-branch");
  else
    took_else = true;
  // NOLINTEND(google-readability-braces-around-statements)
  EXPECT_TRUE(took_else);
  EXPECT_EQ(logger->count(), 0u);
}

TEST(MacrosTest, GenericRuntimeLevelMacroCompilesAndLogs) {
  auto logger = InstallCapturing();
  ScopedDefaultLogger guard(logger);
  LogLevel level = LogLevel::kWarn;
  ICEBERG_LOG(level, "n={}", 7);
  auto records = logger->records();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].message, "n=7");
  EXPECT_EQ(records[0].level, LogLevel::kWarn);
}

TEST(MacrosTest, LogToHonorsOnlyExplicitLoggerNotDefaultGate) {
  auto sink = InstallCapturing();
  ScopedDefaultLogger guard(InstallCapturing());
  SetDefaultLevel(LogLevel::kOff);  // default gate would block everything
  ICEBERG_LOG_TO(*sink, LogLevel::kInfo, "explicit {}", 1);
  EXPECT_EQ(sink->count(), 1u);
}

TEST(MacrosTest, NeverThrowsOnBadRuntimeFormat) {
  auto logger = InstallCapturing();
  ScopedDefaultLogger guard(logger);
  // Invalid runtime format string -> std::vformat throws -> swallowed -> fallback.
  EXPECT_NO_THROW(ICEBERG_LOG_RUNTIME_FMT(LogLevel::kInfo, "{"));
  auto records = logger->records();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].message, "<fmt error>");
}

TEST(MacrosDeathTest, FatalEmitsThenAborts) {
  // Default logger writes to std::cerr; the message must appear before abort.
  EXPECT_DEATH({ ICEBERG_LOG_FATAL("fatalmsg {}", 7); }, "fatalmsg 7");
}

TEST(MacrosDeathTest, FatalAbortsEvenWhenRuntimeDisabled) {
  EXPECT_DEATH(
      {
        SetDefaultLevel(LogLevel::kOff);
        ICEBERG_LOG_FATAL("suppressed");
      },
      "");
}

TEST(MacrosDeathTest, GenericRuntimeFatalEmitsThenAborts) {
  // ICEBERG_LOG with a runtime kFatal level must also emit then abort.
  EXPECT_DEATH({ ICEBERG_LOG(LogLevel::kFatal, "gfatal {}", 1); }, "gfatal 1");
}

TEST(MacrosDeathTest, LogToFatalEmitsThenAborts) {
  // ICEBERG_LOG_TO with kFatal must emit to the explicit logger then abort.
  EXPECT_DEATH(
      {
        CerrLogger sink(LogLevel::kTrace);
        ICEBERG_LOG_TO(sink, LogLevel::kFatal, "tofatal {}", 2);
      },
      "tofatal 2");
}

// Regression guard: ICEBERG_LOG_FATAL must route through the active ScopedLogger
// binding (GetCurrentLogger), not the process default (GetDefaultLogger). A scoped
// CerrLogger emits the record before the abort; the message must reach that sink.
TEST(MacrosDeathTest, FatalRoutesThroughScopedLogger) {
  EXPECT_DEATH(
      {
        SetDefaultLevel(LogLevel::kOff);  // default gate off -> only the scope emits
        auto scoped = std::make_shared<CerrLogger>(LogLevel::kTrace);
        ScopedLogger bind(scoped);
        ICEBERG_LOG_FATAL("scopedfatal {}", 9);
      },
      "scopedfatal 9");
}

}  // namespace iceberg
