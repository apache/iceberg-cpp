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

// Internal/build-generated header is acceptable in a test TU (not installed).
#include "iceberg/logging/config.h"

#ifdef ICEBERG_HAS_SPDLOG

#  include <memory>
#  include <source_location>
#  include <sstream>
#  include <string>
#  include <unordered_map>

#  include <gtest/gtest.h>
#  include <spdlog/logger.h>
#  include <spdlog/sinks/ostream_sink.h>

#  include "iceberg/logging/internal/spdlog_logger.h"
#  include "iceberg/logging/log_level.h"
#  include "iceberg/logging/logger.h"
#  include "iceberg/test/matchers.h"

namespace iceberg {

namespace {

LogMessage MakeMessage(LogLevel level, std::string text) {
  return LogMessage{.level = level,
                    .message = std::move(text),
                    .location = std::source_location::current(),
                    .attributes = {}};
}

internal::SpdLogger MakeCapturing(std::ostringstream& out,
                                  LogLevel level = LogLevel::kTrace) {
  auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(out);
  auto spd = std::make_shared<spdlog::logger>("test", sink);
  return internal::SpdLogger(spd, level);
}

}  // namespace

TEST(SpdLoggerTest, DefaultLevelIsInfo) {
  internal::SpdLogger logger;
  EXPECT_EQ(logger.level(), LogLevel::kInfo);
  EXPECT_FALSE(logger.ShouldLog(LogLevel::kDebug));
  EXPECT_TRUE(logger.ShouldLog(LogLevel::kError));
}

// SetLevel/level() round-trip, and the level actually gates emission.
TEST(SpdLoggerTest, SetLevelFiltersEmission) {
  std::ostringstream out;
  auto logger = MakeCapturing(out, LogLevel::kError);
  EXPECT_EQ(logger.level(), LogLevel::kError);
  EXPECT_FALSE(logger.ShouldLog(LogLevel::kWarn));

  logger.SetLevel(LogLevel::kTrace);
  EXPECT_EQ(logger.level(), LogLevel::kTrace);
  EXPECT_TRUE(logger.ShouldLog(LogLevel::kTrace));
}

// A null spdlog logger is substituted with the default stderr-backed one at
// construction, so the object is always usable: Log/Flush must not crash and the
// level accessors keep working. (The substitution is unconditional -- not a DCHECK
// -- so this holds in release builds too.)
TEST(SpdLoggerTest, NullLoggerIsSubstitutedNotDereferenced) {
  internal::SpdLogger logger(std::shared_ptr<spdlog::logger>{}, LogLevel::kTrace);
  EXPECT_EQ(logger.level(), LogLevel::kTrace);
  // Would crash if logger_ had been left null.
  logger.Log(MakeMessage(LogLevel::kError, "survives-null-ctor"));
  logger.Flush();
  auto status = logger.Initialize({{std::string(kPatternProperty), std::string("%v")}});
  EXPECT_TRUE(status.has_value());
}

// The base Logger::Initialize parses "level"; an unrecognized value is an error.
TEST(SpdLoggerTest, InitializeRejectsInvalidLevel) {
  std::ostringstream out;
  auto logger = MakeCapturing(out);
  auto status =
      logger.Initialize({{std::string(kLevelProperty), std::string("not-a-level")}});
  ASSERT_FALSE(status.has_value());
  EXPECT_THAT(status, IsError(ErrorKind::kInvalidArgument));
}

TEST(SpdLoggerTest, ForwardsMessageToSink) {
  std::ostringstream out;
  auto logger = MakeCapturing(out);
  logger.Log(MakeMessage(LogLevel::kError, "boom 42"));
  logger.Flush();
  EXPECT_NE(out.str().find("boom 42"), std::string::npos);
}

TEST(SpdLoggerTest, MessageBracesAreNotInterpreted) {
  std::ostringstream out;
  auto logger = MakeCapturing(out);
  // A pre-formatted message containing braces must pass through verbatim.
  logger.Log(MakeMessage(LogLevel::kInfo, "literal {not a placeholder}"));
  logger.Flush();
  EXPECT_NE(out.str().find("literal {not a placeholder}"), std::string::npos);
}

TEST(SpdLoggerTest, CriticalAndFatalBothEmit) {
  std::ostringstream out;
  auto logger = MakeCapturing(out);
  logger.Log(MakeMessage(LogLevel::kCritical, "crit"));
  logger.Log(MakeMessage(LogLevel::kFatal, "fatal-tag"));
  logger.Flush();
  EXPECT_NE(out.str().find("crit"), std::string::npos);
  EXPECT_NE(out.str().find("fatal-tag"), std::string::npos);
}

TEST(SpdLoggerTest, PatternPropertyChangesLayout) {
  std::ostringstream out;
  auto logger = MakeCapturing(out);
  auto status =
      logger.Initialize({{std::string(kPatternProperty), std::string("PFX %v")}});
  ASSERT_TRUE(status.has_value());
  logger.Log(MakeMessage(LogLevel::kError, "hello"));
  logger.Flush();
  EXPECT_NE(out.str().find("PFX hello"), std::string::npos);
}

// The record's std::source_location must reach spdlog as source_loc: assert the
// file, line, and function fields render via the %s / %# / %! pattern flags. This
// is the forwarding that makes SpdLogger synchronous-only (source_loc borrows the
// location's const char*), so it needs explicit coverage.
TEST(SpdLoggerTest, ForwardsSourceLocationToSink) {
  std::ostringstream out;
  auto logger = MakeCapturing(out);
  auto status =
      logger.Initialize({{std::string(kPatternProperty), std::string("%s:%# %! %v")}});
  ASSERT_TRUE(status.has_value());

  // Capture the location here (not inside MakeMessage) so the expected file/line
  // belong to this call site.
  const auto here = std::source_location::current();
  logger.Log(LogMessage{.level = LogLevel::kError,
                        .message = "located",
                        .location = here,
                        .attributes = {}});
  logger.Flush();

  const std::string rendered = out.str();
  // %s renders the basename of the file.
  EXPECT_NE(rendered.find("spdlog_logger_test.cc"), std::string::npos) << rendered;
  EXPECT_NE(rendered.find(std::to_string(here.line())), std::string::npos) << rendered;
  // %! renders the function name; gtest bodies are TestBody().
  EXPECT_NE(rendered.find("TestBody"), std::string::npos) << rendered;
  EXPECT_NE(rendered.find("located"), std::string::npos) << rendered;
}

}  // namespace iceberg

#endif  // ICEBERG_HAS_SPDLOG
