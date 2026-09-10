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

#include "iceberg/logging/spdlog_logger_internal.h"

#ifdef ICEBERG_HAS_SPDLOG

#  include <memory>
#  include <source_location>
#  include <sstream>
#  include <string>
#  include <utility>

#  include <gtest/gtest.h>
#  include <spdlog/logger.h>
#  include <spdlog/sinks/ostream_sink.h>

#  include "iceberg/logging/log_level.h"
#  include "iceberg/logging/logger.h"

namespace iceberg {

namespace {

LogMessage MakeMessage(LogLevel level, std::string text) {
  return LogMessage{.level = level,
                    .message = std::move(text),
                    .location = std::source_location::current(),
                    .attributes = {}};
}

internal::SpdLogger MakeCapturing(std::ostringstream& output,
                                  LogLevel level = LogLevel::kInfo) {
  auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(output);
  return internal::SpdLogger(spdlog::logger("test", std::move(sink)), level);
}

std::string Render(internal::SpdLogger& logger, std::ostringstream& output,
                   LogMessage message) {
  logger.Log(std::move(message));
  logger.Flush();
  return output.str();
}

}  // namespace

TEST(SpdLoggerTest, LevelThreshold) {
  internal::SpdLogger default_logger;
  EXPECT_EQ(default_logger.level(), LogLevel::kInfo);
  EXPECT_FALSE(default_logger.ShouldLog(LogLevel::kDebug));
  EXPECT_TRUE(default_logger.ShouldLog(LogLevel::kError));

  std::ostringstream output;
  auto logger = MakeCapturing(output, LogLevel::kError);
  EXPECT_EQ(logger.level(), LogLevel::kError);
  EXPECT_FALSE(logger.ShouldLog(LogLevel::kWarn));

  Log(logger, LogLevel::kWarn, "filtered");
  logger.SetLevel(LogLevel::kTrace);
  EXPECT_EQ(logger.level(), LogLevel::kTrace);
  EXPECT_TRUE(logger.ShouldLog(LogLevel::kTrace));
  Log(logger, LogLevel::kTrace, "visible");
  logger.Flush();
  const auto rendered = output.str();
  EXPECT_EQ(rendered.find("filtered"), std::string::npos);
  EXPECT_NE(rendered.find("visible"), std::string::npos);
}

TEST(SpdLoggerTest, ForwardsMessageVerbatim) {
  std::ostringstream output;
  auto logger = MakeCapturing(output);
  const auto rendered =
      Render(logger, output, MakeMessage(LogLevel::kInfo, "literal {not a placeholder}"));
  EXPECT_NE(rendered.find("literal {not a placeholder}"), std::string::npos);
}

TEST(SpdLoggerTest, MapsCriticalAndFatalToCritical) {
  std::ostringstream output;
  auto logger = MakeCapturing(output);
  auto status =
      logger.Initialize({{std::string(kPatternProperty), std::string("%l %v")}});
  ASSERT_TRUE(status.has_value());
  logger.Log(MakeMessage(LogLevel::kCritical, "crit"));
  const auto rendered = Render(logger, output, MakeMessage(LogLevel::kFatal, "fatal"));
  EXPECT_NE(rendered.find("critical crit"), std::string::npos);
  EXPECT_NE(rendered.find("critical fatal"), std::string::npos);
}

TEST(SpdLoggerTest, InitializeAppliesPatternAndLevel) {
  std::ostringstream output;
  auto logger = MakeCapturing(output);
  auto status = logger.Initialize({{std::string(kPatternProperty), std::string("PFX %v")},
                                   {std::string(kLevelProperty), std::string("error")}});
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(logger.level(), LogLevel::kError);
  const auto rendered = Render(logger, output, MakeMessage(LogLevel::kError, "hello"));
  EXPECT_NE(rendered.find("PFX hello"), std::string::npos);
}

TEST(SpdLoggerTest, ForwardsSourceLocationToSink) {
  std::ostringstream output;
  auto logger = MakeCapturing(output);
  // %s, %#, and %! render the source file, line, and function.
  auto status =
      logger.Initialize({{std::string(kPatternProperty), std::string("%s:%# %! %v")}});
  ASSERT_TRUE(status.has_value());

  const auto here = std::source_location::current();
  const std::string rendered = Render(logger, output,
                                      LogMessage{.level = LogLevel::kError,
                                                 .message = "located",
                                                 .location = here,
                                                 .attributes = {}});
  const std::string expected_location =
      "spdlog_logger_test.cc:" + std::to_string(here.line());
  EXPECT_NE(rendered.find(expected_location), std::string::npos) << rendered;
  EXPECT_NE(rendered.find("TestBody"), std::string::npos) << rendered;
  EXPECT_NE(rendered.find("located"), std::string::npos) << rendered;
}

}  // namespace iceberg

#endif  // ICEBERG_HAS_SPDLOG
