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

#  include <gtest/gtest.h>
#  include <spdlog/logger.h>
#  include <spdlog/sinks/ostream_sink.h>

#  include "iceberg/logging/internal/spdlog_logger.h"
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

}  // namespace iceberg

#endif  // ICEBERG_HAS_SPDLOG
