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

#include "iceberg/logging/loggers.h"

#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <gtest/gtest.h>

// Build-generated, test-only: gates the spdlog-by-property expectation.
#include "iceberg/logging/config.h"
#include "iceberg/logging/log_level.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/logging/logger.h"
#include "iceberg/test/logging_test_helpers.h"

#ifdef ICEBERG_HAS_SPDLOG
#  include <spdlog/logger.h>
#  include <spdlog/sinks/ostream_sink.h>

#  include "iceberg/logging/spdlog_logger_internal.h"
#endif

namespace iceberg {

namespace {

class CerrCapture {
 public:
  CerrCapture() : old_(std::cerr.rdbuf(buffer_.rdbuf())) {}
  ~CerrCapture() { std::cerr.rdbuf(old_); }
  std::string str() const { return buffer_.str(); }

 private:
  std::ostringstream buffer_;
  std::streambuf* old_;
};

}  // namespace

TEST(LoggersTest, LoadDefaultReturnsNonNullNonNoop) {
  auto result = Loggers::Load({});
  ASSERT_TRUE(result.has_value());
  ASSERT_NE(result.value(), nullptr);
  // The default backend (spdlog or cerr) is a real sink, never the no-op.
  EXPECT_FALSE(result.value()->IsNoop());
}

TEST(LoggersTest, LoadNoopByProperty) {
  auto result = Loggers::Load({{std::string(kLoggerImpl), std::string(kLoggerTypeNoop)}});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result.value()->IsNoop());
}

TEST(LoggersTest, LoadCerrByProperty) {
  auto result = Loggers::Load({{std::string(kLoggerImpl), std::string(kLoggerTypeCerr)}});
  ASSERT_TRUE(result.has_value());
  ASSERT_NE(result.value(), nullptr);
  EXPECT_FALSE(result.value()->IsNoop());
}

TEST(LoggersTest, UnknownTypeIsAnError) {
  auto result =
      Loggers::Load({{std::string(kLoggerImpl), std::string("does-not-exist")}});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, ErrorKind::kInvalidArgument);
}

TEST(LoggersTest, RegisterCustomFactoryThenLoad) {
  auto status = Loggers::Register("capturing",
                                  [](const std::unordered_map<std::string, std::string>&)
                                      -> Result<std::unique_ptr<Logger>> {
                                    return std::make_unique<CapturingLogger>();
                                  });
  ASSERT_TRUE(status.has_value());

  auto result = Loggers::Load({{std::string(kLoggerImpl), "capturing"}});
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(dynamic_cast<CapturingLogger*>(result.value().get()), nullptr);
}

TEST(LoggersTest, RegisterRejectsEmptyFactory) {
  auto status = Loggers::Register("bad", LoggerFactory{});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kInvalidArgument);
}

TEST(LoggersTest, LoadAppliesLevelProperty) {
  auto result = Loggers::Load({{std::string(kLoggerImpl), std::string(kLoggerTypeCerr)},
                               {std::string(kLevelProperty), std::string("error")}});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value()->level(), LogLevel::kError);
}

TEST(LoggersTest, LoadRejectsInvalidLevelProperty) {
  auto result =
      Loggers::Load({{std::string(kLoggerImpl), std::string(kLoggerTypeCerr)},
                     {std::string(kLevelProperty), std::string("not-a-level")}});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, ErrorKind::kInvalidArgument);
}

// Registering the same type twice replaces the factory rather than failing, so a
// later Register wins. This pins the documented last-one-wins behavior.
TEST(LoggersTest, RegisterSameTypeTwiceReplacesFactory) {
  constexpr std::string_view kType = "replaceable-test-logger";
  ASSERT_TRUE(
      Loggers::Register(kType, [](const auto&) -> Result<std::unique_ptr<Logger>> {
        return std::make_unique<CapturingLogger>();
      }).has_value());

  // Second registration of the same key must succeed and take effect.
  ASSERT_TRUE(
      Loggers::Register(kType, [](const auto&) -> Result<std::unique_ptr<Logger>> {
        auto logger = std::make_unique<CapturingLogger>();
        logger->SetLevel(LogLevel::kError);  // distinguishes the 2nd factory
        return logger;
      }).has_value());

  auto result = Loggers::Load({{std::string(kLoggerImpl), std::string(kType)}});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ((*result)->level(), LogLevel::kError);
}

// The spdlog backend is reachable through the registry by property when compiled
// in; when it is not, the type is simply unknown. Either way Load must not crash.
TEST(LoggersTest, LoadSpdlogByPropertyWhenCompiledIn) {
  auto result =
      Loggers::Load({{std::string(kLoggerImpl), std::string(kLoggerTypeSpdlog)}});
#ifdef ICEBERG_HAS_SPDLOG
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(*result, nullptr);
  EXPECT_FALSE((*result)->IsNoop());
#else
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, ErrorKind::kInvalidArgument);
#endif
}

TEST(LoggingEndToEndTest, ConfiguredCerrLoggerEmitsFormattedLineThroughMacro) {
  auto result = Loggers::Load({{std::string(kLoggerImpl), std::string(kLoggerTypeCerr)}});
  ASSERT_TRUE(result.has_value());
  ScopedDefaultLogger guard(std::shared_ptr<Logger>(std::move(result.value())));

  std::string out;
  {
    CerrCapture capture;
    ICEBERG_LOG_WARN("u={}", 7);
    out = capture.str();
  }
  ASSERT_FALSE(out.empty());
  EXPECT_NE(out.find("warn"), std::string::npos);
  EXPECT_NE(out.find("u=7"), std::string::npos);
  EXPECT_NE(out.find("loggers_test.cc"), std::string::npos);
  EXPECT_EQ(out.back(), '\n');
}

TEST(LoggingEndToEndTest, ConfiguredLevelByPropertyFiltersThroughMacro) {
  auto result = Loggers::Load({{std::string(kLoggerImpl), std::string(kLoggerTypeCerr)},
                               {std::string(kLevelProperty), std::string("error")}});
  ASSERT_TRUE(result.has_value());
  ScopedDefaultLogger guard(std::shared_ptr<Logger>(std::move(result.value())));

  {
    CerrCapture capture;
    ICEBERG_LOG_INFO("dropped {}", 1);
    EXPECT_TRUE(capture.str().empty());
  }
  {
    CerrCapture capture;
    ICEBERG_LOG_ERROR("kept {}", 2);
    EXPECT_NE(capture.str().find("kept 2"), std::string::npos);
  }
}

#ifdef ICEBERG_HAS_SPDLOG
TEST(LoggingEndToEndTest, MacroLogsThroughRealSpdLogger) {
  std::ostringstream out;
  auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(out);
  ScopedDefaultLogger guard(std::make_shared<internal::SpdLogger>(
      spdlog::logger("e2e", std::move(sink)), LogLevel::kTrace));

  ICEBERG_LOG_INFO("v={}", 9);
  GetDefaultLogger()->Flush();
  EXPECT_NE(out.str().find("v=9"), std::string::npos);
}
#endif  // ICEBERG_HAS_SPDLOG

}  // namespace iceberg
