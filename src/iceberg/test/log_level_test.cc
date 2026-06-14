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

#include "iceberg/logging/log_level.h"

#include <array>
#include <string>

#include <gtest/gtest.h>

namespace iceberg {

namespace {

constexpr std::array<LogLevel, 8> kAllLevels = {
    LogLevel::kTrace, LogLevel::kDebug,    LogLevel::kInfo,  LogLevel::kWarn,
    LogLevel::kError, LogLevel::kCritical, LogLevel::kFatal, LogLevel::kOff};

}  // namespace

TEST(LogLevelTest, ToStringCoversEveryLevel) {
  EXPECT_EQ(ToString(LogLevel::kTrace), "trace");
  EXPECT_EQ(ToString(LogLevel::kDebug), "debug");
  EXPECT_EQ(ToString(LogLevel::kInfo), "info");
  EXPECT_EQ(ToString(LogLevel::kWarn), "warn");
  EXPECT_EQ(ToString(LogLevel::kError), "error");
  EXPECT_EQ(ToString(LogLevel::kCritical), "critical");
  EXPECT_EQ(ToString(LogLevel::kFatal), "fatal");
  EXPECT_EQ(ToString(LogLevel::kOff), "off");
}

TEST(LogLevelTest, FromStringRoundTrips) {
  for (LogLevel level : kAllLevels) {
    auto parsed = LogLevelFromString(ToString(level));
    ASSERT_TRUE(parsed.has_value()) << "failed to parse " << ToString(level);
    EXPECT_EQ(parsed.value(), level);
  }
}

TEST(LogLevelTest, FromStringIsCaseInsensitive) {
  EXPECT_EQ(LogLevelFromString("WARN").value(), LogLevel::kWarn);
  EXPECT_EQ(LogLevelFromString("Warn").value(), LogLevel::kWarn);
  EXPECT_EQ(LogLevelFromString("CRITICAL").value(), LogLevel::kCritical);
}

TEST(LogLevelTest, FromStringRejectsUnknown) {
  auto parsed = LogLevelFromString("verbose");
  ASSERT_FALSE(parsed.has_value());
  EXPECT_EQ(parsed.error().kind, ErrorKind::kInvalidArgument);
}

TEST(LogLevelTest, OrderingIsMonotonicWithOffAsMaximum) {
  EXPECT_LT(LogLevel::kTrace, LogLevel::kDebug);
  EXPECT_LT(LogLevel::kDebug, LogLevel::kInfo);
  EXPECT_LT(LogLevel::kInfo, LogLevel::kWarn);
  EXPECT_LT(LogLevel::kWarn, LogLevel::kError);
  EXPECT_LT(LogLevel::kError, LogLevel::kCritical);
  EXPECT_LT(LogLevel::kCritical, LogLevel::kFatal);
  EXPECT_LT(LogLevel::kFatal, LogLevel::kOff);
}

}  // namespace iceberg
