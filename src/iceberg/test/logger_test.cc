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

#include "iceberg/logging/logger.h"

#include <memory>

#include <gtest/gtest.h>

#include "iceberg/logging/log_level.h"
#include "iceberg/test/logging_test_helpers.h"

namespace iceberg {

TEST(LoggerTest, NoopIsSharedImmortalAndSilent) {
  auto noop = Logger::Noop();
  ASSERT_NE(noop, nullptr);
  EXPECT_TRUE(noop->IsNoop());
  EXPECT_FALSE(noop->ShouldLog(LogLevel::kFatal));
  EXPECT_EQ(noop->level(), LogLevel::kOff);
  // Same singleton instance every call.
  EXPECT_EQ(noop.get(), Logger::Noop().get());
}

TEST(LoggerTest, DefaultLoggerIsNeverNull) { EXPECT_NE(GetDefaultLogger(), nullptr); }

TEST(LoggerTest, SetAndGetDefaultLogger) {
  auto capturing = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(capturing);
  EXPECT_EQ(GetDefaultLogger().get(), capturing.get());
  EXPECT_EQ(detail::CurrentLogger().get(), capturing.get());
}

TEST(LoggerTest, SetNullFallsBackToNoop) {
  ScopedDefaultLogger guard(std::make_shared<CapturingLogger>());
  SetDefaultLogger(nullptr);
  EXPECT_TRUE(GetDefaultLogger()->IsNoop());
}

TEST(LoggerTest, CurrentLoggerTracksSwaps) {
  auto first = std::make_shared<CapturingLogger>();
  auto second = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(first);
  EXPECT_EQ(detail::CurrentLogger().get(), first.get());
  SetDefaultLogger(second);
  // Generation bump must invalidate the thread-local cache.
  EXPECT_EQ(detail::CurrentLogger().get(), second.get());
}

TEST(LoggerTest, SetDefaultLevelSyncsGateAndLogger) {
  auto capturing = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(capturing);
  SetDefaultLevel(LogLevel::kError);
  EXPECT_EQ(detail::EffectiveLevel(), LogLevel::kError);
  EXPECT_EQ(capturing->level(), LogLevel::kError);
}

TEST(LoggerTest, SetDefaultLoggerSeedsGateFromLoggerLevel) {
  auto capturing = std::make_shared<CapturingLogger>();
  capturing->SetLevel(LogLevel::kWarn);
  ScopedDefaultLogger guard(capturing);
  EXPECT_EQ(detail::EffectiveLevel(), LogLevel::kWarn);
}

}  // namespace iceberg
