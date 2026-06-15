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

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

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

TEST(LoggerTest, SetDefaultLevelUpdatesLogger) {
  auto capturing = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(capturing);
  SetDefaultLevel(LogLevel::kError);
  EXPECT_EQ(capturing->level(), LogLevel::kError);
}

// Filtering is decided by the logger's own ShouldLog (no separate cached gate),
// so lowering a logger's level out-of-band (not via SetDefaultLevel) takes effect
// immediately -- this is the regression guard for the dropped g_effective_level gate.
TEST(LoggerTest, OutOfBandLevelLoweringTakesEffect) {
  auto capturing = std::make_shared<CapturingLogger>();
  capturing->SetLevel(LogLevel::kError);
  ScopedDefaultLogger guard(capturing);
  EXPECT_FALSE(detail::CurrentLogger()->ShouldLog(LogLevel::kInfo));
  capturing->SetLevel(LogLevel::kTrace);  // lowered directly on the handle
  EXPECT_TRUE(detail::CurrentLogger()->ShouldLog(LogLevel::kInfo));
}

TEST(LoggerTest, ConcurrentSwapAndReadIsSafe) {
  // Stress CurrentLogger()/GetDefaultLogger() against SetDefaultLogger() swaps.
  // Run under TSan in CI; here it asserts no crash and a valid logger throughout.
  auto a = std::make_shared<CapturingLogger>();
  auto b = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger guard(a);
  std::atomic<bool> stop{false};
  std::atomic<bool> saw_null{false};
  std::vector<std::thread> readers;
  for (int i = 0; i < 6; ++i) {
    readers.emplace_back([&stop, &saw_null] {
      // ASSERT_* doesn't propagate from non-main threads; record via a flag.
      while (!stop.load(std::memory_order_relaxed)) {
        const auto& l = detail::CurrentLogger();
        if (!l) saw_null.store(true, std::memory_order_relaxed);
        (void)l->ShouldLog(LogLevel::kError);
        (void)GetDefaultLogger();
      }
    });
  }
  for (int i = 0; i < 2000; ++i) SetDefaultLogger((i & 1) ? a : b);
  stop.store(true, std::memory_order_relaxed);
  for (auto& t : readers) t.join();
  EXPECT_FALSE(saw_null.load());  // CurrentLogger() is never null across swaps
}

TEST(LoggerTest, InitializeAppliesLevelProperty) {
  CapturingLogger logger;
  auto status = logger.Initialize({{std::string(kLevelProperty), std::string("error")}});
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(logger.level(), LogLevel::kError);
}

TEST(LoggerTest, InitializeRejectsInvalidLevel) {
  CapturingLogger logger;
  auto status =
      logger.Initialize({{std::string(kLevelProperty), std::string("not-a-level")}});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kInvalidArgument);
}

// Logging during thread teardown (from a thread_local destructor) must not crash:
// CurrentLogger() serves the live cache or falls back to an immortal logger. Run
// under ASan in CI for full signal.
TEST(LoggerTest, LoggingFromThreadLocalDestructorIsSafe) {
  std::thread([] {
    struct Probe {
      ~Probe() {
        const auto& logger = detail::CurrentLogger();
        if (logger) {
          detail::Emit(*logger, LogLevel::kInfo, std::source_location::current(),
                       "from thread_local dtor");
        }
      }
    };
    static thread_local Probe probe;
    (void)probe;
  }).join();
  SUCCEED();
}

}  // namespace iceberg
