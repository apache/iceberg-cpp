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

// Compile-time floor set to kOff for this translation unit: every fixed-severity
// macro below kFatal must be stripped to nothing, while ICEBERG_LOG_FATAL must
// still abort (its abort is never gated by the compile-time floor).
#define ICEBERG_LOG_ACTIVE_LEVEL ::iceberg::LogLevel::kOff

#include <memory>

#include <gtest/gtest.h>

#include "iceberg/logging/log_level.h"
#include "iceberg/logging/logger.h"
#include "iceberg/test/logging_test_helpers.h"

namespace iceberg {

TEST(MacrosActiveLevelTest, BelowFloorStatementsAreCompiledOut) {
  auto logger = std::make_shared<CapturingLogger>();
  logger->SetLevel(LogLevel::kTrace);
  ScopedDefaultLogger guard(logger);

  int calls = 0;
  // counted() is only "called" from the compile-time-stripped macros below, so the
  // analyzer sees its init as a dead store -- which is exactly what this verifies.
  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  auto counted = [&calls]() {
    ++calls;
    return 1;
  };
  // Stripped at compile time -> arguments never evaluated, nothing emitted,
  // even though the runtime logger would accept these levels.
  ICEBERG_LOG_INFO("{}", counted());
  ICEBERG_LOG_CRITICAL("{}", counted());
  EXPECT_EQ(calls, 0);
  EXPECT_EQ(logger->count(), 0u);
}

TEST(MacrosActiveLevelDeathTest, FatalStillAbortsWhenEverythingElseStripped) {
  EXPECT_DEATH({ ICEBERG_LOG_FATAL("still fatal"); }, "");
}

}  // namespace iceberg
