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

#define ICEBERG_LOG_ACTIVE_LEVEL ICEBERG_LOG_LEVEL_ERROR

#include <gtest/gtest.h>

#include "iceberg/logging/log_macros.h"

namespace iceberg {
namespace {

struct NotFormattable {};

}  // namespace

TEST(LogMacrosActiveLevelTest, DisabledFixedLevelLogsDoNotEvaluateArguments) {
  int evaluations = 0;

  ICEBERG_LOG_DEBUG("debug side effect {}", ++evaluations);
  ICEBERG_LOG_INFO("info side effect {}", ++evaluations);
  ICEBERG_LOG_WARN("warn side effect {}", ++evaluations);

  EXPECT_EQ(evaluations, 0);
}

TEST(LogMacrosActiveLevelTest, DisabledFixedLevelLogsNeedNotBeFormattable) {
  ICEBERG_LOG_INFO("not formattable {}", NotFormattable{});
  SUCCEED();
}

}  // namespace iceberg
