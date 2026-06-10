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

#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "iceberg/logging/log_level.h"
#include "iceberg/logging/logger.h"
#include "iceberg/test/logging_test_helpers.h"

namespace iceberg {

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

TEST(LoggersTest, LoadAndSetDefaultInstallsLogger) {
  auto previous = GetDefaultLogger();
  auto status = Loggers::LoadAndSetDefault(
      {{std::string(kLoggerImpl), std::string(kLoggerTypeNoop)}});
  ASSERT_TRUE(status.has_value());
  EXPECT_TRUE(GetDefaultLogger()->IsNoop());
  SetDefaultLogger(previous);  // restore
}

}  // namespace iceberg
