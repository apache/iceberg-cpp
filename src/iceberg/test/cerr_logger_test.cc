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

#include "iceberg/logging/cerr_logger.h"

#include <iostream>
#include <source_location>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/logging/log_level.h"
#include "iceberg/logging/logger.h"

namespace iceberg {

namespace {

/// \brief RAII redirect of std::cerr to a stringstream for the test scope.
class CerrCapture {
 public:
  CerrCapture() : old_(std::cerr.rdbuf(buffer_.rdbuf())) {}
  ~CerrCapture() { std::cerr.rdbuf(old_); }
  std::string str() const { return buffer_.str(); }

 private:
  std::ostringstream buffer_;
  std::streambuf* old_;
};

LogMessage MakeMessage(LogLevel level, std::string text) {
  return LogMessage{.level = level,
                    .message = std::move(text),
                    .location = std::source_location::current(),
                    .attributes = {}};
}

}  // namespace

TEST(CerrLoggerTest, DefaultLevelIsInfo) {
  CerrLogger logger;
  EXPECT_EQ(logger.level(), LogLevel::kInfo);
  EXPECT_FALSE(logger.ShouldLog(LogLevel::kDebug));
  EXPECT_TRUE(logger.ShouldLog(LogLevel::kInfo));
  EXPECT_TRUE(logger.ShouldLog(LogLevel::kError));
}

TEST(CerrLoggerTest, SetLevelFilters) {
  CerrLogger logger(LogLevel::kError);
  EXPECT_FALSE(logger.ShouldLog(LogLevel::kWarn));
  logger.SetLevel(LogLevel::kTrace);
  EXPECT_TRUE(logger.ShouldLog(LogLevel::kTrace));
}

TEST(CerrLoggerTest, LineContainsLevelAndMessage) {
  CerrLogger logger;
  CerrCapture capture;
  logger.Log(MakeMessage(LogLevel::kError, "boom 42"));
  std::string out = capture.str();
  EXPECT_NE(out.find("error"), std::string::npos);
  EXPECT_NE(out.find("boom 42"), std::string::npos);
  EXPECT_NE(out.find("cerr_logger_test.cc"), std::string::npos);
  EXPECT_EQ(out.back(), '\n');
}

TEST(CerrLoggerTest, ConcurrentLogsDoNotInterleave) {
  CerrLogger logger(LogLevel::kTrace);
  CerrCapture capture;
  constexpr int kThreads = 8;
  constexpr int kPerThread = 50;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&logger] {
      for (int i = 0; i < kPerThread; ++i) {
        logger.Log(MakeMessage(LogLevel::kInfo, "line"));
      }
    });
  }
  for (auto& thread : threads) thread.join();

  // Every record is exactly one well-formed line; no interleaving means the
  // line count equals the record count.
  std::string out = capture.str();
  int newlines = 0;
  for (char c : out) {
    if (c == '\n') ++newlines;
  }
  EXPECT_EQ(newlines, kThreads * kPerThread);
}

}  // namespace iceberg
