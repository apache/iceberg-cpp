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

#include <cstddef>
#include <format>
#include <iostream>
#include <regex>
#include <set>
#include <source_location>
#include <sstream>
#include <streambuf>
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

/// \brief First line of \p text, without the trailing newline.
std::string FirstLine(const std::string& text) {
  auto pos = text.find('\n');
  return text.substr(0, pos == std::string::npos ? text.size() : pos);
}

/// \brief A streambuf that throws on every write, to drive the never-throw path.
///
/// Throws a payload-less exception on purpose: an exception type with a
/// heap-allocated what() string (e.g. std::runtime_error) trips an ASan
/// alloc-dealloc-mismatch as the string crosses the instrumented binary /
/// system libc++abi boundary during unwinding -- unrelated to the code here.
struct SinkFailure {};
class ThrowingBuf : public std::streambuf {
 protected:
  int overflow(int /*ch*/) override { throw SinkFailure{}; }
  std::streamsize xsputn(const char* /*s*/, std::streamsize /*n*/) override {
    throw SinkFailure{};
  }
};

/// \brief RAII: point std::cerr at a throwing buffer with exceptions armed, and
/// restore all of cerr's state (buffer, exception mask, error flags) on exit --
/// so a thrown assertion can never leak the dangling/throwing stream to other
/// tests.
class CerrThrowingGuard {
 public:
  CerrThrowingGuard()
      : old_buf_(std::cerr.rdbuf(&buf_)), old_exc_(std::cerr.exceptions()) {
    std::cerr.exceptions(std::ios::badbit | std::ios::failbit);  // make << rethrow
  }
  ~CerrThrowingGuard() {
    std::cerr.clear();
    std::cerr.exceptions(old_exc_);
    std::cerr.rdbuf(old_buf_);
  }

 private:
  ThrowingBuf buf_;
  std::streambuf* old_buf_;
  std::ios_base::iostate old_exc_;
};

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

// The whole value of CerrLogger over a backend is its fixed line layout:
// `YYYY-MM-DDThh:mm:ss.mmmZ LEVEL [tid] [file:line] message`. Assert the shape,
// especially the .mmm millisecond field (which silently vanishes if the
// time_point is no longer floored to milliseconds).
TEST(CerrLoggerTest, LineMatchesFixedLayout) {
  CerrLogger logger;
  CerrCapture capture;
  logger.Log(MakeMessage(LogLevel::kWarn, "hello world"));
  const std::regex layout(
      R"(^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z \w+ \[\d+\] \[\S+:\d+\] hello world$)");
  EXPECT_TRUE(std::regex_match(FirstLine(capture.str()), layout)) << capture.str();
}

// CerrLogger deliberately does not override Initialize: it inherits the base,
// which applies "level" and ignores unknown keys like "pattern".
TEST(CerrLoggerTest, InitializeAppliesLevelAndIgnoresPattern) {
  CerrLogger logger;
  auto status = logger.Initialize({{std::string(kLevelProperty), "warn"},
                                   {std::string(kPatternProperty), "ignored"}});
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(logger.level(), LogLevel::kWarn);
}

TEST(CerrLoggerTest, FlushDoesNotThrow) {
  CerrLogger logger;
  CerrCapture capture;
  logger.Log(MakeMessage(LogLevel::kError, "x"));
  logger.Flush();  // best-effort; must not throw
  EXPECT_NE(capture.str().find('x'), std::string::npos);
}

// Log()/Flush() are noexcept: even when every write to the sink throws, they
// must swallow it (a leaked exception from a noexcept function -> std::terminate).
TEST(CerrLoggerTest, LogAndFlushNeverThrowWhenSinkThrows) {
  CerrThrowingGuard guard;  // cerr -> throwing sink; state restored on scope exit
  CerrLogger logger(LogLevel::kTrace);
  logger.Log(MakeMessage(LogLevel::kInfo, "x"));  // write throws -> fallback also throws
  logger.Flush();
  SUCCEED();
}

TEST(CerrLoggerTest, ConcurrentLogsDoNotInterleave) {
  CerrLogger logger(LogLevel::kTrace);
  CerrCapture capture;
  constexpr int kThreads = 8;
  constexpr int kPerThread = 50;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&logger, t] {
      for (int i = 0; i < kPerThread; ++i) {
        // Distinct payload per record so an interleave would corrupt a line.
        logger.Log(MakeMessage(LogLevel::kInfo, std::format("t{}-{}", t, i)));
      }
    });
  }
  for (auto& thread : threads) thread.join();

  // Each output line must be a complete, well-formed record ending in its own
  // payload, and every (thread, index) payload must appear exactly once -- this
  // proves the whole-line write is atomic, not merely that newlines were kept.
  const std::regex line(R"(^.*\] (t\d+-\d+)$)");
  std::set<std::string> seen;
  std::istringstream in(capture.str());
  std::string row;
  int lines = 0;
  while (std::getline(in, row)) {
    ++lines;
    std::smatch m;
    ASSERT_TRUE(std::regex_match(row, m, line)) << "garbled line: " << row;
    EXPECT_TRUE(seen.insert(m[1].str()).second) << "duplicate payload: " << m[1].str();
  }
  EXPECT_EQ(lines, kThreads * kPerThread);
  EXPECT_EQ(seen.size(), static_cast<std::size_t>(kThreads * kPerThread));
}

}  // namespace iceberg
