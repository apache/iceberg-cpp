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

// End-to-end tests: exercise the public surface the way an application does --
// configure/install a real backend via the registry, log through the LOG_*
// macros, and observe the actual output. The per-layer unit tests cover each
// piece in isolation against a fake; these cover the seams between them.

// Internal/build-generated header is acceptable in a test TU (not installed).
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

#include "iceberg/logging/cerr_logger.h"
#include "iceberg/logging/config.h"
#include "iceberg/logging/log_level.h"
#include "iceberg/logging/logger.h"
#include "iceberg/logging/loggers.h"
#include "iceberg/test/logging_test_helpers.h"

#ifdef ICEBERG_HAS_SPDLOG
#  include <spdlog/logger.h>
#  include <spdlog/sinks/ostream_sink.h>

#  include "iceberg/logging/internal/spdlog_logger.h"
#endif

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

}  // namespace

// Configure CerrLogger through the registry, install it as the process default,
// then log via a macro and observe the formatted line on std::cerr -- the full
// registry -> default-slot -> macro -> Emit -> backend -> output path.
TEST(LoggingEndToEndTest, ConfiguredCerrLoggerEmitsFormattedLineThroughMacro) {
  ScopedDefaultLogger guard(GetDefaultLogger());  // save + restore the default
  auto status = Loggers::LoadAndSetDefault(
      {{std::string(kLoggerImpl), std::string(kLoggerTypeCerr)}});
  ASSERT_TRUE(status.has_value());

  std::string out;
  {
    CerrCapture capture;
    ICEBERG_LOG_WARN("u={}", 7);
    out = capture.str();
  }
  EXPECT_NE(out.find("warn"), std::string::npos);
  EXPECT_NE(out.find("u=7"), std::string::npos);
  EXPECT_NE(out.find("logging_end_to_end_test.cc"), std::string::npos);
  EXPECT_EQ(out.back(), '\n');
}

// The level set on the installed default logger gates emission decided through
// the whole macro path (not just a direct ShouldLog() call).
TEST(LoggingEndToEndTest, InstalledLevelFiltersThroughFullMacroPath) {
  ScopedDefaultLogger guard(GetDefaultLogger());
  auto status = Loggers::LoadAndSetDefault(
      {{std::string(kLoggerImpl), std::string(kLoggerTypeCerr)}});
  ASSERT_TRUE(status.has_value());
  SetDefaultLevel(LogLevel::kError);

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

// The "level" property set at configuration time gates emission through the full
// registry -> Initialize -> default-slot -> macro path.
TEST(LoggingEndToEndTest, ConfiguredLevelByPropertyFiltersThroughMacro) {
  ScopedDefaultLogger guard(GetDefaultLogger());
  auto status = Loggers::LoadAndSetDefault(
      {{std::string(kLoggerImpl), std::string(kLoggerTypeCerr)},
       {std::string(kLevelProperty), std::string("error")}});
  ASSERT_TRUE(status.has_value());

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

// The process default with no configuration is a real sink (never the no-op),
// and is the backend the build was compiled with: spdlog when ICEBERG_SPDLOG is
// ON, otherwise the std::cerr logger.
TEST(LoggingEndToEndTest, DefaultLoggerIsTheCompiledBackend) {
  auto def = GetDefaultLogger();
  ASSERT_NE(def, nullptr);
  EXPECT_FALSE(def->IsNoop());
#ifdef ICEBERG_HAS_SPDLOG
  EXPECT_NE(dynamic_cast<internal::SpdLogger*>(def.get()), nullptr);
#else
  EXPECT_NE(dynamic_cast<CerrLogger*>(def.get()), nullptr);
#endif
}

#ifdef ICEBERG_HAS_SPDLOG
// The "spdlog" registry type resolves to the spdlog-backed sink by name.
TEST(LoggingEndToEndTest, SpdlogFactoryLoadsByName) {
  auto result =
      Loggers::Load({{std::string(kLoggerImpl), std::string(kLoggerTypeSpdlog)}});
  ASSERT_TRUE(result.has_value());
  ASSERT_NE(result.value(), nullptr);
  EXPECT_FALSE(result.value()->IsNoop());
  EXPECT_NE(dynamic_cast<internal::SpdLogger*>(result.value().get()), nullptr);
}

// A macro statement reaches a real spdlog sink: install a SpdLogger backed by an
// ostream sink as the default, log through the macro, and observe the output.
TEST(LoggingEndToEndTest, MacroLogsThroughRealSpdLogger) {
  std::ostringstream out;
  auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(out);
  auto spd = std::make_shared<spdlog::logger>("e2e", sink);
  ScopedDefaultLogger guard(std::make_shared<internal::SpdLogger>(spd, LogLevel::kTrace));

  ICEBERG_LOG_INFO("v={}", 9);
  GetDefaultLogger()->Flush();
  EXPECT_NE(out.str().find("v=9"), std::string::npos);
}
#endif  // ICEBERG_HAS_SPDLOG

}  // namespace iceberg
