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

/// \file
/// \brief Covers REST -> ResolvingFileIO -> registry -> Arrow FileIO against the
/// real registered implementations, which mock delegates cannot exercise.

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/arrow/arrow_register.h"
#include "iceberg/catalog/rest/rest_file_io.h"
#include "iceberg/logging/logger.h"
#include "iceberg/storage_credential.h"
#include "iceberg/test/logging_test_helpers.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"

namespace iceberg::rest {

namespace {

bool HasWarning(const CapturingLogger& logger) {
  const auto records = logger.records();
  return std::ranges::any_of(
      records, [](const LogMessage& record) { return record.level == LogLevel::kWarn; });
}

class RestArrowFileIOTest : public TempFileTestBase {
 protected:
  static void SetUpTestSuite() { iceberg::arrow::RegisterAll(); }
  static void TearDownTestSuite() { std::ignore = iceberg::arrow::FinalizeS3(); }
};

TEST_F(RestArrowFileIOTest, ReadsBackWhatItWroteThroughRealLocalFileIO) {
  auto io = MakeTableFileIO({{"warehouse", "logical_warehouse_name"}},
                            /*table_config=*/{}, /*storage_credentials=*/{});
  ASSERT_THAT(io, IsOk());

  const auto path = CreateNewTempFilePathWithSuffix(".txt");
  constexpr std::string_view kContent = "resolved through the real local FileIO";

  ASSERT_THAT(io.value()->WriteFile(path, kContent), IsOk());
  EXPECT_THAT(io.value()->ReadFile(path, std::nullopt),
              HasValue(::testing::Eq(std::string(kContent))));
  EXPECT_THAT(io.value()->DeleteFile(path), IsOk());
}

#if ICEBERG_S3_ENABLED

TEST_F(RestArrowFileIOTest, AppliesOssCredentialThroughRealArrowS3FileIO) {
  auto logger = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger scoped(logger);

  auto io =
      MakeTableFileIO({{"warehouse", "logical_warehouse_name"}}, /*table_config=*/{},
                      {{.prefix = "oss://bucket/table", .config = {{"k", "v"}}}});
  ASSERT_THAT(io, IsOk());

  // Opening builds the delegate and applies the credential. The open itself hits
  // the network, so only the failure modes before that are asserted: a routing
  // break surfaces as kNotSupported, and a dropped credential as the warning.
  auto input = io.value()->NewInputFile("oss://bucket/table/data/file.parquet");
  EXPECT_THAT(input, ::testing::Not(IsError(ErrorKind::kNotSupported)));
  EXPECT_FALSE(HasWarning(*logger));
}

#endif  // ICEBERG_S3_ENABLED

}  // namespace

}  // namespace iceberg::rest
