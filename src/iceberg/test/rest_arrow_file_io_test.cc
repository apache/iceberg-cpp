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
/// \brief Covers REST -> ResolvingFileIO -> FileIORegistry -> ArrowS3FileIO with
/// the real registered Arrow FileIO, which a mock delegate cannot exercise.

#include <algorithm>
#include <memory>
#include <string>
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

namespace iceberg::rest {

namespace {

bool HasWarning(const CapturingLogger& logger) {
  const auto records = logger.records();
  return std::ranges::any_of(
      records, [](const LogMessage& record) { return record.level == LogLevel::kWarn; });
}

class RestArrowFileIOTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { iceberg::arrow::RegisterAll(); }
  static void TearDownTestSuite() { std::ignore = iceberg::arrow::FinalizeS3(); }
};

// A break anywhere in the chain (scheme routing, credential forwarding, or the
// S3 delegate dropping the `oss://` prefix) shows up as the warning.
TEST_F(RestArrowFileIOTest, AppliesOssCredentialThroughRealArrowS3FileIO) {
  auto logger = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger scoped(logger);

  auto io =
      MakeTableFileIO({{"warehouse", "logical_warehouse_name"}}, /*table_config=*/{},
                      {{.prefix = "oss://bucket/table", .config = {{"k", "v"}}}});
  ASSERT_THAT(io, IsOk());

  // Opening builds the delegate and applies the credential; the open itself
  // hits the network, so its result is irrelevant here.
  std::ignore = io.value()->NewInputFile("oss://bucket/table/data/file.parquet");
  EXPECT_FALSE(HasWarning(*logger));
}

}  // namespace

}  // namespace iceberg::rest
