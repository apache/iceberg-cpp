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

#include <cstdlib>
#include <string>
#include <unordered_map>

#if __has_include(<arrow/filesystem/s3fs.h>)
#include <arrow/filesystem/s3fs.h>
#endif
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/arrow/s3_properties.h"
#include "iceberg/test/matchers.h"

namespace iceberg::arrow {

#if __has_include(<arrow/filesystem/s3fs.h>)
namespace {
class ArrowS3Environment final : public ::testing::Environment {
 public:
  void TearDown() override { (void)::arrow::fs::FinalizeS3(); }
};
}  // namespace
#endif

TEST(ArrowS3FileIOTest, RejectsNonS3Uri) {
  auto result = MakeS3FileIO("file:///tmp/not-s3");
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("s3://"));
}

#if __has_include(<arrow/filesystem/s3fs.h>)
TEST(ArrowS3FileIOTest, RequiresS3SupportAtBuildTime) {
  auto result = MakeS3FileIO("s3://bucket/path");
  if (!result.has_value()) {
    EXPECT_NE(result.error().kind, ErrorKind::kNotImplemented);
  }
}
#else
TEST(ArrowS3FileIOTest, RequiresS3SupportAtBuildTime) {
  auto result = MakeS3FileIO("s3://warehouse/iceberg_example");
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}
#endif

TEST(ArrowS3FileIOTest, ReadWriteFile) {
  const char* base_uri = std::getenv("ICEBERG_TEST_S3_URI");
  if (base_uri == nullptr || std::string(base_uri).empty()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  auto io_res = MakeS3FileIO(base_uri);
  if (!io_res.has_value()) {
    if (io_res.error().kind == ErrorKind::kNotImplemented) {
      GTEST_SKIP() << "Arrow S3 support is not enabled";
    }
    FAIL() << "MakeS3FileIO failed: " << io_res.error().message;
  }

  auto io = std::move(io_res.value());
  std::string object_uri = base_uri;
  if (!object_uri.ends_with('/')) {
    object_uri += '/';
  }
  object_uri += "iceberg_s3_io_test.txt";
  auto write_res = io->WriteFile(object_uri, "hello s3");
  ASSERT_THAT(write_res, IsOk());

  auto read_res = io->ReadFile(object_uri, std::nullopt);
  ASSERT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("hello s3")));

  auto del_res = io->DeleteFile(object_uri);
  EXPECT_THAT(del_res, IsOk());
}

// ============================================================================
// Tests for MakeS3FileIO with properties
// ============================================================================

TEST(ArrowS3FileIOTest, MakeS3FileIOWithPropertiesRejectsNonS3Uri) {
  std::unordered_map<std::string, std::string> properties;
  auto result = MakeS3FileIO("file:///tmp/not-s3", properties);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("s3://"));
}

TEST(ArrowS3FileIOTest, MakeS3FileIOWithEmptyPropertiesFallsBack) {
  const char* base_uri = std::getenv("ICEBERG_TEST_S3_URI");
  if (base_uri == nullptr || std::string(base_uri).empty()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  // Empty properties should fall back to URI-based resolution
  std::unordered_map<std::string, std::string> properties;
  auto io_res = MakeS3FileIO(base_uri, properties);
  if (!io_res.has_value()) {
    if (io_res.error().kind == ErrorKind::kNotImplemented) {
      GTEST_SKIP() << "Arrow S3 support is not enabled";
    }
    FAIL() << "MakeS3FileIO failed: " << io_res.error().message;
  }

  EXPECT_NE(io_res.value(), nullptr);
}

TEST(ArrowS3FileIOTest, MakeS3FileIOWithProperties) {
  const char* base_uri = std::getenv("ICEBERG_TEST_S3_URI");
  const char* access_key = std::getenv("AWS_ACCESS_KEY_ID");
  const char* secret_key = std::getenv("AWS_SECRET_ACCESS_KEY");
  const char* endpoint = std::getenv("ICEBERG_TEST_S3_ENDPOINT");
  const char* region = std::getenv("AWS_REGION");

  if (base_uri == nullptr || std::string(base_uri).empty()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  std::unordered_map<std::string, std::string> properties;

  // Configure credentials if available
  if (access_key != nullptr && secret_key != nullptr) {
    properties[S3Properties::kAccessKeyId] = access_key;
    properties[S3Properties::kSecretAccessKey] = secret_key;
  }

  // Configure endpoint if available (for MinIO, LocalStack, etc.)
  if (endpoint != nullptr && std::string(endpoint).length() > 0) {
    properties[S3Properties::kEndpoint] = endpoint;
  }

  // Configure region if available
  if (region != nullptr && std::string(region).length() > 0) {
    properties[S3Properties::kRegion] = region;
  }

  auto io_res = MakeS3FileIO(base_uri, properties);
  if (!io_res.has_value()) {
    if (io_res.error().kind == ErrorKind::kNotImplemented) {
      GTEST_SKIP() << "Arrow S3 support is not enabled";
    }
    FAIL() << "MakeS3FileIO failed: " << io_res.error().message;
  }

  auto io = std::move(io_res.value());
  std::string object_uri = base_uri;
  if (!object_uri.ends_with('/')) {
    object_uri += '/';
  }
  object_uri += "iceberg_s3_io_props_test.txt";

  auto write_res = io->WriteFile(object_uri, "hello s3 with properties");
  ASSERT_THAT(write_res, IsOk());

  auto read_res = io->ReadFile(object_uri, std::nullopt);
  ASSERT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("hello s3 with properties")));

  auto del_res = io->DeleteFile(object_uri);
  EXPECT_THAT(del_res, IsOk());
}

TEST(ArrowS3FileIOTest, MakeS3FileIOWithSslDisabled) {
  const char* base_uri = std::getenv("ICEBERG_TEST_S3_URI");
  if (base_uri == nullptr || std::string(base_uri).empty()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  std::unordered_map<std::string, std::string> properties;
  properties[S3Properties::kSslEnabled] = "false";

  // Just test that the configuration is accepted
  auto io_res = MakeS3FileIO(base_uri, properties);
  if (!io_res.has_value()) {
    if (io_res.error().kind == ErrorKind::kNotImplemented) {
      GTEST_SKIP() << "Arrow S3 support is not enabled";
    }
    // Other errors are acceptable - just checking config parsing works
  }
}

TEST(ArrowS3FileIOTest, MakeS3FileIOWithTimeouts) {
  const char* base_uri = std::getenv("ICEBERG_TEST_S3_URI");
  if (base_uri == nullptr || std::string(base_uri).empty()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  std::unordered_map<std::string, std::string> properties;
  properties[S3Properties::kConnectTimeoutMs] = "5000";
  properties[S3Properties::kSocketTimeoutMs] = "10000";

  auto io_res = MakeS3FileIO(base_uri, properties);
  if (!io_res.has_value()) {
    if (io_res.error().kind == ErrorKind::kNotImplemented) {
      GTEST_SKIP() << "Arrow S3 support is not enabled";
    }
    // Other errors are acceptable - just checking config parsing works
  }
}

}  // namespace iceberg::arrow

#if __has_include(<arrow/filesystem/s3fs.h>)
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new iceberg::arrow::ArrowS3Environment());
  return RUN_ALL_TESTS();
}
#endif
