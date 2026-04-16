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
#include <iostream>
#include <optional>
#include <string>
#include <unordered_map>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/arrow/s3/s3_properties.h"
#include "iceberg/test/matchers.h"

namespace {

std::optional<std::string> GetEnvIfSet(const char* key) {
  const char* value = std::getenv(key);
  if (value == nullptr || std::string_view(value).empty()) {
    return std::nullopt;
  }
  return std::string(value);
}

std::string MakeObjectUri(std::string_view base_uri, std::string_view object_name) {
  std::string object_uri(base_uri);
  if (!object_uri.ends_with('/')) {
    object_uri += '/';
  }
  object_uri += object_name;
  return object_uri;
}

std::unordered_map<std::string, std::string> PropertiesFromEnv() {
  std::unordered_map<std::string, std::string> properties;

  if (const auto access_key = GetEnvIfSet("AWS_ACCESS_KEY_ID")) {
    properties[std::string(iceberg::arrow::S3Properties::kAccessKeyId)] = *access_key;
  }
  if (const auto secret_key = GetEnvIfSet("AWS_SECRET_ACCESS_KEY")) {
    properties[std::string(iceberg::arrow::S3Properties::kSecretAccessKey)] = *secret_key;
  }
  if (const auto endpoint = GetEnvIfSet("ICEBERG_TEST_S3_ENDPOINT")) {
    properties[std::string(iceberg::arrow::S3Properties::kEndpoint)] = *endpoint;
  }
  if (const auto region = GetEnvIfSet("AWS_REGION")) {
    properties[std::string(iceberg::arrow::S3Properties::kRegion)] = *region;
  }

  return properties;
}

}  // namespace

namespace iceberg::arrow {

namespace {

class ArrowS3FileIOTest : public ::testing::Test {
 protected:
  static void TearDownTestSuite() {
    auto status = FinalizeS3();
    if (!status.has_value()) {
      std::cerr << "Warning: FinalizeS3 failed: " << status.error().message << std::endl;
    }
  }

  void SetUp() override { base_uri_ = GetEnvIfSet("ICEBERG_TEST_S3_URI"); }

  std::string ObjectUri(std::string_view object_name) const {
    return MakeObjectUri(*base_uri_, object_name);
  }

  void RequireIntegrationEnv() const {
    if (!base_uri_.has_value()) {
      GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
    }
  }

 private:
  std::optional<std::string> base_uri_;
};

}  // namespace

TEST_F(ArrowS3FileIOTest, CreateWithDefaultProperties) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  EXPECT_NE(result.value(), nullptr);
}

TEST_F(ArrowS3FileIOTest, RequiresS3SupportAtBuildTime) {
  auto result = MakeS3FileIO();
  ASSERT_THAT(result, IsOk());
}

TEST_F(ArrowS3FileIOTest, RejectsIncompleteStaticCredentials) {
  auto result =
      MakeS3FileIO({{std::string(S3Properties::kAccessKeyId), "access-key-only"}});
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage(
                          "S3 client access key ID and secret access key must be set"));
}

TEST_F(ArrowS3FileIOTest, RejectsInvalidBooleanProperties) {
  auto result =
      MakeS3FileIO({{std::string(S3Properties::kPathStyleAccess), "not-a-bool"}});
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

TEST_F(ArrowS3FileIOTest, ReadWriteFile) {
  RequireIntegrationEnv();
  auto io_res = MakeS3FileIO();
  ASSERT_THAT(io_res, IsOk());
  auto io = std::move(io_res).value();

  auto object_uri = ObjectUri("iceberg_s3_io_test.txt");
  auto write_res = io->WriteFile(object_uri, "hello s3");
  ASSERT_THAT(write_res, IsOk());

  auto read_res = io->ReadFile(object_uri, std::nullopt);
  ASSERT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("hello s3")));

  auto del_res = io->DeleteFile(object_uri);
  EXPECT_THAT(del_res, IsOk());
}

TEST_F(ArrowS3FileIOTest, MakeS3FileIOWithProperties) {
  RequireIntegrationEnv();
  auto io_res = MakeS3FileIO(PropertiesFromEnv());
  ASSERT_THAT(io_res, IsOk());
  auto io = std::move(io_res).value();

  auto object_uri = ObjectUri("iceberg_s3_io_props_test.txt");
  auto write_res = io->WriteFile(object_uri, "hello s3 with properties");
  ASSERT_THAT(write_res, IsOk());

  auto read_res = io->ReadFile(object_uri, std::nullopt);
  ASSERT_THAT(read_res, IsOk());
  EXPECT_THAT(read_res, HasValue(::testing::Eq("hello s3 with properties")));

  auto del_res = io->DeleteFile(object_uri);
  EXPECT_THAT(del_res, IsOk());
}

TEST_F(ArrowS3FileIOTest, MakeS3FileIOWithSslDisabled) {
  RequireIntegrationEnv();
  std::unordered_map<std::string, std::string> properties;
  properties[std::string(S3Properties::kSslEnabled)] = "false";

  auto io_res = MakeS3FileIO(properties);
  ASSERT_THAT(io_res, IsOk());
}

TEST_F(ArrowS3FileIOTest, MakeS3FileIOWithTimeouts) {
  RequireIntegrationEnv();
  std::unordered_map<std::string, std::string> properties;
  properties[std::string(S3Properties::kConnectTimeoutMs)] = "5000";
  properties[std::string(S3Properties::kSocketTimeoutMs)] = "10000";

  auto io_res = MakeS3FileIO(properties);
  ASSERT_THAT(io_res, IsOk());
}

}  // namespace iceberg::arrow
