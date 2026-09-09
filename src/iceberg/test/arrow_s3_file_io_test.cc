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

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#if ICEBERG_S3_ENABLED
#  include <arrow/filesystem/s3fs.h>
#endif

#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/arrow/s3/s3_properties.h"
#include "iceberg/file_io.h"
#include "iceberg/logging/logger.h"
#include "iceberg/result.h"
#include "iceberg/storage_credential.h"
#include "iceberg/test/logging_test_helpers.h"
#include "iceberg/test/matchers.h"
#include "iceberg/util/macros.h"

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
    properties[std::string(iceberg::arrow::S3Properties::kClientRegion)] = *region;
  }

  return properties;
}

std::unordered_map<std::string, std::string> BadS3Credentials() {
  return {
      {std::string(iceberg::arrow::S3Properties::kAccessKeyId), "bad-access-key"},
      {std::string(iceberg::arrow::S3Properties::kSecretAccessKey), "bad-secret-key"}};
}

}  // namespace

namespace iceberg::arrow {

#if ICEBERG_S3_ENABLED
Result<::arrow::fs::S3Options> ConfigureS3Options(
    const std::unordered_map<std::string, std::string>& properties);
#endif

namespace {

class ArrowS3FileIOTest : public ::testing::Test {
 protected:
#if ICEBERG_S3_ENABLED
  static void SetUpTestSuite() {
    // Off EC2 every S3 client build waits for this to time out. Not overwritten,
    // so a run that does want those credentials can still ask.
#  ifdef _WIN32
    if (std::getenv("AWS_EC2_METADATA_DISABLED") == nullptr) {
      _putenv_s("AWS_EC2_METADATA_DISABLED", "true");
    }
#  else
    ::setenv("AWS_EC2_METADATA_DISABLED", "true", /*overwrite=*/0);
#  endif
    auto io = MakeS3FileIO({});
    ASSERT_THAT(io, IsOk());
  }
#endif

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

  const std::string& BaseUri() const { return *base_uri_; }

  bool HasIntegrationEnv() const { return base_uri_.has_value(); }

 private:
  std::optional<std::string> base_uri_;
};

bool HasWarning(const CapturingLogger& logger) {
  const auto records = logger.records();
  return std::ranges::any_of(
      records, [](const LogMessage& record) { return record.level == LogLevel::kWarn; });
}

Status CheckReadWrite(FileIO& io, const std::string& object_uri,
                      std::string_view content) {
  ICEBERG_RETURN_UNEXPECTED(io.WriteFile(object_uri, content));
  ICEBERG_ASSIGN_OR_RAISE(auto read, io.ReadFile(object_uri, std::nullopt));
  EXPECT_EQ(read, std::string(content));
  return io.DeleteFile(object_uri);
}

}  // namespace

TEST_F(ArrowS3FileIOTest, Create) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  EXPECT_NE(result.value(), nullptr);
}

TEST_F(ArrowS3FileIOTest, StoresCredentials) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  std::vector<StorageCredential> credentials = {
      {.prefix = "s3://bucket/table",
       .config = {{std::string(S3Properties::kAccessKeyId), "access-key"},
                  {std::string(S3Properties::kSecretAccessKey), "secret"}}}};
  EXPECT_THAT(credentialed->SetStorageCredentials(credentials), IsOk());
  EXPECT_EQ(credentialed->credentials(), credentials);
}

TEST_F(ArrowS3FileIOTest, SkipsNonS3CredentialPrefix) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  // A server may vend credentials for several storage systems at once.
  auto logger = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger scoped(logger);
  std::vector<StorageCredential> credentials = {
      {.prefix = "gs://bucket/table", .config = {{"k", "v"}}},
      {.prefix = "s3://bucket/table",
       .config = {{std::string(S3Properties::kAccessKeyId), "access-key"},
                  {std::string(S3Properties::kSecretAccessKey), "secret"}}}};
  EXPECT_THAT(credentialed->SetStorageCredentials(credentials), IsOk());
  EXPECT_EQ(credentialed->credentials(), credentials);
  // The whole list is retained, but only the S3 one is applied — and it must
  // be, otherwise the skip silently degrades to "no credentials at all".
  EXPECT_FALSE(HasWarning(*logger));
}

// Every prefix form this FileIO serves must be accepted without the warning;
// real selection is covered by AppliesOssCredentialInRealRoundTrip.
TEST_F(ArrowS3FileIOTest, AcceptsEveryS3CompatibleCredentialPrefix) {
  for (std::string_view prefix :
       {"s3", "s3://bucket/table", "s3a://bucket/table", "s3n://bucket/table",
        "oss://bucket/table", "OSS://bucket/table"}) {
    SCOPED_TRACE(prefix);
    auto result = MakeS3FileIO({});
    ASSERT_THAT(result, IsOk());
    auto* credentialed = result.value()->AsSupportsStorageCredentials();
    ASSERT_NE(credentialed, nullptr);

    auto logger = std::make_shared<CapturingLogger>();
    ScopedDefaultLogger scoped(logger);
    std::vector<StorageCredential> credentials = {
        {.prefix = std::string(prefix),
         .config = {{std::string(S3Properties::kAccessKeyId), "access-key"},
                    {std::string(S3Properties::kSecretAccessKey), "secret"}}}};
    EXPECT_THAT(credentialed->SetStorageCredentials(credentials), IsOk());
    EXPECT_FALSE(HasWarning(*logger));
  }
}

TEST_F(ArrowS3FileIOTest, WarnsWhenNoCredentialApplies) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  // Succeeds (S3 falls back to the default credentials) but must not be silent.
  // Bare `S3` is foreign: only URI-form prefixes match case-insensitively.
  auto logger = std::make_shared<CapturingLogger>();
  ScopedDefaultLogger scoped(logger);
  std::vector<StorageCredential> credentials = {
      {.prefix = "gs://bucket/table", .config = {{"k", "v"}}},
      {.prefix = "S3", .config = {{"k", "v"}}}};
  EXPECT_THAT(credentialed->SetStorageCredentials(credentials), IsOk());
  EXPECT_EQ(credentialed->credentials(), credentials);
  EXPECT_TRUE(HasWarning(*logger));
}

TEST_F(ArrowS3FileIOTest, DeleteFilesDispatchesAcrossCredentialPrefixes) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  auto credential = [](std::string_view prefix, std::string_view access_key) {
    return StorageCredential{
        .prefix = std::string(prefix),
        .config = {{std::string(S3Properties::kAccessKeyId), std::string(access_key)},
                   {std::string(S3Properties::kSecretAccessKey), "secret"}}};
  };
  ASSERT_THAT(credentialed->SetStorageCredentials({credential("s3://bucket-a", "key-a"),
                                                   credential("s3://bucket-b", "key-b")}),
              IsOk());

  auto status = result.value()->DeleteFiles({"s3://bucket-a/%ZZ.parquet",
                                             "s3://bucket-a/second.parquet",
                                             "s3://bucket-b/other.parquet"});
  EXPECT_THAT(status, HasErrorMessage("Cannot parse URI"));
}

TEST_F(ArrowS3FileIOTest, OperationsSurviveConcurrentCredentialInstalls) {
  auto result = MakeS3FileIO({});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  auto credential = [](std::string_view access_key) {
    return StorageCredential{
        .prefix = "s3://bucket",
        .config = {{std::string(S3Properties::kAccessKeyId), std::string(access_key)},
                   {std::string(S3Properties::kSecretAccessKey), "secret"}}};
  };
  ASSERT_THAT(credentialed->SetStorageCredentials({credential("first")}), IsOk());

  std::atomic<bool> stop = false;
  std::atomic<int> failures = 0;
  std::vector<std::thread> operations;
  operations.reserve(4);
  for (int i = 0; i < 4; ++i) {
    operations.emplace_back([&] {
      while (!stop.load()) {
        if (!result.value()->NewInputFile("s3://bucket/key").has_value()) {
          ++failures;
        }
      }
    });
  }
  // No assertions until the threads are joined: a fatal assertion here would
  // destroy joinable threads and terminate the binary, masking the failure.
  Status install_status = {};
  for (int round = 0; round < 3 && install_status.has_value(); ++round) {
    install_status = credentialed->SetStorageCredentials({credential("replacement")});
  }
  stop = true;
  for (auto& operation : operations) {
    operation.join();
  }
  ASSERT_THAT(install_status, IsOk());
  EXPECT_EQ(failures, 0);
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

TEST_F(ArrowS3FileIOTest, ReadWrite) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }
  auto io_res = MakeS3FileIO();
  ASSERT_THAT(io_res, IsOk());
  auto io = std::move(io_res).value();

  auto object_uri = ObjectUri("iceberg_s3_io_test.txt");
  EXPECT_THAT(CheckReadWrite(*io, object_uri, "hello s3"), IsOk());
}

TEST_F(ArrowS3FileIOTest, ReadWriteWithProperties) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }
  auto io_res = MakeS3FileIO(PropertiesFromEnv());
  ASSERT_THAT(io_res, IsOk());
  auto io = std::move(io_res).value();

  auto object_uri = ObjectUri("iceberg_s3_io_props_test.txt");
  EXPECT_THAT(CheckReadWrite(*io, object_uri, "hello s3 with properties"), IsOk());
}

TEST_F(ArrowS3FileIOTest, LongestCredentialPrefix) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  auto properties = PropertiesFromEnv();
  if (properties.empty()) {
    GTEST_SKIP() << "Set S3 properties to enable credential routing test";
  }

  auto io_res = MakeS3FileIO(properties);
  ASSERT_THAT(io_res, IsOk());
  auto io = std::move(io_res).value();
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  constexpr std::string_view object_name = "iceberg_s3_io_prefix_test.txt";
  auto object_uri = ObjectUri(object_name);
  const auto partial_prefix =
      object_uri.substr(0, object_uri.size() - object_name.size() + 3);

  auto bad_properties = BadS3Credentials();
  EXPECT_THAT(credentialed->SetStorageCredentials(
                  {{.prefix = BaseUri(), .config = std::move(bad_properties)},
                   {.prefix = partial_prefix, .config = properties}}),
              IsOk());
  EXPECT_THAT(CheckReadWrite(*io, object_uri, "hello s3 with vended credentials"),
              IsOk());
}

// The credential is vended under the oss spelling and the object addressed as
// `s3://`, so they only meet through canonicalization — and every other path
// to authentication is broken. (rest_arrow_file_io_test covers the mirrored
// direction.)
TEST_F(ArrowS3FileIOTest, AppliesOssCredentialInRealRoundTrip) {
  if (!HasIntegrationEnv()) {
    GTEST_SKIP() << "Set ICEBERG_TEST_S3_URI to enable S3 IO test";
  }

  auto properties = PropertiesFromEnv();
  if (!properties.contains(std::string(S3Properties::kAccessKeyId)) ||
      !properties.contains(std::string(S3Properties::kSecretAccessKey))) {
    GTEST_SKIP() << "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to enable "
                    "credential routing test";
  }

  auto bad_defaults = properties;
  for (const auto& [key, value] : BadS3Credentials()) {
    bad_defaults.insert_or_assign(key, value);
  }
  auto io_res = MakeS3FileIO(std::move(bad_defaults));
  ASSERT_THAT(io_res, IsOk());
  auto io = std::move(io_res).value();
  auto* credentialed = io->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  constexpr std::string_view object_name = "iceberg_oss_credential_test.txt";
  const auto object_uri = ObjectUri(object_name);
  const auto scheme_end = object_uri.find("://");
  ASSERT_NE(scheme_end, std::string::npos) << "ICEBERG_TEST_S3_URI must carry a scheme";
  // Both spellings are forced, so they cross whatever scheme the env URI uses.
  const auto oss_spelling = std::string("oss").append(object_uri.substr(scheme_end));
  const auto oss_prefix =
      oss_spelling.substr(0, oss_spelling.size() - object_name.size());
  const auto s3_uri = std::string("s3").append(object_uri.substr(scheme_end));

  EXPECT_THAT(credentialed->SetStorageCredentials(
                  {{.prefix = oss_prefix, .config = std::move(properties)}}),
              IsOk());
  EXPECT_THAT(CheckReadWrite(*io, s3_uri, "hello oss with vended credentials"), IsOk());
}

#if ICEBERG_S3_ENABLED
TEST_F(ArrowS3FileIOTest, ClientRegion) {
  auto result =
      ConfigureS3Options({{std::string(S3Properties::kClientRegion), "us-east-1"}});
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result->region, "us-east-1");
}

TEST_F(ArrowS3FileIOTest, EndpointScheme) {
  struct Case {
    std::string_view endpoint;
    std::string_view endpoint_override;
    std::string_view scheme;
  };
  const std::vector<Case> cases = {
      {"https://s3.example.com:443", "s3.example.com:443", "https"},
      {"http://localhost:9000", "localhost:9000", "http"},
      {"localhost:9000", "localhost:9000", "https"}};

  for (const auto& test_case : cases) {
    auto result = ConfigureS3Options(
        {{std::string(S3Properties::kEndpoint), std::string(test_case.endpoint)}});
    ASSERT_THAT(result, IsOk()) << test_case.endpoint;
    EXPECT_EQ(result->endpoint_override, test_case.endpoint_override);
    EXPECT_EQ(result->scheme, test_case.scheme);
  }
}

TEST_F(ArrowS3FileIOTest, SslEnabled) {
  auto https =
      ConfigureS3Options({{std::string(S3Properties::kEndpoint), "http://localhost:9000"},
                          {std::string(S3Properties::kSslEnabled), "TRUE"}});
  ASSERT_THAT(https, IsOk());
  EXPECT_EQ(https->scheme, "https");

  auto http = ConfigureS3Options(
      {{std::string(S3Properties::kEndpoint), "https://localhost:9000"},
       {std::string(S3Properties::kSslEnabled), "FaLsE"}});
  ASSERT_THAT(http, IsOk());
  EXPECT_EQ(http->scheme, "http");
}

TEST_F(ArrowS3FileIOTest, PathStyleAccess) {
  auto virtual_addressing =
      ConfigureS3Options({{std::string(S3Properties::kPathStyleAccess), "FALSE"}});
  ASSERT_THAT(virtual_addressing, IsOk());
  EXPECT_TRUE(virtual_addressing->force_virtual_addressing);

  auto path_style =
      ConfigureS3Options({{std::string(S3Properties::kPathStyleAccess), "TrUe"}});
  ASSERT_THAT(path_style, IsOk());
  EXPECT_FALSE(path_style->force_virtual_addressing);
}

TEST_F(ArrowS3FileIOTest, Timeouts) {
  auto result =
      ConfigureS3Options({{std::string(S3Properties::kConnectTimeoutMs), "5000"},
                          {std::string(S3Properties::kSocketTimeoutMs), "10000"}});
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result->connect_timeout, 5);
  EXPECT_EQ(result->request_timeout, 10);
}
#endif

}  // namespace iceberg::arrow
