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

#include "iceberg/catalog/rest/rest_file_io.h"

#include <memory>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/catalog/rest/types.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

namespace {

class MockFileIO : public FileIO {
 public:
  Result<std::string> ReadFile(const std::string& /*file_location*/,
                               std::optional<size_t> /*length*/) override {
    return std::string("mock");
  }

  Status WriteFile(const std::string& /*file_location*/,
                   std::string_view /*content*/) override {
    return {};
  }

  Status DeleteFile(const std::string& /*file_location*/) override { return {}; }
};

}  // namespace

TEST(RestFileIOTest, DetectBuiltinKindFromScheme) {
  EXPECT_THAT(DetectBuiltinFileIO("s3://bucket/path"),
              HasValue(::testing::Eq(BuiltinFileIOKind::kArrowS3)));
  EXPECT_THAT(DetectBuiltinFileIO("s3a://bucket/path"),
              HasValue(::testing::Eq(BuiltinFileIOKind::kArrowS3)));
  EXPECT_THAT(DetectBuiltinFileIO("s3n://bucket/path"),
              HasValue(::testing::Eq(BuiltinFileIOKind::kArrowS3)));
  EXPECT_THAT(DetectBuiltinFileIO("/tmp/warehouse"),
              HasValue(::testing::Eq(BuiltinFileIOKind::kArrowLocal)));
  EXPECT_THAT(DetectBuiltinFileIO("file:///tmp/warehouse"),
              HasValue(::testing::Eq(BuiltinFileIOKind::kArrowLocal)));
}

TEST(RestFileIOTest, DetectBuiltinKindRejectsUnsupportedScheme) {
  auto result = DetectBuiltinFileIO("gs://bucket/warehouse");
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
  EXPECT_THAT(result, HasErrorMessage("not supported for automatic FileIO resolution"));
}

TEST(RestFileIOTest, MakeCatalogFileIOMissingImplAndWarehouse) {
  auto result = MakeCatalogFileIO(RestCatalogProperties::default_properties());
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

TEST(RestFileIOTest, MakeCatalogFileIORejectsIncompatibleWarehouse) {
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowS3FileIO),
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  auto config = RestCatalogProperties::FromMap(
      {{"io-impl", std::string(FileIORegistry::kArrowS3FileIO)},
       {"warehouse", "/tmp/warehouse"}});
  auto result = MakeCatalogFileIO(config);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("incompatible"));
}

TEST(RestFileIOTest, MakeCatalogFileIOAutoDetectsFromWarehouse) {
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowLocalFileIO),
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  auto config = RestCatalogProperties::FromMap({{"warehouse", "/tmp/warehouse"}});
  auto result = MakeCatalogFileIO(config);
  ASSERT_THAT(result, IsOk());
}

TEST(RestFileIOTest, MakeCatalogFileIORejectsUnsupportedWarehouseScheme) {
  auto config = RestCatalogProperties::FromMap({{"warehouse", "gs://bucket/warehouse"}});
  auto result = MakeCatalogFileIO(config);
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
  EXPECT_THAT(result, HasErrorMessage("not supported for automatic FileIO resolution"));
}

TEST(RestFileIOTest, MakeCatalogFileIOAllowsCompatibleWarehouse) {
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowS3FileIO),
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  auto config = RestCatalogProperties::FromMap(
      {{"io-impl", std::string(FileIORegistry::kArrowS3FileIO)},
       {"warehouse", "s3://my-bucket/warehouse"}});
  auto result = MakeCatalogFileIO(config);
  ASSERT_THAT(result, IsOk());
}

TEST(RestFileIOTest, MakeCatalogFileIOPassesThroughCustomImpl) {
  const std::string custom_impl = "com.mycompany.CustomFileIO";
  FileIORegistry::Register(
      custom_impl,
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  auto config = RestCatalogProperties::FromMap(
      {{"io-impl", custom_impl}, {"warehouse", "/tmp/warehouse"}});
  auto result = MakeCatalogFileIO(config);
  ASSERT_THAT(result, IsOk());
}

TEST(RestFileIOTest, MakeCatalogFileIOUnregisteredCustomImplReturnsNotFound) {
  auto config = RestCatalogProperties::FromMap(
      {{"io-impl", "com.nonexistent.FileIO"}, {"warehouse", "/tmp/warehouse"}});
  auto result = MakeCatalogFileIO(config);
  EXPECT_THAT(result, IsError(ErrorKind::kNotFound));
}

TEST(RestFileIOTest, MakeCatalogFileIOSkipsCheckWhenWarehouseAbsent) {
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowLocalFileIO),
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  auto config = RestCatalogProperties::FromMap(
      {{"io-impl", std::string(FileIORegistry::kArrowLocalFileIO)}});
  auto result = MakeCatalogFileIO(config);
  ASSERT_THAT(result, IsOk());
}

TEST(RestFileIOTest, SelectS3StorageCredentialPicksLongestMatchingS3Prefix) {
  std::vector<StorageCredential> credentials = {
      {.prefix = "s3", .config = {{"s3.access-key-id", "a"}}},
      {.prefix = "s3://bucket/data", .config = {{"s3.access-key-id", "b"}}},
      {.prefix = "s3://bucket", .config = {{"s3.access-key-id", "c"}}},
  };
  const auto* cred = SelectS3StorageCredential(credentials, "s3://bucket/data/db/t");
  ASSERT_NE(cred, nullptr);
  EXPECT_EQ(cred->prefix, "s3://bucket/data");
}

TEST(RestFileIOTest, SelectS3StorageCredentialMatchesAgainstStorageLocation) {
  std::vector<StorageCredential> credentials = {
      // Globally longest prefix, but for a path that does not cover this table.
      {.prefix = "s3://other/very/long/prefix", .config = {{"s3.access-key-id", "x"}}},
      {.prefix = "s3://bucket", .config = {{"s3.access-key-id", "y"}}},
  };
  const auto* cred = SelectS3StorageCredential(credentials, "s3://bucket/db/t/data");
  ASSERT_NE(cred, nullptr);
  // The longest *matching* prefix wins, not the globally longest one.
  EXPECT_EQ(cred->prefix, "s3://bucket");
}

TEST(RestFileIOTest, SelectS3StorageCredentialMatchesEquivalentS3Schemes) {
  std::vector<StorageCredential> s3_cred = {
      {.prefix = "s3://bucket", .config = {{"s3.access-key-id", "a"}}},
  };
  // s3a:// and s3n:// locations match an s3:// vended prefix (same backend).
  EXPECT_NE(SelectS3StorageCredential(s3_cred, "s3a://bucket/db/t"), nullptr);
  EXPECT_NE(SelectS3StorageCredential(s3_cred, "s3n://bucket/db/t"), nullptr);

  // ...and the inverse: an s3a:// prefix matches an s3:// location.
  std::vector<StorageCredential> s3a_cred = {
      {.prefix = "s3a://bucket", .config = {{"s3.access-key-id", "b"}}},
  };
  const auto* cred = SelectS3StorageCredential(s3a_cred, "s3://bucket/db/t");
  ASSERT_NE(cred, nullptr);
  EXPECT_EQ(cred->prefix, "s3a://bucket");

  // OSS is S3-compatible: an `s3` credential matches an oss:// location (Alibaba
  // DLF vends an `s3` credential for tables whose location uses the oss:// scheme).
  EXPECT_NE(SelectS3StorageCredential(s3_cred, "oss://bucket/db/t"), nullptr);
}

TEST(RestFileIOTest, SelectS3StorageCredentialIgnoresNonS3Prefixes) {
  std::vector<StorageCredential> credentials = {
      {.prefix = "gs://bucket", .config = {{"k", "v"}}},
      {.prefix = "s3", .config = {{"s3.access-key-id", "a"}}},
  };
  const auto* cred = SelectS3StorageCredential(credentials, "s3://bucket/data");
  ASSERT_NE(cred, nullptr);
  EXPECT_EQ(cred->prefix, "s3");
}

TEST(RestFileIOTest, SelectS3StorageCredentialReturnsNullWhenNoneMatch) {
  std::vector<StorageCredential> credentials = {
      {.prefix = "gs://bucket", .config = {{"k", "v"}}},
  };
  // Non-S3 prefix, S3 location.
  EXPECT_EQ(SelectS3StorageCredential(credentials, "s3://bucket"), nullptr);
  // S3 credential whose prefix does not cover the location.
  EXPECT_EQ(SelectS3StorageCredential(
                {{.prefix = "s3://other", .config = {{"s3.access-key-id", "a"}}}},
                "s3://bucket/data"),
            nullptr);
  // No credentials at all.
  EXPECT_EQ(SelectS3StorageCredential({}, "s3://bucket"), nullptr);
}

TEST(RestFileIOTest, HasOnlyNonS3StorageCredentials) {
  // Only GCS/ADLS prefixes -> unsupported, fail fast.
  EXPECT_TRUE(HasOnlyNonS3StorageCredentials(
      {{.prefix = "gs://bucket", .config = {{"k", "v"}}},
       {.prefix = "abfs://c@a.dfs.core.windows.net", .config = {{"k", "v"}}}}));
  // At least one S3 credential present -> not unsupported (may fall back).
  EXPECT_FALSE(HasOnlyNonS3StorageCredentials(
      {{.prefix = "gs://bucket", .config = {{"k", "v"}}},
       {.prefix = "s3://bucket", .config = {{"s3.access-key-id", "a"}}}}));
  // No credentials at all -> not "only non-S3".
  EXPECT_FALSE(HasOnlyNonS3StorageCredentials({}));
}

TEST(RestFileIOTest, MakeS3FileIOFromCredentialMergesConfigWithPrecedence) {
  auto captured = std::make_shared<std::unordered_map<std::string, std::string>>();
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowS3FileIO),
      [captured](const std::unordered_map<std::string, std::string>& properties)
          -> Result<std::unique_ptr<FileIO>> {
        *captured = properties;
        return std::make_unique<MockFileIO>();
      });
  auto result = MakeS3FileIOFromCredential(
      {{"a", "catalog"}, {"shared", "catalog"}}, {{"b", "table"}, {"shared", "table"}},
      StorageCredential{.prefix = "s3", .config = {{"c", "cred"}, {"shared", "cred"}}});
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ((*captured)["a"], "catalog");
  EXPECT_EQ((*captured)["b"], "table");
  EXPECT_EQ((*captured)["c"], "cred");
  EXPECT_EQ((*captured)["shared"], "cred");
}

}  // namespace iceberg::rest
