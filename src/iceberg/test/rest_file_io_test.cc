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

std::vector<StorageCredential> captured_storage_credentials;
std::unordered_map<std::string, std::string> captured_file_io_properties;

class MockCredentialedFileIO : public MockFileIO, public SupportsStorageCredentials {
 public:
  Status SetStorageCredentials(
      const std::vector<StorageCredential>& credentials) override {
    captured_storage_credentials = credentials;
    return {};
  }

  const std::vector<StorageCredential>& credentials() const override {
    return captured_storage_credentials;
  }

  SupportsStorageCredentials* AsSupportsStorageCredentials() override { return this; }
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

// Regression test for #341: file: URIs with fewer than 3 slashes
TEST(RestFileIOTest, DetectBuiltinKindFileUriSingleSlash) {
  EXPECT_THAT(DetectBuiltinFileIO("file:/tmp/warehouse"),
              HasValue(::testing::Eq(BuiltinFileIOKind::kArrowLocal)));
  EXPECT_THAT(DetectBuiltinFileIO("file:/C:/iceberg_warehouse/db"),
              HasValue(::testing::Eq(BuiltinFileIOKind::kArrowLocal)));
  // Windows drive letter (single char before ':') → local path, not a URI
  EXPECT_THAT(DetectBuiltinFileIO("C:/iceberg_warehouse/db"),
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

TEST(RestFileIOTest, TableFileIOMergesConfigAndCredentials) {
  const std::string custom_impl = "com.mycompany.CredentialedFileIO";
  captured_file_io_properties.clear();
  captured_storage_credentials.clear();
  FileIORegistry::Register(
      custom_impl,
      [](const std::unordered_map<std::string, std::string>& properties)
          -> Result<std::unique_ptr<FileIO>> {
        captured_file_io_properties = properties;
        return std::make_unique<MockCredentialedFileIO>();
      });

  auto result = MakeTableFileIO(
      {{"warehouse", "s3://catalog/warehouse"},
       {"catalog-only", "catalog"},
       {"shared", "catalog"}},
      {{"io-impl", custom_impl}, {"table-only", "table"}, {"shared", "table"}},
      {{.prefix = "s3://bucket/table",
        .config = {{"shared", "credential"}, {"credential-only", "value"}}}});
  ASSERT_THAT(result, IsOk());
  auto* credentialed = result.value()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);

  EXPECT_THAT(
      captured_file_io_properties,
      ::testing::UnorderedElementsAre(
          ::testing::Pair("warehouse", "s3://catalog/warehouse"),
          ::testing::Pair("catalog-only", "catalog"),
          ::testing::Pair("io-impl", custom_impl), ::testing::Pair("table-only", "table"),
          ::testing::Pair("shared", "table")));
  ASSERT_EQ(captured_storage_credentials.size(), 1);
  EXPECT_EQ(captured_storage_credentials[0].prefix, "s3://bucket/table");
  EXPECT_THAT(captured_storage_credentials[0].config,
              ::testing::UnorderedElementsAre(::testing::Pair("credential-only", "value"),
                                              ::testing::Pair("shared", "credential")));
  EXPECT_EQ(credentialed->credentials(), captured_storage_credentials);
}

TEST(RestFileIOTest, TableImplOverridesWarehouseScheme) {
  captured_file_io_properties.clear();
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowS3FileIO),
      [](const std::unordered_map<std::string, std::string>& properties)
          -> Result<std::unique_ptr<FileIO>> {
        captured_file_io_properties = properties;
        return std::make_unique<MockFileIO>();
      });

  auto result =
      MakeTableFileIO({{"warehouse", "/tmp/catalog-warehouse"}},
                      {{"io-impl", std::string(FileIORegistry::kArrowS3FileIO)}},
                      /*storage_credentials=*/{});
  ASSERT_THAT(result, IsOk());
  EXPECT_THAT(
      captured_file_io_properties,
      ::testing::UnorderedElementsAre(
          ::testing::Pair("warehouse", "/tmp/catalog-warehouse"),
          ::testing::Pair("io-impl", std::string(FileIORegistry::kArrowS3FileIO))));
}

TEST(RestFileIOTest, TableFileIORejectsCredentials) {
  const std::string custom_impl = "com.mycompany.PlainFileIO";
  FileIORegistry::Register(
      custom_impl,
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  auto result = MakeTableFileIO(
      {{"warehouse", "s3://catalog/warehouse"}}, {{"io-impl", custom_impl}},
      {{.prefix = "s3://bucket/table", .config = {{"k", "v"}}}});
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
  EXPECT_THAT(result, HasErrorMessage("does not support vended storage credentials"));
}

}  // namespace iceberg::rest
