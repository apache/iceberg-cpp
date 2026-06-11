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
#include <string>
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

TEST(RestFileIOTest, MatchStorageCredentialPicksLongestPrefix) {
  std::vector<StorageCredential> credentials = {
      {.prefix = "s3://bucket", .config = {{"k", "broad"}}},
      {.prefix = "s3://bucket/db/table", .config = {{"k", "specific"}}},
      {.prefix = "s3://other", .config = {{"k", "other"}}},
  };

  const auto* match =
      MatchStorageCredential("s3://bucket/db/table/data/f.parquet", credentials);
  ASSERT_NE(match, nullptr);
  EXPECT_EQ(match->prefix, "s3://bucket/db/table");

  EXPECT_EQ(MatchStorageCredential("gs://nope/x", credentials), nullptr);
}

TEST(RestFileIOTest, MatchStorageCredentialEmptyReturnsNull) {
  EXPECT_EQ(MatchStorageCredential("s3://bucket/x", {}), nullptr);
}

TEST(RestFileIOTest, MakeTableFileIOReusesCatalogIOWhenNoOverrides) {
  auto catalog_io = std::make_shared<MockFileIO>();
  auto config = RestCatalogProperties::FromMap(
      {{"io-impl", std::string(FileIORegistry::kArrowS3FileIO)}});

  auto result = MakeTableFileIO(config, catalog_io, "s3://bucket/test",
                                /*table_config=*/{}, /*storage_credentials=*/{});
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value(), catalog_io);  // shared catalog instance reused
}

TEST(RestFileIOTest, MakeTableFileIOAppliesVendedCredentials) {
  auto captured = std::make_shared<std::unordered_map<std::string, std::string>>();
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowS3FileIO),
      [captured](const std::unordered_map<std::string, std::string>& properties)
          -> Result<std::unique_ptr<FileIO>> {
        *captured = properties;
        return std::make_unique<MockFileIO>();
      });

  auto catalog_io = std::make_shared<MockFileIO>();
  auto config = RestCatalogProperties::FromMap(
      {{"io-impl", std::string(FileIORegistry::kArrowS3FileIO)},
       {"s3.access-key-id", "catalog-key"}});

  std::vector<StorageCredential> credentials = {
      {.prefix = "s3://bucket", .config = {{"s3.access-key-id", "broad"}}},
      {.prefix = "s3://bucket/test",
       .config = {{"s3.access-key-id", "vended"}, {"s3.session-token", "tok"}}},
  };

  auto result = MakeTableFileIO(config, catalog_io, "s3://bucket/test/data/f.parquet",
                                /*table_config=*/{{"write.parquet.compression", "zstd"}},
                                credentials);
  ASSERT_THAT(result, IsOk());
  EXPECT_NE(result.value(), catalog_io);  // a new, table-scoped FileIO

  // The most specific vended credential wins over the catalog value.
  EXPECT_EQ((*captured)["s3.access-key-id"], "vended");
  EXPECT_EQ((*captured)["s3.session-token"], "tok");
  // Table-specific config is layered in as well.
  EXPECT_EQ((*captured)["write.parquet.compression"], "zstd");
}

}  // namespace iceberg::rest
