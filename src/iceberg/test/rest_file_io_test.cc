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
#include "iceberg/util/location_util.h"

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

TEST(RestFileIOTest, CanonicalizeS3SchemeTreatsS3CompatibleSchemesAsS3) {
  // s3a/s3n/oss canonicalize to s3:// so a vended `s3` credential prefix-matches
  // them uniformly (DLF vends `s3` for oss:// locations).
  EXPECT_EQ(LocationUtil::CanonicalizeS3Scheme("oss://bucket/db/t"), "s3://bucket/db/t");
  EXPECT_EQ(LocationUtil::CanonicalizeS3Scheme("s3a://bucket/x"), "s3://bucket/x");
  EXPECT_EQ(LocationUtil::CanonicalizeS3Scheme("s3n://bucket/x"), "s3://bucket/x");
  // Already-canonical and non-S3 / scheme-less locations are unchanged.
  EXPECT_EQ(LocationUtil::CanonicalizeS3Scheme("s3://bucket/x"), "s3://bucket/x");
  EXPECT_EQ(LocationUtil::CanonicalizeS3Scheme("gs://bucket"), "gs://bucket");
  EXPECT_EQ(LocationUtil::CanonicalizeS3Scheme("/local/path"), "/local/path");
}

TEST(RestFileIOTest, PathHasPrefixMatchesAtPathBoundary) {
  // Must match only at a path boundary, so a `s3://bucket/db/t1` credential does
  // not capture a sibling table under `s3://bucket/db/t1x/...`.
  EXPECT_TRUE(LocationUtil::PathHasPrefix("s3://bucket/db/t1/data/f.parquet",
                                          "s3://bucket/db/t1"));
  EXPECT_TRUE(LocationUtil::PathHasPrefix("s3://bucket/db/t1", "s3://bucket/db/t1"));
  EXPECT_FALSE(LocationUtil::PathHasPrefix("s3://bucket/db/t1x/data/f.parquet",
                                           "s3://bucket/db/t1"));
  EXPECT_FALSE(LocationUtil::PathHasPrefix("s3://bucket-other/x", "s3://bucket"));

  // A bare-scheme credential (DLF vends `s3`) matches any authority/path.
  EXPECT_TRUE(LocationUtil::PathHasPrefix("s3://bucket/db/t/f", "s3"));
  EXPECT_TRUE(LocationUtil::PathHasPrefix(
      LocationUtil::CanonicalizeS3Scheme("oss://bucket/db/t/f"), "s3"));
  EXPECT_FALSE(LocationUtil::PathHasPrefix("gs://bucket/x", "s3"));
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

}  // namespace iceberg::rest
