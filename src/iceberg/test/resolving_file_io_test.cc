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

#include "iceberg/resolving_file_io.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/file_io_registry.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

namespace {

/// Records every NewInputFile location routed to it.
class RecordingFileIO : public FileIO {
 public:
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override {
    locations.push_back(std::move(file_location));
    return NotImplemented("recording mock");
  }

  std::vector<std::string> locations;
};

class RecordingCredentialedFileIO : public RecordingFileIO,
                                    public SupportsStorageCredentials {
 public:
  Status SetStorageCredentials(
      const std::vector<StorageCredential>& storage_credentials) override {
    credentials_ = storage_credentials;
    return {};
  }

  const std::vector<StorageCredential>& credentials() const override {
    return credentials_;
  }

  SupportsStorageCredentials* AsSupportsStorageCredentials() override { return this; }

 private:
  std::vector<StorageCredential> credentials_;
};

// File-scope recording state: registry factories are process-global, so they
// must not capture test-local objects.
int s3_factory_calls = 0;
int local_factory_calls = 0;
std::unordered_map<std::string, std::string> s3_factory_properties;
RecordingCredentialedFileIO* last_s3_io = nullptr;
RecordingFileIO* last_local_io = nullptr;

/// Registers recording mocks for the builtin S3/local names and resets the
/// recording state.
void RegisterRecordingFileIOs() {
  s3_factory_calls = 0;
  local_factory_calls = 0;
  s3_factory_properties.clear();
  last_s3_io = nullptr;
  last_local_io = nullptr;
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowS3FileIO),
      [](const std::unordered_map<std::string, std::string>& properties)
          -> Result<std::unique_ptr<FileIO>> {
        ++s3_factory_calls;
        s3_factory_properties = properties;
        auto io = std::make_unique<RecordingCredentialedFileIO>();
        last_s3_io = io.get();
        return io;
      });
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowLocalFileIO),
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> {
        ++local_factory_calls;
        auto io = std::make_unique<RecordingFileIO>();
        last_local_io = io.get();
        return io;
      });
}

}  // namespace

TEST(ResolvingFileIOTest, ResolvesImplementationNameFromScheme) {
  EXPECT_THAT(ResolvingFileIO::ResolveFileIOName("s3://bucket/path"),
              HasValue(::testing::Eq(FileIORegistry::kArrowS3FileIO)));
  EXPECT_THAT(ResolvingFileIO::ResolveFileIOName("s3a://bucket/path"),
              HasValue(::testing::Eq(FileIORegistry::kArrowS3FileIO)));
  EXPECT_THAT(ResolvingFileIO::ResolveFileIOName("s3n://bucket/path"),
              HasValue(::testing::Eq(FileIORegistry::kArrowS3FileIO)));
  EXPECT_THAT(ResolvingFileIO::ResolveFileIOName("oss://bucket/path"),
              HasValue(::testing::Eq(FileIORegistry::kArrowS3FileIO)));
  EXPECT_THAT(ResolvingFileIO::ResolveFileIOName("file:///tmp/path"),
              HasValue(::testing::Eq(FileIORegistry::kArrowLocalFileIO)));
  EXPECT_THAT(ResolvingFileIO::ResolveFileIOName("/tmp/path"),
              HasValue(::testing::Eq(FileIORegistry::kArrowLocalFileIO)));

  auto result = ResolvingFileIO::ResolveFileIOName("gs://bucket/path");
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
  EXPECT_THAT(result, HasErrorMessage("not supported for FileIO resolution"));
}

TEST(ResolvingFileIOTest, RoutesPathsAndCachesResolvedImplementations) {
  RegisterRecordingFileIOs();
  ResolvingFileIO io({{"k", "v"}});

  // Errors come from the recording mock; routing is what is under test.
  (void)io.NewInputFile("oss://bucket/db/table/data/file.parquet");
  (void)io.NewInputFile("s3://bucket/db/table/data/file.parquet");
  (void)io.NewInputFile("/tmp/local/file.parquet");

  ASSERT_NE(last_s3_io, nullptr);
  ASSERT_NE(last_local_io, nullptr);
  EXPECT_THAT(last_s3_io->locations,
              ::testing::ElementsAre("oss://bucket/db/table/data/file.parquet",
                                     "s3://bucket/db/table/data/file.parquet"));
  EXPECT_THAT(last_local_io->locations,
              ::testing::ElementsAre("/tmp/local/file.parquet"));

  // Resolved instances are cached; properties pass through to the factory.
  EXPECT_EQ(s3_factory_calls, 1);
  EXPECT_EQ(local_factory_calls, 1);
  EXPECT_THAT(s3_factory_properties,
              ::testing::UnorderedElementsAre(::testing::Pair("k", "v")));

  auto unsupported = io.NewInputFile("gs://bucket/file.parquet");
  EXPECT_THAT(unsupported, IsError(ErrorKind::kNotSupported));
}

TEST(ResolvingFileIOTest, ForwardsAllCredentialsToResolvedImplementations) {
  RegisterRecordingFileIOs();
  ResolvingFileIO io({});

  // The full credential list is forwarded; each implementation applies the
  // prefixes it understands.
  std::vector<StorageCredential> credentials = {
      {.prefix = "oss", .config = {{"k1", "v1"}}},
      {.prefix = "s3", .config = {{"k2", "v2"}}}};
  EXPECT_THAT(io.SetStorageCredentials(credentials), IsOk());
  EXPECT_EQ(io.credentials(), credentials);

  (void)io.NewInputFile("s3://bucket/db/table/data/file.parquet");
  ASSERT_NE(last_s3_io, nullptr);
  EXPECT_EQ(last_s3_io->credentials(), credentials);

  // Credentials set after an implementation was resolved reach it as well.
  std::vector<StorageCredential> refreshed = {{.prefix = "s3", .config = {{"k3", "v3"}}}};
  EXPECT_THAT(io.SetStorageCredentials(refreshed), IsOk());
  EXPECT_EQ(last_s3_io->credentials(), refreshed);

  // The local FileIO does not support credentials; resolving it still works.
  (void)io.NewInputFile("/tmp/local/file.parquet");
  ASSERT_NE(last_local_io, nullptr);
}

}  // namespace iceberg
