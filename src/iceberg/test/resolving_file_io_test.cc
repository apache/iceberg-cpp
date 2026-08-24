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

  Status DeleteFiles(const std::vector<std::string>& file_locations) override {
    deleted_batches.push_back(file_locations);
    return {};
  }

  std::vector<std::string> locations;
  std::vector<std::vector<std::string>> deleted_batches;
};

class RecordingCredentialedFileIO : public RecordingFileIO,
                                    public SupportsStorageCredentials {
 public:
  Status SetStorageCredentials(
      const std::vector<StorageCredential>& storage_credentials) override {
    credentials_ = storage_credentials;
    return {};
  }

  std::vector<StorageCredential> credentials() const override { return credentials_; }

  void SetCredentialRefresher(StorageCredentialRefresher refresher) override {
    refresher_ = std::move(refresher);
  }

  SupportsStorageCredentials* AsSupportsStorageCredentials() override { return this; }

  Status Refresh() {
    if (!refresher_) {
      return NotFound("no refresher installed");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto refreshed, refresher_());
    return SetStorageCredentials(refreshed);
  }

  bool has_refresher() const { return static_cast<bool>(refresher_); }

 private:
  std::vector<StorageCredential> credentials_;
  StorageCredentialRefresher refresher_;
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
      {.create = [](const std::unordered_map<std::string, std::string>& properties)
           -> Result<std::unique_ptr<FileIO>> {
         ++s3_factory_calls;
         s3_factory_properties = properties;
         auto io = std::make_unique<RecordingCredentialedFileIO>();
         last_s3_io = io.get();
         return io;
       },
       .accepts =
           [](std::string_view scheme) {
             return scheme == "s3" || scheme == "s3a" || scheme == "s3n";
           }});
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowLocalFileIO),
      {.create = [](const std::unordered_map<std::string, std::string>& /*properties*/)
           -> Result<std::unique_ptr<FileIO>> {
         ++local_factory_calls;
         auto io = std::make_unique<RecordingFileIO>();
         last_local_io = io.get();
         return io;
       },
       .accepts =
           [](std::string_view scheme) { return scheme.empty() || scheme == "file"; }});
}

}  // namespace

TEST(FileIORegistryTest, ResolvesLatestAndReplacedRegistration) {
  FileIORegistry::Register(
      "test.file-io.old",
      {.create =
           [](const FileIORegistry::Properties&) -> Result<std::unique_ptr<FileIO>> {
         return std::make_unique<RecordingFileIO>();
       },
       .accepts = [](std::string_view scheme) { return scheme == "custom"; }});
  FileIORegistry::Register(
      "test.file-io.new",
      {.create =
           [](const FileIORegistry::Properties&) -> Result<std::unique_ptr<FileIO>> {
         return std::make_unique<RecordingFileIO>();
       },
       .accepts = [](std::string_view scheme) { return scheme == "custom"; }});

  EXPECT_THAT(FileIORegistry::Resolve("CUSTOM"),
              HasValue(::testing::Eq("test.file-io.new")));

  FileIORegistry::Register(
      "test.file-io.old",
      {.create =
           [](const FileIORegistry::Properties&) -> Result<std::unique_ptr<FileIO>> {
         return std::make_unique<RecordingFileIO>();
       },
       .accepts = [](std::string_view scheme) { return scheme == "replacement"; }});

  EXPECT_THAT(FileIORegistry::Resolve("custom"),
              HasValue(::testing::Eq("test.file-io.new")));
  EXPECT_THAT(FileIORegistry::Resolve("replacement"),
              HasValue(::testing::Eq("test.file-io.old")));
}

TEST(ResolvingFileIOTest, RoutesPathsAndCachesResolvedImplementations) {
  RegisterRecordingFileIOs();
  ResolvingFileIO io({{"k", "v"}});

  // Errors come from the recording mock; routing is what is under test.
  (void)io.NewInputFile("s3a://bucket/db/table/data/file.parquet");
  (void)io.NewInputFile("s3://bucket/db/table/data/file.parquet");
  (void)io.NewInputFile("/tmp/local/file.parquet");

  ASSERT_NE(last_s3_io, nullptr);
  ASSERT_NE(last_local_io, nullptr);
  EXPECT_THAT(last_s3_io->locations,
              ::testing::ElementsAre("s3a://bucket/db/table/data/file.parquet",
                                     "s3://bucket/db/table/data/file.parquet"));
  EXPECT_THAT(last_local_io->locations,
              ::testing::ElementsAre("/tmp/local/file.parquet"));

  // One instance per implementation despite two distinct S3 schemes routed to
  // it; properties pass through to the factory.
  EXPECT_EQ(s3_factory_calls, 1);
  EXPECT_EQ(local_factory_calls, 1);
  EXPECT_THAT(s3_factory_properties,
              ::testing::UnorderedElementsAre(::testing::Pair("k", "v")));

  auto unsupported = io.NewInputFile("gs://bucket/file.parquet");
  EXPECT_THAT(unsupported, IsError(ErrorKind::kNotSupported));
}

TEST(ResolvingFileIOTest, GroupsBulkDeletesByResolvedImplementation) {
  RegisterRecordingFileIOs();
  ResolvingFileIO io({});

  EXPECT_THAT(io.DeleteFiles({"s3://bucket/a", "/tmp/local-a", "s3a://bucket/b",
                              "file:///tmp/local-b"}),
              IsOk());

  ASSERT_NE(last_s3_io, nullptr);
  ASSERT_NE(last_local_io, nullptr);
  ASSERT_EQ(last_s3_io->deleted_batches.size(), 1);
  EXPECT_THAT(last_s3_io->deleted_batches.front(),
              ::testing::ElementsAre("s3://bucket/a", "s3a://bucket/b"));
  ASSERT_EQ(last_local_io->deleted_batches.size(), 1);
  EXPECT_THAT(last_local_io->deleted_batches.front(),
              ::testing::ElementsAre("/tmp/local-a", "file:///tmp/local-b"));
}

TEST(ResolvingFileIOTest, DoesNotDeleteWhenPathResolutionFails) {
  RegisterRecordingFileIOs();
  ResolvingFileIO io({});

  auto result = io.DeleteFiles({"s3://bucket/a", "gs://bucket/unknown"});
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
  ASSERT_NE(last_s3_io, nullptr);
  EXPECT_TRUE(last_s3_io->deleted_batches.empty());
}

TEST(ResolvingFileIOTest, DoesNotFallbackAfterSelectedFactoryFails) {
  FileIORegistry::Register(
      "test.file-io.fallback",
      {.create =
           [](const FileIORegistry::Properties&) -> Result<std::unique_ptr<FileIO>> {
         return std::make_unique<RecordingFileIO>();
       },
       .accepts = [](std::string_view scheme) { return scheme == "failure"; }});
  FileIORegistry::Register(
      "test.file-io.selected",
      {.create =
           [](const FileIORegistry::Properties&) -> Result<std::unique_ptr<FileIO>> {
         return InvalidArgument("selected factory failed");
       },
       .accepts = [](std::string_view scheme) { return scheme == "failure"; }});

  ResolvingFileIO io({});
  auto result = io.NewInputFile("failure://bucket/file");
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("selected factory failed"));
}

TEST(ResolvingFileIOTest, ForwardsAllCredentialsToResolvedImplementations) {
  RegisterRecordingFileIOs();
  ResolvingFileIO io({});

  // The full credential list is forwarded; each implementation applies the
  // prefixes it understands.
  std::vector<StorageCredential> credentials = {
      {.prefix = "s3a://bucket", .config = {{"k1", "v1"}}},
      {.prefix = "s3", .config = {{"k2", "v2"}}}};
  EXPECT_THAT(io.SetStorageCredentials(credentials), IsOk());
  EXPECT_EQ(io.credentials(), credentials);

  (void)io.NewInputFile("s3://bucket/db/table/data/file.parquet");
  ASSERT_NE(last_s3_io, nullptr);
  EXPECT_EQ(last_s3_io->credentials(), credentials);
  EXPECT_EQ(s3_factory_calls, 1);

  // Delegates are rebuilt with the new credentials, not mutated in place.
  std::vector<StorageCredential> refreshed = {{.prefix = "s3", .config = {{"k3", "v3"}}}};
  EXPECT_THAT(io.SetStorageCredentials(refreshed), IsOk());
  (void)io.NewInputFile("s3://bucket/db/table/data/other.parquet");
  ASSERT_NE(last_s3_io, nullptr);
  EXPECT_EQ(last_s3_io->credentials(), refreshed);
  EXPECT_EQ(s3_factory_calls, 2);

  // The local FileIO does not support credentials; resolving it still works.
  (void)io.NewInputFile("/tmp/local/file.parquet");
  ASSERT_NE(last_local_io, nullptr);
}

TEST(ResolvingFileIOTest, ForwardsCredentialRefresherToResolvedImplementations) {
  RegisterRecordingFileIOs();
  ResolvingFileIO io({});

  std::vector<StorageCredential> refreshed = {{.prefix = "s3", .config = {{"k2", "v2"}}}};
  io.SetCredentialRefresher(
      [&]() -> Result<std::vector<StorageCredential>> { return refreshed; });
  EXPECT_THAT(io.SetStorageCredentials({{.prefix = "s3", .config = {{"k1", "v1"}}}}),
              IsOk());

  (void)io.NewInputFile("s3://bucket/db/table/data/file.parquet");
  ASSERT_NE(last_s3_io, nullptr);
  ASSERT_TRUE(last_s3_io->has_refresher());
  EXPECT_THAT(last_s3_io->Refresh(), IsOk());
  EXPECT_EQ(last_s3_io->credentials(), refreshed);
}

TEST(ResolvingFileIOTest, RebuildsResolvedImplementationsForALaterRefresher) {
  RegisterRecordingFileIOs();
  ResolvingFileIO io({});

  EXPECT_THAT(io.SetStorageCredentials({{.prefix = "s3", .config = {{"k1", "v1"}}}}),
              IsOk());
  (void)io.NewInputFile("s3://bucket/db/table/data/file.parquet");
  ASSERT_NE(last_s3_io, nullptr);
  EXPECT_FALSE(last_s3_io->has_refresher());
  EXPECT_EQ(s3_factory_calls, 1);

  io.SetCredentialRefresher([]() -> Result<std::vector<StorageCredential>> {
    return std::vector<StorageCredential>{};
  });
  (void)io.NewInputFile("s3://bucket/db/table/data/other.parquet");
  EXPECT_EQ(s3_factory_calls, 2);
  ASSERT_NE(last_s3_io, nullptr);
  EXPECT_TRUE(last_s3_io->has_refresher());
}

}  // namespace iceberg
