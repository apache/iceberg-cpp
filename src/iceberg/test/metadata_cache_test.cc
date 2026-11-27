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

#include <array>
#include <barrier>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/content_cache_internal.h"
#include "iceberg/file_io.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {
using internal::ContentCacheOptions;
using internal::ContentCacheProperties;

std::shared_ptr<internal::ContentCache> MakeCache(
    std::shared_ptr<FileIO> storage = std::make_shared<MockFileIO>(),
    ContentCacheOptions options = {}) {
  return std::make_shared<internal::ContentCache>(std::move(storage), options);
}

class ScopedGlobalCacheLimit {
 public:
  explicit ScopedGlobalCacheLimit(int32_t limit)
      : previous_(internal::ContentCache::SetGlobalCapacity(limit)) {}
  ~ScopedGlobalCacheLimit() { internal::ContentCache::SetGlobalCapacity(previous_); }

 private:
  int32_t previous_;
};

struct FailingOpenState {
  int open_count = 0;
  int size_count = 0;
  int64_t size = 8;
  Error first_error;
  Error fallback_error;
};

class FailingOpenInputFile : public InputFile {
 public:
  explicit FailingOpenInputFile(std::shared_ptr<FailingOpenState> state)
      : state_(std::move(state)) {}

  std::string_view location() const override { return "manifest.avro"; }
  Result<int64_t> Size() const override {
    ++state_->size_count;
    return state_->size;
  }

  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    ++state_->open_count;
    if (state_->open_count == 1) {
      return unexpected<Error>(state_->first_error);
    }
    return unexpected<Error>(state_->fallback_error);
  }

 private:
  std::shared_ptr<FailingOpenState> state_;
};

class FailingOpenFileIO : public FileIO {
 public:
  explicit FailingOpenFileIO(std::shared_ptr<FailingOpenState> state)
      : state_(std::move(state)) {}

  Result<std::unique_ptr<InputFile>> NewInputFile(
      std::string /*file_location*/) override {
    return std::make_unique<FailingOpenInputFile>(state_);
  }

 private:
  std::shared_ptr<FailingOpenState> state_;
};

TEST(MetadataCacheTest, IdenticalLocationsOnDifferentBackendsHaveIndependentContents) {
  auto first = std::make_shared<MockFileIO>();
  auto second = std::make_shared<MockFileIO>();
  first->AddFile("manifest.avro", "first");
  second->AddFile("manifest.avro", "second");

  auto cache = MakeCache(first);
  ASSERT_THAT(cache->Wrap()->ReadFile("manifest.avro", std::nullopt),
              HasValue(::testing::Eq("first")));
  first->RemoveFile("manifest.avro");

  auto replacement = MakeCache(second);
  EXPECT_THAT(replacement->Wrap()->ReadFile("manifest.avro", std::nullopt),
              HasValue(::testing::Eq("second")));
  EXPECT_THAT(cache->Wrap()->ReadFile("manifest.avro", std::nullopt),
              HasValue(::testing::Eq("first")));
}

TEST(MetadataCacheTest, NegativeStorageSizeIsInvalidWithOrWithoutCaching) {
  auto state = std::make_shared<FailingOpenState>();
  state->size = -1;
  std::shared_ptr<FileIO> storage = std::make_shared<FailingOpenFileIO>(state);
  auto cache = MakeCache(storage);
  for (const auto& io : {storage, cache->Wrap()}) {
    auto result = io->ReadFile("manifest.avro", std::nullopt);
    EXPECT_THAT(result, IsError(ErrorKind::kInvalid));
    EXPECT_THAT(result, HasErrorMessage("File size -1 for manifest.avro"));
  }
  EXPECT_EQ(state->open_count, 0);
}

TEST(MetadataCacheTest, CachedInputFileDoesNotRetryNonIOErrors) {
  auto state = std::make_shared<FailingOpenState>(FailingOpenState{
      .first_error = {.kind = ErrorKind::kNotFound, .message = "missing"},
      .fallback_error = {.kind = ErrorKind::kIOError, .message = "unexpected retry"},
  });
  auto cache = MakeCache(std::make_shared<FailingOpenFileIO>(state));
  auto adapter = cache->Wrap();
  ICEBERG_UNWRAP_OR_FAIL(auto input_file, adapter->NewInputFile("manifest.avro"));

  EXPECT_THAT(input_file->Open(), IsError(ErrorKind::kNotFound));
  EXPECT_EQ(state->open_count, 1);
}

TEST(MetadataCacheTest, CachedInputFilePreservesReadAheadAndFallbackErrors) {
  auto state = std::make_shared<FailingOpenState>(FailingOpenState{
      .first_error = {.kind = ErrorKind::kIOError, .message = "read-ahead failed"},
      .fallback_error = {.kind = ErrorKind::kIOError, .message = "open failed"},
  });
  auto cache = MakeCache(std::make_shared<FailingOpenFileIO>(state));
  auto adapter = cache->Wrap();
  ICEBERG_UNWRAP_OR_FAIL(auto input_file, adapter->NewInputFile("manifest.avro"));

  auto result = input_file->Open();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
  EXPECT_THAT(result, HasErrorMessage("read-ahead failed"));
  EXPECT_THAT(result, HasErrorMessage("fallback open failed: open failed"));
  EXPECT_EQ(state->open_count, 2);
}

TEST(MetadataCacheTest, ParsesJavaCompatibleProperties) {
  ContentCacheProperties properties;
  properties.Set(ContentCacheProperties::kEnabled, true)
      .Set(ContentCacheProperties::kMaxTotalBytes, int64_t{200})
      .Set(ContentCacheProperties::kMaxContentLength, int64_t{50});

  auto options = ContentCacheOptions::FromProperties(properties.configs());

  ASSERT_THAT(options, IsOk());
  EXPECT_TRUE(options->enabled);
  EXPECT_EQ(options->max_total_bytes, 200);
  EXPECT_EQ(options->max_content_length, 50);
}

TEST(MetadataCacheTest, ParsesEnabledPropertyIgnoringCase) {
  for (const auto* value : {"true", "TRUE", "True", "tRuE"}) {
    SCOPED_TRACE(value);
    ICEBERG_UNWRAP_OR_FAIL(auto options,
                           ContentCacheOptions::FromProperties(
                               {{ContentCacheProperties::kEnabled.key(), value}}));
    EXPECT_TRUE(options.enabled);
  }
}

TEST(MetadataCacheTest, TreatsOtherEnabledPropertyValuesAsFalseLikeJava) {
  for (const auto* value :
       {"false", "FALSE", "False", "fAlSe", "", "yes", "1", "ture", " true "}) {
    SCOPED_TRACE(value);
    ICEBERG_UNWRAP_OR_FAIL(auto options,
                           ContentCacheOptions::FromProperties(
                               {{ContentCacheProperties::kEnabled.key(), value}}));
    EXPECT_FALSE(options.enabled);
  }
}

TEST(MetadataCacheTest, UsesJavaCompatibleDefaults) {
  auto options = ContentCacheOptions::FromProperties(
      {{ContentCacheProperties::kEnabled.key(), "true"}});

  ASSERT_THAT(options, IsOk());
  EXPECT_TRUE(options->enabled);
  EXPECT_EQ(options->max_total_bytes, 100 * 1024 * 1024);
  EXPECT_EQ(options->max_content_length, 8 * 1024 * 1024);
}

TEST(MetadataCacheTest, InvalidNumericPropertyErrorIncludesPropertyName) {
  auto options = ContentCacheOptions::FromProperties(
      {{ContentCacheProperties::kEnabled.key(), "true"},
       {ContentCacheProperties::kMaxTotalBytes.key(), "invalid"}});

  EXPECT_THAT(options, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(options, HasErrorMessage(ContentCacheProperties::kMaxTotalBytes.key()));
}

TEST(MetadataCacheTest, IgnoresInvalidTuningPropertiesWhenDisabled) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto options, ContentCacheOptions::FromProperties(
                        {{ContentCacheProperties::kEnabled.key(), "false"},
                         {ContentCacheProperties::kMaxTotalBytes.key(), "invalid"}}));
  EXPECT_FALSE(options.enabled);
}

TEST(MetadataCacheTest, PartialBatchDeleteClearsCacheAndPreservesStorageError) {
  auto storage = std::make_shared<MockFileIO>();
  auto cache = MakeCache(storage);
  auto adapter = cache->Wrap();
  cache->Insert("alias", std::make_shared<const std::vector<std::byte>>(1), 0);

  ASSERT_THAT(adapter->DeleteFiles({}), IsOk());
  ASSERT_TRUE(cache->Find("alias"));
  ::testing::InSequence sequence;
  EXPECT_CALL(*storage, DeleteFile("canonical")).WillOnce(::testing::Return(Status{}));
  EXPECT_CALL(*storage, DeleteFile("missing")).WillOnce([] {
    return NotFound("missing");
  });
  EXPECT_THAT(adapter->DeleteFiles({"canonical", "missing"}),
              IsError(ErrorKind::kNotFound));
  uint64_t generation;
  EXPECT_FALSE(cache->Find("alias", &generation));
  EXPECT_EQ(generation, 1);
}

TEST(MetadataCacheTest, OldInputHandleOpensAfterReplacementIsCached) {
  for (const auto* replacement : {"new", "longer"}) {
    SCOPED_TRACE(replacement);
    auto storage = std::make_shared<MockFileIO>();
    storage->AddFile("manifest.avro", "old");
    auto cache = MakeCache(storage);
    auto adapter = cache->Wrap();
    ICEBERG_UNWRAP_OR_FAIL(auto old_file, adapter->NewInputFile("manifest.avro"));
    EXPECT_CALL(*storage, DeleteFile("manifest.avro")).WillOnce([&] {
      storage->RemoveFile("manifest.avro");
      return Status{};
    });
    ASSERT_THAT(adapter->DeleteFile("manifest.avro"), IsOk());
    storage->AddFile("manifest.avro", replacement);
    ASSERT_THAT(adapter->ReadFile("manifest.avro", std::nullopt),
                HasValue(::testing::Eq(replacement)));

    EXPECT_THAT(old_file->Size(), HasValue(::testing::Eq(3)));
    ICEBERG_UNWRAP_OR_FAIL(auto old_stream, old_file->Open());
    std::string bytes(8, ' ');
    EXPECT_THAT(old_stream->Read(std::as_writable_bytes(std::span(bytes))),
                HasValue(::testing::Eq(3)));
    EXPECT_EQ(bytes.substr(0, 3), "old");
    EXPECT_THAT(adapter->ReadFile("manifest.avro", std::nullopt),
                HasValue(::testing::Eq(replacement)));
  }
}

TEST(MetadataCacheTest, OpenRespectsHandleSizeWhenALongerEntryIsAlreadyCached) {
  auto storage = std::make_shared<MockFileIO>();
  storage->AddFile("manifest.avro", "manifest");
  auto cache = MakeCache(storage);
  auto adapter = cache->Wrap();
  ICEBERG_UNWRAP_OR_FAIL(auto short_file, adapter->NewInputFile("manifest.avro", 4));
  ASSERT_THAT(short_file->Size(), HasValue(::testing::Eq(4)));
  ASSERT_THAT(adapter->ReadFile("manifest.avro", std::nullopt),
              HasValue(::testing::Eq("manifest")));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, short_file->Open());
  std::array<std::byte, 8> bytes;
  EXPECT_THAT(stream->Read(bytes), HasValue(::testing::Eq(4)));
  EXPECT_THAT(adapter->ReadFile("manifest.avro", std::nullopt),
              HasValue(::testing::Eq("manifest")));
}

class IgnoredHintFileIO : public MockFileIO {
 public:
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string location,
                                                  size_t length) override {
    ++hinted_calls;
    EXPECT_EQ(length, 4);
    return MockFileIO::NewInputFile(std::move(location));
  }

  int hinted_calls = 0;
};

TEST(MetadataCacheTest, HintedInputUsesSizeReportedByStorage) {
  auto storage = std::make_shared<IgnoredHintFileIO>();
  storage->AddFile("manifest.avro", "manifest");
  for (size_t limit : {4, 8}) {
    SCOPED_TRACE(limit);
    auto cache = MakeCache(storage, ContentCacheOptions{.max_content_length = limit});
    auto adapter = cache->Wrap();
    ICEBERG_UNWRAP_OR_FAIL(auto file, adapter->NewInputFile("manifest.avro", 4));
    EXPECT_THAT(file->Size(), HasValue(::testing::Eq(8)));
    ICEBERG_UNWRAP_OR_FAIL(auto stream, file->Open());
    EXPECT_THAT(stream->Close(), IsOk());
    EXPECT_EQ(static_cast<bool>(cache->Find("manifest.avro")), limit == 8);
    EXPECT_THAT(adapter->ReadFile("manifest.avro", std::nullopt),
                HasValue(::testing::Eq("manifest")));
  }
  EXPECT_EQ(storage->hinted_calls, 2);
}

TEST(MetadataCacheTest, ShortHintOnFirstOpenDoesNotPublishAPrefix) {
  class CountingFileIO : public MockFileIO {
   public:
    using MockFileIO::NewInputFile;
    Result<std::unique_ptr<InputFile>> NewInputFile(std::string location) override {
      ++unhinted_calls;
      return MockFileIO::NewInputFile(std::move(location));
    }
    int unhinted_calls = 0;
  };
  auto storage = std::make_shared<CountingFileIO>();
  storage->AddFile("manifest.avro", "manifest");
  auto cache = MakeCache(storage);
  auto adapter = cache->Wrap();
  ICEBERG_UNWRAP_OR_FAIL(auto file, adapter->NewInputFile("manifest.avro", 4));
  EXPECT_EQ(storage->unhinted_calls, 0);
  ICEBERG_UNWRAP_OR_FAIL(auto stream, file->Open());
  ASSERT_THAT(file->Open(), IsOk());
  EXPECT_EQ(storage->unhinted_calls, 1);
  std::array<std::byte, 8> buffer;
  EXPECT_THAT(stream->Read(buffer), HasValue(::testing::Eq(4)));
  EXPECT_FALSE(cache->Find("manifest.avro"));
  EXPECT_THAT(adapter->ReadFile("manifest.avro", std::nullopt),
              HasValue(::testing::Eq("manifest")));
}

TEST(MetadataCacheTest, BypassedHandlesReuseTheirStorageSize) {
  for (int64_t size : {0, 9}) {
    auto state = std::make_shared<FailingOpenState>();
    state->size = size;
    auto cache = MakeCache(std::make_shared<FailingOpenFileIO>(state),
                           ContentCacheOptions{.max_content_length = 8});
    ICEBERG_UNWRAP_OR_FAIL(auto input, cache->NewInputFile("manifest.avro"));
    EXPECT_THAT(input->Size(), HasValue(::testing::Eq(size)));
    EXPECT_THAT(input->Size(), HasValue(::testing::Eq(size)));
    EXPECT_EQ(state->size_count, 1);
    EXPECT_EQ(state->open_count, 0);
  }
}

TEST(MetadataCacheTest, UnknownNamespaceSkipsReadAheadAndHintVerification) {
  class UntrackedFileIO : public FailingOpenFileIO, public SupportsStorageCredentials {
   public:
    using FailingOpenFileIO::FailingOpenFileIO;
    SupportsStorageCredentials* AsSupportsStorageCredentials() override { return this; }
    Status SetStorageCredentials(const std::vector<StorageCredential>&) override {
      return {};
    }
    std::vector<StorageCredential> credentials() const override { return {}; }
  };
  auto state = std::make_shared<FailingOpenState>();
  state->first_error = {.kind = ErrorKind::kIOError, .message = "storage unavailable"};
  auto cache = MakeCache(std::make_shared<UntrackedFileIO>(state));
  ICEBERG_UNWRAP_OR_FAIL(auto input, cache->Wrap()->NewInputFile("manifest.avro", 8));
  EXPECT_EQ(state->size_count, 0);
  EXPECT_THAT(input->Open(), IsError(ErrorKind::kIOError));
  EXPECT_EQ(state->open_count, 1);
}

TEST(MetadataCacheTest, NestedCacheFollowsInnerOverwrites) {
  auto storage = std::make_shared<MockFileIO>();
  storage->AddFile("manifest", "old");
  auto inner = MakeCache(storage)->Wrap();
  auto outer = MakeCache(inner)->Wrap();
  ASSERT_THAT(outer->ReadFile("manifest", std::nullopt), HasValue(::testing::Eq("old")));
  ASSERT_THAT(inner->WriteFile("manifest", "new"), IsOk());
  EXPECT_THAT(outer->ReadFile("manifest", std::nullopt), HasValue(::testing::Eq("new")));
}

TEST(MetadataCacheTest, ConcurrentInsertionsShareTheWinnerForTheSameSizeAndGeneration) {
  auto cache = MakeCache();
  auto insert = [&](size_t size, uint64_t generation) {
    return cache->Insert("manifest", std::make_shared<const std::vector<std::byte>>(size),
                         generation);
  };
  std::barrier start(2);
  internal::MetadataContent first;
  std::jthread loader([&] {
    start.arrive_and_wait();
    first = insert(8, 0);
  });
  start.arrive_and_wait();
  auto second = insert(8, 0);
  loader.join();
  ASSERT_EQ(first, second);
  EXPECT_NE(insert(4, 0), first);
  EXPECT_EQ(cache->Find("manifest"), first);
  cache->Clear();
  auto replacement = insert(8, 1);
  EXPECT_NE(insert(8, 0), replacement);
  EXPECT_EQ(cache->Find("manifest"), replacement);
}

TEST(MetadataCacheTest, SizeMismatchDoesNotPromoteAFileWithinItsTable) {
  auto cache = MakeCache(std::make_shared<MockFileIO>(),
                         ContentCacheOptions{.max_total_bytes = 4});
  auto bytes = std::make_shared<const std::vector<std::byte>>(2);
  cache->Insert("a", bytes, 0);
  cache->Insert("b", bytes, 0);
  EXPECT_FALSE(cache->FindForHandle("a", 0, 1));
  cache->Insert("a", std::make_shared<const std::vector<std::byte>>(1), 0);
  cache->Insert("c", bytes, 0);
  EXPECT_FALSE(cache->Find("a"));
  EXPECT_EQ(cache->Find("b"), bytes);
}

TEST(MetadataCacheTest, MissInAnActiveTableDoesNotRefreshItsGlobalSlot) {
  ScopedGlobalCacheLimit limit(2);
  auto first = MakeCache();
  auto second = MakeCache();
  auto third = MakeCache();
  auto bytes = std::make_shared<const std::vector<std::byte>>(1);
  first->Insert("a", bytes, 0);
  second->Insert("b", bytes, 0);
  EXPECT_FALSE(first->Find("missing"));
  third->Insert("c", bytes, 0);
  EXPECT_FALSE(first->Find("a"));
  EXPECT_TRUE(second->Find("b"));
}

TEST(MetadataCacheTest, ReadersOverlappingFreshWritesCannotPublishPartialBytes) {
  auto storage = std::make_shared<MockFileIO>();
  auto cache = MakeCache(storage);
  auto adapter = cache->Wrap();
  auto warm = std::make_shared<const std::vector<std::byte>>(8);
  cache->Insert("warm", warm, 0);
  ICEBERG_UNWRAP_OR_FAIL(auto output, adapter->NewOutputFile("fresh"));
  ICEBERG_UNWRAP_OR_FAIL(auto writer, output->CreateOrOverwrite());
  auto bytes = std::as_bytes(std::span("ab", 2));
  ASSERT_THAT(writer->Write(bytes), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto partial, adapter->NewInputFile("fresh"));
  EXPECT_THAT(adapter->ReadFile("fresh", std::nullopt), HasValue(::testing::Eq("ab")));
  ASSERT_FALSE(cache->Find("fresh"));
  ASSERT_THAT(writer->Write(bytes), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
  ASSERT_THAT(partial->Open(), IsOk());
  EXPECT_FALSE(cache->Find("fresh"));
  EXPECT_EQ(cache->Find("warm"), warm);
  EXPECT_THAT(adapter->ReadFile("fresh", std::nullopt), HasValue(::testing::Eq("abab")));
}

TEST(MetadataCacheTest, PrefixReadDoesNotPopulateWholeFileCache) {
  auto storage = std::make_shared<MockFileIO>();
  storage->AddFile("manifest.avro", "manifest");
  auto cache = MakeCache(storage);
  auto adapter = cache->Wrap();

  EXPECT_CALL(*storage, ReadFile("manifest.avro", std::optional<size_t>{4}))
      .WillOnce(::testing::Return(std::string("mani")));
  EXPECT_THAT(adapter->ReadFile("manifest.avro", 4), HasValue(::testing::Eq("mani")));
  EXPECT_FALSE(cache->Find("manifest.avro"));
  EXPECT_THAT(adapter->ReadFile("manifest.avro", std::nullopt),
              HasValue(::testing::Eq("manifest")));
}

TEST(MetadataCacheTest, OversizedReadDoesNotDisplaceAWarmCache) {
  ScopedGlobalCacheLimit limit(1);
  auto warm = MakeCache();
  warm->Insert("warm.avro", std::make_shared<const std::vector<std::byte>>(1), 0);

  auto storage = std::make_shared<MockFileIO>();
  storage->AddFile("large.avro", "large");
  auto cold = MakeCache(storage, ContentCacheOptions{.max_content_length = 4});
  EXPECT_THAT(cold->Wrap()->ReadFile("large.avro", std::nullopt),
              HasValue(::testing::Eq("large")));
  EXPECT_FALSE(cold->Find("large.avro"));
  EXPECT_TRUE(warm->Find("warm.avro"));
}

TEST(MetadataCacheTest, GlobalEvictionReleasesBytesAndReadersKeepTheirContent) {
  ScopedGlobalCacheLimit limit(1);
  auto first = MakeCache();
  auto content = std::make_shared<const std::vector<std::byte>>(8);
  std::weak_ptr<const std::vector<std::byte>> weak_content = content;
  first->Insert("manifest.avro", std::move(content), 0);
  auto adapter = first->Wrap();
  ICEBERG_UNWRAP_OR_FAIL(auto file, adapter->NewInputFile("manifest.avro"));
  ICEBERG_UNWRAP_OR_FAIL(auto reader, file->Open());
  file.reset();
  auto second = MakeCache();
  second->Insert("other.avro", std::make_shared<const std::vector<std::byte>>(8), 0);
  EXPECT_FALSE(first->Find("manifest.avro"));
  // Only the active reader still owns these bytes after global eviction.
  EXPECT_FALSE(weak_content.expired());
  std::array<std::byte, 8> out;
  ASSERT_THAT(reader->ReadFully(0, out), IsOk());
  EXPECT_EQ(out, (std::array<std::byte, 8>{}));
  reader.reset();
  EXPECT_TRUE(weak_content.expired());
}

TEST(MetadataCacheTest, DestroyingLastCacheHandleReleasesRetainedBytes) {
  auto cache = MakeCache();
  auto content = std::make_shared<const std::vector<std::byte>>(8);
  std::weak_ptr<const std::vector<std::byte>> weak_content = content;
  cache->Insert("manifest.avro", std::move(content), 0);
  ASSERT_FALSE(weak_content.expired());
  cache.reset();
  EXPECT_TRUE(weak_content.expired());
}

TEST(MetadataCacheTest, DestroyedCacheFreesItsSlotWithoutEvictingALiveCache) {
  ScopedGlobalCacheLimit limit(2);
  auto first = MakeCache();
  first->Insert("first", std::make_shared<const std::vector<std::byte>>(1), 0);
  std::weak_ptr<internal::ContentCache> weak_cache;
  {
    auto second = MakeCache();
    weak_cache = second;
    second->Insert("second", std::make_shared<const std::vector<std::byte>>(1), 0);
  }
  ASSERT_TRUE(weak_cache.expired());
  auto third = MakeCache();
  third->Insert("third", std::make_shared<const std::vector<std::byte>>(1), 0);
  EXPECT_TRUE(first->Find("first"));
  EXPECT_TRUE(third->Find("third"));
}

TEST(MetadataCacheTest, LoweringGlobalCapacityEvictsLeastRecentlyUsedTables) {
  ScopedGlobalCacheLimit limit(3);
  auto first = MakeCache();
  auto second = MakeCache();
  auto third = MakeCache();
  const auto bytes = std::make_shared<const std::vector<std::byte>>(1);
  first->Insert("first", bytes, 0);
  second->Insert("second", bytes, 0);
  third->Insert("third", bytes, 0);
  ASSERT_TRUE(first->Find("first"));
  EXPECT_THROW(internal::ContentCache::SetGlobalCapacity(0), std::invalid_argument);
  EXPECT_THAT([] { internal::ContentCache::SetGlobalCapacity(-3); },
              ::testing::ThrowsMessage<std::invalid_argument>(
                  "Global manifest cache limit must be positive, got -3"));
  EXPECT_EQ(internal::ContentCache::SetGlobalCapacity(1), 3);
  EXPECT_TRUE(first->Find("first"));
  EXPECT_FALSE(second->Find("second"));
  EXPECT_FALSE(third->Find("third"));
}

TEST(MetadataCacheTest, ClearingCacheFreesItsGlobalSlot) {
  ScopedGlobalCacheLimit limit(2);
  auto first = MakeCache();
  auto second = MakeCache();
  auto third = MakeCache();
  const auto bytes = std::make_shared<const std::vector<std::byte>>(1);
  first->Insert("first", bytes, 0);
  second->Insert("second", bytes, 0);
  second->Clear();
  third->Insert("third", bytes, 0);
  EXPECT_TRUE(first->Find("first"));
  EXPECT_FALSE(second->Find("second"));
  EXPECT_TRUE(third->Find("third"));
}

TEST(MetadataCacheTest, InvalidatedLoadCannotPopulateRecreatedCacheState) {
  ScopedGlobalCacheLimit limit(1);
  auto cache = MakeCache();
  uint64_t generation;
  ASSERT_FALSE(cache->Find("manifest.avro", &generation));
  auto other = MakeCache();
  other->Insert("other.avro", std::make_shared<const std::vector<std::byte>>(8), 0);
  cache->Clear();
  cache->Insert("manifest.avro", std::make_shared<const std::vector<std::byte>>(8),
                generation);
  EXPECT_FALSE(cache->Find("manifest.avro"));
}

}  // namespace
}  // namespace iceberg
