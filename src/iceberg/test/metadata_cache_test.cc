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
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
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

#ifdef GTEST_HAS_DEATH_TEST
int SetManifestCacheLimitEnv(const char* value) {
  constexpr const char* kLimitEnv = "ICEBERG_IO_MANIFEST_CACHE_LIMIT";
#  ifdef _WIN32
  return _putenv_s(kLimitEnv, value);
#  else
  return setenv(kLimitEnv, value, /*overwrite=*/1);
#  endif
}

TEST(MetadataCacheDeathTest, InitializesGlobalLimitFromEnvironment) {
  // Start with a new registry for every environment setting.
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  for (const auto& [value, expected_limit] :
       {std::pair{"2", 2}, std::pair{"0", 8}, std::pair{"-1", 8}, std::pair{"2x", 8},
        std::pair{"2147483648", 8}}) {
    SCOPED_TRACE(value);
    EXPECT_EXIT(
        {
          if (SetManifestCacheLimitEnv(value) != 0) {
            std::_Exit(EXIT_FAILURE);
          }
          const auto initial_limit = internal::ContentCache::SetGlobalCapacity(3);
          const auto overridden_limit = internal::ContentCache::SetGlobalCapacity(4);
          std::_Exit(initial_limit == expected_limit && overridden_limit == 3
                         ? EXIT_SUCCESS
                         : EXIT_FAILURE);
        },
        ::testing::ExitedWithCode(EXIT_SUCCESS), "");
  }
}
#endif

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
  Error first_error;
  Error fallback_error;
};

class FailingOpenInputFile : public InputFile {
 public:
  explicit FailingOpenInputFile(std::shared_ptr<FailingOpenState> state)
      : state_(std::move(state)) {}

  std::string_view location() const override { return "manifest.avro"; }
  Result<int64_t> Size() const override { return 8; }

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

TEST(MetadataCacheTest, CachedInputFileDoesNotRetryNonIOErrors) {
  auto state = std::make_shared<FailingOpenState>(FailingOpenState{
      .first_error = {.kind = ErrorKind::kNotFound, .message = "missing"},
      .fallback_error = {.kind = ErrorKind::kIOError, .message = "unexpected retry"},
  });
  auto cache = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
  auto adapter = cache->Wrap(std::make_shared<FailingOpenFileIO>(state));
  ICEBERG_UNWRAP_OR_FAIL(auto input_file, adapter->NewInputFile("manifest.avro"));

  EXPECT_THAT(input_file->Open(), IsError(ErrorKind::kNotFound));
  EXPECT_EQ(state->open_count, 1);
}

TEST(MetadataCacheTest, CachedInputFilePreservesReadAheadAndFallbackErrors) {
  auto state = std::make_shared<FailingOpenState>(FailingOpenState{
      .first_error = {.kind = ErrorKind::kIOError, .message = "read-ahead failed"},
      .fallback_error = {.kind = ErrorKind::kIOError, .message = "open failed"},
  });
  auto cache = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
  auto adapter = cache->Wrap(std::make_shared<FailingOpenFileIO>(state));
  ICEBERG_UNWRAP_OR_FAIL(auto input_file, adapter->NewInputFile("manifest.avro"));

  auto result = input_file->Open();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
  EXPECT_THAT(result, HasErrorMessage("read-ahead failed"));
  EXPECT_THAT(result, HasErrorMessage("fallback open failed: open failed"));
  EXPECT_EQ(state->open_count, 2);
}

TEST(MetadataCacheTest, ParsesJavaCompatibleProperties) {
  std::unordered_map<std::string, std::string> properties = {
      {std::string(ContentCacheOptions::kEnabled), "true"},
      {std::string(ContentCacheOptions::kMaxTotalBytes), "200"},
      {std::string(ContentCacheOptions::kMaxContentLength), "50"},
  };

  auto options = ContentCacheOptions::FromProperties(properties);

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
                               {{std::string(ContentCacheOptions::kEnabled), value}}));
    EXPECT_TRUE(options.enabled);
  }
}

TEST(MetadataCacheTest, TreatsOtherEnabledPropertyValuesAsFalseLikeJava) {
  for (const auto* value :
       {"false", "FALSE", "False", "fAlSe", "", "yes", "1", "ture", " true "}) {
    SCOPED_TRACE(value);
    ICEBERG_UNWRAP_OR_FAIL(auto options,
                           ContentCacheOptions::FromProperties(
                               {{std::string(ContentCacheOptions::kEnabled), value}}));
    EXPECT_FALSE(options.enabled);
  }
}

TEST(MetadataCacheTest, UsesJavaCompatibleDefaults) {
  auto options = ContentCacheOptions::FromProperties(
      {{std::string(ContentCacheOptions::kEnabled), "true"}});

  ASSERT_THAT(options, IsOk());
  EXPECT_TRUE(options->enabled);
  EXPECT_EQ(options->max_total_bytes, 100 * 1024 * 1024);
  EXPECT_EQ(options->max_content_length, 8 * 1024 * 1024);
}

TEST(MetadataCacheTest, InvalidNumericPropertyErrorIncludesPropertyName) {
  auto options = ContentCacheOptions::FromProperties(
      {{std::string(ContentCacheOptions::kEnabled), "true"},
       {std::string(ContentCacheOptions::kMaxTotalBytes), "invalid"}});

  EXPECT_THAT(options, HasErrorMessage(std::string(ContentCacheOptions::kMaxTotalBytes)));
}

TEST(MetadataCacheTest, IgnoresInvalidTuningPropertiesWhenDisabled) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto options, ContentCacheOptions::FromProperties(
                        {{std::string(ContentCacheOptions::kEnabled), "false"},
                         {std::string(ContentCacheOptions::kMaxTotalBytes), "invalid"}}));
  EXPECT_FALSE(options.enabled);
}

TEST(MetadataCacheTest, PrefixReadDoesNotPopulateWholeFileCache) {
  auto storage = std::make_shared<MockFileIO>();
  storage->AddFile("manifest.avro", "manifest");
  auto cache = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
  auto adapter = cache->Wrap(storage);

  EXPECT_CALL(*storage, ReadFile("manifest.avro", std::optional<size_t>{4}))
      .WillOnce(::testing::Return(std::string("mani")));
  EXPECT_THAT(adapter->ReadFile("manifest.avro", 4), HasValue(::testing::Eq("mani")));
  EXPECT_FALSE(cache->Find("manifest.avro"));
  EXPECT_THAT(adapter->ReadFile("manifest.avro", std::nullopt),
              HasValue(::testing::Eq("manifest")));
}

TEST(MetadataCacheTest, OversizedReadDoesNotDisplaceAWarmCache) {
  ScopedGlobalCacheLimit limit(1);
  auto warm = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
  warm->Insert("warm.avro", std::make_shared<const std::vector<std::byte>>(1), 0);

  auto storage = std::make_shared<MockFileIO>();
  storage->AddFile("large.avro", "large");
  auto cold = std::make_shared<internal::ContentCache>(
      ContentCacheOptions{.max_content_length = 4});
  EXPECT_THAT(cold->Wrap(storage)->ReadFile("large.avro", std::nullopt),
              HasValue(::testing::Eq("large")));
  EXPECT_FALSE(cold->Find("large.avro"));
  EXPECT_TRUE(warm->Find("warm.avro"));
}

TEST(MetadataCacheTest, GlobalEvictionReleasesBytesAndReadersKeepTheirContent) {
  ScopedGlobalCacheLimit limit(1);
  auto first = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
  auto content = std::make_shared<const std::vector<std::byte>>(8);
  std::weak_ptr<const std::vector<std::byte>> weak_content = content;
  first->Insert("manifest.avro", std::move(content), 0);
  auto adapter = first->Wrap(std::make_shared<MockFileIO>());
  ICEBERG_UNWRAP_OR_FAIL(auto file, adapter->NewInputFile("manifest.avro"));
  ICEBERG_UNWRAP_OR_FAIL(auto reader, file->Open());
  file.reset();
  auto second = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
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
  auto cache = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
  auto content = std::make_shared<const std::vector<std::byte>>(8);
  std::weak_ptr<const std::vector<std::byte>> weak_content = content;
  cache->Insert("manifest.avro", std::move(content), 0);
  ASSERT_FALSE(weak_content.expired());
  cache.reset();
  EXPECT_TRUE(weak_content.expired());
}

TEST(MetadataCacheTest, InvalidatedLoadCannotPopulateRecreatedCacheState) {
  ScopedGlobalCacheLimit limit(1);
  auto cache = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
  uint64_t generation;
  ASSERT_FALSE(cache->Find("manifest.avro", &generation));
  auto other = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
  other->Insert("other.avro", std::make_shared<const std::vector<std::byte>>(8), 0);
  cache->Invalidate("manifest.avro");
  cache->Insert("manifest.avro", std::make_shared<const std::vector<std::byte>>(8),
                generation);
  EXPECT_FALSE(cache->Find("manifest.avro"));
}

}  // namespace
}  // namespace iceberg
