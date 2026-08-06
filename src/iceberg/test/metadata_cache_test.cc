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

#include "iceberg/metadata_cache.h"

#include <atomic>
#include <barrier>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <gtest/gtest.h>

#include "iceberg/file_io_registry.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

using namespace std::chrono_literals;

MetadataCacheOptions EnabledOptions(size_t max_total_bytes = 100,
                                    size_t max_content_length = 100) {
  return {
      .enabled = true,
      .expiration_interval_ms = 0,
      .max_total_bytes = max_total_bytes,
      .max_content_length = max_content_length,
  };
}

class CountingInputFile : public InputFile {
 public:
  CountingInputFile(std::unique_ptr<InputFile> input_file, int* open_count)
      : input_file_(std::move(input_file)), open_count_(open_count) {}

  std::string_view location() const override { return input_file_->location(); }

  Result<int64_t> Size() const override { return input_file_->Size(); }

  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    ++*open_count_;
    return input_file_->Open();
  }

 private:
  std::unique_ptr<InputFile> input_file_;
  int* open_count_;
};

class CountingMockFileIO : public MockFileIO {
 public:
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override {
    ICEBERG_ASSIGN_OR_RAISE(auto input_file,
                            MockFileIO::NewInputFile(std::move(file_location)));
    return std::make_unique<CountingInputFile>(std::move(input_file), &open_count);
  }

  int open_count = 0;
};

TEST(MetadataCacheTest, ReusesContentAcrossLoads) {
  auto cache_result = MetadataCache::Make(EnabledOptions());
  ASSERT_THAT(cache_result, IsOk());
  auto cache = std::move(cache_result).value();

  int loads = 0;
  auto loader = [&]() -> Result<MetadataCache::Content> {
    ++loads;
    return std::make_shared<const std::string>("metadata");
  };

  auto first = cache->Get("s3://warehouse/table/metadata/v1.metadata.json", 8, loader);
  auto second = cache->Get("s3://warehouse/table/metadata/v1.metadata.json", 8, loader);

  ASSERT_THAT(first, IsOk());
  ASSERT_THAT(second, IsOk());
  EXPECT_EQ(loads, 1);
  EXPECT_EQ(first.value(), second.value());
  EXPECT_EQ(cache->size(), 1);
  EXPECT_EQ(cache->total_bytes(), 8);
}

TEST(MetadataCacheTest, FileIOReusesCachedContentAcrossInputFiles) {
  CountingMockFileIO file_io;
  file_io.AddFile("manifest.avro", "manifest");
  ASSERT_THAT(file_io.ConfigureMetadataCache(
                  {{std::string(MetadataCacheOptions::kEnabled), "true"}}),
              IsOk());

  auto first_file = file_io.NewCachedInputFile("manifest.avro");
  ASSERT_THAT(first_file, IsOk());
  auto first_stream = first_file.value()->Open();
  ASSERT_THAT(first_stream, IsOk());
  ASSERT_THAT(first_stream.value()->Close(), IsOk());

  auto second_file = file_io.NewCachedInputFile("manifest.avro");
  ASSERT_THAT(second_file, IsOk());
  auto second_stream = second_file.value()->Open();
  ASSERT_THAT(second_stream, IsOk());
  ASSERT_THAT(second_stream.value()->Close(), IsOk());

  EXPECT_EQ(file_io.open_count, 1);
}

TEST(MetadataCacheTest, FileIORejectsConflictingReconfiguration) {
  CountingMockFileIO file_io;
  std::unordered_map<std::string, std::string> properties = {
      {std::string(MetadataCacheOptions::kEnabled), "true"},
  };
  ASSERT_THAT(file_io.ConfigureMetadataCache(properties), IsOk());
  ASSERT_THAT(file_io.ConfigureMetadataCache(properties), IsOk());
  properties[std::string(MetadataCacheOptions::kMaxTotalBytes)] = "1024";

  EXPECT_THAT(file_io.ConfigureMetadataCache(properties),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(MetadataCacheTest, RegistryWithoutCachePropertiesAllowsLaterConfiguration) {
  constexpr std::string_view kFileIOName = "metadata-cache-test";
  FileIORegistry::Register(
      std::string(kFileIOName),
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> {
        return std::make_unique<CountingMockFileIO>();
      });

  auto loaded = FileIORegistry::Load(std::string(kFileIOName), {});
  ASSERT_THAT(loaded, IsOk());
  auto file_io = std::move(loaded).value();
  EXPECT_FALSE(file_io->MetadataCacheEnabled());
  EXPECT_THAT(file_io->ConfigureMetadataCache(
                  {{std::string(MetadataCacheOptions::kEnabled), "true"}}),
              IsOk());
  EXPECT_TRUE(file_io->MetadataCacheEnabled());
}

TEST(MetadataCacheTest, FileIORejectsConcurrentConflictingConfiguration) {
  CountingMockFileIO file_io;
  std::barrier start(3);
  auto configure = [&](size_t max_total_bytes) {
    start.arrive_and_wait();
    return file_io.ConfigureMetadataCache(
        {{std::string(MetadataCacheOptions::kEnabled), "true"},
         {std::string(MetadataCacheOptions::kMaxTotalBytes),
          std::to_string(max_total_bytes)}});
  };
  auto first = std::async(std::launch::async, configure, 1024);
  auto second = std::async(std::launch::async, configure, 2048);
  start.arrive_and_wait();

  auto first_result = first.get();
  auto second_result = second.get();

  EXPECT_NE(first_result.has_value(), second_result.has_value());
  const auto& failed = first_result.has_value() ? second_result : first_result;
  EXPECT_THAT(failed, IsError(ErrorKind::kInvalidArgument));
}

TEST(MetadataCacheTest, DisabledCacheAlwaysLoads) {
  auto cache_result = MetadataCache::Make(MetadataCacheOptions{});
  ASSERT_THAT(cache_result, IsOk());
  auto cache = std::move(cache_result).value();
  int loads = 0;
  auto loader = [&]() -> Result<MetadataCache::Content> {
    ++loads;
    return std::make_shared<const std::string>("manifest");
  };

  ASSERT_THAT(cache->Get("manifest.avro", 8, loader), IsOk());
  ASSERT_THAT(cache->Get("manifest.avro", 8, loader), IsOk());

  EXPECT_EQ(loads, 2);
  EXPECT_EQ(cache->size(), 0);
}

TEST(MetadataCacheTest, DoesNotCacheLoadFailures) {
  auto cache_result = MetadataCache::Make(EnabledOptions());
  ASSERT_THAT(cache_result, IsOk());
  auto cache = std::move(cache_result).value();
  int loads = 0;
  auto loader = [&]() -> Result<MetadataCache::Content> {
    ++loads;
    if (loads == 1) {
      return IOError("temporary read failure");
    }
    return std::make_shared<const std::string>("manifest");
  };

  EXPECT_THAT(cache->Get("manifest.avro", 8, loader), IsError(ErrorKind::kIOError));
  EXPECT_THAT(cache->Get("manifest.avro", 8, loader), IsOk());

  EXPECT_EQ(loads, 2);
  EXPECT_EQ(cache->size(), 1);
}

TEST(MetadataCacheTest, CoalescesConcurrentLoadFailures) {
  auto cache_result = MetadataCache::Make(EnabledOptions());
  ASSERT_THAT(cache_result, IsOk());
  auto cache = std::move(cache_result).value();
  std::atomic<int> loads = 0;
  std::promise<void> load_started;
  std::promise<void> release_load;
  auto release = release_load.get_future().share();
  auto loader = [&]() -> Result<MetadataCache::Content> {
    if (++loads == 1) {
      load_started.set_value();
      release.wait();
    }
    return IOError("temporary read failure");
  };

  auto first = std::async(std::launch::async,
                          [&] { return cache->Get("manifest.avro", 8, loader); });
  load_started.get_future().wait();
  std::promise<void> second_started;
  auto second = std::async(std::launch::async, [&] {
    second_started.set_value();
    return cache->Get("manifest.avro", 8, loader);
  });
  second_started.get_future().wait();
  EXPECT_EQ(second.wait_for(20ms), std::future_status::timeout);
  release_load.set_value();

  EXPECT_THAT(first.get(), IsError(ErrorKind::kIOError));
  EXPECT_THAT(second.get(), IsError(ErrorKind::kIOError));
  EXPECT_EQ(loads.load(), 1);
}

TEST(MetadataCacheTest, SkipsContentAboveMaximumLength) {
  auto cache_result = MetadataCache::Make(EnabledOptions(100, 4));
  ASSERT_THAT(cache_result, IsOk());
  auto cache = std::move(cache_result).value();
  int loads = 0;
  auto loader = [&]() -> Result<MetadataCache::Content> {
    ++loads;
    return std::make_shared<const std::string>("manifest");
  };

  ASSERT_THAT(cache->Get("manifest.avro", 8, loader), IsOk());
  ASSERT_THAT(cache->Get("manifest.avro", 8, loader), IsOk());

  EXPECT_EQ(loads, 2);
  EXPECT_EQ(cache->size(), 0);
}

TEST(MetadataCacheTest, EvictsLeastRecentlyUsedContentByWeight) {
  auto cache_result = MetadataCache::Make(EnabledOptions(6));
  ASSERT_THAT(cache_result, IsOk());
  auto cache = std::move(cache_result).value();
  auto load = [](std::string content) {
    return [content = std::move(content)]() -> Result<MetadataCache::Content> {
      return std::make_shared<const std::string>(content);
    };
  };

  ASSERT_THAT(cache->Get("a", 2, load("aa")), IsOk());
  ASSERT_THAT(cache->Get("b", 3, load("bbb")), IsOk());
  ASSERT_NE(cache->GetIfPresent("a"), nullptr);
  ASSERT_THAT(cache->Get("c", 4, load("cccc")), IsOk());

  EXPECT_NE(cache->GetIfPresent("a"), nullptr);
  EXPECT_EQ(cache->GetIfPresent("b"), nullptr);
  EXPECT_NE(cache->GetIfPresent("c"), nullptr);
  EXPECT_EQ(cache->total_bytes(), 6);
}

TEST(MetadataCacheTest, InvalidatesOneLocation) {
  auto cache_result = MetadataCache::Make(EnabledOptions());
  ASSERT_THAT(cache_result, IsOk());
  auto cache = std::move(cache_result).value();
  auto loader = []() -> Result<MetadataCache::Content> {
    return std::make_shared<const std::string>("manifest-list");
  };
  ASSERT_THAT(cache->Get("snap.avro", 13, loader), IsOk());

  cache->Invalidate("snap.avro");
  EXPECT_EQ(cache->GetIfPresent("snap.avro"), nullptr);
  EXPECT_EQ(cache->size(), 0);
  EXPECT_EQ(cache->total_bytes(), 0);
}

TEST(MetadataCacheTest, ClearsCachedContent) {
  auto cache_result = MetadataCache::Make(EnabledOptions());
  ASSERT_THAT(cache_result, IsOk());
  auto cache = std::move(cache_result).value();
  int loads = 0;
  auto loader = [&]() -> Result<MetadataCache::Content> {
    ++loads;
    return std::make_shared<const std::string>("manifest-list");
  };
  ASSERT_THAT(cache->Get("snap.avro", 13, loader), IsOk());

  cache->Clear();

  EXPECT_EQ(cache->size(), 0);
  EXPECT_EQ(cache->total_bytes(), 0);
  ASSERT_THAT(cache->Get("snap.avro", 13, loader), IsOk());
  EXPECT_EQ(loads, 2);
}

TEST(MetadataCacheTest, ClearWaitsForInflightLoad) {
  auto cache_result = MetadataCache::Make(EnabledOptions());
  ASSERT_THAT(cache_result, IsOk());
  auto cache = std::move(cache_result).value();
  std::promise<void> load_started;
  std::promise<void> release_load;
  auto release = release_load.get_future().share();
  auto load = std::async(std::launch::async, [&] {
    return cache->Get("snap.avro", 13, [&]() -> Result<MetadataCache::Content> {
      load_started.set_value();
      release.wait();
      return std::make_shared<const std::string>("manifest-list");
    });
  });
  load_started.get_future().wait();
  std::promise<void> clear_started;
  auto clear = std::async(std::launch::async, [&] {
    clear_started.set_value();
    cache->Clear();
  });
  clear_started.get_future().wait();
  EXPECT_EQ(clear.wait_for(20ms), std::future_status::timeout);

  release_load.set_value();
  EXPECT_THAT(load.get(), IsOk());
  clear.get();

  EXPECT_EQ(cache->size(), 0);
  EXPECT_EQ(cache->total_bytes(), 0);
}

TEST(MetadataCacheTest, ParsesJavaCompatibleProperties) {
  std::unordered_map<std::string, std::string> properties = {
      {std::string(MetadataCacheOptions::kEnabled), "true"},
      {std::string(MetadataCacheOptions::kExpirationIntervalMs), "1234"},
      {std::string(MetadataCacheOptions::kMaxTotalBytes), "200"},
      {std::string(MetadataCacheOptions::kMaxContentLength), "50"},
  };

  auto options = MetadataCacheOptions::FromProperties(properties);

  ASSERT_THAT(options, IsOk());
  EXPECT_TRUE(options->enabled);
  EXPECT_EQ(options->expiration_interval_ms, 1234);
  EXPECT_EQ(options->max_total_bytes, 200);
  EXPECT_EQ(options->max_content_length, 50);
}

TEST(MetadataCacheTest, UsesJavaCompatibleDefaults) {
  auto options = MetadataCacheOptions::FromProperties(
      {{std::string(MetadataCacheOptions::kEnabled), "true"}});

  ASSERT_THAT(options, IsOk());
  EXPECT_TRUE(options->enabled);
  EXPECT_EQ(options->expiration_interval_ms, 60 * 1000);
  EXPECT_EQ(options->max_total_bytes, 100 * 1024 * 1024);
  EXPECT_EQ(options->max_content_length, 8 * 1024 * 1024);
}

TEST(MetadataCacheTest, RejectsInvalidEnabledConfiguration) {
  auto options = EnabledOptions();
  options.expiration_interval_ms = -1;

  EXPECT_THAT(MetadataCache::Make(options), IsError(ErrorKind::kInvalidArgument));
}

TEST(MetadataCacheTest, InvalidNumericPropertyErrorIncludesPropertyName) {
  auto options = MetadataCacheOptions::FromProperties(
      {{std::string(MetadataCacheOptions::kEnabled), "true"},
       {std::string(MetadataCacheOptions::kMaxTotalBytes), "invalid"}});

  EXPECT_THAT(options,
              HasErrorMessage(std::string(MetadataCacheOptions::kMaxTotalBytes)));
}

}  // namespace
}  // namespace iceberg
