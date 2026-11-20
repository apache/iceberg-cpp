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
#include <atomic>
#include <barrier>
#include <cstdint>
#include <future>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/content_cache_internal.h"
#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/file_reader.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {
using internal::ContentCacheOptions;

class ScopedGlobalCacheLimit {
 public:
  explicit ScopedGlobalCacheLimit(int32_t limit)
      : previous_(internal::ContentCache::SetGlobalCapacity(limit)) {}
  ~ScopedGlobalCacheLimit() { internal::ContentCache::SetGlobalCapacity(previous_); }

 private:
  int32_t previous_;
};

class CountingInputStream : public SeekableInputStream {
 public:
  CountingInputStream(std::unique_ptr<SeekableInputStream> stream, int* read_count)
      : stream_(std::move(stream)), read_count_(read_count) {}

  Result<int64_t> Position() const override { return stream_->Position(); }
  Status Seek(int64_t position) override { return stream_->Seek(position); }

  Result<int64_t> Read(std::span<std::byte> out) override {
    ++*read_count_;
    return stream_->Read(out);
  }

  Status ReadFully(int64_t position, std::span<std::byte> out) override {
    ++*read_count_;
    return stream_->ReadFully(position, out);
  }

  Status Close() override { return stream_->Close(); }

 private:
  std::unique_ptr<SeekableInputStream> stream_;
  int* read_count_;
};

class CountingInputFile : public InputFile {
 public:
  CountingInputFile(std::unique_ptr<InputFile> input_file, int* open_count,
                    int* size_count, int* read_count, int* failing_opens)
      : input_file_(std::move(input_file)),
        open_count_(open_count),
        size_count_(size_count),
        read_count_(read_count),
        failing_opens_(failing_opens) {}

  std::string_view location() const override { return input_file_->location(); }

  Result<int64_t> Size() const override {
    ++*size_count_;
    return input_file_->Size();
  }

  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    ++*open_count_;
    if (*failing_opens_ > 0) {
      --*failing_opens_;
      return NotFound("Simulated open failure");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto stream, input_file_->Open());
    return std::make_unique<CountingInputStream>(std::move(stream), read_count_);
  }

 private:
  std::unique_ptr<InputFile> input_file_;
  int* open_count_;
  int* size_count_;
  int* read_count_;
  int* failing_opens_;
};

class CountingMockFileIO : public FileIOWithProperties<MockFileIO> {
 public:
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override {
    ICEBERG_ASSIGN_OR_RAISE(auto input_file,
                            MockFileIO::NewInputFile(std::move(file_location)));
    return std::make_unique<CountingInputFile>(std::move(input_file), &open_count,
                                               &size_count, &read_count, &failing_opens);
  }

  int open_count = 0;
  int size_count = 0;
  int read_count = 0;
  // Number of upcoming Open calls that fail with kNotFound.
  int failing_opens = 0;
};

// Configure storage before creating an independently cached table.
template <typename IO>
Result<std::shared_ptr<Table>> MakeCachedTable(
    IO& io, const std::unordered_map<std::string, std::string>& properties = {},
    std::string metadata_location = "metadata.json",
    const std::unordered_map<std::string, std::string>& table_properties = {}) {
  io.config = properties;
  auto metadata = std::make_shared<TableMetadata>();
  metadata->table_uuid = "cache-test-table";
  metadata->properties = TableProperties::FromMap(table_properties);
  auto storage = std::shared_ptr<FileIO>(&io, [](FileIO*) {});
  return Table::Make({.ns = {}, .name = "test"}, std::move(metadata),
                     std::move(metadata_location), std::move(storage),
                     std::make_shared<MockCatalog>());
}

// Read a whole immutable file through a table's manifest I/O context.
Result<std::string> ReadCached(FileIO& file_io, const std::string& location) {
  ICEBERG_ASSIGN_OR_RAISE(auto input_file, file_io.NewInputFile(location));
  ICEBERG_ASSIGN_OR_RAISE(auto size, input_file->Size());
  ICEBERG_ASSIGN_OR_RAISE(auto stream, input_file->Open());
  std::string content(static_cast<size_t>(size), '\0');
  ICEBERG_RETURN_UNEXPECTED(
      stream->ReadFully(/*position=*/0, std::as_writable_bytes(std::span(content))));
  ICEBERG_RETURN_UNEXPECTED(stream->Close());
  return content;
}

class NormalizingMockFileIO : public CountingMockFileIO {
 public:
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override {
    constexpr std::string_view kAliasPrefix = "alias://";
    if (file_location.starts_with(kAliasPrefix)) {
      file_location.erase(0, kAliasPrefix.size());
    }
    return CountingMockFileIO::NewInputFile(std::move(file_location));
  }
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

class FailingOpenFileIO : public FileIOWithProperties<FileIO> {
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

TEST(MetadataCacheTest, TableReusesCachedContentAcrossInputFiles) {
  CountingMockFileIO file_io;
  file_io.AddFile("manifest.avro", "manifest");
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(file_io, {{std::string(ContentCacheOptions::kEnabled), "true"}}));

  auto first_file = internal::ManifestIO(*table)->NewInputFile("manifest.avro");
  ASSERT_THAT(first_file, IsOk());
  auto first_stream = first_file.value()->Open();
  ASSERT_THAT(first_stream, IsOk());
  ASSERT_THAT(first_stream.value()->Close(), IsOk());

  auto second_file = internal::ManifestIO(*table)->NewInputFile("manifest.avro");
  ASSERT_THAT(second_file, IsOk());
  auto second_stream = second_file.value()->Open();
  ASSERT_THAT(second_stream, IsOk());
  ASSERT_THAT(second_stream.value()->Close(), IsOk());

  EXPECT_EQ(file_io.open_count, 1);
}

TEST(MetadataCacheTest, TableUsesNormalizedLocationForCacheHits) {
  NormalizingMockFileIO file_io;
  file_io.AddFile("manifest.avro", "manifest");
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(file_io, {{std::string(ContentCacheOptions::kEnabled), "true"}}));

  for (const auto* location :
       {"alias://manifest.avro", "alias://manifest.avro", "manifest.avro"}) {
    ICEBERG_UNWRAP_OR_FAIL(auto input_file,
                           internal::ManifestIO(*table)->NewInputFile(location));
    EXPECT_EQ(input_file->location(), "manifest.avro");
    ICEBERG_UNWRAP_OR_FAIL(auto stream, input_file->Open());
    ASSERT_THAT(stream->Close(), IsOk());
  }

  EXPECT_EQ(file_io.size_count, 1);
  EXPECT_EQ(file_io.open_count, 1);
}

TEST(MetadataCacheTest, CachedInputFileDoesNotRetryNonIOErrors) {
  auto state = std::make_shared<FailingOpenState>(FailingOpenState{
      .first_error = {.kind = ErrorKind::kNotFound, .message = "missing"},
      .fallback_error = {.kind = ErrorKind::kIOError, .message = "unexpected retry"},
  });
  FailingOpenFileIO file_io(state);
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(file_io, {{std::string(ContentCacheOptions::kEnabled), "true"}}));
  ICEBERG_UNWRAP_OR_FAIL(auto input_file,
                         internal::ManifestIO(*table)->NewInputFile("manifest.avro"));

  EXPECT_THAT(input_file->Open(), IsError(ErrorKind::kNotFound));
  EXPECT_EQ(state->open_count, 1);
}

TEST(MetadataCacheTest, CachedInputFilePreservesReadAheadAndFallbackErrors) {
  auto state = std::make_shared<FailingOpenState>(FailingOpenState{
      .first_error = {.kind = ErrorKind::kIOError, .message = "read-ahead failed"},
      .fallback_error = {.kind = ErrorKind::kIOError, .message = "open failed"},
  });
  FailingOpenFileIO file_io(state);
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(file_io, {{std::string(ContentCacheOptions::kEnabled), "true"}}));
  ICEBERG_UNWRAP_OR_FAIL(auto input_file,
                         internal::ManifestIO(*table)->NewInputFile("manifest.avro"));

  auto result = input_file->Open();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
  EXPECT_THAT(result, HasErrorMessage("read-ahead failed"));
  EXPECT_THAT(result, HasErrorMessage("fallback open failed: open failed"));
  EXPECT_EQ(state->open_count, 2);
}

TEST(MetadataCacheTest, TableDoesNotCacheLoadFailures) {
  CountingMockFileIO file_io;
  file_io.AddFile("manifest.avro", "manifest");
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(file_io, {{std::string(ContentCacheOptions::kEnabled), "true"}}));
  file_io.failing_opens = 1;

  EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"),
              IsError(ErrorKind::kNotFound));
  EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"),
              HasValue(::testing::Eq("manifest")));
  EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"),
              HasValue(::testing::Eq("manifest")));
  // The failed open, then the successful load. The last read comes from the cache.
  EXPECT_EQ(file_io.open_count, 2);
}

TEST(MetadataCacheTest, TableReadsUncacheableFilesDirectly) {
  for (auto limit :
       {ContentCacheOptions::kMaxTotalBytes, ContentCacheOptions::kMaxContentLength}) {
    SCOPED_TRACE(limit);
    CountingMockFileIO file_io;
    file_io.AddFile("manifest.avro", "manifest");
    ICEBERG_UNWRAP_OR_FAIL(
        auto table,
        MakeCachedTable(file_io, {{std::string(ContentCacheOptions::kEnabled), "true"},
                                  {std::string(limit), "4"}}));

    // The 8-byte file can never be cached, so opening it must not load the whole file
    // into memory first.
    ICEBERG_UNWRAP_OR_FAIL(auto input_file,
                           internal::ManifestIO(*table)->NewInputFile("manifest.avro"));
    ICEBERG_UNWRAP_OR_FAIL(auto stream, input_file->Open());
    EXPECT_EQ(file_io.read_count, 0);
    ASSERT_THAT(stream->Close(), IsOk());

    EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"),
                HasValue(::testing::Eq("manifest")));
    EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"),
                HasValue(::testing::Eq("manifest")));
    EXPECT_EQ(file_io.open_count, 3);
  }
}

TEST(MetadataCacheTest, TableEvictsLeastRecentlyUsedContentByBytes) {
  CountingMockFileIO file_io;
  file_io.AddFile("a", "aa");
  file_io.AddFile("b", "bbb");
  file_io.AddFile("c", "cccc");
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(file_io,
                      {{std::string(ContentCacheOptions::kEnabled), "true"},
                       {std::string(ContentCacheOptions::kMaxTotalBytes), "6"}}));

  // Return the number of underlying opens after reading `location`.
  auto read = [&file_io, &table](const std::string& location) {
    EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), location), IsOk());
    return file_io.open_count;
  };
  EXPECT_EQ(read("a"), 1);
  EXPECT_EQ(read("b"), 2);
  EXPECT_EQ(read("a"), 2);  // Cached, and now the most recently used.
  EXPECT_EQ(read("c"), 3);  // 2 + 3 + 4 bytes exceed 6, so "b" is evicted.
  EXPECT_EQ(read("a"), 3);
  EXPECT_EQ(read("c"), 3);
  EXPECT_EQ(read("b"), 4);
}

TEST(MetadataCacheTest, TableClearsCachedContent) {
  CountingMockFileIO file_io;
  file_io.AddFile("snap.avro", "manifest-list");
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(file_io, {{std::string(ContentCacheOptions::kEnabled), "true"}}));
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "snap.avro"), IsOk());

  table->ClearManifestCache();

  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "snap.avro"), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "snap.avro"), IsOk());
  EXPECT_EQ(file_io.open_count, 2);
}

TEST(MetadataCacheTest, ManifestDeletesInvalidateOnlyTheAttemptedLocations) {
  CountingMockFileIO io;
  io.AddFile("single.avro", "old");
  io.AddFile("batch.avro", "old");
  io.AddFile("retained.avro", "retained");
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(io, {{std::string(ContentCacheOptions::kEnabled), "true"}}));
  auto manifest_io = internal::ManifestIO(*table);
  for (const auto* name : {"single.avro", "batch.avro", "retained.avro"}) {
    ASSERT_THAT(ReadCached(*manifest_io, name), IsOk());
  }
  EXPECT_CALL(io, DeleteFile("single.avro")).WillOnce(::testing::Return(Status{}));
  EXPECT_CALL(io, DeleteFile("batch.avro")).WillOnce(::testing::Return(Status{}));
  ASSERT_THAT(manifest_io->DeleteFile("single.avro"), IsOk());
  ASSERT_THAT(manifest_io->DeleteFiles({"batch.avro"}), IsOk());
  for (const auto* name : {"single.avro", "batch.avro"}) {
    io.AddFile(name, "new");
    EXPECT_THAT(ReadCached(*manifest_io, name), HasValue(::testing::Eq("new")));
  }
  EXPECT_THAT(ReadCached(*manifest_io, "retained.avro"),
              HasValue(::testing::Eq("retained")));
  EXPECT_EQ(io.open_count, 5);
}

TEST(MetadataCacheTest, TableConcurrentReadsReturnFileContent) {
  // CountingMockFileIO's counters are not thread-safe, so use the plain mock.
  FileIOWithProperties<MockFileIO> file_io;
  constexpr int kLocations = 8;
  std::vector<std::string> contents;
  for (int id = 0; id < kLocations; ++id) {
    contents.emplace_back(static_cast<size_t>(id % 4 + 1), static_cast<char>('a' + id));
    file_io.AddFile(std::to_string(id), contents.back());
  }
  // The files add up to 20 bytes, so reads keep evicting from the 16-byte cache.
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(file_io,
                      {{std::string(ContentCacheOptions::kEnabled), "true"},
                       {std::string(ContentCacheOptions::kMaxTotalBytes), "16"}}));

  constexpr int kThreads = 4;
  constexpr int kIterations = 1000;
  std::atomic<bool> mismatch = false;
  std::vector<std::future<void>> workers;
  for (int thread = 0; thread < kThreads; ++thread) {
    workers.push_back(std::async(std::launch::async, [&, thread] {
      for (int i = 0; i < kIterations; ++i) {
        const int id = (thread + i) % kLocations;
        auto content = ReadCached(*internal::ManifestIO(*table), std::to_string(id));
        if (!content.has_value() || content.value() != contents[id]) {
          mismatch.store(true, std::memory_order_relaxed);
        }
      }
    }));
  }
  for (auto& worker : workers) {
    worker.get();
  }

  EXPECT_FALSE(mismatch.load(std::memory_order_relaxed));
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

TEST(MetadataCacheTest, ReaderOptionsKeepsPositionalAggregateInitialization) {
  auto io = std::make_shared<MockFileIO>();
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{});
  // Verify positional initialization remains compatible with the existing API.
  // NOLINTNEXTLINE(modernize-use-designated-initializers)
  ReaderOptions options{"manifest.avro", {}, {}, io, schema};
  EXPECT_EQ(options.path, "manifest.avro");
  EXPECT_EQ(options.io, io);
  EXPECT_EQ(options.projection, schema);
}

class LengthTrackingFileIO : public FileIOWithProperties<MockFileIO> {
 public:
  using MockFileIO::NewInputFile;
  int length_calls = 0;
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string location,
                                                  size_t length) override {
    ++length_calls;
    return MockFileIO::NewInputFile(std::move(location), length);
  }
};

TEST(MetadataCacheTest, RejectsOversizedLengthBeforeCallingStorage) {
  if constexpr (sizeof(size_t) <= sizeof(int32_t)) return;
  LengthTrackingFileIO io;
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(io, {{std::string(ContentCacheOptions::kEnabled), "true"}}));
  EXPECT_THAT(internal::ManifestIO(*table)->NewInputFile(
                  "manifest.avro", std::numeric_limits<size_t>::max()),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_EQ(io.length_calls, 0);
}

TEST(MetadataCacheTest, DisabledTableUsesStorageDirectly) {
  CountingMockFileIO io;
  io.AddFile("manifest.avro", "manifest");
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeCachedTable(io));
  EXPECT_FALSE(table->ManifestCacheEnabled());
  EXPECT_EQ(internal::ManifestIO(*table), table->io());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
  EXPECT_EQ(io.open_count, 2);
}

TEST(MetadataCacheTest, TablePropertiesDoNotEnableManifestCache) {
  CountingMockFileIO io;
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(io, {}, "metadata.json",
                      {{std::string(ContentCacheOptions::kEnabled), "true"}}));
  EXPECT_FALSE(table->ManifestCacheEnabled());
  EXPECT_EQ(internal::ManifestIO(*table), table->io());
}

TEST(MetadataCacheTest, TablePropertiesDoNotOverrideFileIOCacheSettings) {
  CountingMockFileIO io;
  io.AddFile("manifest.avro", "manifest");
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(io,
                      {{std::string(ContentCacheOptions::kEnabled), "true"},
                       {std::string(ContentCacheOptions::kMaxTotalBytes), "16"},
                       {std::string(ContentCacheOptions::kMaxContentLength), "16"}},
                      "metadata.json",
                      {{std::string(ContentCacheOptions::kEnabled), "false"},
                       {std::string(ContentCacheOptions::kMaxTotalBytes), "invalid"},
                       {std::string(ContentCacheOptions::kMaxContentLength), "1"}}));
  EXPECT_TRUE(table->ManifestCacheEnabled());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
  EXPECT_EQ(io.open_count, 1);
}

TEST(MetadataCacheTest, TablesSharingStorageHaveIndependentCachesAndLimits) {
  CountingMockFileIO io;
  io.AddFile("manifest.avro", "manifest");
  ICEBERG_UNWRAP_OR_FAIL(
      auto first,
      MakeCachedTable(io, {{std::string(ContentCacheOptions::kEnabled), "true"},
                           {std::string(ContentCacheOptions::kMaxTotalBytes), "16"}}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second,
      MakeCachedTable(io, {{std::string(ContentCacheOptions::kEnabled), "true"},
                           {std::string(ContentCacheOptions::kMaxTotalBytes), "4"}}));
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*first), "manifest.avro"), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*second), "manifest.avro"), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*first), "manifest.avro"), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*second), "manifest.avro"), IsOk());
  EXPECT_EQ(io.open_count, 3);
  // Ordinary FileIO input remains independent of either table's cache.
  ASSERT_THAT(ReadCached(io, "manifest.avro"), IsOk());
  EXPECT_EQ(io.open_count, 4);
}

TEST(MetadataCacheTest, GlobalLimitEvictsLeastRecentlyUsedCacheWithLiveTableHandles) {
  ScopedGlobalCacheLimit limit(2);
  CountingMockFileIO io;
  io.AddFile("manifest.avro", "manifest");
  std::unordered_map<std::string, std::string> props = {
      {std::string(ContentCacheOptions::kEnabled), "true"}};
  ICEBERG_UNWRAP_OR_FAIL(auto first, MakeCachedTable(io, props));
  ICEBERG_UNWRAP_OR_FAIL(auto second, MakeCachedTable(io, props));
  ICEBERG_UNWRAP_OR_FAIL(auto third, MakeCachedTable(io, props));
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*first), "manifest.avro"), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*second), "manifest.avro"), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*first), "manifest.avro"), IsOk());
  EXPECT_EQ(io.open_count, 2);
  // Touching the first cache makes the second the next victim. The adapter must
  // retain only a weak reference to its cached content, even while the table lives.
  auto second_adapter = internal::ManifestIO(*second);
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*third), "manifest.avro"), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*first), "manifest.avro"), IsOk());
  EXPECT_EQ(io.open_count, 3);
  ASSERT_THAT(ReadCached(*second_adapter, "manifest.avro"), IsOk());
  EXPECT_EQ(io.open_count, 4);
}

TEST(MetadataCacheTest, GlobalEvictionReleasesBytesAndReadersKeepTheirContent) {
  ScopedGlobalCacheLimit limit(1);
  auto first = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
  auto content = std::make_shared<const std::vector<std::byte>>(8);
  std::weak_ptr<const std::vector<std::byte>> weak_content = content;
  first->Insert("manifest.avro", std::move(content), 0);
  CountingMockFileIO io;
  auto adapter = first->Wrap(std::shared_ptr<FileIO>(&io, [](FileIO*) {}));
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

TEST(MetadataCacheTest, LoweringGlobalLimitImmediatelyReleasesOldCacheContent) {
  ScopedGlobalCacheLimit limit(2);
  auto first = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
  auto second = std::make_shared<internal::ContentCache>(ContentCacheOptions{});
  auto content = std::make_shared<const std::vector<std::byte>>(8);
  std::weak_ptr<const std::vector<std::byte>> weak_content = content;
  first->Insert("first.avro", std::move(content), 0);
  second->Insert("second.avro", std::make_shared<const std::vector<std::byte>>(8), 0);
  ASSERT_THAT(Table::SetManifestCacheLimit(1), IsOk());
  EXPECT_TRUE(weak_content.expired());
  EXPECT_TRUE(second->Find("second.avro"));
}

TEST(MetadataCacheTest, RejectsNonPositiveGlobalLimits) {
  EXPECT_THAT(Table::SetManifestCacheLimit(0), IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(Table::SetManifestCacheLimit(-1), IsError(ErrorKind::kInvalidArgument));
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

TEST(MetadataCacheTest, ConcurrentReadsSurviveGlobalEviction) {
  ScopedGlobalCacheLimit limit(2);
  FileIOWithProperties<MockFileIO> io;
  io.AddFile("manifest.avro", "manifest");
  std::vector<std::shared_ptr<Table>> tables;
  for (int i = 0; i < 4; ++i) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto table,
        MakeCachedTable(io, {{std::string(ContentCacheOptions::kEnabled), "true"}}));
    tables.push_back(std::move(table));
  }
  std::barrier start(4);
  std::vector<std::future<bool>> readers;
  for (const auto& table : tables) {
    readers.push_back(std::async(std::launch::async, [&start, table] {
      start.arrive_and_wait();
      for (int i = 0; i < 1000; ++i) {
        auto content = ReadCached(*internal::ManifestIO(*table), "manifest.avro");
        if (!content || *content != "manifest") return false;
      }
      return true;
    }));
  }
  for (auto& reader : readers) {
    EXPECT_TRUE(reader.get());
  }
}

TEST(MetadataCacheTest, CompatibleRefreshPreservesCachedContent) {
  CountingMockFileIO io;
  io.AddFile("manifest.avro", "manifest");
  std::unordered_map<std::string, std::string> props = {
      {std::string(ContentCacheOptions::kEnabled), "true"}};
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeCachedTable(io, props));
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto refreshed, MakeCachedTable(io, props, "metadata-2.json"));
  auto catalog = std::dynamic_pointer_cast<MockCatalog>(table->catalog());
  EXPECT_CALL(*catalog, LoadTable(::testing::_)).WillOnce(::testing::Return(refreshed));
  ASSERT_THAT(table->Refresh(), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
  EXPECT_EQ(io.open_count, 1);
  table->ClearManifestCache();
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
  EXPECT_EQ(io.open_count, 2);
}

TEST(MetadataCacheTest, RefreshWithChangedLimitsReplacesCache) {
  CountingMockFileIO io;
  io.AddFile("manifest.avro", "manifest");
  std::unordered_map<std::string, std::string> props = {
      {std::string(ContentCacheOptions::kEnabled), "true"}};
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeCachedTable(io, props));
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
  props[std::string(ContentCacheOptions::kMaxTotalBytes)] = "4";
  ICEBERG_UNWRAP_OR_FAIL(auto refreshed, MakeCachedTable(io, props, "metadata-2.json"));
  auto catalog = std::dynamic_pointer_cast<MockCatalog>(table->catalog());
  EXPECT_CALL(*catalog, LoadTable(::testing::_)).WillOnce(::testing::Return(refreshed));
  ASSERT_THAT(table->Refresh(), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
  EXPECT_EQ(io.open_count, 3);
}

TEST(MetadataCacheTest, RefreshWithDifferentStorageDiscardsCachedBytes) {
  CountingMockFileIO first_io;
  CountingMockFileIO second_io;
  first_io.AddFile("manifest.avro", "first");
  second_io.AddFile("manifest.avro", "second");
  std::unordered_map<std::string, std::string> props = {
      {std::string(ContentCacheOptions::kEnabled), "true"}};
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeCachedTable(first_io, props));
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"),
              HasValue(::testing::Eq("first")));
  ICEBERG_UNWRAP_OR_FAIL(auto refreshed,
                         MakeCachedTable(second_io, props, "metadata-2.json"));
  auto catalog = std::dynamic_pointer_cast<MockCatalog>(table->catalog());
  EXPECT_CALL(*catalog, LoadTable(::testing::_)).WillOnce(::testing::Return(refreshed));
  ASSERT_THAT(table->Refresh(), IsOk());
  EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"),
              HasValue(::testing::Eq("second")));
}

TEST(MetadataCacheTest, RefreshWithRotatedCredentialsPreservesCacheAndUsesNewStorage) {
  for (const auto* metadata_location : {"metadata.json", "metadata-2.json"}) {
    SCOPED_TRACE(metadata_location);
    for (bool explicit_endpoint : {false, true}) {
      SCOPED_TRACE(explicit_endpoint);
      CountingMockFileIO first_io;
      CountingMockFileIO second_io;
      first_io.AddFile("manifest.avro", "cached");
      second_io.AddFile("new-manifest.avro", "new manifest");
      std::unordered_map<std::string, std::string> props = {
          {std::string(ContentCacheOptions::kEnabled), "true"},
          {"s3.access-key-id", "old-key"},
          {"s3.secret-access-key", "old-secret"},
          {"s3.session-token", "old-token"},
          {"s3.session-token-expires-at-ms", "1000"}};
      if (explicit_endpoint) {
        props["s3.endpoint"] = "https://storage.example";
      }
      ICEBERG_UNWRAP_OR_FAIL(auto table, MakeCachedTable(first_io, props));
      ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());

      props["s3.access-key-id"] = "new-key";
      props["s3.secret-access-key"] = "new-secret";
      props["s3.session-token"] = "new-token";
      props["s3.session-token-expires-at-ms"] = "2000";
      ICEBERG_UNWRAP_OR_FAIL(auto refreshed,
                             MakeCachedTable(second_io, props, metadata_location));
      auto catalog = std::dynamic_pointer_cast<MockCatalog>(table->catalog());
      EXPECT_CALL(*catalog, LoadTable(::testing::_))
          .WillOnce(::testing::Return(refreshed));
      ASSERT_THAT(table->Refresh(), IsOk());
      EXPECT_EQ(table->io().get(), &second_io);
      EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"),
                  HasValue(::testing::Eq("cached")));
      EXPECT_EQ(first_io.open_count, 1);
      EXPECT_EQ(second_io.open_count, 0);
      EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), "new-manifest.avro"),
                  HasValue(::testing::Eq("new manifest")));
      EXPECT_EQ(second_io.open_count, 1);
    }
  }
}

TEST(MetadataCacheTest, RefreshWithChangedStoragePropertiesDiscardsCachedBytes) {
  for (const auto* key : {"s3.endpoint", "client.region", "io-impl"}) {
    SCOPED_TRACE(key);
    CountingMockFileIO first_io;
    CountingMockFileIO second_io;
    first_io.AddFile("manifest.avro", "first");
    second_io.AddFile("manifest.avro", "second");
    std::unordered_map<std::string, std::string> props = {
        {std::string(ContentCacheOptions::kEnabled), "true"}, {key, "old"}};
    ICEBERG_UNWRAP_OR_FAIL(auto table, MakeCachedTable(first_io, props));
    ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
    props[key] = "new";
    ICEBERG_UNWRAP_OR_FAIL(auto refreshed,
                           MakeCachedTable(second_io, props, "metadata-2.json"));
    auto catalog = std::dynamic_pointer_cast<MockCatalog>(table->catalog());
    EXPECT_CALL(*catalog, LoadTable(::testing::_)).WillOnce(::testing::Return(refreshed));
    ASSERT_THAT(table->Refresh(), IsOk());
    EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"),
                HasValue(::testing::Eq("second")));
    EXPECT_EQ(second_io.open_count, 1);
  }
}

TEST(MetadataCacheTest, InvalidCacheLimitsFallBackToStorage) {
  for (const auto key :
       {ContentCacheOptions::kMaxTotalBytes, ContentCacheOptions::kMaxContentLength}) {
    for (const auto value : {"0", "-1", "9223372036854775808", "invalid"}) {
      CountingMockFileIO io;
      ICEBERG_UNWRAP_OR_FAIL(
          auto table,
          MakeCachedTable(io, {{std::string(ContentCacheOptions::kEnabled), "true"},
                               {std::string(key), value}}));
      EXPECT_FALSE(table->ManifestCacheEnabled());
      EXPECT_EQ(internal::ManifestIO(*table), table->io());
    }
  }
}

TEST(MetadataCacheTest, EmptyFilesAreNotRetained) {
  CountingMockFileIO io;
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(io, {{std::string(ContentCacheOptions::kEnabled), "true"},
                           {std::string(ContentCacheOptions::kMaxTotalBytes), "1"}}));
  for (int i = 0; i < 100; ++i) {
    io.AddFile(std::to_string(i), "");
    ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), std::to_string(i)), IsOk());
  }
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "0"), IsOk());
  EXPECT_EQ(io.open_count, 101);
}

class UnavailableFileIO : public CountingMockFileIO {
 public:
  bool unavailable = false;
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string location) override {
    if (unavailable) return IOError("storage resolution unavailable");
    return CountingMockFileIO::NewInputFile(std::move(location));
  }
};

class UnavailableNormalizingFileIO : public NormalizingMockFileIO {
 public:
  bool unavailable = false;
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string location) override {
    if (unavailable) return IOError("storage resolution unavailable");
    return NormalizingMockFileIO::NewInputFile(std::move(location));
  }
};

TEST(MetadataCacheTest, CanonicalLocationHitDoesNotResolveStorage) {
  UnavailableNormalizingFileIO io;
  io.AddFile("manifest.avro", "manifest");
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(io, {{std::string(ContentCacheOptions::kEnabled), "true"}}));
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "alias://manifest.avro"), IsOk());
  io.unavailable = true;
  EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"),
              HasValue(::testing::Eq("manifest")));
  EXPECT_EQ(io.open_count, 1);
}

TEST(MetadataCacheTest, HitDoesNotResolveStorage) {
  UnavailableFileIO io;
  io.AddFile("manifest.avro", "manifest");
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      MakeCachedTable(io, {{std::string(ContentCacheOptions::kEnabled), "true"}}));
  ASSERT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"), IsOk());
  io.unavailable = true;
  EXPECT_THAT(ReadCached(*internal::ManifestIO(*table), "manifest.avro"),
              HasValue(::testing::Eq("manifest")));
}

}  // namespace
}  // namespace iceberg
