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

#include "iceberg/content_cache_internal.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <utility>

#include "iceberg/logging/log_macros.h"
#include "iceberg/util/error_util_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg::internal {

struct ContentCacheState {
  explicit ContentCacheState(size_t max_total_bytes)
      : lru(static_cast<int64_t>(max_total_bytes)) {}

  LruCache<std::string, MetadataContent, MetadataContentWeigher, StringHash, StringEqual>
      lru;
};

// Owns cache states, never their per-table handles or storage FileIOs. Callers hold
// the handle's mutex before accessing a state; the registry mutex only protects the
// global LRU. Eviction releases content without calling back into a table handle.
class ContentCacheRegistry {
 public:
  explicit ContentCacheRegistry(int32_t capacity) : caches_(capacity) {}

  std::shared_ptr<ContentCacheState> Get(const ContentCache* owner,
                                         const ContentCacheOptions& options,
                                         std::weak_ptr<ContentCacheState>& weak_state) {
    std::lock_guard lock(mutex_);
    if (auto* state = caches_.Find(owner)) {
      return *state;
    }
    auto state = weak_state.lock();
    if (!state) {
      state = std::make_shared<ContentCacheState>(options.max_total_bytes);
    }
    caches_.Replace(owner, state);
    weak_state = state;
    return state;
  }

  void Erase(const ContentCache* owner) {
    std::lock_guard lock(mutex_);
    caches_.Erase(owner);
  }

  int32_t SetCapacity(int32_t max_caches) {
    ICEBERG_DCHECK(max_caches > 0, "Global manifest cache limit must be positive");
    std::lock_guard lock(mutex_);
    auto previous = static_cast<int32_t>(caches_.max_weight());
    caches_.Resize(max_caches);
    return previous;
  }

 private:
  std::mutex mutex_;
  LruCache<const ContentCache*, std::shared_ptr<ContentCacheState>> caches_;
};

namespace {

int32_t InitialGlobalCacheCapacity() {
  constexpr int32_t kDefaultCapacity = 8;
  constexpr const char* kLimitEnv = "ICEBERG_IO_MANIFEST_CACHE_LIMIT";
  if (const char* configured_limit = std::getenv(kLimitEnv)) {
    auto limit = StringUtils::ParseNumber<int32_t>(configured_limit);
    if (limit && *limit > 0) {
      return *limit;
    }
    ICEBERG_LOG_WARN("{} must be a positive integer that fits in int32_t. Using {}.",
                     kLimitEnv, kDefaultCapacity);
  }
  return kDefaultCapacity;
}

std::shared_ptr<ContentCacheRegistry> GlobalContentCacheRegistry() {
  // Handles keep the registry alive even when destroyed during static teardown.
  static auto registry =
      std::make_shared<ContentCacheRegistry>(InitialGlobalCacheCapacity());
  return registry;
}

template <typename T>
Result<T> ParseNumberProperty(
    const std::unordered_map<std::string, std::string>& properties, std::string_view key,
    T fallback) {
  auto it = properties.find(std::string(key));
  if (it == properties.end()) return fallback;
  auto result = StringUtils::ParseNumber<T>(it->second);
  if (!result) {
    return InvalidArgument("Invalid numeric value '{}' for property '{}': {}", it->second,
                           key, result.error().message);
  }
  return result;
}

Result<std::vector<std::byte>> ReadInputFile(InputFile& input_file, int64_t read_size,
                                             std::string_view file_location) {
  if (read_size < 0) {
    return Invalid("Invalid negative file size {} for {}", read_size, file_location);
  }
  if (static_cast<uint64_t>(read_size) >
      static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
    return Invalid("File size {} exceeds size_t max for {}", read_size, file_location);
  }

  auto size = static_cast<size_t>(read_size);
  std::vector<std::byte> content(size);
  ICEBERG_ASSIGN_OR_RAISE(auto stream, input_file.Open());
  Status read_status = {};
  if (size > 0) {
    read_status = stream->ReadFully(/*position=*/0, content);
  }
  ICEBERG_RETURN_UNEXPECTED(
      FinishWithCloseStatus(std::move(read_status), stream->Close()));
  return content;
}

class CachedSeekableInputStream : public SeekableInputStream {
 public:
  explicit CachedSeekableInputStream(MetadataContent content)
      : content_(std::move(content)) {}

  Result<int64_t> Position() const override { return position_; }

  Status Seek(int64_t position) override {
    ICEBERG_PRECHECK(!closed_, "Input stream is closed");
    ICEBERG_PRECHECK(position >= 0, "Position must not be negative: {}", position);
    ICEBERG_PRECHECK(static_cast<uint64_t>(position) <= content_->size(),
                     "Position {} exceeds file size {}", position, content_->size());
    position_ = position;
    return {};
  }

  Result<int64_t> Read(std::span<std::byte> out) override {
    ICEBERG_PRECHECK(!closed_, "Input stream is closed");
    auto position = static_cast<size_t>(position_);
    auto bytes_to_read = std::min(out.size(), content_->size() - position);
    if (bytes_to_read > 0) {
      std::memcpy(out.data(), content_->data() + position, bytes_to_read);
      position_ += static_cast<int64_t>(bytes_to_read);
    }
    return static_cast<int64_t>(bytes_to_read);
  }

  Status ReadFully(int64_t position, std::span<std::byte> out) override {
    ICEBERG_PRECHECK(!closed_, "Input stream is closed");
    ICEBERG_PRECHECK(position >= 0, "Position must not be negative: {}", position);
    ICEBERG_PRECHECK(static_cast<uint64_t>(position) <= content_->size(),
                     "Position {} exceeds file size {}", position, content_->size());
    auto offset = static_cast<size_t>(position);
    ICEBERG_PRECHECK(out.size() <= content_->size() - offset,
                     "Read out of bounds: offset {} + length {} exceeds file size {}",
                     position, out.size(), content_->size());
    if (!out.empty()) {
      std::memcpy(out.data(), content_->data() + offset, out.size());
    }
    return {};
  }

  Status Close() override {
    closed_ = true;
    return {};
  }

 private:
  MetadataContent content_;
  int64_t position_ = 0;
  bool closed_ = false;
};

class CachedInputFile : public InputFile {
 public:
  CachedInputFile(std::unique_ptr<InputFile> input, std::shared_ptr<ContentCache> cache,
                  int64_t size)
      : input_(std::move(input)),
        cache_(std::move(cache)),
        location_(input_->location()),
        size_(size) {}
  CachedInputFile(std::string location, MetadataContent content)
      : location_(std::move(location)),
        size_(static_cast<int64_t>(content->size())),
        content_(std::move(content)) {}
  std::string_view location() const override { return location_; }
  Result<int64_t> Size() const override { return size_; }
  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    if (content_) return std::make_unique<CachedSeekableInputStream>(content_);
    uint64_t generation;
    if (auto cached = cache_->Find(location_, &generation)) {
      return std::make_unique<CachedSeekableInputStream>(std::move(cached));
    }
    auto loaded = ReadInputFile(*input_, size_, location_);
    if (!loaded) {
      auto error = std::move(loaded).error();
      if (error.kind != ErrorKind::kIOError) return unexpected<Error>(std::move(error));
      auto fallback = input_->Open();
      if (!fallback) {
        error.message += "; fallback open failed: " + fallback.error().message;
        return unexpected<Error>(std::move(error));
      }
      return fallback;
    }
    auto content =
        std::make_shared<const std::vector<std::byte>>(std::move(loaded).value());
    cache_->Insert(location_, content, generation);
    return std::make_unique<CachedSeekableInputStream>(std::move(content));
  }

 private:
  std::unique_ptr<InputFile> input_;
  std::shared_ptr<ContentCache> cache_;
  std::string location_;
  int64_t size_;
  MetadataContent content_;
};

// Only manifest readers receive this adapter. Output and deletion operations are
// forwarded unchanged, allowing manifest maintenance to use the same read context.
class ManifestFileIO : public FileIO {
 public:
  ManifestFileIO(std::shared_ptr<FileIO> io, std::shared_ptr<ContentCache> cache)
      : io_(std::move(io)), cache_(std::move(cache)) {}
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string location) override {
    return cache_->NewInputFile(io_, std::move(location));
  }
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string location,
                                                  size_t length) override {
    return cache_->NewInputFile(io_, std::move(location), length);
  }
  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string location) override {
    return io_->NewOutputFile(std::move(location));
  }
  Status DeleteFile(const std::string& location) override {
    auto status = io_->DeleteFile(location);
    cache_->Invalidate(location);
    return status;
  }
  Status DeleteFiles(const std::vector<std::string>& locations) override {
    auto status = io_->DeleteFiles(locations);
    // A failed bulk delete can still remove some files.
    for (const auto& location : locations) {
      cache_->Invalidate(location);
    }
    return status;
  }
  const std::unordered_map<std::string, std::string>& properties() const override {
    return io_->properties();
  }

 private:
  std::shared_ptr<FileIO> io_;
  std::shared_ptr<ContentCache> cache_;
};

}  // namespace

Result<ContentCacheOptions> ContentCacheOptions::FromProperties(
    const std::unordered_map<std::string, std::string>& properties) {
  ContentCacheOptions options;
  if (auto it = properties.find(std::string(kEnabled)); it != properties.end()) {
    options.enabled = StringUtils::ParseBoolean(it->second);
  }
  if (!options.enabled) {
    return options;
  }
  ICEBERG_ASSIGN_OR_RAISE(
      options.max_total_bytes,
      ParseNumberProperty(properties, kMaxTotalBytes, options.max_total_bytes));
  ICEBERG_ASSIGN_OR_RAISE(
      options.max_content_length,
      ParseNumberProperty(properties, kMaxContentLength, options.max_content_length));
  ICEBERG_PRECHECK(
      options.max_total_bytes > 0 &&
          std::cmp_less_equal(options.max_total_bytes,
                              std::numeric_limits<int64_t>::max()),
      "Manifest cache maximum total bytes must be positive and fit in int64_t");
  ICEBERG_PRECHECK(
      options.max_content_length > 0 &&
          std::cmp_less_equal(options.max_content_length,
                              std::numeric_limits<int64_t>::max()),
      "Manifest cache maximum content length must be positive and fit in int64_t");
  return options;
}

ContentCache::ContentCache(ContentCacheOptions options)
    : options_(options), registry_(GlobalContentCacheRegistry()) {}

ContentCache::~ContentCache() { registry_->Erase(this); }

int32_t ContentCache::SetGlobalCapacity(int32_t max_caches) {
  return GlobalContentCacheRegistry()->SetCapacity(max_caches);
}

std::shared_ptr<FileIO> ContentCache::Wrap(std::shared_ptr<FileIO> io) {
  return std::make_shared<ManifestFileIO>(std::move(io), shared_from_this());
}

MetadataContent ContentCache::Find(std::string_view location, uint64_t* generation) {
  std::lock_guard lock(mutex_);
  if (generation) *generation = generation_;
  auto state = registry_->Get(this, options_, state_);
  auto* content = state->lru.Find(location);
  return content ? *content : nullptr;
}

void ContentCache::Insert(const std::string& location, MetadataContent content,
                          uint64_t generation) {
  // Empty/corrupt metadata must not accumulate unbounded zero-weight entries.
  if (content->empty()) return;
  std::lock_guard lock(mutex_);
  if (generation == generation_) {
    registry_->Get(this, options_, state_)->lru.Replace(location, std::move(content));
  }
}

void ContentCache::Invalidate(std::string_view location) {
  std::lock_guard lock(mutex_);
  if (auto state = state_.lock()) {
    state->lru.Erase(location);
  }
  ++generation_;
}

void ContentCache::Clear() {
  std::lock_guard lock(mutex_);
  registry_->Erase(this);
  state_.reset();
  ++generation_;
}

Result<std::unique_ptr<InputFile>> ContentCache::NewInputFile(
    const std::shared_ptr<FileIO>& io, std::string location,
    std::optional<size_t> length) {
  if (length && std::cmp_greater(*length, std::numeric_limits<int64_t>::max())) {
    return InvalidArgument("File length {} exceeds int64_t max", *length);
  }
  // A hit needs neither storage resolution nor working remote credentials.
  if (auto content = Find(location)) {
    return std::make_unique<CachedInputFile>(std::move(location), std::move(content));
  }
  std::unique_ptr<InputFile> input;
  if (length) {
    ICEBERG_ASSIGN_OR_RAISE(input, io->NewInputFile(location, *length));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(input, io->NewInputFile(location));
  }
  if (auto content = Find(input->location())) {
    return std::make_unique<CachedInputFile>(std::string(input->location()),
                                             std::move(content));
  }
  int64_t size;
  if (length) {
    size = static_cast<int64_t>(*length);
  } else {
    ICEBERG_ASSIGN_OR_RAISE(size, input->Size());
  }
  ICEBERG_PRECHECK(size >= 0, "Invalid negative file size {}", size);
  if (size == 0 || std::cmp_greater(size, options_.max_content_length) ||
      std::cmp_greater(size, options_.max_total_bytes)) {
    return input;
  }
  return std::make_unique<CachedInputFile>(std::move(input), shared_from_this(), size);
}

}  // namespace iceberg::internal
