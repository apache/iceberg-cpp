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

#include <algorithm>
#include <cstring>
#include <limits>
#include <list>
#include <stdexcept>
#include <utility>

#include "iceberg/content_cache_internal.h"
#include "iceberg/util/cache_internal.h"
#include "iceberg/util/error_util_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/property_util.h"
#include "iceberg/util/string_util.h"

namespace iceberg::internal {

using CachedFiles = LruCache<std::string, MetadataContent, StringHash, StringEqual>;

// Owned by the table's cache handle. Only the registry accesses these fields under
// its mutex. A position is valid while files is non-null.
struct ContentCacheRegistration {
  using Entries = std::list<std::weak_ptr<ContentCacheRegistration>>;
  Entries::iterator position;
  std::shared_ptr<CachedFiles> files;
};

class ContentCacheRegistry {
 public:
  explicit ContentCacheRegistry(int32_t capacity) : capacity_(capacity) {}

  // Pin the files for the calling operation. Their entries are protected by the
  // per-table mutex, so accessing bytes never requires holding the registry lock.
  std::shared_ptr<CachedFiles> Find(const ContentCacheRegistration& entry) {
    std::lock_guard lock(mutex_);
    if (entry.files) {
      entries_.splice(entries_.begin(), entries_, entry.position);
    }
    return entry.files;
  }

  std::shared_ptr<CachedFiles> GetOrCreate(
      const std::shared_ptr<ContentCacheRegistration>& entry, size_t max_total_bytes) {
    std::shared_ptr<CachedFiles> evicted;
    std::lock_guard lock(mutex_);
    if (entry->files) {
      entries_.splice(entries_.begin(), entries_, entry->position);
    } else {
      auto files = std::make_shared<CachedFiles>(static_cast<int64_t>(max_total_bytes));
      entry->position = entries_.emplace(entries_.begin(), entry);
      entry->files = std::move(files);
      if (std::cmp_greater(entries_.size(), capacity_)) {
        evicted = PopLeastRecentlyUsed();
      }
    }
    return entry->files;
  }

  void Erase(ContentCacheRegistration& entry) {
    std::shared_ptr<CachedFiles> evicted;
    std::lock_guard lock(mutex_);
    if (entry.files) {
      entries_.erase(entry.position);
      evicted = std::move(entry.files);
    }
  }

  int32_t SetCapacity(int32_t capacity) {
    if (capacity <= 0) {
      throw std::invalid_argument("Global manifest cache limit must be positive");
    }
    std::vector<std::shared_ptr<CachedFiles>> evicted;
    std::lock_guard lock(mutex_);
    if (std::cmp_greater(entries_.size(), capacity)) {
      evicted.reserve(entries_.size() - static_cast<size_t>(capacity));
      while (std::cmp_greater(entries_.size(), capacity)) {
        evicted.push_back(PopLeastRecentlyUsed());
      }
    }
    return std::exchange(capacity_, capacity);
  }

 private:
  // Transfer ownership to the caller so destruction happens after unlocking.
  std::shared_ptr<CachedFiles> PopLeastRecentlyUsed() {
    auto entry = entries_.back().lock();
    entries_.pop_back();
    return entry ? std::move(entry->files) : nullptr;
  }

  std::mutex mutex_;
  int32_t capacity_;
  ContentCacheRegistration::Entries entries_;
};

namespace {

// Match Iceberg Java's default for iceberg.io.manifest.cache.fileio-max.
constexpr int32_t kDefaultMaxTableCaches = 8;

std::shared_ptr<ContentCacheRegistry> GlobalContentCacheRegistry() {
  // Handles keep the registry alive even when destroyed during static teardown.
  static auto registry = std::make_shared<ContentCacheRegistry>(kDefaultMaxTableCaches);
  return registry;
}

Result<std::vector<std::byte>> ReadInputFile(InputFile& input_file, int64_t read_size,
                                             std::string_view file_location) {
  if (read_size < 0) {
    return Invalid("Invalid negative file size {} for {}", read_size, file_location);
  }
  if (std::cmp_greater(read_size, std::numeric_limits<size_t>::max())) {
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
    ICEBERG_PRECHECK(std::cmp_less_equal(position, content_->size()),
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
    ICEBERG_PRECHECK(std::cmp_less_equal(position, content_->size()),
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
                  int64_t size, uint64_t generation)
      : input_(std::move(input)),
        cache_(std::move(cache)),
        location_(input_->location()),
        size_(size),
        generation_(generation) {}
  CachedInputFile(std::string location, MetadataContent content)
      : location_(std::move(location)),
        size_(static_cast<int64_t>(content->size())),
        content_(std::move(content)) {}
  std::string_view location() const override { return location_; }
  Result<int64_t> Size() const override { return size_; }
  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    if (content_) {
      return std::make_unique<CachedSeekableInputStream>(content_);
    }
    uint64_t generation;
    auto cached = cache_->Find(location_, &generation);
    if (cached && generation == generation_ && std::cmp_equal(cached->size(), size_)) {
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
    if (!cached) cache_->Insert(location_, content, generation_);
    return std::make_unique<CachedSeekableInputStream>(std::move(content));
  }

 private:
  std::unique_ptr<InputFile> input_;
  std::shared_ptr<ContentCache> cache_;
  std::string location_;
  int64_t size_;
  uint64_t generation_ = 0;
  MetadataContent content_;
};

// Clear once after each cleanup, including partially successful batch deletes.
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
  Result<std::string> ReadFile(const std::string& location,
                               std::optional<size_t> length) override {
    // A requested byte count can cover only a prefix of an immutable file.
    if (length) {
      return io_->ReadFile(location, length);
    }
    return FileIO::ReadFile(location, std::nullopt);
  }
  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string location) override {
    return io_->NewOutputFile(std::move(location));
  }
  Status DeleteFile(const std::string& location) override {
    ClearAfterDelete cleanup{*cache_};
    return io_->DeleteFile(location);
  }
  Status DeleteFiles(const std::vector<std::string>& locations) override {
    if (locations.empty()) return io_->DeleteFiles(locations);
    ClearAfterDelete cleanup{*cache_};
    return io_->DeleteFiles(locations);
  }
  const std::unordered_map<std::string, std::string>& properties() const override {
    return io_->properties();
  }

 private:
  struct ClearAfterDelete {
    ContentCache& cache;
    ~ClearAfterDelete() { cache.Clear(); }
  };
  std::shared_ptr<FileIO> io_;
  std::shared_ptr<ContentCache> cache_;
};

}  // namespace

Result<ContentCacheOptions> ContentCacheOptions::FromProperties(
    const std::unordered_map<std::string, std::string>& properties) {
  ContentCacheOptions options;
  const auto& enabled = ContentCacheProperties::kEnabled;
  options.enabled =
      PropertyUtil::PropertyAsBoolean(properties, enabled.key(), enabled.value());
  if (!options.enabled) {
    return options;
  }
  const auto read_limit = [&properties](const auto& entry) -> Result<size_t> {
    ICEBERG_ASSIGN_OR_RAISE(auto limit, PropertyUtil::PropertyAsInt64(
                                            properties, entry.key(), entry.value()));
    ICEBERG_PRECHECK(limit > 0 && std::in_range<size_t>(limit),
                     "Property '{}' must be positive and fit in size_t: {}", entry.key(),
                     limit);
    return static_cast<size_t>(limit);
  };
  ICEBERG_ASSIGN_OR_RAISE(options.max_total_bytes,
                          read_limit(ContentCacheProperties::kMaxTotalBytes));
  ICEBERG_ASSIGN_OR_RAISE(options.max_content_length,
                          read_limit(ContentCacheProperties::kMaxContentLength));
  return options;
}

ContentCache::ContentCache(ContentCacheOptions options)
    : options_(options),
      registry_(GlobalContentCacheRegistry()),
      registration_(std::make_shared<ContentCacheRegistration>()) {}

ContentCache::~ContentCache() { registry_->Erase(*registration_); }

int32_t ContentCache::SetGlobalCapacity(int32_t max_caches) {
  return GlobalContentCacheRegistry()->SetCapacity(max_caches);
}

std::shared_ptr<FileIO> ContentCache::Wrap(std::shared_ptr<FileIO> io) {
  return std::make_shared<ManifestFileIO>(std::move(io), shared_from_this());
}

MetadataContent ContentCache::Find(std::string_view location, uint64_t* generation) {
  std::lock_guard lock(mutex_);
  if (generation) *generation = generation_;
  auto files = registry_->Find(*registration_);
  const auto* file = files ? files->Find(location) : nullptr;
  return file ? *file : nullptr;
}

void ContentCache::Insert(std::string_view location, MetadataContent content,
                          uint64_t generation) {
  // Empty/corrupt metadata must not accumulate unbounded zero-weight entries.
  if (content->empty() || content->size() > options_.max_content_length ||
      content->size() > options_.max_total_bytes) {
    return;
  }
  std::lock_guard lock(mutex_);
  if (generation == generation_) {
    const auto weight = static_cast<int64_t>(content->size());
    registry_->GetOrCreate(registration_, options_.max_total_bytes)
        ->Replace(std::string(location), std::move(content), weight);
  }
}

void ContentCache::Clear() {
  std::lock_guard lock(mutex_);
  registry_->Erase(*registration_);
  ++generation_;
}

Result<std::unique_ptr<InputFile>> ContentCache::NewInputFile(
    const std::shared_ptr<FileIO>& io, std::string location,
    std::optional<size_t> length) {
  if (length && std::cmp_greater(*length, std::numeric_limits<int64_t>::max())) {
    return InvalidArgument("File length {} exceeds int64_t max", *length);
  }
  // A hit needs neither storage resolution nor working remote credentials.
  uint64_t generation;
  if (auto content = Find(location, &generation)) {
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
  ICEBERG_ASSIGN_OR_RAISE(auto size, input->Size());
  ICEBERG_PRECHECK(size >= 0, "Invalid negative file size {}", size);
  if (size == 0 || std::cmp_greater(size, options_.max_content_length) ||
      std::cmp_greater(size, options_.max_total_bytes)) {
    return input;
  }
  return std::make_unique<CachedInputFile>(std::move(input), shared_from_this(), size,
                                           generation);
}

}  // namespace iceberg::internal
