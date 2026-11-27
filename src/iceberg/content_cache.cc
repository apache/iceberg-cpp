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
#include <atomic>
#include <cstring>
#include <format>
#include <list>
#include <mutex>
#include <shared_mutex>
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

// Owned by the table. The registry guards files and position under its mutex.
// A position is valid while files is non-null.
struct ContentCacheRegistration {
  using Entries = std::list<std::weak_ptr<ContentCacheRegistration>>;
  Entries::iterator position;
  std::shared_ptr<CachedFiles> files;
  // Accessed under the per-table mutex. A miss needs no registry lock.
  std::weak_ptr<CachedFiles> last_files;
};

// Adapters over one storage FileIO share mutation state. Each table still owns
// its own cached bytes. The registry mutex protects the number of bound caches.
struct ContentCacheStorage {
  explicit ContentCacheStorage(FileIO* file_io) : io(file_io) {}
  FileIO* const io;
  size_t caches = 0;
  std::shared_mutex mutex;
  size_t active_writes = 0;
  uint64_t finished_writes = 0;
  uint64_t invalidations = 0;
  std::atomic<bool> ever_cached = false;
};

class ContentCacheRegistry {
 public:
  explicit ContentCacheRegistry(int32_t capacity) : capacity_(capacity) {}

  ContentCacheStorage& Bind(FileIO* io) {
    if (!io) {
      throw std::invalid_argument("Content cache requires a storage FileIO");
    }
    std::lock_guard lock(mutex_);
    auto& storage = storage_.try_emplace(io, io).first->second;
    ++storage.caches;
    return storage;
  }

  void Unbind(ContentCacheStorage& storage) {
    std::lock_guard lock(mutex_);
    if (--storage.caches == 0) {
      storage_.erase(storage.io);
    }
  }

  bool IsActive(const ContentCacheRegistration& entry,
                const std::shared_ptr<CachedFiles>& files, bool promote) {
    std::lock_guard lock(mutex_);
    if (entry.files != files) {
      return false;
    }
    if (promote) {
      entries_.splice(entries_.begin(), entries_, entry.position);
    }
    return true;
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
      throw std::invalid_argument(
          std::format("Global manifest cache limit must be positive, got {}", capacity));
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
  std::unordered_map<const FileIO*, ContentCacheStorage> storage_;
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
  if (!std::in_range<size_t>(read_size)) {
    return Invalid("File size {} for {} does not fit in size_t", read_size,
                   file_location);
  }

  std::vector<std::byte> content(static_cast<size_t>(read_size));
  ICEBERG_ASSIGN_OR_RAISE(auto stream, input_file.Open());
  auto read_status = stream->ReadFully(/*position=*/0, content);
  ICEBERG_RETURN_UNEXPECTED(
      FinishWithCloseStatus(std::move(read_status), stream->Close()));
  return content;
}

class SizedInputStream : public SeekableInputStream {
 public:
  explicit SizedInputStream(MetadataContent content)
      : size_(content->size()), content_(std::move(content)) {}
  SizedInputStream(std::unique_ptr<SeekableInputStream> input, size_t size)
      : size_(size), input_(std::move(input)) {}

  Result<int64_t> Position() const override {
    return input_ ? input_->Position() : Result<int64_t>(position_);
  }

  Status Seek(int64_t position) override {
    ICEBERG_PRECHECK(!closed_, "Input stream is closed");
    ICEBERG_PRECHECK(position >= 0, "Position must not be negative: {}", position);
    ICEBERG_PRECHECK(std::cmp_less_equal(position, size_),
                     "Position {} exceeds file size {}", position, size_);
    if (input_) {
      return input_->Seek(position);
    }
    position_ = position;
    return {};
  }

  Result<int64_t> Read(std::span<std::byte> out) override {
    ICEBERG_PRECHECK(!closed_, "Input stream is closed");
    if (input_) {
      ICEBERG_ASSIGN_OR_RAISE(auto current, input_->Position());
      ICEBERG_PRECHECK(current >= 0 && std::cmp_less_equal(current, size_),
                       "Invalid stream position {}", current);
      return input_->Read(
          out.first(std::min(out.size(), size_ - static_cast<size_t>(current))));
    }
    auto position = static_cast<size_t>(position_);
    auto bytes_to_read = std::min(out.size(), size_ - position);
    if (bytes_to_read > 0) {
      std::memcpy(out.data(), content_->data() + position, bytes_to_read);
      position_ += static_cast<int64_t>(bytes_to_read);
    }
    return static_cast<int64_t>(bytes_to_read);
  }

  Status ReadFully(int64_t position, std::span<std::byte> out) override {
    ICEBERG_PRECHECK(!closed_, "Input stream is closed");
    ICEBERG_PRECHECK(position >= 0, "Position must not be negative: {}", position);
    ICEBERG_PRECHECK(std::cmp_less_equal(position, size_),
                     "Position {} exceeds file size {}", position, size_);
    auto offset = static_cast<size_t>(position);
    ICEBERG_PRECHECK(out.size() <= size_ - offset,
                     "Read out of bounds: offset {} + length {} exceeds file size {}",
                     position, out.size(), size_);
    if (input_) {
      return input_->ReadFully(position, out);
    }
    if (!out.empty()) {
      std::memcpy(out.data(), content_->data() + offset, out.size());
    }
    return {};
  }

  Status Close() override {
    closed_ = true;
    return input_ ? input_->Close() : Status{};
  }

 private:
  size_t size_;
  std::unique_ptr<SeekableInputStream> input_;
  MetadataContent content_;
  int64_t position_ = 0;
  bool closed_ = false;
};

class CachedInputFile : public InputFile {
 public:
  CachedInputFile(std::unique_ptr<InputFile> input, std::shared_ptr<ContentCache> cache,
                  int64_t size, uint64_t generation,
                  std::optional<std::string> verify_location)
      : input_(std::move(input)),
        cache_(std::move(cache)),
        location_(input_->location()),
        size_(size),
        generation_(generation),
        verify_location_(std::move(verify_location)) {}
  CachedInputFile(std::string location, MetadataContent content)
      : location_(std::move(location)),
        size_(static_cast<int64_t>(content->size())),
        content_(std::move(content)) {}
  std::string_view location() const override { return location_; }
  Result<int64_t> Size() const override { return size_; }
  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    if (content_) {
      return std::make_unique<SizedInputStream>(content_);
    }
    if (!cache_) {
      return input_->Open();
    }
    bool can_load;
    if (auto cached = cache_->FindForHandle(location_, generation_, size_, &can_load)) {
      return std::make_unique<SizedInputStream>(std::move(cached));
    }
    if (can_load && verify_location_) {
      // A hinted Arrow stream can report EOF at the hint. Confirm the full length
      // independently once, only if this handle actually needs to populate the cache.
      std::call_once(verify_once_, [&] {
        complete_ = cache_->MatchesStorageSize(*verify_location_, size_);
      });
      can_load = complete_;
    }
    if (!can_load) {
      ICEBERG_ASSIGN_OR_RAISE(auto stream, input_->Open());
      return std::make_unique<SizedInputStream>(std::move(stream),
                                                static_cast<size_t>(size_));
    }
    auto loaded = ReadInputFile(*input_, size_, location_);
    if (!loaded) {
      auto error = std::move(loaded).error();
      if (error.kind != ErrorKind::kIOError) {
        return unexpected<Error>(std::move(error));
      }
      auto fallback = input_->Open();
      if (!fallback) {
        error.message += "; fallback open failed: " + fallback.error().message;
        return unexpected<Error>(std::move(error));
      }
      return fallback;
    }
    auto content =
        std::make_shared<const std::vector<std::byte>>(std::move(loaded).value());
    content = cache_->Insert(location_, std::move(content), generation_);
    return std::make_unique<SizedInputStream>(std::move(content));
  }

 private:
  std::unique_ptr<InputFile> input_;
  std::shared_ptr<ContentCache> cache_;
  std::string location_;
  int64_t size_;
  uint64_t generation_ = 0;
  std::optional<std::string> verify_location_;
  std::once_flag verify_once_;
  bool complete_ = false;
  MetadataContent content_;
};

}  // namespace

// Clear once after each cleanup, including partially successful batch deletes.
class ContentCache::ManifestFileIO final : public FileIO {
 public:
  explicit ManifestFileIO(std::shared_ptr<ContentCache> cache)
      : cache_(std::move(cache)) {}
  static const ContentCache* GetCache(FileIO* io) {
    const auto* adapter = dynamic_cast<const ManifestFileIO*>(io);
    return adapter ? adapter->cache_.get() : nullptr;
  }
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string location) override {
    return cache_->NewInputFile(std::move(location));
  }
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string location,
                                                  size_t length) override {
    return cache_->NewInputFile(std::move(location), length);
  }
  Result<std::string> ReadFile(const std::string& location,
                               std::optional<size_t> length) override {
    // A requested byte count can cover only a prefix of an immutable file.
    if (length) {
      return cache_->io_->ReadFile(location, length);
    }
    return FileIO::ReadFile(location, std::nullopt);
  }
  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string location) override {
    // Record each mutation once, even when the read adapters are nested.
    ICEBERG_ASSIGN_OR_RAISE(auto output,
                            cache_->storage_.io->NewOutputFile(std::move(location)));
    return std::make_unique<CachedOutputFile>(std::move(output), cache_);
  }
  Status DeleteFile(const std::string& location) override {
    ClearAfterDelete cleanup{*cache_};
    return cache_->storage_.io->DeleteFile(location);
  }
  Status DeleteFiles(const std::vector<std::string>& locations) override {
    if (locations.empty()) {
      return cache_->storage_.io->DeleteFiles(locations);
    }
    ClearAfterDelete cleanup{*cache_};
    return cache_->storage_.io->DeleteFiles(locations);
  }
  const std::unordered_map<std::string, std::string>& properties() const override {
    return cache_->io_->properties();
  }

 private:
  // A reader overlapping any write can see partial bytes through another spelling
  // of the path. Existing immutable entries remain usable during create-only writes.
  class WritingStream : public PositionOutputStream {
   public:
    explicit WritingStream(std::shared_ptr<ContentCache> cache)
        : cache_(std::move(cache)) {
      std::lock_guard lock(cache_->storage_.mutex);
      ++cache_->storage_.active_writes;
      may_have_cached_files_ =
          cache_->storage_.ever_cached.load(std::memory_order_relaxed);
    }

    ~WritingStream() override {
      stream_.reset();
      Finish();
    }

    Status Open(OutputFile& output, bool overwrite) {
      if (overwrite && !may_have_cached_files_) {
        ICEBERG_ASSIGN_OR_RAISE(stream_, output.CreateOrOverwrite());
        return {};
      }
      auto created = output.Create();
      if (!created && overwrite) {
        // The path may already exist under a cached alias. Clear before storage can
        // truncate it, including when opening the overwrite stream itself fails.
        cache_->InvalidateStorage();
        created = output.CreateOrOverwrite();
      }
      ICEBERG_ASSIGN_OR_RAISE(stream_, std::move(created));
      return {};
    }

    Result<int64_t> Position() const override { return stream_->Position(); }
    Result<int64_t> StoredLength() const override { return stream_->StoredLength(); }
    Status Write(std::span<const std::byte> data) override {
      return stream_->Write(data);
    }
    Status Flush() override { return stream_->Flush(); }
    Status Close() override {
      auto status = stream_->Close();
      if (status) {
        Finish();
      }
      return status;
    }

   private:
    void Finish() {
      if (auto cache = std::move(cache_)) {
        std::lock_guard lock(cache->storage_.mutex);
        --cache->storage_.active_writes;
        ++cache->storage_.finished_writes;
      }
    }
    std::shared_ptr<ContentCache> cache_;
    std::unique_ptr<PositionOutputStream> stream_;
    bool may_have_cached_files_;
  };

  class CachedOutputFile : public OutputFile {
   public:
    CachedOutputFile(std::unique_ptr<OutputFile> output,
                     std::shared_ptr<ContentCache> cache)
        : output_(std::move(output)), cache_(std::move(cache)) {}
    std::string_view location() const override { return output_->location(); }
    Result<std::unique_ptr<PositionOutputStream>> Create() override {
      return Open(false);
    }
    Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
      return Open(true);
    }

   private:
    Result<std::unique_ptr<PositionOutputStream>> Open(bool overwrite) {
      auto stream = std::make_unique<WritingStream>(cache_);
      ICEBERG_RETURN_UNEXPECTED(stream->Open(*output_, overwrite));
      return stream;
    }
    std::unique_ptr<OutputFile> output_;
    std::shared_ptr<ContentCache> cache_;
  };

  struct ClearAfterDelete {
    ContentCache& cache;
    ~ClearAfterDelete() { cache.InvalidateStorage(); }
  };
  std::shared_ptr<ContentCache> cache_;
};

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

ContentCache::ContentCache(std::shared_ptr<FileIO> io, ContentCacheOptions options)
    : io_(std::move(io)),
      inner_cache_(ManifestFileIO::GetCache(io_.get())),
      options_(options),
      registry_(GlobalContentCacheRegistry()),
      registration_(std::make_shared<ContentCacheRegistration>()),
      storage_(registry_->Bind(inner_cache_ ? inner_cache_->storage_.io : io_.get())) {}

ContentCache::~ContentCache() {
  registry_->Erase(*registration_);
  registry_->Unbind(storage_);
}

int32_t ContentCache::SetGlobalCapacity(int32_t max_caches) {
  return GlobalContentCacheRegistry()->SetCapacity(max_caches);
}

std::shared_ptr<FileIO> ContentCache::Wrap() {
  return std::make_shared<ManifestFileIO>(shared_from_this());
}

uint64_t ContentCache::SyncGenerationLocked() {
  auto invalidations = storage_.invalidations;
  // An explicit table clear also invalidates adapters layered on that table.
  for (const ContentCache* cache = this; cache; cache = cache->inner_cache_) {
    invalidations += cache->local_clears_;
  }
  if (observed_invalidations_ != invalidations) {
    registry_->Erase(*registration_);
    observed_invalidations_ = invalidations;
  }
  // A fresh create only fences in-flight loads. Retained entries stay usable.
  return invalidations + storage_.finished_writes;
}

MetadataContent ContentCache::Find(std::string_view location, uint64_t* generation) {
  std::shared_lock storage_lock(storage_.mutex);
  std::lock_guard lock(mutex_);
  const auto current = SyncGenerationLocked();
  if (generation) {
    *generation = current;
  }
  return FindLocked(location, std::nullopt);
}

MetadataContent ContentCache::FindForHandle(std::string_view location,
                                            uint64_t generation,
                                            std::optional<int64_t> size, bool* can_load) {
  std::shared_lock storage_lock(storage_.mutex);
  std::lock_guard lock(mutex_);
  const bool current = generation == SyncGenerationLocked();
  if (can_load) {
    *can_load = current && storage_.active_writes == 0;
  }
  if (!current) {
    return nullptr;
  }
  return FindLocked(location, size);
}

MetadataContent ContentCache::FindLocked(std::string_view location,
                                         std::optional<int64_t> size) {
  auto files = registration_->last_files.lock();
  if (!files) {
    return nullptr;
  }
  auto fits = [size](const MetadataContent& value) {
    return !size || std::cmp_equal(value->size(), *size);
  };
  const auto* file = files->FindAndPromoteIf(location, fits);
  if (!file || !fits(*file)) {
    return nullptr;
  }
  return registry_->IsActive(*registration_, files, /*promote=*/true) ? *file : nullptr;
}

MetadataContent ContentCache::Insert(std::string_view location, MetadataContent content,
                                     uint64_t generation) {
  // Empty/corrupt metadata must not accumulate unbounded zero-weight entries.
  if (content->empty() || content->size() > options_.max_content_length ||
      content->size() > options_.max_total_bytes) {
    return content;
  }
  std::shared_lock storage_lock(storage_.mutex);
  std::lock_guard lock(mutex_);
  if (generation != SyncGenerationLocked() || storage_.active_writes != 0) {
    return content;
  }
  if (auto retained = registration_->last_files.lock()) {
    auto fits = [&](const MetadataContent& value) {
      return value->size() == content->size();
    };
    if (auto* winner = retained->FindAndPromoteIf(location, fits)) {
      const bool same_size = fits(*winner);
      if (registry_->IsActive(*registration_, retained, /*promote=*/same_size)) {
        return same_size ? *winner : content;
      }
    }
  }
  auto files = registry_->GetOrCreate(registration_, options_.max_total_bytes);
  registration_->last_files = files;
  auto entry = files->Replace(std::string(location), content,
                              static_cast<int64_t>(content->size()));
  if (entry && entry->second != nullptr) {
    storage_.ever_cached.store(true, std::memory_order_relaxed);
  }
  return content;
}

void ContentCache::Clear() {
  std::lock_guard lock(storage_.mutex);
  ++local_clears_;
  SyncGenerationLocked();
}

void ContentCache::InvalidateStorage() {
  std::lock_guard lock(storage_.mutex);
  ++storage_.invalidations;
  SyncGenerationLocked();
}

Result<std::unique_ptr<InputFile>> ContentCache::NewInputFile(
    std::string location, std::optional<size_t> length) {
  ICEBERG_PRECHECK(!length || std::in_range<int64_t>(*length),
                   "File length {} exceeds int64_t max", *length);
  // A hit needs neither storage resolution nor working remote credentials.
  uint64_t generation;
  if (auto content = Find(location, &generation)) {
    return std::make_unique<CachedInputFile>(std::move(location), std::move(content));
  }
  std::unique_ptr<InputFile> input;
  if (length) {
    ICEBERG_ASSIGN_OR_RAISE(input, io_->NewInputFile(location, *length));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(input, io_->NewInputFile(location));
  }
  if (input->location() != location) {
    if (auto content = FindForHandle(input->location(), generation)) {
      return std::make_unique<CachedInputFile>(std::string(input->location()),
                                               std::move(content));
    }
  }
  ICEBERG_ASSIGN_OR_RAISE(auto size, input->Size());
  if (size < 0) {
    return Invalid("File size {} for {} does not fit in size_t", size, input->location());
  }
  const bool read_ahead = size > 0 &&
                          std::cmp_less_equal(size, options_.max_content_length) &&
                          std::cmp_less_equal(size, options_.max_total_bytes);
  std::optional<std::string> verify_location;
  if (read_ahead && length && std::cmp_equal(size, *length)) {
    verify_location = std::move(location);
  }
  return std::make_unique<CachedInputFile>(std::move(input),
                                           read_ahead ? shared_from_this() : nullptr,
                                           size, generation, std::move(verify_location));
}

bool ContentCache::MatchesStorageSize(const std::string& location, int64_t size) const {
  auto whole = io_->NewInputFile(location);
  return whole && (*whole)->Size().value_or(-1) == size;
}

}  // namespace iceberg::internal
