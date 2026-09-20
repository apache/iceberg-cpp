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

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <list>
#include <mutex>
#include <utility>

#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

namespace {

Result<bool> ParseBoolean(std::string_view key, const std::string& value) {
  if (value == "true") {
    return true;
  }
  if (value == "false") {
    return false;
  }
  return InvalidArgument("Invalid boolean value '{}' for property {}", value, key);
}

template <typename T>
Result<T> ParseNumberProperty(
    const std::unordered_map<std::string, std::string>& properties, std::string_view key,
    T default_value) {
  auto it = properties.find(std::string(key));
  if (it == properties.end()) {
    return default_value;
  }
  auto value = StringUtils::ParseNumber<T>(it->second);
  if (!value.has_value()) {
    return InvalidArgument("Invalid numeric value '{}' for property '{}': {}", it->second,
                           key, value.error().message);
  }
  return value;
}

}  // namespace

Result<MetadataCacheOptions> MetadataCacheOptions::FromProperties(
    const std::unordered_map<std::string, std::string>& properties) {
  MetadataCacheOptions options;
  if (auto it = properties.find(std::string(kEnabled)); it != properties.end()) {
    ICEBERG_ASSIGN_OR_RAISE(options.enabled, ParseBoolean(kEnabled, it->second));
  }

  if (!options.enabled) {
    return options;
  }

  ICEBERG_ASSIGN_OR_RAISE(options.expiration_interval_ms,
                          ParseNumberProperty(properties, kExpirationIntervalMs,
                                              options.expiration_interval_ms));
  ICEBERG_ASSIGN_OR_RAISE(
      options.max_total_bytes,
      ParseNumberProperty(properties, kMaxTotalBytes, options.max_total_bytes));
  ICEBERG_ASSIGN_OR_RAISE(
      options.max_content_length,
      ParseNumberProperty(properties, kMaxContentLength, options.max_content_length));
  return options;
}

class MetadataCache::Impl {
 public:
  using Clock = std::chrono::steady_clock;

  explicit Impl(MetadataCacheOptions options) : options_(std::move(options)) {}

  struct Entry {
    bool loading = true;
    Content content;
    std::optional<Error> error;
    Clock::time_point last_access;
    std::list<std::string>::iterator lru_position;
    std::condition_variable loaded;
  };

  bool IsExpired(const Entry& entry, Clock::time_point now) const {
    return options_.expiration_interval_ms > 0 &&
           now - entry.last_access >=
               std::chrono::milliseconds(options_.expiration_interval_ms);
  }

  void Touch(const std::string& location, Entry& entry, Clock::time_point now) {
    entry.last_access = now;
    lru_.splice(lru_.begin(), lru_, entry.lru_position);
    entry.lru_position = lru_.begin();
  }

  void EraseLoaded(std::unordered_map<std::string, std::shared_ptr<Entry>>::iterator it) {
    auto& entry = *it->second;
    total_bytes_ -= entry.content->size();
    lru_.erase(entry.lru_position);
    entries_.erase(it);
  }

  void PruneExpired(Clock::time_point now) {
    while (!lru_.empty()) {
      auto it = entries_.find(lru_.back());
      if (it == entries_.end()) {
        lru_.pop_back();
        continue;
      }
      if (!IsExpired(*it->second, now)) {
        break;
      }
      EraseLoaded(it);
    }
  }

  void EvictToFit(size_t incoming_bytes) {
    while (!lru_.empty() && incoming_bytes > options_.max_total_bytes - total_bytes_) {
      auto it = entries_.find(lru_.back());
      if (it == entries_.end()) {
        lru_.pop_back();
      } else {
        EraseLoaded(it);
      }
    }
  }

  MetadataCacheOptions options_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<Entry>> entries_;
  std::list<std::string> lru_;
  size_t total_bytes_ = 0;
  bool clearing_ = false;
  std::condition_variable clear_completed_;
};

Result<std::shared_ptr<MetadataCache>> MetadataCache::Make(MetadataCacheOptions options) {
  if (options.enabled) {
    ICEBERG_PRECHECK(options.expiration_interval_ms >= 0,
                     "Metadata cache expiration interval must not be negative: {}",
                     options.expiration_interval_ms);
    ICEBERG_PRECHECK(options.max_total_bytes > 0,
                     "Metadata cache maximum total bytes must be positive");
    ICEBERG_PRECHECK(options.max_content_length > 0,
                     "Metadata cache maximum content length must be positive");
    ICEBERG_PRECHECK(options.max_content_length <=
                         static_cast<size_t>(std::numeric_limits<int64_t>::max()),
                     "Metadata cache maximum content length exceeds int64_t max: {}",
                     options.max_content_length);
  }
  return std::shared_ptr<MetadataCache>(new MetadataCache(std::move(options)));
}

MetadataCache::MetadataCache(MetadataCacheOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

MetadataCache::~MetadataCache() = default;

const MetadataCacheOptions& MetadataCache::options() const noexcept {
  return impl_->options_;
}

Result<MetadataCache::Content> MetadataCache::Get(std::string location,
                                                  std::optional<size_t> length,
                                                  Loader loader) {
  if (!impl_->options_.enabled ||
      (length.has_value() && *length > impl_->options_.max_content_length)) {
    auto loaded = loader();
    if (loaded.has_value() && loaded.value() == nullptr) {
      return Invalid("Metadata cache loader returned null content for {}", location);
    }
    return loaded;
  }

  std::shared_ptr<Impl::Entry> entry;
  {
    std::unique_lock lock(impl_->mutex_);
    impl_->clear_completed_.wait(lock, [this] { return !impl_->clearing_; });
    auto now = Impl::Clock::now();
    impl_->PruneExpired(now);

    while (true) {
      auto it = impl_->entries_.find(location);
      if (it == impl_->entries_.end()) {
        entry = std::make_shared<Impl::Entry>();
        impl_->entries_.emplace(location, entry);
        break;
      }

      entry = it->second;
      if (entry->loading) {
        entry->loaded.wait(lock, [&entry] { return !entry->loading; });
        if (entry->error.has_value()) {
          return std::unexpected<Error>(*entry->error);
        }
        if (entry->content != nullptr) {
          if (auto current = impl_->entries_.find(location);
              current != impl_->entries_.end() && current->second == entry) {
            impl_->Touch(location, *entry, Impl::Clock::now());
          }
          return entry->content;
        }
        continue;
      }

      now = Impl::Clock::now();
      if (impl_->IsExpired(*entry, now)) {
        impl_->EraseLoaded(it);
        continue;
      }
      impl_->Touch(location, *entry, now);
      return entry->content;
    }
  }

  auto loaded = loader();
  if (loaded.has_value() && loaded.value() == nullptr) {
    loaded = Invalid("Metadata cache loader returned null content for {}", location);
  }
  std::unique_lock lock(impl_->mutex_);
  auto it = impl_->entries_.find(location);
  if (it == impl_->entries_.end() || it->second != entry) {
    if (loaded.has_value()) {
      entry->content = loaded.value();
    } else {
      entry->error = loaded.error();
    }
    entry->loading = false;
    entry->loaded.notify_all();
    return loaded;
  }

  if (!loaded.has_value()) {
    impl_->entries_.erase(it);
    entry->error = loaded.error();
    entry->loading = false;
    entry->loaded.notify_all();
    return loaded;
  }

  if (loaded.value()->size() > impl_->options_.max_content_length ||
      loaded.value()->size() > impl_->options_.max_total_bytes) {
    impl_->entries_.erase(it);
    entry->content = loaded.value();
    entry->loading = false;
    entry->loaded.notify_all();
    return loaded;
  }

  impl_->PruneExpired(Impl::Clock::now());
  impl_->EvictToFit(loaded.value()->size());
  entry->content = loaded.value();
  entry->last_access = Impl::Clock::now();
  impl_->lru_.push_front(location);
  entry->lru_position = impl_->lru_.begin();
  entry->loading = false;
  impl_->total_bytes_ += entry->content->size();
  entry->loaded.notify_all();
  return entry->content;
}

MetadataCache::Content MetadataCache::GetIfPresent(std::string_view location) {
  if (!impl_->options_.enabled) {
    return nullptr;
  }
  std::lock_guard lock(impl_->mutex_);
  auto it = impl_->entries_.find(std::string(location));
  if (it == impl_->entries_.end() || it->second->loading) {
    return nullptr;
  }
  auto now = Impl::Clock::now();
  if (impl_->IsExpired(*it->second, now)) {
    impl_->EraseLoaded(it);
    return nullptr;
  }
  impl_->Touch(it->first, *it->second, now);
  return it->second->content;
}

void MetadataCache::Invalidate(std::string_view location) {
  std::unique_lock lock(impl_->mutex_);
  while (true) {
    auto it = impl_->entries_.find(std::string(location));
    if (it == impl_->entries_.end()) {
      return;
    }
    auto entry = it->second;
    if (entry->loading) {
      entry->loaded.wait(lock, [&entry] { return !entry->loading; });
      continue;
    }
    impl_->EraseLoaded(it);
    return;
  }
}

void MetadataCache::Clear() {
  std::unique_lock lock(impl_->mutex_);
  impl_->clearing_ = true;
  while (true) {
    auto loading = std::ranges::find_if(
        impl_->entries_, [](const auto& item) { return item.second->loading; });
    if (loading == impl_->entries_.end()) {
      break;
    }
    auto entry = loading->second;
    entry->loaded.wait(lock, [&entry] { return !entry->loading; });
  }
  impl_->entries_.clear();
  impl_->lru_.clear();
  impl_->total_bytes_ = 0;
  impl_->clearing_ = false;
  lock.unlock();
  impl_->clear_completed_.notify_all();
}

size_t MetadataCache::size() const {
  std::lock_guard lock(impl_->mutex_);
  return impl_->lru_.size();
}

size_t MetadataCache::total_bytes() const {
  std::lock_guard lock(impl_->mutex_);
  return impl_->total_bytes_;
}

}  // namespace iceberg
