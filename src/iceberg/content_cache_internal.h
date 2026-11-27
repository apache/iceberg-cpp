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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/file_io.h"
#include "iceberg/storage_namespace.h"
#include "iceberg/util/config.h"

namespace iceberg::internal {

class ContentCacheProperties : public ConfigBase<ContentCacheProperties> {
 public:
  inline static const Entry<bool> kEnabled{"io.manifest.cache-enabled", false};
  inline static const Entry<int64_t> kMaxTotalBytes{"io.manifest.cache.max-total-bytes",
                                                    100 * 1024 * 1024};
  inline static const Entry<int64_t> kMaxContentLength{
      "io.manifest.cache.max-content-length", 8 * 1024 * 1024};
};

// Retain only validated cache settings, independent of the storage credentials.
struct ICEBERG_EXPORT ContentCacheOptions {
  bool enabled = ContentCacheProperties::kEnabled.value();
  size_t max_total_bytes =
      static_cast<size_t>(ContentCacheProperties::kMaxTotalBytes.value());
  size_t max_content_length =
      static_cast<size_t>(ContentCacheProperties::kMaxContentLength.value());
  friend bool operator==(const ContentCacheOptions&,
                         const ContentCacheOptions&) = default;
  static Result<ContentCacheOptions> FromProperties(
      const std::unordered_map<std::string, std::string>& properties);
};

using MetadataContent = std::shared_ptr<const std::vector<std::byte>>;

struct ContentCacheRegistration;
class ContentCacheRegistry;

// The per-table handle owns its cached files. The process-wide registry tracks
// active caches through weak references and can evict their contents at capacity.
class ICEBERG_EXPORT ContentCache : public std::enable_shared_from_this<ContentCache> {
 public:
  // A cache and all its adapters use this FileIO for their entire lifetime.
  // Construct a new cache when replacing the storage FileIO. FileIOs with mutable
  // routing must report their storage namespace generation for location-only hits.
  ContentCache(std::shared_ptr<FileIO> io, ContentCacheOptions options);
  ~ContentCache();
  // Return the previous process-wide capacity. The new capacity must be positive.
  static int32_t SetGlobalCapacity(int32_t max_caches);
  const ContentCacheOptions& options() const { return options_; }
  std::shared_ptr<FileIO> Wrap();
  Result<std::unique_ptr<InputFile>> NewInputFile(
      std::string location, std::optional<size_t> length = std::nullopt);
  // A load that overlaps invalidation may serve its own reader but cannot repopulate
  // the cache. Capture the generation under the same lock as the lookup.
  MetadataContent Find(std::string_view location, uint64_t* generation = nullptr);
  MetadataContent FindForHandle(std::string_view location, uint64_t generation,
                                std::optional<int64_t> size = std::nullopt,
                                bool* can_load = nullptr);
  bool MatchesStorageSize(const std::string& location, int64_t size) const;
  // A competing load of the same size in this generation shares the winner's bytes.
  MetadataContent Insert(std::string_view location, MetadataContent content,
                         uint64_t generation);
  void Clear();

 private:
  class ManifestFileIO;

  bool RefreshNamespace();
  MetadataContent FindLocked(std::string_view location, std::optional<int64_t> size);
  const std::shared_ptr<FileIO> io_;
  const StorageNamespace* const routes_;
  const ContentCacheOptions options_;
  const std::shared_ptr<ContentCacheRegistry> registry_;
  std::mutex mutex_;
  const std::shared_ptr<ContentCacheRegistration> registration_;
  size_t active_writes_ = 0;
  std::optional<uint64_t> namespace_generation_ = 0;
  uint64_t generation_ = 0;
};

}  // namespace iceberg::internal
