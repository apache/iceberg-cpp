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

#include <algorithm>
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
#include "iceberg/util/cache_internal.h"
#include "iceberg/util/string_util.h"

namespace iceberg::internal {

// Cache limits apply independently to each loaded table. Expiration is not supported.
struct ICEBERG_EXPORT ContentCacheOptions {
  inline static constexpr std::string_view kEnabled = "io.manifest.cache-enabled";
  inline static constexpr std::string_view kMaxTotalBytes =
      "io.manifest.cache.max-total-bytes";
  inline static constexpr std::string_view kMaxContentLength =
      "io.manifest.cache.max-content-length";
  bool enabled = false;
  size_t max_total_bytes = 100 * 1024 * 1024;
  size_t max_content_length = 8 * 1024 * 1024;
  friend bool operator==(const ContentCacheOptions&,
                         const ContentCacheOptions&) = default;
  static Result<ContentCacheOptions> FromProperties(
      const std::unordered_map<std::string, std::string>& properties);
};

using MetadataContent = std::shared_ptr<const std::vector<std::byte>>;
struct MetadataContentWeigher {
  int64_t operator()(const std::string&, const MetadataContent& content) const {
    return static_cast<int64_t>(content->size());
  }
};

// Bound the bookkeeping for normalized aliases separately from retained content.
struct LocationAliasWeigher {
  int64_t operator()(const std::string& alias, const std::string& canonical) const {
    constexpr size_t kMaxAliasPart = 65537;
    return static_cast<int64_t>(std::min(alias.size(), kMaxAliasPart) +
                                std::min(canonical.size(), kMaxAliasPart));
  }
};

// A table owns this cache; readers share it through a metadata-only FileIO adapter.
// Storage FileIOs remain independent of the cache and its configuration.
class ICEBERG_EXPORT ContentCache : public std::enable_shared_from_this<ContentCache> {
 public:
  explicit ContentCache(ContentCacheOptions options);
  const ContentCacheOptions& options() const { return options_; }
  std::shared_ptr<FileIO> Wrap(std::shared_ptr<FileIO> io);
  Result<std::unique_ptr<InputFile>> NewInputFile(
      const std::shared_ptr<FileIO>& io, std::string location,
      std::optional<size_t> length = std::nullopt);
  // Optionally return the canonical location represented by a cache hit.
  MetadataContent Find(std::string_view location,
                       std::string* canonical_location = nullptr);
  void Insert(const std::string& location, MetadataContent content);
  void Clear();

 private:
  const ContentCacheOptions options_;
  std::mutex mutex_;
  LruCache<std::string, MetadataContent, MetadataContentWeigher, StringHash, StringEqual>
      lru_;
  // Alias strings do not pin content and are capped at 64 KiB in total.
  LruCache<std::string, std::string, LocationAliasWeigher, StringHash, StringEqual>
      aliases_;
};

}  // namespace iceberg::internal
