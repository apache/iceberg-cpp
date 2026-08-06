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

/// \file iceberg/metadata_cache.h
/// \brief A bounded cache for immutable Iceberg metadata file content.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Configuration for cached metadata file content.
///
/// The property names and defaults match Apache Iceberg Java's manifest content cache.
/// The cache is disabled by default.
struct ICEBERG_EXPORT MetadataCacheOptions {
  inline static constexpr std::string_view kEnabled = "io.manifest.cache-enabled";
  inline static constexpr std::string_view kExpirationIntervalMs =
      "io.manifest.cache.expiration-interval-ms";
  inline static constexpr std::string_view kMaxTotalBytes =
      "io.manifest.cache.max-total-bytes";
  inline static constexpr std::string_view kMaxContentLength =
      "io.manifest.cache.max-content-length";

  bool enabled = false;
  /// Zero disables time-based expiration; positive values expire after last access.
  int64_t expiration_interval_ms = 60 * 1000;
  /// Maximum combined weight of cached entries.
  size_t max_total_bytes = 100 * 1024 * 1024;
  /// Files larger than this value are read without being cached.
  size_t max_content_length = 8 * 1024 * 1024;

  friend bool operator==(const MetadataCacheOptions&,
                         const MetadataCacheOptions&) = default;

  /// \brief Parse cache options from catalog/FileIO properties.
  static Result<MetadataCacheOptions> FromProperties(
      const std::unordered_map<std::string, std::string>& properties);
};

/// \brief Thread-safe, size-bounded cache of immutable metadata file bytes.
///
/// Entries expire after the configured interval since their last access and are evicted
/// in least-recently-used order to enforce the total byte limit. Concurrent loads for
/// the same location are coalesced. Load failures are not cached.
class ICEBERG_EXPORT MetadataCache {
 public:
  using Content = std::shared_ptr<const std::string>;
  using Loader = std::function<Result<Content>()>;

  static Result<std::shared_ptr<MetadataCache>> Make(MetadataCacheOptions options);

  ~MetadataCache();
  MetadataCache(const MetadataCache&) = delete;
  MetadataCache& operator=(const MetadataCache&) = delete;

  const MetadataCacheOptions& options() const noexcept;

  /// \brief Return cached content or invoke loader and cache its result when eligible.
  Result<Content> Get(std::string location, std::optional<size_t> length, Loader loader);

  /// \brief Return an unexpired cached entry without loading it.
  Content GetIfPresent(std::string_view location);

  /// \brief Invalidate one location, waiting for an in-progress load if necessary.
  void Invalidate(std::string_view location);

  /// \brief Invalidate all currently cached entries.
  void Clear();

  size_t size() const;
  size_t total_bytes() const;

 private:
  class Impl;

  explicit MetadataCache(MetadataCacheOptions options);

  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg
