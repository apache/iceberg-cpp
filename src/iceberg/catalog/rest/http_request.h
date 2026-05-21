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

#include <cstdint>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iceberg/catalog/rest/iceberg_rest_export.h"

namespace iceberg::rest {

/// \brief HTTP method enumeration.
enum class HttpMethod : uint8_t { kGet, kPost, kPut, kDelete, kHead };

/// \brief Convert HttpMethod to string representation.
constexpr std::string_view ToString(HttpMethod method);

/// \brief Ordered collection of HTTP headers preserving repeated values.
///
/// Name comparison is case-insensitive (RFC 7230), insertion order is
/// preserved, and multiple entries with the same name coexist. The map-style
/// methods (`operator[]`, `at`, `try_emplace`, `find`) act on the *first*
/// matching entry; `add` appends a new entry even when the name already
/// exists. Not thread-safe. `add`, `try_emplace`, `operator[]` (when
/// inserting) and `erase` invalidate iterators.
class ICEBERG_REST_EXPORT HttpHeaders {
 public:
  using Entry = std::pair<std::string, std::string>;
  using container_type = std::vector<Entry>;
  using iterator = container_type::iterator;
  using const_iterator = container_type::const_iterator;

  HttpHeaders() = default;
  HttpHeaders(std::initializer_list<Entry> init) : entries_(init) {}

  iterator begin() noexcept { return entries_.begin(); }
  iterator end() noexcept { return entries_.end(); }
  const_iterator begin() const noexcept { return entries_.begin(); }
  const_iterator end() const noexcept { return entries_.end(); }
  const_iterator cbegin() const noexcept { return entries_.cbegin(); }
  const_iterator cend() const noexcept { return entries_.cend(); }

  bool empty() const noexcept { return entries_.empty(); }
  std::size_t size() const noexcept { return entries_.size(); }
  void clear() noexcept { entries_.clear(); }
  void reserve(std::size_t n) { entries_.reserve(n); }

  /// \brief Case-insensitive lookup. Returns iterator to the first entry whose
  /// name matches, or end() if none.
  iterator find(std::string_view name);
  const_iterator find(std::string_view name) const;

  bool contains(std::string_view name) const { return find(name) != end(); }

  /// \brief Returns the value of the first entry with the given name.
  /// Throws std::out_of_range if none.
  std::string& at(std::string_view name);
  const std::string& at(std::string_view name) const;

  /// \brief Map-like upsert: returns reference to the first matching entry's
  /// value, inserting a new entry with empty value if none exists.
  std::string& operator[](std::string_view name);

  /// \brief Insert only if no entry with the same name exists.
  void try_emplace(std::string name, std::string value);

  /// \brief Append an entry, preserving any existing entries with the same
  /// name. Use this when repeated headers must survive (e.g. multiple
  /// Set-Cookie values).
  void add(std::string name, std::string value) {
    entries_.emplace_back(std::move(name), std::move(value));
  }

  /// \brief Remove all entries with the given name (case-insensitive). Returns
  /// the number of entries removed.
  std::size_t erase(std::string_view name);

 private:
  container_type entries_;
};

/// \brief An outgoing HTTP request. Mirrors Java's HttpRequest so signing
/// implementations like SigV4 see method, url, headers, and body together.
struct ICEBERG_REST_EXPORT HttpRequest {
  HttpMethod method = HttpMethod::kGet;
  std::string url;
  HttpHeaders headers;
  std::string body;
};

}  // namespace iceberg::rest
