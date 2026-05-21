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

#include "iceberg/catalog/rest/http_request.h"

#include <algorithm>
#include <ranges>

#include "iceberg/util/string_util.h"

namespace iceberg::rest {

HttpHeaders::iterator HttpHeaders::find(std::string_view name) {
  return std::ranges::find_if(entries_, [name](const Entry& e) {
    return StringUtils::EqualsIgnoreCase(e.first, name);
  });
}

HttpHeaders::const_iterator HttpHeaders::find(std::string_view name) const {
  return std::ranges::find_if(entries_, [name](const Entry& e) {
    return StringUtils::EqualsIgnoreCase(e.first, name);
  });
}

std::string& HttpHeaders::at(std::string_view name) {
  auto it = find(name);
  if (it == entries_.end()) {
    throw std::out_of_range("HttpHeaders::at: no header named '" + std::string(name) +
                            "'");
  }
  return it->second;
}

const std::string& HttpHeaders::at(std::string_view name) const {
  auto it = find(name);
  if (it == entries_.end()) {
    throw std::out_of_range("HttpHeaders::at: no header named '" + std::string(name) +
                            "'");
  }
  return it->second;
}

std::string& HttpHeaders::operator[](std::string_view name) {
  auto it = find(name);
  if (it == entries_.end()) {
    entries_.emplace_back(std::string(name), std::string{});
    return entries_.back().second;
  }
  return it->second;
}

void HttpHeaders::try_emplace(std::string name, std::string value) {
  if (find(name) == entries_.end()) {
    entries_.emplace_back(std::move(name), std::move(value));
  }
}

std::size_t HttpHeaders::erase(std::string_view name) {
  auto removed = std::ranges::remove_if(entries_, [name](const Entry& e) {
    return StringUtils::EqualsIgnoreCase(e.first, name);
  });
  std::size_t count = removed.size();
  entries_.erase(removed.begin(), removed.end());
  return count;
}

}  // namespace iceberg::rest
