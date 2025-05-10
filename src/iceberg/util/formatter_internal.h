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

#include <concepts>
#include <format>
#include <map>
#include <ranges>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/util/formatter.h"

/// \brief Concept for smart pointer types
template <typename T>
concept SmartPointerType = requires(T t) {
  { t.operator->() } -> std::same_as<typename T::element_type*>;
  { *t } -> std::convertible_to<typename T::element_type&>;
  { static_cast<bool>(t) } -> std::same_as<bool>;
  typename T::element_type;
};

/// \brief Helper function to format an item using concepts to differentiate types
template <typename T>
std::string FormatItem(const T& item) {
  if constexpr (SmartPointerType<T>) {
    if (item) {
      return std::format("{}", *item);
    } else {
      return "null";
    }
  } else {
    return std::format("{}", item);
  }
}

/// \brief Generic function to join a range of elements with a separator and wrap with
/// delimiters
template <std::ranges::input_range Range>
std::string FormatRange(const Range& range, std::string_view separator,
                        std::string_view prefix, std::string_view suffix) {
  if (std::ranges::empty(range)) {
    return std::format("{}{}", prefix, suffix);
  }

  std::stringstream ss;
  ss << prefix;

  bool first = true;
  for (const auto& element : range) {
    if (!first) {
      ss << separator;
    }
    ss << std::format("{}", element);
    first = false;
  }

  ss << suffix;
  return ss.str();
}

/// \brief Helper template for formatting map-like containers
template <typename MapType>
std::string FormatMap(const MapType& map) {
  // Transform the map items into formatted key-value pairs
  std::ranges::transform_view formatted_range =
      map | std::views::transform([](const auto& pair) -> std::string {
        const auto& [key, value] = pair;
        return std::format("{}: {}", FormatItem(key), FormatItem(value));
      });
  return FormatRange(formatted_range, ", ", "{", "}");
}

/// \brief std::formatter specialization for std::map
template <typename K, typename V>
struct std::formatter<std::map<K, V>> : std::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const std::map<K, V>& map, FormatContext& ctx) const {
    return std::formatter<std::string_view>::format(FormatMap(map), ctx);
  }
};

/// \brief std::formatter specialization for std::unordered_map
template <typename K, typename V>
struct std::formatter<std::unordered_map<K, V>> : std::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const std::unordered_map<K, V>& map, FormatContext& ctx) const {
    return std::formatter<std::string_view>::format(FormatMap(map), ctx);
  }
};

/// \brief std::formatter specialization for std::vector
template <typename T>
struct std::formatter<std::vector<T>> : std::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const std::vector<T>& vec, FormatContext& ctx) const {
    auto formatted_range =
        vec | std::views::transform([](const auto& item) { return FormatItem(item); });
    return std::formatter<std::string_view>::format(
        FormatRange(formatted_range, ", ", "[", "]"), ctx);
  }
};

/// \brief std::formatter specialization for std::span
template <typename T, size_t Extent>
struct std::formatter<std::span<T, Extent>> : std::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const std::span<T, Extent>& span, FormatContext& ctx) const {
    auto formatted_range =
        span | std::views::transform([](const auto& item) { return FormatItem(item); });
    return std::formatter<std::string_view>::format(
        FormatRange(formatted_range, ", ", "[", "]"), ctx);
  }
};

/// \brief std::formatter specialization for std::unordered_set
template <typename T, typename Hash, typename KeyEqual, typename Allocator>
struct std::formatter<std::unordered_set<T, Hash, KeyEqual, Allocator>>
    : std::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const std::unordered_set<T, Hash, KeyEqual, Allocator>& set,
              FormatContext& ctx) const {
    auto formatted_range =
        set | std::views::transform([](const auto& item) { return FormatItem(item); });
    return std::formatter<std::string_view>::format(
        FormatRange(formatted_range, ", ", "[", "]"), ctx);
  }
};
