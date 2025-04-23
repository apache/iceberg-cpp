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

/// \file iceberg/util/formatter.h
/// A specialization of std::formatter for Formattable objects.  This header
/// is separate from iceberg/util/formattable.h so that the latter (which is
/// meant to be included widely) does not leak <format> unnecessarily into
/// other headers.  You must include this header to format a Formattable.

#include <concepts>
#include <format>
#include <map>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/util/formattable.h"

/// \brief Make all classes deriving from iceberg::util::Formattable
///   formattable with std::format.
template <std::derived_from<iceberg::util::Formattable> Derived>
struct std::formatter<Derived> : std::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const iceberg::util::Formattable& obj, FormatContext& ctx) const {
    return std::formatter<string_view>::format(obj.ToString(), ctx);
  }
};

/// \brief std::formatter specialization for std::vector
template <typename T>
struct std::formatter<std::vector<T>> : std::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const std::vector<T>& vec, FormatContext& ctx) const {
    std::string result = "[";

    bool first = true;
    for (const auto& item : vec) {
      if (!first) {
        std::format_to(std::back_inserter(result), ", ");
      }
      if constexpr (requires { *item; }) {
        if (item) {
          std::format_to(std::back_inserter(result), "{}", *item);
        } else {
          std::format_to(std::back_inserter(result), "null");
        }
      } else {
        std::format_to(std::back_inserter(result), "{}", item);
      }
      first = false;
    }

    std::format_to(std::back_inserter(result), "]");
    return std::formatter<std::string_view>::format(result, ctx);
  }
};

/// \brief Helper template for formatting map-like containers
template <typename MapType>
std::string FormatMap(const MapType& map) {
  std::string result = "{";

  bool first = true;
  for (const auto& [key, value] : map) {
    if (!first) {
      std::format_to(std::back_inserter(result), ", ");
    }

    // Format key (handle if it's a smart pointer)
    if constexpr (requires { *key; }) {
      if (key) {
        std::format_to(std::back_inserter(result), "{}: ", *key);
      } else {
        std::format_to(std::back_inserter(result), "null: ");
      }
    } else {
      std::format_to(std::back_inserter(result), "{}: ", key);
    }

    // Format value (handle if it's a smart pointer)
    if constexpr (requires { *value; }) {
      if (value) {
        std::format_to(std::back_inserter(result), "{}", *value);
      } else {
        std::format_to(std::back_inserter(result), "null");
      }
    } else {
      std::format_to(std::back_inserter(result), "{}", value);
    }

    first = false;
  }

  std::format_to(std::back_inserter(result), "}}");
  return result;
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

/// \brief std::formatter specialization for any type that has a ToString function
template <typename T>
  requires requires(const T& t) {
    { ToString(t) } -> std::convertible_to<std::string>;
  }
struct std::formatter<T> : std::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const T& value, FormatContext& ctx) const {
    return std::formatter<std::string_view>::format(ToString(value), ctx);
  }
};
