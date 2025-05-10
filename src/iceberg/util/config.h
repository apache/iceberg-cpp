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

#include <format>
#include <functional>
#include <string>
#include <unordered_map>

#include "iceberg/exception.h"

namespace iceberg {
namespace internal {
// Default conversion functions
template <typename U>
std::string DefaultToString(const U& val) {
  if constexpr ((std::is_signed_v<U> && std::is_integral_v<U>) ||
                std::is_floating_point_v<U>) {
    return std::to_string(val);
  } else if constexpr (std::is_same_v<U, bool>) {
    return val ? "true" : "false";
  } else if constexpr (std::is_same_v<U, std::string> ||
                       std::is_same_v<U, std::string_view>) {
    return val;
  } else {
    throw IcebergError(
        std::format("Explicit to_str() is required for {}", typeid(U).name()));
  }
}

template <typename U>
U DefaultFromString(const std::string& val) {
  if constexpr (std::is_same_v<U, std::string>) {
    return val;
  } else if constexpr (std::is_same_v<U, bool>) {
    return val == "true";
  } else if constexpr (std::is_signed_v<U> && std::is_integral_v<U>) {
    return static_cast<U>(std::stoll(val));
  } else if constexpr (std::is_floating_point_v<U>) {
    return static_cast<U>(std::stod(val));
  } else {
    throw IcebergError(
        std::format("Explicit from_str() is required for {}", typeid(U).name()));
  }
}
}  // namespace internal

template <class ConcreteConfig>
class ConfigBase {
 public:
  template <typename T>
  class Entry {
   public:
    Entry(std::string key, const T& val,
          std::function<std::string(const T&)> to_str = internal::DefaultToString<T>,
          std::function<T(const std::string&)> from_str = internal::DefaultFromString<T>)
        : key_{std::move(key)}, default_{val}, to_str_{to_str}, from_str_{from_str} {}

   private:
    const std::string key_;
    const T default_;
    const std::function<std::string(const T&)> to_str_;
    const std::function<T(const std::string&)> from_str_;

    friend ConfigBase;
    friend ConcreteConfig;

   public:
    const std::string& key() const { return key_; }

    const T& value() const { return default_; }
  };

  template <typename T>
  ConfigBase& Set(const Entry<T>& entry, const T& val) {
    configs_.emplace(entry.key_, entry.to_str_(val));
    return *this;
  }

  template <typename T>
  ConfigBase& Unset(const Entry<T>& entry) {
    configs_.erase(entry.key_);
    return *this;
  }

  ConfigBase& Reset() {
    configs_.clear();
    return *this;
  }

  template <typename T>
  T Get(const Entry<T>& entry) const {
    auto iter = configs_.find(entry.key_);
    return iter != configs_.cend() ? entry.from_str_(iter->second) : entry.default_;
  }

  const std::unordered_map<std::string, std::string>& configs() const { return configs_; }

 protected:
  std::unordered_map<std::string, std::string> configs_;
};

}  // namespace iceberg
