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
#include <map>
#include <string>

namespace iceberg {

template <class ConcreteConfig>
class ConfigBase {
 public:
  template <typename T>
  class Entry {
    Entry(
        std::string key, const T& val,
        std::function<std::string(const T&)> toStr =
            [](const T& val) {
              if constexpr ((std::is_signed_v<T> && std::is_integral_v<T>) ||
                            std::is_floating_point_v<T>) {
                return std::to_string(val);
              } else if constexpr (std::is_same_v<T, bool>) {
                return val ? "true" : "false";
              } else if constexpr (std::is_same_v<T, std::string> ||
                                   std::is_same_v<T, std::string_view>) {
                return val;
              } else {
                throw std::runtime_error(
                    std::format("Explicit toStr() is required for {}", typeid(T).name()));
              }
            },
        std::function<T(const std::string&)> toT = [](const std::string& val) -> T {
          if constexpr (std::is_same_v<T, std::string>) {
            return val;
          } else if constexpr (std::is_same_v<T, bool>) {
            return val == "true";
          } else if constexpr (std::is_signed_v<T> && std::is_integral_v<T>) {
            return static_cast<T>(std::stoll(val));
          } else if constexpr (std::is_floating_point_v<T>) {
            return static_cast<T>(std::stod(val));
          } else {
            throw std::runtime_error(
                std::format("Explicit toT() is required for {}", typeid(T).name()));
          }
        })
        : key_{std::move(key)}, default_{val}, toStr_{toStr}, toT_{toT} {}

    const std::string key_;
    const T default_;
    const std::function<std::string(const T&)> toStr_;
    const std::function<T(const std::string&)> toT_;

    friend ConfigBase;
    friend ConcreteConfig;

   public:
    const std::string& key() const { return key_; }

    T value() const { return default_; }
  };

  template <typename T>
  ConfigBase& set(const Entry<T>& entry, const T& val) {
    configs_[entry.key_] = entry.toStr_(val);
    return *this;
  }

  template <typename T>
  ConfigBase& unset(const Entry<T>& entry) {
    configs_.erase(entry.key_);
    return *this;
  }

  ConfigBase& reset() {
    configs_.clear();
    return *this;
  }

  template <typename T>
  T get(const Entry<T>& entry) const {
    try {
      auto iter = configs_.find(entry.key_);
      return iter != configs_.cend() ? entry.toT_(iter->second) : entry.default_;
    } catch (std::exception& e) {
      throw std::runtime_error(
          std::format("Failed to get config {} with error: {}", entry.key_, e.what()));
    }
  }

  std::map<std::string, std::string> const& configs() const { return configs_; }

 protected:
  std::map<std::string, std::string> configs_;
};

}  // namespace iceberg
