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

#include <optional>

#include <nlohmann/json.hpp>

#include "iceberg/result.h"
#include "iceberg/util/macros.h"

/// \file iceberg/util/json_util_internal.h
/// \brief Internal utilities for JSON serialization and deserialization.

namespace iceberg {

template <typename T>
void SetOptionalField(nlohmann::json& json, std::string_view key,
                      const std::optional<T>& value) {
  if (value.has_value()) {
    json[key] = *value;
  }
}

template <typename T>
  requires requires(const T& t) { t.empty(); }
void SetContainerField(nlohmann::json& json, std::string_view key, const T& value) {
  if (!value.empty()) {
    json[key] = value;
  }
}

inline void SetOptionalStringField(nlohmann::json& json, std::string_view key,
                                   const std::string& value) {
  if (!value.empty()) {
    json[key] = value;
  }
}

inline std::string SafeDumpJson(const nlohmann::json& json) {
  return json.dump(/*indent=*/-1, /*indent_char=*/' ', /*ensure_ascii=*/false,
                   nlohmann::detail::error_handler_t::ignore);
}

template <typename T>
Result<T> GetTypedJsonValue(const nlohmann::json& json) {
  try {
    return json.get<T>();
  } catch (const std::exception& ex) {
    return JsonParseError("Failed to parse {}: {}", SafeDumpJson(json), ex.what());
  }
}

template <typename T>
Result<T> GetJsonValueImpl(const nlohmann::json& json, std::string_view key) {
  try {
    return json.at(key).get<T>();
  } catch (const std::exception& ex) {
    return JsonParseError("Failed to parse '{}' from {}: {}", key, SafeDumpJson(json),
                          ex.what());
  }
}

template <typename T>
Result<std::optional<T>> GetJsonValueOptional(const nlohmann::json& json,
                                              std::string_view key) {
  if (!json.contains(key) || json.at(key).is_null()) {
    return std::nullopt;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto value, GetJsonValueImpl<T>(json, key));
  return std::optional<T>(std::move(value));
}

template <typename T>
Result<T> GetJsonValue(const nlohmann::json& json, std::string_view key) {
  if (!json.contains(key) || json.at(key).is_null()) {
    return JsonParseError("Missing '{}' in {}", key, SafeDumpJson(json));
  }
  return GetJsonValueImpl<T>(json, key);
}

template <typename T>
Result<T> GetJsonValueOrDefault(const nlohmann::json& json, std::string_view key,
                                T default_value = T{}) {
  if (!json.contains(key) || json.at(key).is_null()) {
    return default_value;
  }
  return GetJsonValueImpl<T>(json, key);
}

/// \brief Convert a list of items to a json array.
///
/// Note that ToJson(const T&) is required for this function to work.
template <typename T>
nlohmann::json::array_t ToJsonList(const std::vector<T>& list) {
  return std::accumulate(list.cbegin(), list.cend(), nlohmann::json::array(),
                         [](nlohmann::json::array_t arr, const T& item) {
                           arr.push_back(ToJson(item));
                           return arr;
                         });
}

/// \brief Overload of the above function for a list of shared pointers.
template <typename T>
nlohmann::json::array_t ToJsonList(const std::vector<std::shared_ptr<T>>& list) {
  return std::accumulate(list.cbegin(), list.cend(), nlohmann::json::array(),
                         [](nlohmann::json::array_t arr, const std::shared_ptr<T>& item) {
                           arr.push_back(ToJson(*item));
                           return arr;
                         });
}

/// \brief Parse a list of items from a JSON object.
///
/// \param[in] json The JSON object to parse.
/// \param[in] key The key to parse.
/// \param[in] from_json The function to parse an item from a JSON object.
/// \return The list of items.
template <typename T>
Result<std::vector<T>> FromJsonList(
    const nlohmann::json& json, std::string_view key,
    const std::function<Result<T>(const nlohmann::json&)>& from_json) {
  std::vector<T> list{};
  if (json.contains(key)) {
    ICEBERG_ASSIGN_OR_RAISE(auto list_json, GetJsonValue<nlohmann::json>(json, key));
    if (!list_json.is_array()) {
      return JsonParseError("Cannot parse '{}' from non-array: {}", key,
                            SafeDumpJson(list_json));
    }
    for (const auto& entry_json : list_json) {
      ICEBERG_ASSIGN_OR_RAISE(auto entry, from_json(entry_json));
      list.emplace_back(std::move(entry));
    }
  }
  return list;
}

/// \brief Parse a list of items from a JSON object.
///
/// \param[in] json The JSON object to parse.
/// \param[in] key The key to parse.
/// \param[in] from_json The function to parse an item from a JSON object.
/// \return The list of items.
template <typename T>
Result<std::vector<std::shared_ptr<T>>> FromJsonList(
    const nlohmann::json& json, std::string_view key,
    const std::function<Result<std::shared_ptr<T>>(const nlohmann::json&)>& from_json) {
  std::vector<std::shared_ptr<T>> list{};
  if (json.contains(key)) {
    ICEBERG_ASSIGN_OR_RAISE(auto list_json, GetJsonValue<nlohmann::json>(json, key));
    if (!list_json.is_array()) {
      return JsonParseError("Cannot parse '{}' from non-array: {}", key,
                            SafeDumpJson(list_json));
    }
    for (const auto& entry_json : list_json) {
      ICEBERG_ASSIGN_OR_RAISE(auto entry, from_json(entry_json));
      list.emplace_back(std::move(entry));
    }
  }
  return list;
}

/// \brief Convert a map of type <std::string, T> to a json object.
///
/// Note that ToJson(const T&) is required for this function to work.
template <typename T>
nlohmann::json::object_t ToJsonMap(const std::unordered_map<std::string, T>& map) {
  return std::accumulate(map.cbegin(), map.cend(), nlohmann::json::object(),
                         [](nlohmann::json::object_t obj, const auto& item) {
                           obj[item.first] = ToJson(item.second);
                           return obj;
                         });
}

/// \brief Overload of the above function for a map of type <std::string,
/// std::shared_ptr<T>>.
template <typename T>
nlohmann::json::object_t ToJsonMap(
    const std::unordered_map<std::string, std::shared_ptr<T>>& map) {
  return std::accumulate(map.cbegin(), map.cend(), nlohmann::json::object(),
                         [](nlohmann::json::object_t obj, const auto& item) {
                           obj[item.first] = ToJson(*item.second);
                           return obj;
                         });
}

/// \brief Parse a map of type <std::string, T> from a JSON object.
///
/// \param[in] json The JSON object to parse.
/// \param[in] key The key to parse.
/// \param[in] from_json The function to parse an item from a JSON object.
/// \return The map of items.
template <typename T = std::string>
Result<std::unordered_map<std::string, T>> FromJsonMap(
    const nlohmann::json& json, std::string_view key,
    const std::function<Result<T>(const nlohmann::json&)>& from_json =
        [](const nlohmann::json& json) -> Result<T> {
      static_assert(std::is_same_v<T, std::string>, "T must be std::string");
      try {
        return json.get<std::string>();
      } catch (const std::exception& ex) {
        return JsonParseError("Cannot parse {} to a string value: {}", SafeDumpJson(json),
                              ex.what());
      }
    }) {
  std::unordered_map<std::string, T> map{};
  if (json.contains(key)) {
    ICEBERG_ASSIGN_OR_RAISE(auto map_json, GetJsonValue<nlohmann::json>(json, key));
    if (!map_json.is_object()) {
      return JsonParseError("Cannot parse '{}' from non-object: {}", key,
                            SafeDumpJson(map_json));
    }
    for (const auto& [key, value] : map_json.items()) {
      ICEBERG_ASSIGN_OR_RAISE(auto entry, from_json(value));
      map[key] = std::move(entry);
    }
  }
  return map;
}

}  // namespace iceberg
