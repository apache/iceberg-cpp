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

#include "iceberg/json_internal.h"

#include <format>

#include <nlohmann/json.hpp>

#include "iceberg/sort_order.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"

namespace iceberg {

namespace {

constexpr std::string_view kTransform = "transform";
constexpr std::string_view kSourceId = "source-id";
constexpr std::string_view kDirection = "direction";
constexpr std::string_view kNullOrder = "null-order";

constexpr std::string_view kOrderId = "order-id";
constexpr std::string_view kFields = "fields";

// --- helper for safe JSON extraction ---
template <typename T>
expected<T, Error> GetJsonValue(const nlohmann::json& json, std::string_view key) {
  if (!json.contains(key)) {
    return unexpected<Error>({.kind = ErrorKind::kInvalidArgument,
                              .message = "Missing key: " + std::string(key)});
  }
  try {
    return json.at(key).get<T>();
  } catch (const std::exception& ex) {
    return unexpected<Error>({.kind = ErrorKind::kInvalidArgument,
                              .message = std::string("Failed to parse key: ") +
                                         key.data() + ", " + ex.what()});
  }
}

#define TRY_ASSIGN(json_value, expr)                                    \
  auto _tmp_##json_value = (expr);                                      \
  if (!_tmp_##json_value) return unexpected(_tmp_##json_value.error()); \
  auto json_value = std::move(_tmp_##json_value.value());
}  // namespace

nlohmann::json ToJson(const SortField& sort_field) {
  nlohmann::json json;
  json[kTransform] = std::format("{}", *sort_field.transform());
  json[kSourceId] = sort_field.source_id();
  json[kDirection] = SortDirectionToString(sort_field.direction());
  json[kNullOrder] = NullOrderToString(sort_field.null_order());
  return json;
}

nlohmann::json ToJson(const SortOrder& sort_order) {
  nlohmann::json json;
  json[kOrderId] = sort_order.order_id();

  nlohmann::json fields_json = nlohmann::json::array();
  for (const auto& field : sort_order.fields()) {
    fields_json.push_back(ToJson(field));
  }
  json[kFields] = fields_json;
  return json;
}

expected<std::unique_ptr<SortField>, Error> SortFieldFromJson(
    const nlohmann::json& json) {
  TRY_ASSIGN(transform_str, GetJsonValue<std::string>(json, kTransform));
  TRY_ASSIGN(transform, TransformFunctionFromString(transform_str));
  TRY_ASSIGN(source_id, GetJsonValue<int32_t>(json, kSourceId));
  TRY_ASSIGN(direction_str, GetJsonValue<std::string>(json, kDirection));
  TRY_ASSIGN(direction, SortDirectionFromString(direction_str));
  TRY_ASSIGN(null_order_str, GetJsonValue<std::string>(json, kNullOrder));
  TRY_ASSIGN(null_order, NullOrderFromString(null_order_str));

  return std::make_unique<SortField>(source_id, std::move(transform), direction,
                                     null_order);
}

expected<std::unique_ptr<SortOrder>, Error> SortOrderFromJson(
    const nlohmann::json& json) {
  TRY_ASSIGN(order_id, GetJsonValue<int32_t>(json, kOrderId));

  std::vector<SortField> sort_fields;
  for (const auto& field_json : json.at(kFields)) {
    TRY_ASSIGN(sort_field, SortFieldFromJson(field_json));
    sort_fields.push_back(*sort_field);
  }

  return std::make_unique<SortOrder>(order_id, std::move(sort_fields));
}

}  // namespace iceberg
