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
#include <regex>

#include <nlohmann/json.hpp>

#include "iceberg/expected.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/sort_order.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/formatter.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

constexpr std::string_view kTransform = "transform";
constexpr std::string_view kSourceId = "source-id";
constexpr std::string_view kDirection = "direction";
constexpr std::string_view kNullOrder = "null-order";

constexpr std::string_view kOrderId = "order-id";
constexpr std::string_view kFields = "fields";

constexpr std::string_view kSchemaId = "schema-id";
constexpr std::string_view kIdentifierFieldIds = "identifier-field-ids";

constexpr std::string_view kType = "type";
constexpr std::string_view kStruct = "struct";
constexpr std::string_view kList = "list";
constexpr std::string_view kMap = "map";
constexpr std::string_view kElement = "element";
constexpr std::string_view kKey = "key";
constexpr std::string_view kValue = "value";
constexpr std::string_view kDoc = "doc";
constexpr std::string_view kName = "name";
constexpr std::string_view kId = "id";
constexpr std::string_view kInitialDefault = "initial-default";
constexpr std::string_view kWriteDefault = "write-default";
constexpr std::string_view kElementId = "element-id";
constexpr std::string_view kKeyId = "key-id";
constexpr std::string_view kValueId = "value-id";
constexpr std::string_view kRequired = "required";
constexpr std::string_view kElementRequired = "element-required";
constexpr std::string_view kValueRequired = "value-required";

template <typename T>
Result<T> GetJsonValue(const nlohmann::json& json, std::string_view key) {
  if (!json.contains(key)) {
    return unexpected<Error>({
        .kind = ErrorKind::kJsonParseError,
        .message = std::format("Missing '{}' in {}", key, json.dump()),
    });
  }
  try {
    return json.at(key).get<T>();
  } catch (const std::exception& ex) {
    return unexpected<Error>({
        .kind = ErrorKind::kJsonParseError,
        .message = std::format("Failed to parse key '{}' in {}", key, json.dump()),
    });
  }
}

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

Result<std::unique_ptr<SortField>> SortFieldFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto source_id, GetJsonValue<int32_t>(json, kSourceId));
  ICEBERG_ASSIGN_OR_RAISE(
      auto transform,
      GetJsonValue<std::string>(json, kTransform).and_then(TransformFunctionFromString));
  ICEBERG_ASSIGN_OR_RAISE(
      auto direction,
      GetJsonValue<std::string>(json, kDirection).and_then(SortDirectionFromString));
  ICEBERG_ASSIGN_OR_RAISE(
      auto null_order,
      GetJsonValue<std::string>(json, kNullOrder).and_then(NullOrderFromString));
  return std::make_unique<SortField>(source_id, std::move(transform), direction,
                                     null_order);
}

Result<std::unique_ptr<SortOrder>> SortOrderFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto order_id, GetJsonValue<int32_t>(json, kOrderId));
  ICEBERG_ASSIGN_OR_RAISE(auto fields, GetJsonValue<nlohmann::json>(json, kFields));

  std::vector<SortField> sort_fields;
  for (const auto& field_json : fields) {
    ICEBERG_ASSIGN_OR_RAISE(auto sort_field, SortFieldFromJson(field_json));
    sort_fields.push_back(std::move(*sort_field));
  }
  return std::make_unique<SortOrder>(order_id, std::move(sort_fields));
}

nlohmann::json FieldToJson(const SchemaField& field) {
  nlohmann::json json;
  json[kId] = field.field_id();
  json[kName] = field.name();
  json[kRequired] = !field.optional();
  json[kType] = TypeToJson(*field.type());
  return json;
}

nlohmann::json TypeToJson(const Type& type) {
  switch (type.type_id()) {
    case TypeId::kStruct: {
      const auto& struct_type = static_cast<const StructType&>(type);
      nlohmann::json json;
      json[kType] = kStruct;
      nlohmann::json fields_json = nlohmann::json::array();
      for (const auto& field : struct_type.fields()) {
        fields_json.push_back(FieldToJson(field));
        // TODO(gangwu): add default values
      }
      json[kFields] = fields_json;
      return json;
    }
    case TypeId::kList: {
      const auto& list_type = static_cast<const ListType&>(type);
      nlohmann::json json;
      json[kType] = kList;

      const auto& element_field = list_type.fields().front();
      json[kElementId] = element_field.field_id();
      json[kElementRequired] = !element_field.optional();
      json[kElement] = TypeToJson(*element_field.type());
      return json;
    }
    case TypeId::kMap: {
      const auto& map_type = static_cast<const MapType&>(type);
      nlohmann::json json;
      json[std::string(kType)] = kMap;

      const auto& key_field = map_type.key();
      json[kKeyId] = key_field.field_id();
      json[kKey] = TypeToJson(*key_field.type());

      const auto& value_field = map_type.value();
      json[kValueId] = value_field.field_id();
      json[kValueRequired] = !value_field.optional();
      json[kValue] = TypeToJson(*value_field.type());
      return json;
    }
    case TypeId::kBoolean:
      return "boolean";
    case TypeId::kInt:
      return "int";
    case TypeId::kLong:
      return "long";
    case TypeId::kFloat:
      return "float";
    case TypeId::kDouble:
      return "double";
    case TypeId::kDecimal: {
      const auto& decimal_type = static_cast<const DecimalType&>(type);
      return std::format("decimal({},{})", decimal_type.precision(),
                         decimal_type.scale());
    }
    case TypeId::kDate:
      return "date";
    case TypeId::kTime:
      return "time";
    case TypeId::kTimestamp:
      return "timestamp";
    case TypeId::kTimestampTz:
      return "timestamptz";
    case TypeId::kString:
      return "string";
    case TypeId::kBinary:
      return "binary";
    case TypeId::kFixed: {
      const auto& fixed_type = static_cast<const FixedType&>(type);
      return std::format("fixed[{}]", fixed_type.length());
    }
    case TypeId::kUuid:
      return "uuid";
  }
}

nlohmann::json SchemaToJson(const Schema& schema) {
  nlohmann::json json = TypeToJson(static_cast<const Type&>(schema));
  json[kSchemaId] = schema.schema_id();
  // TODO(gangwu): add identifier-field-ids.
  return json;
}

namespace {

Result<std::unique_ptr<Type>> StructTypeFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto json_fields, GetJsonValue<nlohmann::json>(json, kFields));

  std::vector<SchemaField> fields;
  for (const auto& field_json : json_fields) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, FieldFromJson(field_json));
    fields.emplace_back(std::move(*field));
  }

  return std::make_unique<StructType>(std::move(fields));
}

Result<std::unique_ptr<Type>> ListTypeFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto element_type, TypeFromJson(json[kElement]));
  ICEBERG_ASSIGN_OR_RAISE(auto element_id, GetJsonValue<int32_t>(json, kElementId));
  ICEBERG_ASSIGN_OR_RAISE(auto element_required,
                          GetJsonValue<bool>(json, kElementRequired));

  return std::make_unique<ListType>(
      SchemaField(element_id, std::string(ListType::kElementName),
                  std::move(element_type), !element_required));
}

Result<std::unique_ptr<Type>> MapTypeFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto key_type, GetJsonValue<nlohmann::json>(json, kKey).and_then(TypeFromJson));
  ICEBERG_ASSIGN_OR_RAISE(
      auto value_type, GetJsonValue<nlohmann::json>(json, kValue).and_then(TypeFromJson));

  ICEBERG_ASSIGN_OR_RAISE(auto key_id, GetJsonValue<int32_t>(json, kKeyId));
  ICEBERG_ASSIGN_OR_RAISE(auto value_id, GetJsonValue<int32_t>(json, kValueId));
  ICEBERG_ASSIGN_OR_RAISE(auto value_required, GetJsonValue<bool>(json, kValueRequired));

  SchemaField key_field(key_id, std::string(MapType::kKeyName), std::move(key_type),
                        /*optional=*/false);
  SchemaField value_field(value_id, std::string(MapType::kValueName),
                          std::move(value_type), !value_required);
  return std::make_unique<MapType>(std::move(key_field), std::move(value_field));
}

}  // namespace

Result<std::unique_ptr<Type>> TypeFromJson(const nlohmann::json& json) {
  if (json.is_string()) {
    std::string type_str = json.get<std::string>();
    if (type_str == "boolean") {
      return std::make_unique<BooleanType>();
    } else if (type_str == "int") {
      return std::make_unique<IntType>();
    } else if (type_str == "long") {
      return std::make_unique<LongType>();
    } else if (type_str == "float") {
      return std::make_unique<FloatType>();
    } else if (type_str == "double") {
      return std::make_unique<DoubleType>();
    } else if (type_str == "date") {
      return std::make_unique<DateType>();
    } else if (type_str == "time") {
      return std::make_unique<TimeType>();
    } else if (type_str == "timestamp") {
      return std::make_unique<TimestampType>();
    } else if (type_str == "timestamptz") {
      return std::make_unique<TimestampTzType>();
    } else if (type_str == "string") {
      return std::make_unique<StringType>();
    } else if (type_str == "binary") {
      return std::make_unique<BinaryType>();
    } else if (type_str == "uuid") {
      return std::make_unique<UuidType>();
    } else if (type_str.starts_with("fixed")) {
      std::regex fixed_regex(R"(fixed\[\s*(\d+)\s*\])");
      std::smatch match;
      if (std::regex_match(type_str, match, fixed_regex)) {
        return std::make_unique<FixedType>(std::stoi(match[1].str()));
      }
      return unexpected<Error>({
          .kind = ErrorKind::kJsonParseError,
          .message = std::format("Invalid fixed type: {}", type_str),
      });
    } else if (type_str.starts_with("decimal")) {
      std::regex decimal_regex(R"(decimal\(\s*(\d+)\s*,\s*(\d+)\s*\))");
      std::smatch match;
      if (std::regex_match(type_str, match, decimal_regex)) {
        return std::make_unique<DecimalType>(std::stoi(match[1].str()),
                                             std::stoi(match[2].str()));
      }
      return unexpected<Error>({
          .kind = ErrorKind::kJsonParseError,
          .message = std::format("Invalid decimal type: {}", type_str),
      });
    } else {
      return unexpected<Error>({
          .kind = ErrorKind::kJsonParseError,
          .message = std::format("Unknown primitive type: {}", type_str),
      });
    }
  }

  // For complex types like struct, list, and map
  ICEBERG_ASSIGN_OR_RAISE(auto type_str, GetJsonValue<std::string>(json, kType));
  if (type_str == kStruct) {
    return StructTypeFromJson(json);
  } else if (type_str == kList) {
    return ListTypeFromJson(json);
  } else if (type_str == kMap) {
    return MapTypeFromJson(json);
  } else {
    return unexpected<Error>({
        .kind = ErrorKind::kJsonParseError,
        .message = std::format("Unknown complex type: {}", type_str),
    });
  }
}

Result<std::unique_ptr<SchemaField>> FieldFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto type, GetJsonValue<nlohmann::json>(json, kType).and_then(TypeFromJson));
  ICEBERG_ASSIGN_OR_RAISE(auto field_id, GetJsonValue<int32_t>(json, kId));
  ICEBERG_ASSIGN_OR_RAISE(auto name, GetJsonValue<std::string>(json, kName));
  ICEBERG_ASSIGN_OR_RAISE(auto required, GetJsonValue<bool>(json, kRequired));

  return std::make_unique<SchemaField>(field_id, std::move(name), std::move(type),
                                       !required);
}

Result<std::unique_ptr<Schema>> SchemaFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema_id, GetJsonValue<int32_t>(json, kSchemaId));
  ICEBERG_ASSIGN_OR_RAISE(auto type, TypeFromJson(json));

  if (type->type_id() != TypeId::kStruct) [[unlikely]] {
    return unexpected<Error>({
        .kind = ErrorKind::kJsonParseError,
        .message = std::format("Schema must be a struct type, but got {}", json.dump()),
    });
  }

  auto& struct_type = static_cast<StructType&>(*type);
  return FromStructType(std::move(struct_type), schema_id);
}

}  // namespace iceberg
