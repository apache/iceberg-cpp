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

#include "iceberg/schema_internal.h"

#include <cstring>
#include <format>
#include <optional>
#include <regex>
#include <string>

#include <nlohmann/json.hpp>

#include "iceberg/expected.h"
#include "iceberg/schema.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

// Constants for Arrow schema metadata
constexpr const char* kArrowExtensionName = "ARROW:extension:name";
constexpr const char* kArrowExtensionMetadata = "ARROW:extension:metadata";
constexpr const char* kArrowUuidExtensionName = "arrow.uuid";
constexpr int32_t kUnknownFieldId = -1;

// Constants for schema json serialization
constexpr std::string_view kSchemaId = "schema-id";
constexpr std::string_view kIdentifierFieldIds = "identifier-field-ids";
constexpr std::string_view kType = "type";
constexpr std::string_view kStruct = "struct";
constexpr std::string_view kList = "list";
constexpr std::string_view kMap = "map";
constexpr std::string_view kFields = "fields";
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

// Convert an Iceberg type to Arrow schema. Return value is Nanoarrow error code.
ArrowErrorCode ToArrowSchema(const Type& type, bool optional, std::string_view name,
                             std::optional<int32_t> field_id, ArrowSchema* schema) {
  ArrowBuffer metadata_buffer;
  NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderInit(&metadata_buffer, nullptr));
  if (field_id.has_value()) {
    NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderAppend(
        &metadata_buffer, ArrowCharView(std::string(kFieldIdKey).c_str()),
        ArrowCharView(std::to_string(field_id.value()).c_str())));
  }

  switch (type.type_id()) {
    case TypeId::kStruct: {
      NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_STRUCT));

      const auto& struct_type = static_cast<const StructType&>(type);
      const auto& fields = struct_type.fields();
      NANOARROW_RETURN_NOT_OK(ArrowSchemaAllocateChildren(schema, fields.size()));

      for (size_t i = 0; i < fields.size(); i++) {
        const auto& field = fields[i];
        NANOARROW_RETURN_NOT_OK(ToArrowSchema(*field.type(), field.optional(),
                                              field.name(), field.field_id(),
                                              schema->children[i]));
      }
    } break;
    case TypeId::kList: {
      NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_LIST));

      const auto& list_type = static_cast<const ListType&>(type);
      const auto& elem_field = list_type.fields()[0];
      NANOARROW_RETURN_NOT_OK(ToArrowSchema(*elem_field.type(), elem_field.optional(),
                                            elem_field.name(), elem_field.field_id(),
                                            schema->children[0]));
    } break;
    case TypeId::kMap: {
      NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_MAP));

      const auto& map_type = static_cast<const MapType&>(type);
      const auto& key_field = map_type.key();
      const auto& value_field = map_type.value();
      NANOARROW_RETURN_NOT_OK(ToArrowSchema(*key_field.type(), key_field.optional(),
                                            key_field.name(), key_field.field_id(),
                                            schema->children[0]->children[0]));
      NANOARROW_RETURN_NOT_OK(ToArrowSchema(*value_field.type(), value_field.optional(),
                                            value_field.name(), value_field.field_id(),
                                            schema->children[0]->children[1]));
    } break;
    case TypeId::kBoolean:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_BOOL));
      break;
    case TypeId::kInt:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_INT32));
      break;
    case TypeId::kLong:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_INT64));
      break;
    case TypeId::kFloat:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_FLOAT));
      break;
    case TypeId::kDouble:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_DOUBLE));
      break;
    case TypeId::kDecimal: {
      ArrowSchemaInit(schema);
      const auto& decimal_type = static_cast<const DecimalType&>(type);
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDecimal(schema, NANOARROW_TYPE_DECIMAL128,
                                                        decimal_type.precision(),
                                                        decimal_type.scale()));
    } break;
    case TypeId::kDate:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_DATE32));
      break;
    case TypeId::kTime: {
      ArrowSchemaInit(schema);
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIME64,
                                                         NANOARROW_TIME_UNIT_MICRO,
                                                         /*timezone=*/nullptr));
    } break;
    case TypeId::kTimestamp: {
      ArrowSchemaInit(schema);
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP,
                                                         NANOARROW_TIME_UNIT_MICRO,
                                                         /*timezone=*/nullptr));
    } break;
    case TypeId::kTimestampTz: {
      ArrowSchemaInit(schema);
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(
          schema, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MICRO, "UTC"));
    } break;
    case TypeId::kString:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_STRING));
      break;
    case TypeId::kBinary:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_BINARY));
      break;
    case TypeId::kFixed: {
      ArrowSchemaInit(schema);
      const auto& fixed_type = static_cast<const FixedType&>(type);
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeFixedSize(
          schema, NANOARROW_TYPE_FIXED_SIZE_BINARY, fixed_type.length()));
    } break;
    case TypeId::kUuid: {
      ArrowSchemaInit(schema);
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeFixedSize(
          schema, NANOARROW_TYPE_FIXED_SIZE_BINARY, /*fixed_size=*/16));
      NANOARROW_RETURN_NOT_OK(
          ArrowMetadataBuilderAppend(&metadata_buffer, ArrowCharView(kArrowExtensionName),
                                     ArrowCharView(kArrowUuidExtensionName)));
    } break;
  }

  if (!name.empty()) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema, std::string(name).c_str()));
  }

  NANOARROW_RETURN_NOT_OK(ArrowSchemaSetMetadata(
      schema, reinterpret_cast<const char*>(metadata_buffer.data)));
  ArrowBufferReset(&metadata_buffer);

  if (optional) {
    schema->flags |= ARROW_FLAG_NULLABLE;
  } else {
    schema->flags &= ~ARROW_FLAG_NULLABLE;
  }

  return NANOARROW_OK;
}

}  // namespace

expected<void, Error> ToArrowSchema(const Schema& schema, ArrowSchema* out) {
  if (out == nullptr) [[unlikely]] {
    return unexpected<Error>{{.kind = ErrorKind::kInvalidArgument,
                              .message = "Output Arrow schema cannot be null"}};
  }

  if (ArrowErrorCode errorCode = ToArrowSchema(schema, /*optional=*/false, /*name=*/"",
                                               /*field_id=*/std::nullopt, out);
      errorCode != NANOARROW_OK) {
    return unexpected<Error>{
        {.kind = ErrorKind::kInvalidSchema,
         .message = std::format(
             "Failed to convert Iceberg schema to Arrow schema, error code: {}",
             errorCode)}};
  }

  return {};
}

namespace {

int32_t GetFieldId(const ArrowSchema& schema) {
  if (schema.metadata == nullptr) {
    return kUnknownFieldId;
  }

  ArrowStringView field_id_key{.data = kFieldIdKey.data(),
                               .size_bytes = kFieldIdKey.size()};
  ArrowStringView field_id_value;
  if (ArrowMetadataGetValue(schema.metadata, field_id_key, &field_id_value) !=
      NANOARROW_OK) {
    return kUnknownFieldId;
  }

  return std::stoi(std::string(field_id_value.data, field_id_value.size_bytes));
}

expected<std::shared_ptr<Type>, Error> FromArrowSchema(const ArrowSchema& schema) {
  auto to_schema_field =
      [](const ArrowSchema& schema) -> expected<std::unique_ptr<SchemaField>, Error> {
    auto field_type_result = FromArrowSchema(schema);
    if (!field_type_result) {
      return unexpected<Error>(field_type_result.error());
    }

    auto field_id = GetFieldId(schema);
    bool is_optional = (schema.flags & ARROW_FLAG_NULLABLE) != 0;
    return std::make_unique<SchemaField>(
        field_id, schema.name, std::move(field_type_result.value()), is_optional);
  };

  ArrowError arrow_error;
  ArrowErrorInit(&arrow_error);

  ArrowSchemaView schema_view;
  if (auto error_code = ArrowSchemaViewInit(&schema_view, &schema, &arrow_error);
      error_code != NANOARROW_OK) {
    return unexpected<Error>{
        {.kind = ErrorKind::kInvalidSchema,
         .message = std::format("Failed to read Arrow schema, code: {}, message: {}",
                                error_code, arrow_error.message)}};
  }

  switch (schema_view.type) {
    case NANOARROW_TYPE_STRUCT: {
      std::vector<SchemaField> fields;
      fields.reserve(schema.n_children);

      for (int i = 0; i < schema.n_children; i++) {
        auto field_result = to_schema_field(*schema.children[i]);
        if (!field_result) {
          return unexpected<Error>(field_result.error());
        }
        fields.emplace_back(std::move(*field_result.value()));
      }

      return std::make_shared<StructType>(std::move(fields));
    }
    case NANOARROW_TYPE_LIST: {
      auto element_field_result = to_schema_field(*schema.children[0]);
      if (!element_field_result) {
        return unexpected<Error>(element_field_result.error());
      }
      return std::make_shared<ListType>(std::move(*element_field_result.value()));
    }
    case NANOARROW_TYPE_MAP: {
      auto key_field_result = to_schema_field(*schema.children[0]->children[0]);
      if (!key_field_result) {
        return unexpected<Error>(key_field_result.error());
      }

      auto value_field_result = to_schema_field(*schema.children[0]->children[1]);
      if (!value_field_result) {
        return unexpected<Error>(value_field_result.error());
      }

      return std::make_shared<MapType>(std::move(*key_field_result.value()),
                                       std::move(*value_field_result.value()));
    }
    case NANOARROW_TYPE_BOOL:
      return std::make_shared<BooleanType>();
    case NANOARROW_TYPE_INT32:
      return std::make_shared<IntType>();
    case NANOARROW_TYPE_INT64:
      return std::make_shared<LongType>();
    case NANOARROW_TYPE_FLOAT:
      return std::make_shared<FloatType>();
    case NANOARROW_TYPE_DOUBLE:
      return std::make_shared<DoubleType>();
    case NANOARROW_TYPE_DECIMAL128:
      return std::make_shared<DecimalType>(schema_view.decimal_precision,
                                           schema_view.decimal_scale);
    case NANOARROW_TYPE_DATE32:
      return std::make_shared<DateType>();
    case NANOARROW_TYPE_TIME64:
      if (schema_view.time_unit != NANOARROW_TIME_UNIT_MICRO) {
        return unexpected<Error>{
            {.kind = ErrorKind::kInvalidSchema,
             .message = std::format("Unsupported time unit for Arrow time type: {}",
                                    static_cast<int>(schema_view.time_unit))}};
      }
      return std::make_shared<TimeType>();
    case NANOARROW_TYPE_TIMESTAMP: {
      bool with_timezone =
          schema_view.timezone != nullptr && std::strlen(schema_view.timezone) > 0;
      if (schema_view.time_unit != NANOARROW_TIME_UNIT_MICRO) {
        return unexpected<Error>{
            {.kind = ErrorKind::kInvalidSchema,
             .message = std::format("Unsupported time unit for Arrow timestamp type: {}",
                                    static_cast<int>(schema_view.time_unit))}};
      }
      if (with_timezone) {
        return std::make_shared<TimestampTzType>();
      } else {
        return std::make_shared<TimestampType>();
      }
    }
    case NANOARROW_TYPE_STRING:
      return std::make_shared<StringType>();
    case NANOARROW_TYPE_BINARY:
      return std::make_shared<BinaryType>();
    case NANOARROW_TYPE_FIXED_SIZE_BINARY: {
      if (auto extension_name = std::string_view(schema_view.extension_name.data,
                                                 schema_view.extension_name.size_bytes);
          extension_name == kArrowUuidExtensionName) {
        if (schema_view.fixed_size != 16) {
          return unexpected<Error>{{.kind = ErrorKind::kInvalidSchema,
                                    .message = "UUID type must have a fixed size of 16"}};
        }
        return std::make_shared<UuidType>();
      }
      return std::make_shared<FixedType>(schema_view.fixed_size);
    }
    default:
      return unexpected<Error>{
          {.kind = ErrorKind::kInvalidSchema,
           .message = std::format("Unsupported Arrow type: {}",
                                  ArrowTypeString(schema_view.type))}};
  }
}

std::unique_ptr<Schema> FromStructType(StructType&& struct_type, int32_t schema_id) {
  std::vector<SchemaField> fields;
  fields.reserve(struct_type.fields().size());
  for (auto& field : struct_type.fields()) {
    fields.emplace_back(std::move(field));
  }
  return std::make_unique<Schema>(schema_id, std::move(fields));
}

}  // namespace

expected<std::unique_ptr<Schema>, Error> FromArrowSchema(const ArrowSchema& schema,
                                                         int32_t schema_id) {
  auto type_result = FromArrowSchema(schema);
  if (!type_result) {
    return unexpected<Error>(type_result.error());
  }

  auto& type = type_result.value();
  if (type->type_id() != TypeId::kStruct) {
    return unexpected<Error>{
        {.kind = ErrorKind::kInvalidSchema,
         .message = "Arrow schema must be a struct type for Iceberg schema"}};
  }

  auto& struct_type = static_cast<StructType&>(*type);
  return FromStructType(std::move(struct_type), schema_id);
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

#define ICEBERG_CHECK_JSON_FIELD(field_name, json)                             \
  if (!json.contains(field_name)) [[unlikely]] {                               \
    return unexpected<Error>({                                                 \
        .kind = ErrorKind::kJsonParseError,                                    \
        .message = std::format("Missing '{}' in {}", field_name, json.dump()), \
    });                                                                        \
  }

expected<std::unique_ptr<Type>, Error> StructTypeFromJson(const nlohmann::json& json) {
  ICEBERG_CHECK_JSON_FIELD(kFields, json);

  std::vector<SchemaField> fields;
  for (const auto& field_json : json[kFields]) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, FieldFromJson(field_json));
    fields.emplace_back(std::move(*field));
  }

  return std::make_unique<StructType>(std::move(fields));
}

expected<std::unique_ptr<Type>, Error> ListTypeFromJson(const nlohmann::json& json) {
  ICEBERG_CHECK_JSON_FIELD(kElement, json);
  ICEBERG_CHECK_JSON_FIELD(kElementId, json);
  ICEBERG_CHECK_JSON_FIELD(kElementRequired, json);

  ICEBERG_ASSIGN_OR_RAISE(auto element_type, TypeFromJson(json[kElement]));
  int32_t element_id = json[kElementId].get<int32_t>();
  bool element_required = json[kElementRequired].get<bool>();

  return std::make_unique<ListType>(
      SchemaField(element_id, std::string(ListType::kElementName),
                  std::move(element_type), !element_required));
}

expected<std::unique_ptr<Type>, Error> MapTypeFromJson(const nlohmann::json& json) {
  ICEBERG_CHECK_JSON_FIELD(kKey, json);
  ICEBERG_CHECK_JSON_FIELD(kValue, json);
  ICEBERG_CHECK_JSON_FIELD(kKeyId, json);
  ICEBERG_CHECK_JSON_FIELD(kValueId, json);
  ICEBERG_CHECK_JSON_FIELD(kValueRequired, json);

  ICEBERG_ASSIGN_OR_RAISE(auto key_type, TypeFromJson(json[kKey]));
  ICEBERG_ASSIGN_OR_RAISE(auto value_type, TypeFromJson(json[kValue]));
  int32_t key_id = json[kKeyId].get<int32_t>();
  int32_t value_id = json[kValueId].get<int32_t>();
  bool value_required = json[kValueRequired].get<bool>();

  SchemaField key_field(key_id, std::string(MapType::kKeyName), std::move(key_type),
                        /*optional=*/false);
  SchemaField value_field(value_id, std::string(MapType::kValueName),
                          std::move(value_type), !value_required);
  return std::make_unique<MapType>(std::move(key_field), std::move(value_field));
}

}  // namespace

expected<std::unique_ptr<Type>, Error> TypeFromJson(const nlohmann::json& json) {
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
  ICEBERG_CHECK_JSON_FIELD(kType, json);
  std::string type_str = json[kType].get<std::string>();
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

expected<std::unique_ptr<SchemaField>, Error> FieldFromJson(const nlohmann::json& json) {
  ICEBERG_CHECK_JSON_FIELD(kId, json);
  ICEBERG_CHECK_JSON_FIELD(kName, json);
  ICEBERG_CHECK_JSON_FIELD(kType, json);
  ICEBERG_CHECK_JSON_FIELD(kRequired, json);

  ICEBERG_ASSIGN_OR_RAISE(auto type, TypeFromJson(json[kType]));
  int32_t field_id = json[kId].get<int32_t>();
  std::string name = json[kName].get<std::string>();
  bool required = json[kRequired].get<bool>();

  return std::make_unique<SchemaField>(field_id, std::move(name), std::move(type),
                                       !required);
}

expected<std::unique_ptr<Schema>, Error> SchemaFromJson(const nlohmann::json& json) {
  ICEBERG_CHECK_JSON_FIELD(kType, json);
  ICEBERG_CHECK_JSON_FIELD(kSchemaId, json);

  ICEBERG_ASSIGN_OR_RAISE(auto type, TypeFromJson(json));
  if (type->type_id() != TypeId::kStruct) [[unlikely]] {
    return unexpected<Error>({
        .kind = ErrorKind::kJsonParseError,
        .message = std::format("Schema must be a struct type, but got {}", json.dump()),
    });
  }

  int32_t schema_id = json[kSchemaId].get<int32_t>();
  auto& struct_type = static_cast<StructType&>(*type);
  return FromStructType(std::move(struct_type), schema_id);
}

}  // namespace iceberg
