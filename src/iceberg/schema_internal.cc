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

#include <charconv>
#include <cstring>
#include <optional>
#include <string>

#include "iceberg/constants.h"
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

// Convert an Iceberg type to Arrow schema. Return value is Nanoarrow error code.
ArrowErrorCode ToArrowSchema(const Type& type, bool optional, std::string_view name,
                             std::optional<int32_t> field_id, ArrowSchema* schema) {
  ArrowBuffer metadata_buffer;
  NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderInit(&metadata_buffer, nullptr));
  if (field_id.has_value()) {
    NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderAppend(
        &metadata_buffer, ArrowCharView(std::string(kParquetFieldIdKey).c_str()),
        ArrowCharView(std::to_string(field_id.value()).c_str())));
  }

  switch (type.type_id()) {
    case TypeId::kStruct: {
      const auto& struct_type = static_cast<const StructType&>(type);
      const auto& fields = struct_type.fields();
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(schema, fields.size()));

      for (size_t i = 0; i < fields.size(); i++) {
        const auto& field = fields[i];
        NANOARROW_RETURN_NOT_OK(ToArrowSchema(*field.type(), field.optional(),
                                              field.name(), field.field_id(),
                                              schema->children[i]));
      }
    } break;
    case TypeId::kList: {
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_LIST));

      const auto& list_type = static_cast<const ListType&>(type);
      const auto& elem_field = list_type.fields()[0];
      NANOARROW_RETURN_NOT_OK(ToArrowSchema(*elem_field.type(), elem_field.optional(),
                                            elem_field.name(), elem_field.field_id(),
                                            schema->children[0]));
    } break;
    case TypeId::kMap: {
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_MAP));

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
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BOOL));
      break;
    case TypeId::kInt:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT32));
      break;
    case TypeId::kLong:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT64));
      break;
    case TypeId::kFloat:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_FLOAT));
      break;
    case TypeId::kDouble:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_DOUBLE));
      break;
    case TypeId::kDecimal: {
      const auto& decimal_type = static_cast<const DecimalType&>(type);
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDecimal(schema, NANOARROW_TYPE_DECIMAL128,
                                                        decimal_type.precision(),
                                                        decimal_type.scale()));
    } break;
    case TypeId::kDate:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_DATE32));
      break;
    case TypeId::kTime: {
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIME64,
                                                         NANOARROW_TIME_UNIT_MICRO,
                                                         /*timezone=*/nullptr));
    } break;
    case TypeId::kTimestamp: {
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP,
                                                         NANOARROW_TIME_UNIT_MICRO,
                                                         /*timezone=*/nullptr));
    } break;
    case TypeId::kTimestampTz: {
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(
          schema, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MICRO, "UTC"));
    } break;
    case TypeId::kString:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING));
      break;
    case TypeId::kBinary:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
      break;
    case TypeId::kFixed: {
      const auto& fixed_type = static_cast<const FixedType&>(type);
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeFixedSize(
          schema, NANOARROW_TYPE_FIXED_SIZE_BINARY, fixed_type.length()));
    } break;
    case TypeId::kUuid: {
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

Status ToArrowSchema(const Schema& schema, ArrowSchema* out) {
  if (out == nullptr) [[unlikely]] {
    return InvalidArgument("Output Arrow schema cannot be null");
  }

  ArrowSchemaInit(out);

  if (ArrowErrorCode errorCode = ToArrowSchema(schema, /*optional=*/false, /*name=*/"",
                                               /*field_id=*/std::nullopt, out);
      errorCode != NANOARROW_OK) {
    return InvalidSchema(
        "Failed to convert Iceberg schema to Arrow schema, error code: {}", errorCode);
  }

  return {};
}

namespace {

int32_t GetFieldId(const ArrowSchema& schema) {
  if (schema.metadata == nullptr) {
    return kUnknownFieldId;
  }

  ArrowStringView field_id_key{.data = kParquetFieldIdKey.data(),
                               .size_bytes = kParquetFieldIdKey.size()};
  ArrowStringView field_id_value;
  if (ArrowMetadataGetValue(schema.metadata, field_id_key, &field_id_value) !=
      NANOARROW_OK) {
    return kUnknownFieldId;
  }

  int32_t field_id = kUnknownFieldId;
  std::from_chars(field_id_value.data, field_id_value.data + field_id_value.size_bytes,
                  field_id);

  return field_id;
}

Result<std::shared_ptr<Type>> FromArrowSchema(const ArrowSchema& schema) {
  auto to_schema_field =
      [](const ArrowSchema& schema) -> Result<std::unique_ptr<SchemaField>> {
    ICEBERG_ASSIGN_OR_RAISE(auto field_type, FromArrowSchema(schema));

    auto field_id = GetFieldId(schema);
    bool is_optional = (schema.flags & ARROW_FLAG_NULLABLE) != 0;
    return std::make_unique<SchemaField>(field_id, schema.name, std::move(field_type),
                                         is_optional);
  };

  ArrowError arrow_error;
  ArrowErrorInit(&arrow_error);

  ArrowSchemaView schema_view;
  if (auto error_code = ArrowSchemaViewInit(&schema_view, &schema, &arrow_error);
      error_code != NANOARROW_OK) {
    return InvalidSchema("Failed to read Arrow schema, code: {}, message: {}", error_code,
                         arrow_error.message);
  }

  switch (schema_view.type) {
    case NANOARROW_TYPE_STRUCT: {
      std::vector<SchemaField> fields;
      fields.reserve(schema.n_children);

      for (int i = 0; i < schema.n_children; i++) {
        ICEBERG_ASSIGN_OR_RAISE(auto field, to_schema_field(*schema.children[i]));
        fields.emplace_back(std::move(*field));
      }

      return std::make_shared<StructType>(std::move(fields));
    }
    case NANOARROW_TYPE_LIST: {
      ICEBERG_ASSIGN_OR_RAISE(auto element_field_result,
                              to_schema_field(*schema.children[0]));
      return std::make_shared<ListType>(std::move(*element_field_result));
    }
    case NANOARROW_TYPE_MAP: {
      ICEBERG_ASSIGN_OR_RAISE(auto key_field,
                              to_schema_field(*schema.children[0]->children[0]));
      ICEBERG_ASSIGN_OR_RAISE(auto value_field,
                              to_schema_field(*schema.children[0]->children[1]));

      return std::make_shared<MapType>(std::move(*key_field), std::move(*value_field));
    }
    case NANOARROW_TYPE_BOOL:
      return iceberg::boolean();
    case NANOARROW_TYPE_INT32:
      return iceberg::int32();
    case NANOARROW_TYPE_INT64:
      return iceberg::int64();
    case NANOARROW_TYPE_FLOAT:
      return iceberg::float32();
    case NANOARROW_TYPE_DOUBLE:
      return iceberg::float64();
    case NANOARROW_TYPE_DECIMAL128:
      return iceberg::decimal(schema_view.decimal_precision, schema_view.decimal_scale);
    case NANOARROW_TYPE_DATE32:
      return iceberg::date();
    case NANOARROW_TYPE_TIME64:
      if (schema_view.time_unit != NANOARROW_TIME_UNIT_MICRO) {
        return InvalidSchema("Unsupported time unit for Arrow time type: {}",
                             static_cast<int>(schema_view.time_unit));
      }
      return iceberg::time();
    case NANOARROW_TYPE_TIMESTAMP: {
      bool with_timezone =
          schema_view.timezone != nullptr && std::strlen(schema_view.timezone) > 0;
      if (schema_view.time_unit != NANOARROW_TIME_UNIT_MICRO) {
        return InvalidSchema("Unsupported time unit for Arrow timestamp type: {}",
                             static_cast<int>(schema_view.time_unit));
      }
      if (with_timezone) {
        return iceberg::timestamp_tz();
      } else {
        return iceberg::timestamp();
      }
    }
    case NANOARROW_TYPE_STRING:
      return iceberg::string();
    case NANOARROW_TYPE_BINARY:
      return iceberg::binary();
    case NANOARROW_TYPE_FIXED_SIZE_BINARY: {
      if (auto extension_name = std::string_view(schema_view.extension_name.data,
                                                 schema_view.extension_name.size_bytes);
          extension_name == kArrowUuidExtensionName) {
        if (schema_view.fixed_size != 16) {
          return InvalidSchema("UUID type must have a fixed size of 16");
        }
        return iceberg::uuid();
      }
      return iceberg::fixed(schema_view.fixed_size);
    }
    default:
      return InvalidSchema("Unsupported Arrow type: {}",
                           ArrowTypeString(schema_view.type));
  }
}

}  // namespace

std::unique_ptr<Schema> FromStructType(StructType&& struct_type,
                                       std::optional<int32_t> schema_id_opt) {
  std::vector<SchemaField> fields;
  fields.reserve(struct_type.fields().size());
  for (auto& field : struct_type.fields()) {
    fields.emplace_back(std::move(field));
  }
  auto schema_id = schema_id_opt.value_or(Schema::kInitialSchemaId);
  return std::make_unique<Schema>(std::move(fields), schema_id);
}

Result<std::unique_ptr<Schema>> FromArrowSchema(const ArrowSchema& schema,
                                                std::optional<int32_t> schema_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto type, FromArrowSchema(schema));

  if (type->type_id() != TypeId::kStruct) {
    return InvalidSchema("Arrow schema must be a struct type for Iceberg schema");
  }

  auto& struct_type = static_cast<StructType&>(*type);
  return FromStructType(std::move(struct_type), schema_id);
}

std::unique_ptr<StructType> ToStructType(const Schema& schema) {
  std::vector<SchemaField> fields(schema.fields().begin(), schema.fields().end());
  return std::make_unique<StructType>(std::move(fields));
}

}  // namespace iceberg
