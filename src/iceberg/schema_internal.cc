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

#include <format>
#include <optional>
#include <string>

#include "iceberg/expected.h"
#include "iceberg/schema.h"

namespace iceberg {

namespace {

constexpr const char* kArrowExtensionName = "ARROW:extension:name";
constexpr const char* kArrowExtensionMetadata = "ARROW:extension:metadata";

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
                                     ArrowCharView("arrow.uuid")));
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

expected<std::unique_ptr<Schema>, Error> FromArrowSchema(const ArrowSchema& schema,
                                                         int32_t schema_id) {
  // TODO(wgtmac): Implement this
  return unexpected<Error>{
      {.kind = ErrorKind::kInvalidSchema, .message = "Not implemented yet"}};
}

}  // namespace iceberg
