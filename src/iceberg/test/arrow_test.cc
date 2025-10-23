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

#include <memory>
#include <unordered_map>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/extension/uuid.h>
#include <arrow/result.h>
#include <arrow/type_fwd.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>

#include "iceberg/constants.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

struct ToArrowSchemaParam {
  std::shared_ptr<Type> iceberg_type;
  bool optional = true;
  std::shared_ptr<arrow::DataType> arrow_type;
};

class ToArrowSchemaTest : public ::testing::TestWithParam<ToArrowSchemaParam> {};

TEST_P(ToArrowSchemaTest, PrimitiveType) {
  constexpr std::string_view kFieldName = "foo";
  constexpr int32_t kFieldId = 1024;
  const auto& param = GetParam();
  Schema schema(
      {param.optional ? SchemaField::MakeOptional(kFieldId, std::string(kFieldName),
                                                  param.iceberg_type)
                      : SchemaField::MakeRequired(kFieldId, std::string(kFieldName),
                                                  param.iceberg_type)},
      /*schema_id=*/0);
  ArrowSchema arrow_schema;
  ASSERT_THAT(ToArrowSchema(schema, &arrow_schema), IsOk());

  auto imported_schema = ::arrow::ImportSchema(&arrow_schema).ValueOrDie();
  ASSERT_EQ(imported_schema->num_fields(), 1);

  auto field = imported_schema->field(0);
  ASSERT_EQ(field->name(), kFieldName);
  ASSERT_EQ(field->nullable(), param.optional);
  ASSERT_TRUE(field->type()->Equals(param.arrow_type));

  auto metadata = field->metadata();
  ASSERT_TRUE(metadata->Contains(kParquetFieldIdKey));
  ASSERT_EQ(metadata->Get(kParquetFieldIdKey), std::to_string(kFieldId));
}

INSTANTIATE_TEST_SUITE_P(
    SchemaConversion, ToArrowSchemaTest,
    ::testing::Values(
        ToArrowSchemaParam{.iceberg_type = iceberg::boolean(),
                           .optional = false,
                           .arrow_type = ::arrow::boolean()},
        ToArrowSchemaParam{.iceberg_type = iceberg::int32(),
                           .optional = true,
                           .arrow_type = ::arrow::int32()},
        ToArrowSchemaParam{.iceberg_type = iceberg::int64(),
                           .arrow_type = ::arrow::int64()},
        ToArrowSchemaParam{.iceberg_type = iceberg::float32(),
                           .arrow_type = ::arrow::float32()},
        ToArrowSchemaParam{.iceberg_type = iceberg::float64(),
                           .arrow_type = ::arrow::float64()},
        ToArrowSchemaParam{.iceberg_type = iceberg::decimal(10, 2),
                           .arrow_type = ::arrow::decimal128(10, 2)},
        ToArrowSchemaParam{.iceberg_type = iceberg::date(),
                           .arrow_type = ::arrow::date32()},
        ToArrowSchemaParam{.iceberg_type = iceberg::time(),
                           .arrow_type = ::arrow::time64(arrow::TimeUnit::MICRO)},
        ToArrowSchemaParam{.iceberg_type = iceberg::timestamp(),
                           .arrow_type = ::arrow::timestamp(arrow::TimeUnit::MICRO)},
        ToArrowSchemaParam{.iceberg_type = iceberg::timestamp(),
                           .arrow_type = ::arrow::timestamp(arrow::TimeUnit::MICRO)},
        ToArrowSchemaParam{.iceberg_type = iceberg::string(),
                           .arrow_type = ::arrow::utf8()},
        ToArrowSchemaParam{.iceberg_type = iceberg::binary(),
                           .arrow_type = ::arrow::binary()},
        ToArrowSchemaParam{.iceberg_type = iceberg::uuid(),
                           .arrow_type = ::arrow::extension::uuid()},
        ToArrowSchemaParam{.iceberg_type = iceberg::fixed(20),
                           .arrow_type = ::arrow::fixed_size_binary(20)}));

namespace {

void CheckArrowField(const ::arrow::Field& field, ::arrow::Type::type type_id,
                     std::string_view name, bool nullable, int32_t field_id) {
  ASSERT_EQ(field.name(), name);
  ASSERT_EQ(field.nullable(), nullable);
  ASSERT_EQ(field.type()->id(), type_id);

  auto metadata = field.metadata();
  ASSERT_TRUE(metadata != nullptr);
  ASSERT_TRUE(metadata->Contains(kParquetFieldIdKey));
  ASSERT_EQ(metadata->Get(kParquetFieldIdKey), std::to_string(field_id));
}

}  // namespace

TEST(ToArrowSchemaTest, StructType) {
  constexpr int32_t kStructFieldId = 1;
  constexpr int32_t kIntFieldId = 2;
  constexpr int32_t kStrFieldId = 3;

  constexpr std::string_view kStructFieldName = "struct_field";
  constexpr std::string_view kIntFieldName = "int_field";
  constexpr std::string_view kStrFieldName = "str_field";

  auto struct_type = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeRequired(kIntFieldId, std::string(kIntFieldName),
                                iceberg::int32()),
      SchemaField::MakeOptional(kStrFieldId, std::string(kStrFieldName),
                                iceberg::string())});
  Schema schema({SchemaField::MakeRequired(kStructFieldId, std::string(kStructFieldName),
                                           struct_type)},
                /*schema_id=*/0);

  ArrowSchema arrow_schema;
  ASSERT_THAT(ToArrowSchema(schema, &arrow_schema), IsOk());

  auto imported_schema = ::arrow::ImportSchema(&arrow_schema).ValueOrDie();
  ASSERT_EQ(imported_schema->num_fields(), 1);

  auto field = imported_schema->field(0);
  ASSERT_NO_FATAL_FAILURE(CheckArrowField(*field, ::arrow::Type::STRUCT, kStructFieldName,
                                          /*nullable=*/false, kStructFieldId));

  auto struct_field = std::static_pointer_cast<::arrow::StructType>(field->type());
  ASSERT_EQ(struct_field->num_fields(), 2);

  ASSERT_NO_FATAL_FAILURE(CheckArrowField(*struct_field->field(0), ::arrow::Type::INT32,
                                          kIntFieldName, /*nullable=*/false,
                                          kIntFieldId));
  ASSERT_NO_FATAL_FAILURE(CheckArrowField(*struct_field->field(1), ::arrow::Type::STRING,
                                          kStrFieldName, /*nullable=*/true, kStrFieldId));
}

TEST(ToArrowSchemaTest, ListType) {
  constexpr std::string_view kListFieldName = "list_field";
  constexpr std::string_view kElemFieldName = "element";
  constexpr int32_t kListFieldId = 1;
  constexpr int32_t kElemFieldId = 2;

  auto list_type = std::make_shared<ListType>(SchemaField::MakeOptional(
      kElemFieldId, std::string(kElemFieldName), iceberg::int64()));
  Schema schema(
      {SchemaField::MakeRequired(kListFieldId, std::string(kListFieldName), list_type)},
      /*schema_id=*/0);

  ArrowSchema arrow_schema;
  ASSERT_THAT(ToArrowSchema(schema, &arrow_schema), IsOk());

  auto imported_schema = ::arrow::ImportSchema(&arrow_schema).ValueOrDie();
  ASSERT_EQ(imported_schema->num_fields(), 1);

  auto field = imported_schema->field(0);
  ASSERT_NO_FATAL_FAILURE(CheckArrowField(*field, ::arrow::Type::LIST, kListFieldName,
                                          /*nullable=*/false, kListFieldId));

  auto list_field = std::static_pointer_cast<::arrow::ListType>(field->type());
  ASSERT_NO_FATAL_FAILURE(CheckArrowField(*list_field->value_field(),
                                          ::arrow::Type::INT64, kElemFieldName,
                                          /*nullable=*/true, kElemFieldId));
}

TEST(ToArrowSchemaTest, MapType) {
  constexpr std::string_view kMapFieldName = "map_field";
  constexpr std::string_view kKeyFieldName = "key";
  constexpr std::string_view kValueFieldName = "value";

  constexpr int32_t kFieldId = 1;
  constexpr int32_t kKeyFieldId = 2;
  constexpr int32_t kValueFieldId = 3;

  auto map_type = std::make_shared<MapType>(
      SchemaField::MakeRequired(kKeyFieldId, std::string(kKeyFieldName),
                                iceberg::string()),
      SchemaField::MakeOptional(kValueFieldId, std::string(kValueFieldName),
                                iceberg::int32()));

  Schema schema(
      {SchemaField::MakeRequired(kFieldId, std::string(kMapFieldName), map_type)},
      /*schema_id=*/0);

  ArrowSchema arrow_schema;
  ASSERT_THAT(ToArrowSchema(schema, &arrow_schema), IsOk());

  auto imported_schema = ::arrow::ImportSchema(&arrow_schema).ValueOrDie();
  ASSERT_EQ(imported_schema->num_fields(), 1);

  auto field = imported_schema->field(0);
  ASSERT_NO_FATAL_FAILURE(CheckArrowField(*field, ::arrow::Type::MAP, kMapFieldName,
                                          /*nullable=*/false, kFieldId));

  auto map_field = std::static_pointer_cast<::arrow::MapType>(field->type());

  auto key_field = map_field->key_field();
  ASSERT_NO_FATAL_FAILURE(CheckArrowField(*key_field, ::arrow::Type::STRING,
                                          kKeyFieldName,
                                          /*nullable=*/false, kKeyFieldId));

  auto value_field = map_field->item_field();
  ASSERT_NO_FATAL_FAILURE(CheckArrowField(*value_field, ::arrow::Type::INT32,
                                          kValueFieldName,
                                          /*nullable=*/true, kValueFieldId));
}

struct FromArrowSchemaParam {
  std::shared_ptr<arrow::DataType> arrow_type;
  bool optional = true;
  std::shared_ptr<Type> iceberg_type;
};

class FromArrowSchemaTest : public ::testing::TestWithParam<FromArrowSchemaParam> {};

TEST_P(FromArrowSchemaTest, PrimitiveType) {
  constexpr std::string_view kFieldName = "foo";
  constexpr int32_t kFieldId = 1024;
  const auto& param = GetParam();

  auto metadata =
      ::arrow::key_value_metadata(std::unordered_map<std::string, std::string>{
          {std::string(kParquetFieldIdKey), std::to_string(kFieldId)}});
  auto arrow_schema = ::arrow::schema({::arrow::field(
      std::string(kFieldName), param.arrow_type, param.optional, std::move(metadata))});
  ArrowSchema exported_schema;
  ASSERT_TRUE(::arrow::ExportSchema(*arrow_schema, &exported_schema).ok());

  auto type_result = FromArrowSchema(exported_schema, /*schema_id=*/1);
  ASSERT_THAT(type_result, IsOk());
  ArrowSchemaRelease(&exported_schema);

  const auto& schema = type_result.value();
  ASSERT_EQ(schema->schema_id(), 1);
  ASSERT_EQ(schema->fields().size(), 1);

  const auto& field = schema->fields()[0];
  ASSERT_EQ(field.name(), kFieldName);
  ASSERT_EQ(field.field_id(), kFieldId);
  ASSERT_EQ(field.optional(), param.optional);
  ASSERT_EQ(*field.type(), *param.iceberg_type);
}

INSTANTIATE_TEST_SUITE_P(
    SchemaConversion, FromArrowSchemaTest,
    ::testing::Values(
        FromArrowSchemaParam{.arrow_type = ::arrow::boolean(),
                             .optional = false,
                             .iceberg_type = iceberg::boolean()},
        FromArrowSchemaParam{.arrow_type = ::arrow::int32(),
                             .optional = true,
                             .iceberg_type = iceberg::int32()},
        FromArrowSchemaParam{.arrow_type = ::arrow::int64(),
                             .iceberg_type = iceberg::int64()},
        FromArrowSchemaParam{.arrow_type = ::arrow::float32(),
                             .iceberg_type = iceberg::float32()},
        FromArrowSchemaParam{.arrow_type = ::arrow::float64(),
                             .iceberg_type = iceberg::float64()},
        FromArrowSchemaParam{.arrow_type = ::arrow::decimal128(10, 2),
                             .iceberg_type = iceberg::decimal(10, 2)},
        FromArrowSchemaParam{.arrow_type = ::arrow::date32(),
                             .iceberg_type = iceberg::date()},
        FromArrowSchemaParam{.arrow_type = ::arrow::time64(arrow::TimeUnit::MICRO),
                             .iceberg_type = iceberg::time()},
        FromArrowSchemaParam{.arrow_type = ::arrow::timestamp(arrow::TimeUnit::MICRO),
                             .iceberg_type = iceberg::timestamp()},
        FromArrowSchemaParam{
            .arrow_type = ::arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
            .iceberg_type = std::make_shared<TimestampTzType>()},
        FromArrowSchemaParam{.arrow_type = ::arrow::utf8(),
                             .iceberg_type = iceberg::string()},
        FromArrowSchemaParam{.arrow_type = ::arrow::binary(),
                             .iceberg_type = iceberg::binary()},
        FromArrowSchemaParam{.arrow_type = ::arrow::extension::uuid(),
                             .iceberg_type = iceberg::uuid()},
        FromArrowSchemaParam{.arrow_type = ::arrow::fixed_size_binary(20),
                             .iceberg_type = iceberg::fixed(20)}));

TEST(FromArrowSchemaTest, StructType) {
  constexpr int32_t kStructFieldId = 1;
  constexpr int32_t kIntFieldId = 2;
  constexpr int32_t kStrFieldId = 3;

  constexpr std::string_view kStructFieldName = "struct_field";
  constexpr std::string_view kIntFieldName = "int_field";
  constexpr std::string_view kStrFieldName = "str_field";

  auto int_field = ::arrow::field(
      std::string(kIntFieldName), ::arrow::int32(), /*nullable=*/false,
      ::arrow::key_value_metadata(std::unordered_map<std::string, std::string>{
          {std::string(kParquetFieldIdKey), std::to_string(kIntFieldId)}}));
  auto str_field = ::arrow::field(
      std::string(kStrFieldName), ::arrow::utf8(), /*nullable=*/true,
      ::arrow::key_value_metadata(std::unordered_map<std::string, std::string>{
          {std::string(kParquetFieldIdKey), std::to_string(kStrFieldId)}}));
  auto struct_type = ::arrow::struct_({int_field, str_field});
  auto struct_field = ::arrow::field(
      std::string(kStructFieldName), struct_type, /*nullable=*/false,
      ::arrow::key_value_metadata(std::unordered_map<std::string, std::string>{
          {std::string(kParquetFieldIdKey), std::to_string(kStructFieldId)}}));
  auto arrow_schema = ::arrow::schema({struct_field});
  ArrowSchema exported_schema;
  ASSERT_TRUE(::arrow::ExportSchema(*arrow_schema, &exported_schema).ok());

  auto schema_result = FromArrowSchema(exported_schema, /*schema_id=*/0);
  ASSERT_THAT(schema_result, IsOk());
  ArrowSchemaRelease(&exported_schema);

  const auto& iceberg_schema = schema_result.value();
  ASSERT_EQ(iceberg_schema->schema_id(), 0);
  ASSERT_EQ(iceberg_schema->fields().size(), 1);

  const auto& field = iceberg_schema->fields()[0];
  ASSERT_EQ(field.name(), kStructFieldName);
  ASSERT_EQ(field.field_id(), kStructFieldId);
  ASSERT_FALSE(field.optional());
  ASSERT_EQ(field.type()->type_id(), TypeId::kStruct);

  auto* struct_field_type = dynamic_cast<const StructType*>(field.type().get());
  ASSERT_NE(struct_field_type, nullptr);
  ASSERT_EQ(struct_field_type->fields().size(), 2);

  const auto& int_iceberg_field = struct_field_type->fields()[0];
  ASSERT_EQ(int_iceberg_field.name(), kIntFieldName);
  ASSERT_EQ(int_iceberg_field.field_id(), kIntFieldId);
  ASSERT_FALSE(int_iceberg_field.optional());
  ASSERT_EQ(int_iceberg_field.type()->type_id(), TypeId::kInt);

  const auto& str_iceberg_field = struct_field_type->fields()[1];
  ASSERT_EQ(str_iceberg_field.name(), kStrFieldName);
  ASSERT_EQ(str_iceberg_field.field_id(), kStrFieldId);
  ASSERT_TRUE(str_iceberg_field.optional());
  ASSERT_EQ(str_iceberg_field.type()->type_id(), TypeId::kString);
}

TEST(FromArrowSchemaTest, ListType) {
  constexpr std::string_view kListFieldName = "list_field";
  constexpr std::string_view kElemFieldName = "element";
  constexpr int32_t kListFieldId = 1;
  constexpr int32_t kElemFieldId = 2;

  auto element_field = ::arrow::field(
      std::string(kElemFieldName), ::arrow::int64(), /*nullable=*/true,
      ::arrow::key_value_metadata(std::unordered_map<std::string, std::string>{
          {std::string(kParquetFieldIdKey), std::to_string(kElemFieldId)}}));
  auto list_type = ::arrow::list(element_field);
  auto list_field = ::arrow::field(
      std::string(kListFieldName), list_type, /*nullable=*/false,
      ::arrow::key_value_metadata(std::unordered_map<std::string, std::string>{
          {std::string(kParquetFieldIdKey), std::to_string(kListFieldId)}}));
  auto arrow_schema = ::arrow::schema({list_field});

  ArrowSchema exported_schema;
  ASSERT_TRUE(::arrow::ExportSchema(*arrow_schema, &exported_schema).ok());

  auto schema_result = FromArrowSchema(exported_schema, /*schema_id=*/0);
  ASSERT_THAT(schema_result, IsOk());
  ArrowSchemaRelease(&exported_schema);

  const auto& iceberg_schema = schema_result.value();
  ASSERT_EQ(iceberg_schema->schema_id(), 0);
  ASSERT_EQ(iceberg_schema->fields().size(), 1);

  const auto& field = iceberg_schema->fields()[0];
  ASSERT_EQ(field.name(), kListFieldName);
  ASSERT_EQ(field.field_id(), kListFieldId);
  ASSERT_FALSE(field.optional());
  ASSERT_EQ(field.type()->type_id(), TypeId::kList);

  auto* list_field_type = dynamic_cast<const ListType*>(field.type().get());
  ASSERT_NE(list_field_type, nullptr);

  const auto& element = list_field_type->fields()[0];
  ASSERT_EQ(element.name(), kElemFieldName);
  ASSERT_EQ(element.field_id(), kElemFieldId);
  ASSERT_TRUE(element.optional());
  ASSERT_EQ(element.type()->type_id(), TypeId::kLong);
}

TEST(FromArrowSchemaTest, MapType) {
  constexpr std::string_view kMapFieldName = "map_field";
  constexpr std::string_view kKeyFieldName = "key";
  constexpr std::string_view kValueFieldName = "value";

  constexpr int32_t kFieldId = 1;
  constexpr int32_t kKeyFieldId = 2;
  constexpr int32_t kValueFieldId = 3;

  auto key_field = ::arrow::field(
      std::string(kKeyFieldName), ::arrow::utf8(), /*nullable=*/false,
      ::arrow::key_value_metadata(std::unordered_map<std::string, std::string>{
          {std::string(kParquetFieldIdKey), std::to_string(kKeyFieldId)}}));
  auto value_field = ::arrow::field(
      std::string(kValueFieldName), ::arrow::int32(), /*nullable=*/true,
      ::arrow::key_value_metadata(std::unordered_map<std::string, std::string>{
          {std::string(kParquetFieldIdKey), std::to_string(kValueFieldId)}}));
  auto map_type = std::make_shared<::arrow::MapType>(key_field, value_field);
  auto map_field = ::arrow::field(
      std::string(kMapFieldName), map_type, /*nullable=*/true,
      ::arrow::key_value_metadata(std::unordered_map<std::string, std::string>{
          {std::string(kParquetFieldIdKey), std::to_string(kFieldId)}}));
  auto arrow_schema = ::arrow::schema({map_field});

  ArrowSchema exported_schema;
  ASSERT_TRUE(::arrow::ExportSchema(*arrow_schema, &exported_schema).ok());

  auto schema_result = FromArrowSchema(exported_schema, /*schema_id=*/0);
  ASSERT_THAT(schema_result, IsOk());
  ArrowSchemaRelease(&exported_schema);

  const auto& iceberg_schema = schema_result.value();
  ASSERT_EQ(iceberg_schema->schema_id(), 0);
  ASSERT_EQ(iceberg_schema->fields().size(), 1);

  const auto& field = iceberg_schema->fields()[0];
  ASSERT_EQ(field.name(), kMapFieldName);
  ASSERT_EQ(field.field_id(), kFieldId);
  ASSERT_TRUE(field.optional());
  ASSERT_EQ(field.type()->type_id(), TypeId::kMap);

  auto* map_field_type = dynamic_cast<const MapType*>(field.type().get());
  ASSERT_NE(map_field_type, nullptr);

  const auto& key = map_field_type->key();
  ASSERT_EQ(key.name(), kKeyFieldName);
  ASSERT_EQ(key.field_id(), kKeyFieldId);
  ASSERT_FALSE(key.optional());
  ASSERT_EQ(key.type()->type_id(), TypeId::kString);

  const auto& value = map_field_type->value();
  ASSERT_EQ(value.name(), kValueFieldName);
  ASSERT_EQ(value.field_id(), kValueFieldId);
  ASSERT_TRUE(value.optional());
  ASSERT_EQ(value.type()->type_id(), TypeId::kInt);
}

}  // namespace iceberg
