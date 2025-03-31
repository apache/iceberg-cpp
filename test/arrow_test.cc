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

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/extension/uuid.h>
#include <arrow/result.h>
#include <arrow/type_fwd.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>

#include "iceberg/arrow_c_data_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "matchers.h"

namespace iceberg {

TEST(ArrowCDataTest, CheckArrowSchemaAndArrayByNanoarrow) {
  auto [schema, array] = internal::CreateExampleArrowSchemaAndArrayByNanoarrow();

  auto arrow_schema = ::arrow::ImportSchema(&schema).ValueOrDie();
  EXPECT_EQ(arrow_schema->num_fields(), 2);

  auto id_field = arrow_schema->field(0);
  EXPECT_EQ(id_field->name(), "id");
  EXPECT_EQ(id_field->type()->id(), ::arrow::Type::INT64);
  EXPECT_FALSE(id_field->nullable());

  auto name_field = arrow_schema->field(1);
  EXPECT_EQ(name_field->name(), "name");
  EXPECT_EQ(name_field->type()->id(), ::arrow::Type::STRING);
  EXPECT_TRUE(name_field->nullable());

  auto arrow_record_batch = ::arrow::ImportRecordBatch(&array, arrow_schema).ValueOrDie();
  EXPECT_EQ(arrow_record_batch->num_rows(), 3);
  EXPECT_EQ(arrow_record_batch->num_columns(), 2);

  auto id_column = arrow_record_batch->column(0);
  EXPECT_EQ(id_column->type()->id(), ::arrow::Type::INT64);
  EXPECT_EQ(id_column->GetScalar(0).ValueOrDie()->ToString(), "1");
  EXPECT_EQ(id_column->GetScalar(1).ValueOrDie()->ToString(), "2");
  EXPECT_EQ(id_column->GetScalar(2).ValueOrDie()->ToString(), "3");

  auto name_column = arrow_record_batch->column(1);
  EXPECT_EQ(name_column->type()->id(), ::arrow::Type::STRING);
  EXPECT_EQ(name_column->GetScalar(0).ValueOrDie()->ToString(), "a");
  EXPECT_EQ(name_column->GetScalar(1).ValueOrDie()->ToString(), "b");
  EXPECT_EQ(name_column->GetScalar(2).ValueOrDie()->ToString(), "c");
}

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
      /*schema_id=*/0,
      {param.optional ? SchemaField::MakeOptional(kFieldId, std::string(kFieldName),
                                                  param.iceberg_type)
                      : SchemaField::MakeRequired(kFieldId, std::string(kFieldName),
                                                  param.iceberg_type)});
  ArrowSchema arrow_schema;
  ASSERT_THAT(ToArrowSchema(schema, &arrow_schema), IsOk());

  auto imported_schema = ::arrow::ImportSchema(&arrow_schema).ValueOrDie();
  ASSERT_EQ(imported_schema->num_fields(), 1);

  auto field = imported_schema->field(0);
  ASSERT_EQ(field->name(), kFieldName);
  ASSERT_EQ(field->nullable(), param.optional);
  ASSERT_TRUE(field->type()->Equals(param.arrow_type));

  auto metadata = field->metadata();
  ASSERT_TRUE(metadata->Contains(kFieldIdKey));
  ASSERT_EQ(metadata->Get(kFieldIdKey), std::to_string(kFieldId));
}

INSTANTIATE_TEST_SUITE_P(
    SchemaConversion, ToArrowSchemaTest,
    ::testing::Values(
        ToArrowSchemaParam{.iceberg_type = std::make_shared<BooleanType>(),
                           .optional = false,
                           .arrow_type = ::arrow::boolean()},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<IntType>(),
                           .optional = true,
                           .arrow_type = ::arrow::int32()},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<LongType>(),
                           .arrow_type = ::arrow::int64()},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<FloatType>(),
                           .arrow_type = ::arrow::float32()},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<DoubleType>(),
                           .arrow_type = ::arrow::float64()},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<DecimalType>(10, 2),
                           .arrow_type = ::arrow::decimal128(10, 2)},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<DateType>(),
                           .arrow_type = ::arrow::date32()},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<TimeType>(),
                           .arrow_type = ::arrow::time64(arrow::TimeUnit::MICRO)},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<TimestampType>(),
                           .arrow_type = ::arrow::timestamp(arrow::TimeUnit::MICRO)},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<TimestampType>(),
                           .arrow_type = ::arrow::timestamp(arrow::TimeUnit::MICRO)},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<StringType>(),
                           .arrow_type = ::arrow::utf8()},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<BinaryType>(),
                           .arrow_type = ::arrow::binary()},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<UuidType>(),
                           .arrow_type = ::arrow::extension::uuid()},
        ToArrowSchemaParam{.iceberg_type = std::make_shared<FixedType>(20),
                           .arrow_type = ::arrow::fixed_size_binary(20)}));

namespace {

void CheckArrowField(const ::arrow::Field& field, ::arrow::Type::type type_id,
                     std::string_view name, bool nullable, int32_t field_id) {
  ASSERT_EQ(field.name(), name);
  ASSERT_EQ(field.nullable(), nullable);
  ASSERT_EQ(field.type()->id(), type_id);

  auto metadata = field.metadata();
  ASSERT_TRUE(metadata != nullptr);
  ASSERT_TRUE(metadata->Contains(kFieldIdKey));
  ASSERT_EQ(metadata->Get(kFieldIdKey), std::to_string(field_id));
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
                                std::make_shared<IntType>()),
      SchemaField::MakeOptional(kStrFieldId, std::string(kStrFieldName),
                                std::make_shared<StringType>())});
  Schema schema(
      /*schema_id=*/0, {SchemaField::MakeRequired(
                           kStructFieldId, std::string(kStructFieldName), struct_type)});

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
      kElemFieldId, std::string(kElemFieldName), std::make_shared<LongType>()));
  Schema schema(
      /*schema_id=*/0,
      {SchemaField::MakeRequired(kListFieldId, std::string(kListFieldName), list_type)});

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
                                std::make_shared<StringType>()),
      SchemaField::MakeOptional(kValueFieldId, std::string(kValueFieldName),
                                std::make_shared<IntType>()));

  Schema schema(
      /*schema_id=*/0,
      {SchemaField::MakeRequired(kFieldId, std::string(kMapFieldName), map_type)});

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

}  // namespace iceberg
