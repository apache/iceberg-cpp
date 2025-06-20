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

#include <ranges>

#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <avro/Compiler.hh>
#include <avro/Generic.hh>
#include <avro/Node.hh>
#include <avro/Types.hh>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iceberg/arrow/arrow_error_transform_internal.h>
#include <iceberg/avro/avro_data_util_internal.h>
#include <iceberg/avro/avro_schema_util_internal.h>
#include <iceberg/schema.h>
#include <iceberg/schema_internal.h>
#include <iceberg/schema_util.h>
#include <iceberg/type.h>
#include <iceberg/util/macros.h>

#include "matchers.h"

namespace iceberg::avro {

/// \brief Test case structure for parameterized primitive type tests
struct AppendDatumParam {
  std::string name;
  std::shared_ptr<Type> projected_type;
  std::shared_ptr<Type> source_type;
  std::function<void(::avro::GenericDatum&, int)> value_setter;
  std::string expected_json;
};

/// \brief Helper function to create test data for a primitive type
std::vector<::avro::GenericDatum> CreateTestData(
    const ::avro::NodePtr& avro_node,
    const std::function<void(::avro::GenericDatum&, int)>& value_setter, int count = 3) {
  std::vector<::avro::GenericDatum> avro_data;
  for (int i = 0; i < count; ++i) {
    ::avro::GenericDatum avro_datum(avro_node);
    value_setter(avro_datum, i);
    avro_data.push_back(avro_datum);
  }
  return avro_data;
}

/// \brief Utility function to verify AppendDatumToBuilder behavior
void VerifyAppendDatumToBuilder(const Schema& projected_schema,
                                const ::avro::NodePtr& avro_node,
                                const std::vector<::avro::GenericDatum>& avro_data,
                                std::string_view expected_array_json) {
  // Create 1 to 1 projection
  auto projection_result = Project(projected_schema, avro_node, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());
  auto projection = std::move(projection_result.value());

  // Create arrow schema and array builder
  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(projected_schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&arrow_c_schema).ValueOrDie();
  auto arrow_struct_type = std::make_shared<::arrow::StructType>(arrow_schema->fields());
  auto builder = ::arrow::MakeBuilder(arrow_struct_type).ValueOrDie();

  // Call AppendDatumToBuilder repeatedly to append the datum
  for (const auto& avro_datum : avro_data) {
    ASSERT_THAT(AppendDatumToBuilder(avro_node, avro_datum, projection, projected_schema,
                                     builder.get()),
                IsOk());
  }

  // Verify the result
  auto array = builder->Finish().ValueOrDie();
  auto expected_array =
      ::arrow::json::ArrayFromJSONString(arrow_struct_type, expected_array_json)
          .ValueOrDie();
  ASSERT_TRUE(array->Equals(*expected_array));
}

/// \brief Test class for primitive types using parameterized tests
class AppendDatumToBuilderTest : public ::testing::TestWithParam<AppendDatumParam> {};

TEST_P(AppendDatumToBuilderTest, PrimitiveType) {
  const auto& test_case = GetParam();

  Schema projected_schema({SchemaField::MakeRequired(
      /*field_id=*/1, /*name=*/"a", test_case.projected_type)});
  Schema source_schema({SchemaField::MakeRequired(
      /*field_id=*/1, /*name=*/"a", test_case.source_type)});

  ::avro::NodePtr avro_node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(source_schema, &avro_node), IsOk());

  auto avro_data = CreateTestData(avro_node, test_case.value_setter);
  ASSERT_NO_FATAL_FAILURE(VerifyAppendDatumToBuilder(projected_schema, avro_node,
                                                     avro_data, test_case.expected_json));
}

// Define test cases for all primitive types
const std::vector<AppendDatumParam> kPrimitiveTestCases = {
    {
        .name = "Boolean",
        .projected_type = std::make_shared<BooleanType>(),
        .source_type = std::make_shared<BooleanType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<bool>() =
                  (i % 2 == 0);
            },
        .expected_json = R"([{"a": true}, {"a": false}, {"a": true}])",
    },
    {
        .name = "Int",
        .projected_type = std::make_shared<IntType>(),
        .source_type = std::make_shared<IntType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<int32_t>() = i * 100;
            },
        .expected_json = R"([{"a": 0}, {"a": 100}, {"a": 200}])",
    },
    {
        .name = "Long",
        .projected_type = std::make_shared<LongType>(),
        .source_type = std::make_shared<LongType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<int64_t>() =
                  i * 1000000LL;
            },
        .expected_json = R"([{"a": 0}, {"a": 1000000}, {"a": 2000000}])",
    },
    {
        .name = "Float",
        .projected_type = std::make_shared<FloatType>(),
        .source_type = std::make_shared<FloatType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<float>() = i * 3.14f;
            },
        .expected_json = R"([{"a": 0.0}, {"a": 3.14}, {"a": 6.28}])",
    },
    {
        .name = "Double",
        .projected_type = std::make_shared<DoubleType>(),
        .source_type = std::make_shared<DoubleType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<double>() =
                  i * 1.234567890;
            },
        .expected_json = R"([{"a": 0.0}, {"a": 1.234567890}, {"a": 2.469135780}])",
    },
    {
        .name = "String",
        .projected_type = std::make_shared<StringType>(),
        .source_type = std::make_shared<StringType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<std::string>() =
                  "test_string_" + std::to_string(i);
            },
        .expected_json =
            R"([{"a": "test_string_0"}, {"a": "test_string_1"}, {"a": "test_string_2"}])",
    },
    {
        .name = "Binary",
        .projected_type = std::make_shared<BinaryType>(),
        .source_type = std::make_shared<BinaryType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>()
                  .fieldAt(0)
                  .value<std::vector<uint8_t>>() = {static_cast<uint8_t>('a' + i),
                                                    static_cast<uint8_t>('b' + i),
                                                    static_cast<uint8_t>('c' + i)};
            },
        .expected_json = R"([{"a": "abc"}, {"a": "bcd"}, {"a": "cde"}])",
    },
    {
        .name = "Fixed",
        .projected_type = std::make_shared<FixedType>(4),
        .source_type = std::make_shared<FixedType>(4),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>()
                  .fieldAt(0)
                  .value<::avro::GenericFixed>()
                  .value() = {
                  static_cast<uint8_t>('a' + i), static_cast<uint8_t>('b' + i),
                  static_cast<uint8_t>('c' + i), static_cast<uint8_t>('d' + i)};
            },
        .expected_json = R"([{"a": "abcd"}, {"a": "bcde"}, {"a": "cdef"}])",
    },
    /// FIXME: NotImplemented: MakeBuilder: cannot construct builder for type
    /// extension<arrow.uuid>
    // {
    //     .name = "UUID",
    //     .projected_type = std::make_shared<UuidType>(),
    //     .source_type = std::make_shared<UuidType>(),
    //     .value_setter =
    //         [](::avro::GenericDatum& datum, int i) {
    //           datum.value<::avro::GenericRecord>()
    //               .fieldAt(0)
    //               .value<::avro::GenericFixed>()
    //               .value() = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
    //                           'i', 'j', 'k', 'l', 'm', 'n', 'o',
    //                           static_cast<uint8_t>(i)};
    //         },
    //     .expected_json = R"([{"a": "abcdefghijklmnop"}, {"a": "bcdefghijklmnopq"},
    //     {"a": "cdefghijklmnopqr"}])",
    // },
    {
        .name = "Decimal",
        .projected_type = std::make_shared<DecimalType>(10, 2),
        .source_type = std::make_shared<DecimalType>(10, 2),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              int32_t decimal_value = i * 1000 + i;
              std::vector<uint8_t>& fixed = datum.value<::avro::GenericRecord>()
                                                .fieldAt(0)
                                                .value<::avro::GenericFixed>()
                                                .value();
              // The byte array must contain the two's-complement representation of
              // the unscaled integer value in big-endian byte order.
              for (uint8_t& rvalue : std::ranges::reverse_view(fixed)) {
                rvalue = static_cast<uint8_t>(decimal_value & 0xFF);
                decimal_value >>= 8;
              }
            },
        .expected_json = R"([{"a": "0.00"}, {"a": "10.01"}, {"a": "20.02"}])",
    },
    {
        .name = "Date",
        .projected_type = std::make_shared<DateType>(),
        .source_type = std::make_shared<DateType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              // Date as days since epoch (1970-01-01)
              // 0 = 1970-01-01, 1 = 1970-01-02, etc.
              datum.value<::avro::GenericRecord>().fieldAt(0).value<int32_t>() =
                  18000 + i;  // ~2019-04-11 + i days
            },
        .expected_json = R"([{"a": 18000}, {"a": 18001}, {"a": 18002}])",
    },
    {
        .name = "Time",
        .projected_type = std::make_shared<TimeType>(),
        .source_type = std::make_shared<TimeType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              // Time as microseconds since midnight
              // 12:30:45.123456 + i seconds = 45045123456 + i*1000000 microseconds
              datum.value<::avro::GenericRecord>().fieldAt(0).value<int64_t>() =
                  45045123456LL + i * 1000000LL;
            },
        .expected_json =
            R"([{"a": 45045123456}, {"a": 45046123456}, {"a": 45047123456}])",
    },
    {
        .name = "Timestamp",
        .projected_type = std::make_shared<TimestampType>(),
        .source_type = std::make_shared<TimestampType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<int64_t>() =
                  i * 1000000LL;
            },
        .expected_json = R"([{"a": 0}, {"a": 1000000}, {"a": 2000000}])",
    },
    {
        .name = "TimestampTz",
        .projected_type = std::make_shared<TimestampTzType>(),
        .source_type = std::make_shared<TimestampTzType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<int64_t>() =
                  1672531200000000LL + i * 1000000LL;
            },
        .expected_json =
            R"([{"a": 1672531200000000}, {"a": 1672531201000000}, {"a": 1672531202000000}])",
    },
    {
        .name = "IntToLongPromotion",
        .projected_type = std::make_shared<LongType>(),
        .source_type = std::make_shared<IntType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<int32_t>() = i * 100;
            },
        .expected_json = R"([{"a": 0}, {"a": 100}, {"a": 200}])",
    },
    {
        .name = "FloatToDoublePromotion",
        .projected_type = std::make_shared<DoubleType>(),
        .source_type = std::make_shared<FloatType>(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<float>() = i * 1.0f;
            },
        .expected_json = R"([{"a": 0.0}, {"a": 1.0}, {"a": 2.0}])",
    },
    {
        .name = "DecimalPrecisionPromotion",
        .projected_type = std::make_shared<DecimalType>(10, 2),
        .source_type = std::make_shared<DecimalType>(6, 2),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              int32_t decimal_value = i * 1000 + i;
              std::vector<uint8_t>& fixed = datum.value<::avro::GenericRecord>()
                                                .fieldAt(0)
                                                .value<::avro::GenericFixed>()
                                                .value();
              for (uint8_t& rvalue : std::ranges::reverse_view(fixed)) {
                rvalue = static_cast<uint8_t>(decimal_value & 0xFF);
                decimal_value >>= 8;
              }
            },
        .expected_json = R"([{"a": "0.00"}, {"a": "10.01"}, {"a": "20.02"}])",
    },
};

INSTANTIATE_TEST_SUITE_P(AllPrimitiveTypes, AppendDatumToBuilderTest,
                         ::testing::ValuesIn(kPrimitiveTestCases),
                         [](const ::testing::TestParamInfo<AppendDatumParam>& info) {
                           return info.param.name;
                         });

TEST(AppendDatumToBuilderTest, TwoFieldsRecord) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "id", std::make_shared<IntType>()),
      SchemaField::MakeRequired(2, "name", std::make_shared<StringType>()),
  });
  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  std::vector<::avro::GenericDatum> avro_data;
  ::avro::GenericDatum avro_datum(avro_node);
  auto& record = avro_datum.value<::avro::GenericRecord>();
  record.fieldAt(0).value<int32_t>() = 42;
  record.fieldAt(1).value<std::string>() = "test";
  avro_data.push_back(avro_datum);

  ASSERT_NO_FATAL_FAILURE(VerifyAppendDatumToBuilder(iceberg_schema, avro_node, avro_data,
                                                     R"([{"id": 42, "name": "test"}])"));
}

}  // namespace iceberg::avro
