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
#include <arrow/util/decimal.h>
#include <avro/Compiler.hh>
#include <avro/Generic.hh>
#include <avro/Node.hh>
#include <avro/Types.hh>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_data_util_internal.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/schema_util.h"
#include "iceberg/type.h"
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
  ASSERT_TRUE(array->Equals(*expected_array))
      << "array: " << array->ToString()
      << "\nexpected_array: " << expected_array->ToString();
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
        .projected_type = iceberg::boolean(),
        .source_type = iceberg::boolean(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<bool>() =
                  (i % 2 == 0);
            },
        .expected_json = R"([{"a": true}, {"a": false}, {"a": true}])",
    },
    {
        .name = "Int",
        .projected_type = iceberg::int32(),
        .source_type = iceberg::int32(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<int32_t>() = i * 100;
            },
        .expected_json = R"([{"a": 0}, {"a": 100}, {"a": 200}])",
    },
    {
        .name = "Long",
        .projected_type = iceberg::int64(),
        .source_type = iceberg::int64(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<int64_t>() =
                  i * 1000000LL;
            },
        .expected_json = R"([{"a": 0}, {"a": 1000000}, {"a": 2000000}])",
    },
    {
        .name = "Float",
        .projected_type = iceberg::float32(),
        .source_type = iceberg::float32(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<float>() = i * 3.14f;
            },
        .expected_json = R"([{"a": 0.0}, {"a": 3.14}, {"a": 6.28}])",
    },
    {
        .name = "Double",
        .projected_type = iceberg::float64(),
        .source_type = iceberg::float64(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<double>() =
                  i * 1.234567890;
            },
        .expected_json = R"([{"a": 0.0}, {"a": 1.234567890}, {"a": 2.469135780}])",
    },
    {
        .name = "String",
        .projected_type = iceberg::string(),
        .source_type = iceberg::string(),
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
        .projected_type = iceberg::binary(),
        .source_type = iceberg::binary(),
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
        .projected_type = iceberg::fixed(4),
        .source_type = iceberg::fixed(4),
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
    /// extension<arrow.uuid>. Need to fix this in the upstream Arrow.
    // {
    //     .name = "UUID",
    //     .projected_type = iceberg::uuid(),
    //     .source_type = iceberg::uuid(),
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
        .projected_type = iceberg::decimal(10, 2),
        .source_type = iceberg::decimal(10, 2),
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
        .projected_type = iceberg::date(),
        .source_type = iceberg::date(),
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
        .projected_type = iceberg::time(),
        .source_type = iceberg::time(),
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
        .projected_type = iceberg::timestamp(),
        .source_type = iceberg::timestamp(),
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
        .projected_type = iceberg::int64(),
        .source_type = iceberg::int32(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<int32_t>() = i * 100;
            },
        .expected_json = R"([{"a": 0}, {"a": 100}, {"a": 200}])",
    },
    {
        .name = "FloatToDoublePromotion",
        .projected_type = iceberg::float64(),
        .source_type = iceberg::float32(),
        .value_setter =
            [](::avro::GenericDatum& datum, int i) {
              datum.value<::avro::GenericRecord>().fieldAt(0).value<float>() = i * 1.0f;
            },
        .expected_json = R"([{"a": 0.0}, {"a": 1.0}, {"a": 2.0}])",
    },
    {
        .name = "DecimalPrecisionPromotion",
        .projected_type = iceberg::decimal(10, 2),
        .source_type = iceberg::decimal(6, 2),
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

TEST(AppendDatumToBuilderTest, StructWithTwoFields) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "id", iceberg::int32()),
      SchemaField::MakeRequired(2, "name", iceberg::string()),
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

TEST(AppendDatumToBuilderTest, NestedStruct) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "id", iceberg::int32()),
      SchemaField::MakeRequired(
          2, "person",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeRequired(3, "name", iceberg::string()),
              SchemaField::MakeRequired(4, "age", iceberg::int32()),
          })),
  });

  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  std::vector<::avro::GenericDatum> avro_data;
  for (int i = 0; i < 2; ++i) {
    ::avro::GenericDatum avro_datum(avro_node);
    auto& record = avro_datum.value<::avro::GenericRecord>();

    // Set id field
    record.fieldAt(0).value<int32_t>() = i + 1;

    // Set nested person struct
    auto& person_record = record.fieldAt(1).value<::avro::GenericRecord>();
    person_record.fieldAt(0).value<std::string>() = "Person" + std::to_string(i);
    person_record.fieldAt(1).value<int32_t>() = 25 + i;

    avro_data.push_back(avro_datum);
  }

  const std::string expected_json = R"([
    {"id": 1, "person": {"name": "Person0", "age": 25}},
    {"id": 2, "person": {"name": "Person1", "age": 26}}
  ])";
  ASSERT_NO_FATAL_FAILURE(
      VerifyAppendDatumToBuilder(iceberg_schema, avro_node, avro_data, expected_json));
}

TEST(AppendDatumToBuilderTest, ListOfIntegers) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "numbers",
                                std::make_shared<ListType>(SchemaField::MakeRequired(
                                    2, "element", iceberg::int32()))),
  });

  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  std::vector<::avro::GenericDatum> avro_data;
  for (int i = 0; i < 2; ++i) {
    ::avro::GenericDatum avro_datum(avro_node);
    auto& record = avro_datum.value<::avro::GenericRecord>();

    // Create array with values [i*10, i*10+1, i*10+2]
    auto& array = record.fieldAt(0).value<::avro::GenericArray>();
    for (int j = 0; j < 3; ++j) {
      ::avro::GenericDatum element(avro_node->leafAt(0)->leafAt(0));
      element.value<int32_t>() = i * 10 + j;
      array.value().push_back(element);
    }

    avro_data.push_back(avro_datum);
  }

  const std::string expected_json = R"([
    {"numbers": [0, 1, 2]},
    {"numbers": [10, 11, 12]}
  ])";
  ASSERT_NO_FATAL_FAILURE(
      VerifyAppendDatumToBuilder(iceberg_schema, avro_node, avro_data, expected_json));
}

TEST(AppendDatumToBuilderTest, ListOfStructs) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(
          1, "people",
          std::make_shared<ListType>(SchemaField::MakeRequired(
              2, "element",
              std::make_shared<StructType>(std::vector<SchemaField>{
                  SchemaField::MakeRequired(3, "name", iceberg::string()),
                  SchemaField::MakeRequired(4, "age", iceberg::int32()),
              })))),
  });

  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  std::vector<::avro::GenericDatum> avro_data;
  for (int i = 0; i < 2; ++i) {
    ::avro::GenericDatum avro_datum(avro_node);
    auto& record = avro_datum.value<::avro::GenericRecord>();

    auto& array = record.fieldAt(0).value<::avro::GenericArray>();
    for (int j = 0; j < 2; ++j) {
      ::avro::GenericDatum element(avro_node->leafAt(0)->leafAt(0));
      auto& person_record = element.value<::avro::GenericRecord>();
      person_record.fieldAt(0).value<std::string>() =
          "Person" + std::to_string(i) + "_" + std::to_string(j);
      person_record.fieldAt(1).value<int32_t>() = 20 + i * 10 + j;
      array.value().push_back(element);
    }

    avro_data.push_back(avro_datum);
  }

  const std::string expected_json = R"([
    {"people": [
      {"name": "Person0_0", "age": 20},
      {"name": "Person0_1", "age": 21}
    ]},
    {"people": [
      {"name": "Person1_0", "age": 30},
      {"name": "Person1_1", "age": 31}
    ]}
  ])";
  ASSERT_NO_FATAL_FAILURE(
      VerifyAppendDatumToBuilder(iceberg_schema, avro_node, avro_data, expected_json));
}

TEST(AppendDatumToBuilderTest, MapStringToInt) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(
          1, "scores",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(2, "key", iceberg::string()),
              SchemaField::MakeRequired(3, "value", iceberg::int32()))),
  });

  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  std::vector<::avro::GenericDatum> avro_data;
  for (int i = 0; i < 2; ++i) {
    ::avro::GenericDatum avro_datum(avro_node);
    auto& record = avro_datum.value<::avro::GenericRecord>();

    auto& map = record.fieldAt(0).value<::avro::GenericMap>();
    auto& map_container = map.value();

    map_container.emplace_back("score_" + std::to_string(i * 2),
                               ::avro::GenericDatum(static_cast<int32_t>(100 + i * 10)));
    map_container.emplace_back(
        "score_" + std::to_string(i * 2 + 1),
        ::avro::GenericDatum(static_cast<int32_t>(100 + i * 10 + 5)));

    avro_data.push_back(avro_datum);
  }

  const std::string expected_json = R"([
    {"scores": [["score_0", 100], ["score_1", 105]]},
    {"scores": [["score_2", 110], ["score_3", 115]]}
  ])";
  ASSERT_NO_FATAL_FAILURE(
      VerifyAppendDatumToBuilder(iceberg_schema, avro_node, avro_data, expected_json));
}

TEST(AppendDatumToBuilderTest, MapIntToStringAsArray) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(
          1, "names",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(2, "key", iceberg::int32()),
              SchemaField::MakeRequired(3, "value", iceberg::string()))),
  });

  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  std::vector<::avro::GenericDatum> avro_data;
  for (int i = 0; i < 2; ++i) {
    ::avro::GenericDatum avro_datum(avro_node);
    auto& record = avro_datum.value<::avro::GenericRecord>();

    auto& array = record.fieldAt(0).value<::avro::GenericArray>();
    for (int j = 0; j < 2; ++j) {
      ::avro::GenericDatum kv_pair(avro_node->leafAt(0)->leafAt(0));
      auto& kv_record = kv_pair.value<::avro::GenericRecord>();
      kv_record.fieldAt(0).value<int32_t>() = i * 10 + j;
      kv_record.fieldAt(1).value<std::string>() = "name_" + std::to_string(i * 10 + j);
      array.value().push_back(kv_pair);
    }

    avro_data.push_back(avro_datum);
  }

  const std::string expected_json = R"([
    {"names": [[0, "name_0"], [1, "name_1"]]},
    {"names": [[10, "name_10"], [11, "name_11"]]}
  ])";
  ASSERT_NO_FATAL_FAILURE(
      VerifyAppendDatumToBuilder(iceberg_schema, avro_node, avro_data, expected_json));
}

TEST(AppendDatumToBuilderTest, MapStringToStruct) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(
          1, "users",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(2, "key", iceberg::string()),
              SchemaField::MakeRequired(
                  3, "value",
                  std::make_shared<StructType>(std::vector<SchemaField>{
                      SchemaField::MakeRequired(4, "id", iceberg::int32()),
                      SchemaField::MakeRequired(5, "email", iceberg::string()),
                  })))),
  });

  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  std::vector<::avro::GenericDatum> avro_data;
  for (int i = 0; i < 2; ++i) {
    ::avro::GenericDatum avro_datum(avro_node);
    auto& record = avro_datum.value<::avro::GenericRecord>();

    auto& map = record.fieldAt(0).value<::avro::GenericMap>();
    auto& map_container = map.value();

    ::avro::GenericDatum struct_value(avro_node->leafAt(0)->leafAt(1));
    auto& struct_record = struct_value.value<::avro::GenericRecord>();
    struct_record.fieldAt(0).value<int32_t>() = 1000 + i;
    struct_record.fieldAt(1).value<std::string>() =
        "user" + std::to_string(i) + "@example.com";

    map_container.emplace_back("user_" + std::to_string(i), std::move(struct_value));

    avro_data.push_back(avro_datum);
  }

  const std::string expected_json = R"([
    {"users": [["user_0", {"id": 1000, "email": "user0@example.com"}]]},
    {"users": [["user_1", {"id": 1001, "email": "user1@example.com"}]]}
  ])";
  ASSERT_NO_FATAL_FAILURE(
      VerifyAppendDatumToBuilder(iceberg_schema, avro_node, avro_data, expected_json));
}

TEST(AppendDatumToBuilderTest, StructWithMissingOptionalField) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "id", iceberg::int32()),
      SchemaField::MakeRequired(2, "name", iceberg::string()),
      SchemaField::MakeOptional(3, "age",
                                iceberg::int32()),  // Missing in Avro
      SchemaField::MakeOptional(4, "email",
                                iceberg::string()),  // Missing in Avro
  });

  // Create Avro schema that only has id and name fields (missing age and email)
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "person",
    "fields": [
      {"name": "id", "type": "int", "field-id": 1},
      {"name": "name", "type": "string", "field-id": 2}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  std::vector<::avro::GenericDatum> avro_data;
  for (int i = 0; i < 2; ++i) {
    ::avro::GenericDatum avro_datum(avro_schema.root());
    auto& record = avro_datum.value<::avro::GenericRecord>();
    record.fieldAt(0).value<int32_t>() = i + 1;
    record.fieldAt(1).value<std::string>() = "Person" + std::to_string(i);
    avro_data.push_back(avro_datum);
  }

  const std::string expected_json = R"([
    {"id": 1, "name": "Person0", "age": null, "email": null},
    {"id": 2, "name": "Person1", "age": null, "email": null}
  ])";
  ASSERT_NO_FATAL_FAILURE(VerifyAppendDatumToBuilder(iceberg_schema, avro_schema.root(),
                                                     avro_data, expected_json));
}

TEST(AppendDatumToBuilderTest, NestedStructWithMissingOptionalFields) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "id", iceberg::int32()),
      SchemaField::MakeRequired(
          2, "person",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeRequired(3, "name", iceberg::string()),
              SchemaField::MakeOptional(4, "age",
                                        iceberg::int32()),  // Missing
              SchemaField::MakeOptional(5, "phone",
                                        iceberg::string()),  // Missing
          })),
      SchemaField::MakeOptional(6, "department",
                                iceberg::string()),  // Missing
  });

  // Create Avro schema with only id, person.name fields
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "employee",
    "fields": [
      {"name": "id", "type": "int", "field-id": 1},
      {"name": "person", "type": {
        "type": "record",
        "name": "person_info",
        "fields": [
          {"name": "name", "type": "string", "field-id": 3}
        ]
      }, "field-id": 2}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  std::vector<::avro::GenericDatum> avro_data;
  for (int i = 0; i < 2; ++i) {
    ::avro::GenericDatum avro_datum(avro_schema.root());
    auto& record = avro_datum.value<::avro::GenericRecord>();

    record.fieldAt(0).value<int32_t>() = i + 100;

    auto& person_record = record.fieldAt(1).value<::avro::GenericRecord>();
    person_record.fieldAt(0).value<std::string>() = "Employee" + std::to_string(i);

    avro_data.push_back(avro_datum);
  }

  const std::string expected_json = R"([
    {"id": 100, "person": {"name": "Employee0", "age": null, "phone": null}, "department": null},
    {"id": 101, "person": {"name": "Employee1", "age": null, "phone": null}, "department": null}
  ])";
  ASSERT_NO_FATAL_FAILURE(VerifyAppendDatumToBuilder(iceberg_schema, avro_schema.root(),
                                                     avro_data, expected_json));
}

TEST(AppendDatumToBuilderTest, ListWithMissingOptionalElementFields) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(
          1, "people",
          std::make_shared<ListType>(SchemaField::MakeRequired(
              2, "element",
              std::make_shared<StructType>(std::vector<SchemaField>{
                  SchemaField::MakeRequired(3, "name", iceberg::string()),
                  SchemaField::MakeOptional(4, "age",
                                            iceberg::int32()),  // Missing in Avro
                  SchemaField::MakeOptional(5, "email",
                                            iceberg::string()),  // Missing in Avro
              })))),
  });

  // Create Avro schema with list of structs that only have name field
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "people_list",
    "fields": [
      {"name": "people", "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "person",
          "fields": [
            {"name": "name", "type": "string", "field-id": 3}
          ]
        },
        "element-id": 2
      }, "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  std::vector<::avro::GenericDatum> avro_data;
  for (int i = 0; i < 2; ++i) {
    ::avro::GenericDatum avro_datum(avro_schema.root());
    auto& record = avro_datum.value<::avro::GenericRecord>();

    auto& array = record.fieldAt(0).value<::avro::GenericArray>();
    for (int j = 0; j < 2; ++j) {
      ::avro::GenericDatum element(avro_schema.root()->leafAt(0)->leafAt(0));
      auto& person_record = element.value<::avro::GenericRecord>();
      person_record.fieldAt(0).value<std::string>() =
          "Person" + std::to_string(i) + "_" + std::to_string(j);
      array.value().push_back(element);
    }

    avro_data.push_back(avro_datum);
  }

  const std::string expected_json = R"([
    {"people": [
      {"name": "Person0_0", "age": null, "email": null},
      {"name": "Person0_1", "age": null, "email": null}
    ]},
    {"people": [
      {"name": "Person1_0", "age": null, "email": null},
      {"name": "Person1_1", "age": null, "email": null}
    ]}
  ])";
  ASSERT_NO_FATAL_FAILURE(VerifyAppendDatumToBuilder(iceberg_schema, avro_schema.root(),
                                                     avro_data, expected_json));
}

struct ExtractDatumParam {
  std::string name;
  std::shared_ptr<Type> iceberg_type;
  std::string arrow_json;
  std::function<void(const ::avro::GenericDatum&, int)> value_verifier;
};

void VerifyExtractDatumFromArray(const ExtractDatumParam& test_case) {
  Schema iceberg_schema({SchemaField::MakeRequired(
      /*field_id=*/1, /*name=*/"a", test_case.iceberg_type)});
  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(iceberg_schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&arrow_c_schema).ValueOrDie();
  auto arrow_struct_type = std::make_shared<::arrow::StructType>(arrow_schema->fields());
  auto arrow_array =
      ::arrow::json::ArrayFromJSONString(arrow_struct_type, test_case.arrow_json)
          .ValueOrDie();

  for (int64_t i = 0; i < arrow_array->length(); ++i) {
    ::avro::GenericDatum extracted_datum(avro_node);
    ASSERT_THAT(ExtractDatumFromArray(*arrow_array, i, &extracted_datum), IsOk())
        << "Failed to extract at index " << i;
    test_case.value_verifier(extracted_datum, static_cast<int>(i));
  }
}

class ExtractDatumFromArrayTest : public ::testing::TestWithParam<ExtractDatumParam> {};

TEST_P(ExtractDatumFromArrayTest, PrimitiveType) {
  ASSERT_NO_FATAL_FAILURE(VerifyExtractDatumFromArray(GetParam()));
}

const std::vector<ExtractDatumParam> kExtractDatumTestCases = {
    {
        .name = "Boolean",
        .iceberg_type = boolean(),
        .arrow_json = R"([{"a": true}, {"a": false}, {"a": true}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              bool expected = (i % 2 == 0);
              EXPECT_EQ(record.fieldAt(0).value<bool>(), expected);
            },
    },
    {
        .name = "Int",
        .iceberg_type = int32(),
        .arrow_json = R"([{"a": 0}, {"a": 100}, {"a": 200}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              EXPECT_EQ(record.fieldAt(0).value<int32_t>(), i * 100);
            },
    },
    {
        .name = "Long",
        .iceberg_type = int64(),
        .arrow_json = R"([{"a": 0}, {"a": 1000000}, {"a": 2000000}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              EXPECT_EQ(record.fieldAt(0).value<int64_t>(), i * 1000000LL);
            },
    },
    {
        .name = "Float",
        .iceberg_type = float32(),
        .arrow_json = R"([{"a": 0.0}, {"a": 3.14}, {"a": 6.28}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              EXPECT_FLOAT_EQ(record.fieldAt(0).value<float>(), i * 3.14f);
            },
    },
    {
        .name = "Double",
        .iceberg_type = float64(),
        .arrow_json = R"([{"a": 0.0}, {"a": 1.234567890}, {"a": 2.469135780}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              EXPECT_DOUBLE_EQ(record.fieldAt(0).value<double>(), i * 1.234567890);
            },
    },
    {
        .name = "String",
        .iceberg_type = string(),
        .arrow_json =
            R"([{"a": "test_string_0"}, {"a": "test_string_1"}, {"a": "test_string_2"}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              std::string expected = "test_string_" + std::to_string(i);
              EXPECT_EQ(record.fieldAt(0).value<std::string>(), expected);
            },
    },
    {
        .name = "Binary",
        .iceberg_type = binary(),
        .arrow_json = R"([{"a": "abc"}, {"a": "bcd"}, {"a": "cde"}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              const auto& bytes = record.fieldAt(0).value<std::vector<uint8_t>>();
              EXPECT_EQ(bytes.size(), 3);
              EXPECT_EQ(bytes[0], static_cast<uint8_t>('a' + i));
              EXPECT_EQ(bytes[1], static_cast<uint8_t>('b' + i));
              EXPECT_EQ(bytes[2], static_cast<uint8_t>('c' + i));
            },
    },
    {
        .name = "Fixed",
        .iceberg_type = fixed(4),
        .arrow_json = R"([{"a": "abcd"}, {"a": "bcde"}, {"a": "cdef"}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              const auto& fixed = record.fieldAt(0).value<::avro::GenericFixed>();
              EXPECT_EQ(fixed.value().size(), 4);
              EXPECT_EQ(static_cast<char>(fixed.value()[0]), static_cast<char>('a' + i));
              EXPECT_EQ(static_cast<char>(fixed.value()[1]), static_cast<char>('b' + i));
              EXPECT_EQ(static_cast<char>(fixed.value()[2]), static_cast<char>('c' + i));
              EXPECT_EQ(static_cast<char>(fixed.value()[3]), static_cast<char>('d' + i));
            },
    },
    {
        .name = "Decimal",
        .iceberg_type = decimal(10, 2),
        .arrow_json = R"([{"a": "0.00"}, {"a": "10.01"}, {"a": "20.02"}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              const auto& fixed = record.fieldAt(0).value<::avro::GenericFixed>();

              const auto& bytes = fixed.value();
              auto decimal =
                  ::arrow::Decimal128::FromBigEndian(
                      reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size())
                      .ValueOrDie();
              int64_t expected_unscaled = i * 1000 + i;
              EXPECT_EQ(decimal.low_bits(), static_cast<uint64_t>(expected_unscaled));
              EXPECT_EQ(decimal.high_bits(), 0);
            },
    },
    {
        .name = "Date",
        .iceberg_type = date(),
        .arrow_json = R"([{"a": 18000}, {"a": 18001}, {"a": 18002}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              EXPECT_EQ(record.fieldAt(0).value<int32_t>(), 18000 + i);
            },
    },
    {
        .name = "Time",
        .iceberg_type = time(),
        .arrow_json = R"([{"a": 45045123456}, {"a": 45046123456}, {"a": 45047123456}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              EXPECT_EQ(record.fieldAt(0).value<int64_t>(),
                        45045123456LL + i * 1000000LL);
            },
    },
    {
        .name = "Timestamp",
        .iceberg_type = timestamp(),
        .arrow_json = R"([{"a": 0}, {"a": 1000000}, {"a": 2000000}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              EXPECT_EQ(record.fieldAt(0).value<int64_t>(), i * 1000000LL);
            },
    },
    {
        .name = "TimestampTz",
        .iceberg_type = timestamp_tz(),
        .arrow_json =
            R"([{"a": 1672531200000000}, {"a": 1672531201000000}, {"a": 1672531202000000}])",
        .value_verifier =
            [](const ::avro::GenericDatum& datum, int i) {
              const auto& record = datum.value<::avro::GenericRecord>();
              EXPECT_EQ(record.fieldAt(0).value<int64_t>(),
                        1672531200000000LL + i * 1000000LL);
            },
    },
};

INSTANTIATE_TEST_SUITE_P(AllPrimitiveTypes, ExtractDatumFromArrayTest,
                         ::testing::ValuesIn(kExtractDatumTestCases),
                         [](const ::testing::TestParamInfo<ExtractDatumParam>& info) {
                           return info.param.name;
                         });

TEST(ExtractDatumFromArrayTest, StructWithTwoFields) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(2, "name", string()),
  });
  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(iceberg_schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&arrow_c_schema).ValueOrDie();
  auto arrow_struct_type = std::make_shared<::arrow::StructType>(arrow_schema->fields());

  auto arrow_array = ::arrow::json::ArrayFromJSONString(arrow_struct_type,
                                                        R"([
                                                          {"id": 42, "name": "Alice"},
                                                          {"id": 43, "name": "Bob"},
                                                          {"id": 44, "name": "Charlie"}
                                                        ])")
                         .ValueOrDie();

  struct ExpectedData {
    int32_t id;
    std::string name;
  };
  std::vector<ExpectedData> expected = {{.id = 42, .name = "Alice"},
                                        {.id = 43, .name = "Bob"},
                                        {.id = 44, .name = "Charlie"}};

  auto verify_record = [&](int64_t index, const ExpectedData& expected_data) {
    ::avro::GenericDatum extracted_datum(avro_node);
    ASSERT_THAT(ExtractDatumFromArray(*arrow_array, index, &extracted_datum), IsOk());
    const auto& record = extracted_datum.value<::avro::GenericRecord>();
    EXPECT_EQ(record.fieldAt(0).value<int32_t>(), expected_data.id);
    EXPECT_EQ(record.fieldAt(1).value<std::string>(), expected_data.name);
  };

  for (size_t i = 0; i < expected.size(); ++i) {
    verify_record(i, expected[i]);
  }
}

TEST(ExtractDatumFromArrayTest, NestedStruct) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(2, "person",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeRequired(3, "name", string()),
                                    SchemaField::MakeRequired(4, "age", int32()),
                                })),
  });

  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(iceberg_schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&arrow_c_schema).ValueOrDie();
  auto arrow_struct_type = std::make_shared<::arrow::StructType>(arrow_schema->fields());

  const std::string arrow_json = R"([
    {"id": 1, "person": {"name": "Alice", "age": 25}},
    {"id": 2, "person": {"name": "Bob", "age": 30}},
    {"id": 3, "person": {"name": "Charlie", "age": 35}}
  ])";
  auto arrow_array =
      ::arrow::json::ArrayFromJSONString(arrow_struct_type, arrow_json).ValueOrDie();

  struct ExpectedData {
    int32_t id;
    std::string name;
    int32_t age;
  };
  std::vector<ExpectedData> expected = {{.id = 1, .name = "Alice", .age = 25},
                                        {.id = 2, .name = "Bob", .age = 30},
                                        {.id = 3, .name = "Charlie", .age = 35}};

  auto verify_record = [&](int64_t index, const ExpectedData& expected_data) {
    ::avro::GenericDatum extracted_datum(avro_node);
    ASSERT_THAT(ExtractDatumFromArray(*arrow_array, index, &extracted_datum), IsOk());
    const auto& record = extracted_datum.value<::avro::GenericRecord>();
    EXPECT_EQ(record.fieldAt(0).value<int32_t>(), expected_data.id);
    const auto& person_record = record.fieldAt(1).value<::avro::GenericRecord>();
    EXPECT_EQ(person_record.fieldAt(0).value<std::string>(), expected_data.name);
    EXPECT_EQ(person_record.fieldAt(1).value<int32_t>(), expected_data.age);
  };

  for (size_t i = 0; i < expected.size(); ++i) {
    verify_record(i, expected[i]);
  }
}

TEST(ExtractDatumFromArrayTest, ListOfIntegers) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(
          1, "numbers",
          std::make_shared<ListType>(SchemaField::MakeRequired(2, "element", int32()))),
  });

  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(iceberg_schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&arrow_c_schema).ValueOrDie();
  auto arrow_struct_type = std::make_shared<::arrow::StructType>(arrow_schema->fields());

  const std::string arrow_json = R"([
    {"numbers": [10, 11, 12]},
    {"numbers": [20, 21]},
    {"numbers": [30, 31, 32, 33]}
  ])";
  auto arrow_array =
      ::arrow::json::ArrayFromJSONString(arrow_struct_type, arrow_json).ValueOrDie();

  std::vector<std::vector<int32_t>> expected = {{10, 11, 12}, {20, 21}, {30, 31, 32, 33}};

  auto verify_record = [&](int64_t index, const std::vector<int32_t>& expected_numbers) {
    ::avro::GenericDatum extracted_datum(avro_node);
    ASSERT_THAT(ExtractDatumFromArray(*arrow_array, index, &extracted_datum), IsOk());
    const auto& record = extracted_datum.value<::avro::GenericRecord>();
    const auto& array = record.fieldAt(0).value<::avro::GenericArray>();
    const auto& elements = array.value();

    ASSERT_EQ(elements.size(), expected_numbers.size());
    for (size_t i = 0; i < expected_numbers.size(); ++i) {
      EXPECT_EQ(elements[i].value<int32_t>(), expected_numbers[i]);
    }
  };

  for (size_t i = 0; i < expected.size(); ++i) {
    verify_record(i, expected[i]);
  }
}

TEST(ExtractDatumFromArrayTest, MapStringToInt) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(
          1, "scores",
          std::make_shared<MapType>(SchemaField::MakeRequired(2, "key", string()),
                                    SchemaField::MakeRequired(3, "value", int32()))),
  });

  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(iceberg_schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&arrow_c_schema).ValueOrDie();
  auto arrow_struct_type = std::make_shared<::arrow::StructType>(arrow_schema->fields());

  const std::string arrow_json = R"([
    {"scores": [["alice", 95], ["bob", 87]]},
    {"scores": [["charlie", 92], ["diana", 98], ["eve", 89]]},
    {"scores": [["frank", 91]]}
  ])";
  auto arrow_array =
      ::arrow::json::ArrayFromJSONString(arrow_struct_type, arrow_json).ValueOrDie();

  using MapEntry = std::pair<std::string, int32_t>;
  std::vector<std::vector<MapEntry>> expected = {
      {{"alice", 95}, {"bob", 87}},
      {{"charlie", 92}, {"diana", 98}, {"eve", 89}},
      {{"frank", 91}}};

  auto verify_record = [&](int64_t index, const std::vector<MapEntry>& expected_entries) {
    ::avro::GenericDatum extracted_datum(avro_node);
    ASSERT_THAT(ExtractDatumFromArray(*arrow_array, index, &extracted_datum), IsOk());
    const auto& record = extracted_datum.value<::avro::GenericRecord>();
    const auto& map = record.fieldAt(0).value<::avro::GenericMap>();
    const auto& entries = map.value();

    ASSERT_EQ(entries.size(), expected_entries.size());
    for (size_t i = 0; i < expected_entries.size(); ++i) {
      EXPECT_EQ(entries[i].first, expected_entries[i].first);
      EXPECT_EQ(entries[i].second.value<int32_t>(), expected_entries[i].second);
    }
  };

  for (size_t i = 0; i < expected.size(); ++i) {
    verify_record(i, expected[i]);
  }
}

TEST(ExtractDatumFromArrayTest, ErrorHandling) {
  Schema iceberg_schema({SchemaField::MakeRequired(1, "a", int32())});
  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(iceberg_schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&arrow_c_schema).ValueOrDie();
  auto arrow_struct_type = std::make_shared<::arrow::StructType>(arrow_schema->fields());

  auto arrow_array = ::arrow::json::ArrayFromJSONString(
                         arrow_struct_type, R"([{"a": 1}, {"a": 2}, {"a": 3}])")
                         .ValueOrDie();

  ::avro::GenericDatum datum(avro_node);

  // Test negative index
  EXPECT_THAT(ExtractDatumFromArray(*arrow_array, -1, &datum),
              HasErrorMessage("Cannot extract datum from array at index -1"));

  // Test index beyond array length
  EXPECT_THAT(ExtractDatumFromArray(*arrow_array, 3, &datum),
              HasErrorMessage("Cannot extract datum from array at index 3"));
}

TEST(ExtractDatumFromArrayTest, NullHandling) {
  Schema iceberg_schema({SchemaField::MakeOptional(1, "a", int32())});
  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(iceberg_schema, &avro_node), IsOk());

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(iceberg_schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&arrow_c_schema).ValueOrDie();
  auto arrow_struct_type = std::make_shared<::arrow::StructType>(arrow_schema->fields());

  auto arrow_array =
      ::arrow::json::ArrayFromJSONString(arrow_struct_type, R"([{"a": 42}, {"a": null}])")
          .ValueOrDie();

  ::avro::GenericDatum datum(avro_node);
  ASSERT_THAT(ExtractDatumFromArray(*arrow_array, 0, &datum), IsOk());

  const auto& record = datum.value<::avro::GenericRecord>();
  EXPECT_EQ(record.fieldAt(0).unionBranch(), 1);
  EXPECT_EQ(record.fieldAt(0).type(), ::avro::AVRO_INT);
  EXPECT_EQ(record.fieldAt(0).value<int32_t>(), 42);

  ASSERT_THAT(ExtractDatumFromArray(*arrow_array, 1, &datum), IsOk());
  const auto& record2 = datum.value<::avro::GenericRecord>();
  EXPECT_EQ(record2.fieldAt(0).unionBranch(), 0);
  EXPECT_EQ(record2.fieldAt(0).type(), ::avro::AVRO_NULL);
}

struct RoundTripParam {
  std::string name;
  Schema iceberg_schema;
  std::string arrow_json;
};

void VerifyRoundTripConversion(const RoundTripParam& test_case) {
  ::avro::NodePtr avro_node;
  ASSERT_THAT(ToAvroNodeVisitor{}.Visit(test_case.iceberg_schema, &avro_node), IsOk());

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(test_case.iceberg_schema, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&arrow_c_schema).ValueOrDie();
  auto arrow_struct_type = std::make_shared<::arrow::StructType>(arrow_schema->fields());

  auto original_array =
      ::arrow::json::ArrayFromJSONString(arrow_struct_type, test_case.arrow_json)
          .ValueOrDie();

  std::vector<::avro::GenericDatum> extracted_data;
  for (int64_t i = 0; i < original_array->length(); ++i) {
    ::avro::GenericDatum datum(avro_node);
    ASSERT_THAT(ExtractDatumFromArray(*original_array, i, &datum), IsOk())
        << "Failed to extract datum at index " << i;
    extracted_data.push_back(datum);
  }

  auto projection_result =
      Project(test_case.iceberg_schema, avro_node, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());
  auto projection = std::move(projection_result.value());

  auto builder = ::arrow::MakeBuilder(arrow_struct_type).ValueOrDie();
  for (const auto& datum : extracted_data) {
    ASSERT_THAT(AppendDatumToBuilder(avro_node, datum, projection,
                                     test_case.iceberg_schema, builder.get()),
                IsOk());
  }

  auto rebuilt_array = builder->Finish().ValueOrDie();

  ASSERT_TRUE(original_array->Equals(*rebuilt_array))
      << "Round-trip consistency failed!\n"
      << "Original array: " << original_array->ToString() << "\n"
      << "Rebuilt array:  " << rebuilt_array->ToString();
}

class AvroRoundTripConversionTest : public ::testing::TestWithParam<RoundTripParam> {};

TEST_P(AvroRoundTripConversionTest, ConvertTypes) {
  ASSERT_NO_FATAL_FAILURE(VerifyRoundTripConversion(GetParam()));
}

const std::vector<RoundTripParam> kRoundTripTestCases = {
    {
        .name = "SimpleStruct",
        .iceberg_schema = Schema({
            SchemaField::MakeRequired(1, "id", int32()),
            SchemaField::MakeRequired(2, "name", string()),
            SchemaField::MakeOptional(3, "age", int32()),
        }),
        .arrow_json = R"([
          {"id": 100, "name": "Alice", "age": 25},
          {"id": 101, "name": "Bob", "age": null},
          {"id": 102, "name": "Charlie", "age": 35}
        ])",
    },
    {
        .name = "PrimitiveTypes",
        .iceberg_schema = Schema({
            SchemaField::MakeRequired(1, "bool_field", boolean()),
            SchemaField::MakeRequired(2, "int_field", int32()),
            SchemaField::MakeRequired(3, "long_field", int64()),
            SchemaField::MakeRequired(4, "float_field", float32()),
            SchemaField::MakeRequired(5, "double_field", float64()),
            SchemaField::MakeRequired(6, "string_field", string()),
        }),
        .arrow_json = R"([
          {"bool_field": true, "int_field": 42, "long_field": 1000000, "float_field": 3.14, "double_field": 2.718281828, "string_field": "hello"},
          {"bool_field": false, "int_field": -42, "long_field": -1000000, "float_field": -3.14, "double_field": -2.718281828, "string_field": "world"}
        ])",
    },
    {
        .name = "NestedStruct",
        .iceberg_schema = Schema({
            SchemaField::MakeRequired(1, "id", int32()),
            SchemaField::MakeRequired(
                2, "person",
                std::make_shared<StructType>(std::vector<SchemaField>{
                    SchemaField::MakeRequired(3, "name", string()),
                    SchemaField::MakeRequired(4, "age", int32()),
                })),
        }),
        .arrow_json = R"([
          {"id": 1, "person": {"name": "Alice", "age": 30}},
          {"id": 2, "person": {"name": "Bob", "age": 25}}
        ])",
    },
    {
        .name = "ListOfIntegers",
        .iceberg_schema = Schema({
            SchemaField::MakeRequired(
                1, "numbers",
                std::make_shared<ListType>(
                    SchemaField::MakeRequired(2, "element", int32()))),
        }),
        .arrow_json = R"([
          {"numbers": [1, 2, 3]},
          {"numbers": [10, 20]},
          {"numbers": []}
        ])",
    },
    {
        .name = "MapStringToInt",
        .iceberg_schema = Schema({
            SchemaField::MakeRequired(
                1, "scores",
                std::make_shared<MapType>(
                    SchemaField::MakeRequired(2, "key", string()),
                    SchemaField::MakeRequired(3, "value", int32()))),
        }),
        .arrow_json = R"([
          {"scores": [["alice", 95], ["bob", 87]]},
          {"scores": [["charlie", 92]]},
          {"scores": []}
        ])",
    },
    {
        .name = "ComplexNested",
        .iceberg_schema = Schema({
            SchemaField::MakeRequired(
                1, "data",
                std::make_shared<StructType>(std::vector<SchemaField>{
                    SchemaField::MakeRequired(2, "id", int32()),
                    SchemaField::MakeRequired(
                        3, "tags",
                        std::make_shared<ListType>(
                            SchemaField::MakeRequired(4, "element", string()))),
                    SchemaField::MakeOptional(
                        5, "metadata",
                        std::make_shared<MapType>(
                            SchemaField::MakeRequired(6, "key", string()),
                            SchemaField::MakeRequired(7, "value", string()))),
                })),
        }),
        .arrow_json = R"([
           {"data": {"id": 1, "tags": ["tag1", "tag2"], "metadata": [["key1", "value1"]]}},
           {"data": {"id": 2, "tags": [], "metadata": null}}
         ])",
    },
    {
        .name = "NullablePrimitives",
        .iceberg_schema = Schema({
            SchemaField::MakeOptional(1, "optional_bool", boolean()),
            SchemaField::MakeOptional(2, "optional_int", int32()),
            SchemaField::MakeOptional(3, "optional_long", int64()),
            SchemaField::MakeOptional(4, "optional_string", string()),
            SchemaField::MakeRequired(5, "required_id", int32()),
        }),
        .arrow_json = R"([
           {"optional_bool": true, "optional_int": 42, "optional_long": 1000000, "optional_string": "hello", "required_id": 1},
           {"optional_bool": null, "optional_int": null, "optional_long": null, "optional_string": null, "required_id": 2},
           {"optional_bool": false, "optional_int": null, "optional_long": 2000000, "optional_string": null, "required_id": 3},
           {"optional_bool": null, "optional_int": 123, "optional_long": null, "optional_string": "world", "required_id": 4}
         ])",
    },
    {
        .name = "NullableNestedStruct",
        .iceberg_schema = Schema({
            SchemaField::MakeRequired(1, "id", int32()),
            SchemaField::MakeOptional(
                2, "person",
                std::make_shared<StructType>(std::vector<SchemaField>{
                    SchemaField::MakeRequired(3, "name", string()),
                    SchemaField::MakeOptional(4, "age", int32()),
                    SchemaField::MakeOptional(5, "email", string()),
                })),
            SchemaField::MakeOptional(6, "department", string()),
        }),
        .arrow_json = R"([
           {"id": 1, "person": {"name": "Alice", "age": 30, "email": "alice@example.com"}, "department": "Engineering"},
           {"id": 2, "person": null, "department": null},
           {"id": 3, "person": {"name": "Bob", "age": null, "email": null}, "department": "Sales"},
           {"id": 4, "person": {"name": "Charlie", "age": 25, "email": null}, "department": null}
         ])",
    },
    {
        .name = "NullableListElements",
        .iceberg_schema = Schema({
            SchemaField::MakeRequired(1, "id", int32()),
            SchemaField::MakeOptional(
                2, "numbers",
                std::make_shared<ListType>(
                    SchemaField::MakeOptional(3, "element", int32()))),
            SchemaField::MakeRequired(
                4, "tags",
                std::make_shared<ListType>(
                    SchemaField::MakeOptional(5, "element", string()))),
        }),
        .arrow_json = R"([
           {"id": 1, "numbers": [1, null, 3], "tags": ["tag1", null, "tag3"]},
           {"id": 2, "numbers": null, "tags": ["only_tag"]},
           {"id": 3, "numbers": [null, null], "tags": [null, null, null]},
           {"id": 4, "numbers": [], "tags": []}
         ])",
    },
    {
        .name = "NullableMapValues",
        .iceberg_schema = Schema({
            SchemaField::MakeRequired(1, "id", int32()),
            SchemaField::MakeOptional(
                2, "scores",
                std::make_shared<MapType>(
                    SchemaField::MakeRequired(3, "key", string()),
                    SchemaField::MakeOptional(4, "value", int32()))),
            SchemaField::MakeRequired(
                5, "metadata",
                std::make_shared<MapType>(
                    SchemaField::MakeRequired(6, "key", string()),
                    SchemaField::MakeOptional(7, "value", string()))),
        }),
        .arrow_json = R"([
           {"id": 1, "scores": [["alice", 95], ["bob", null]], "metadata": [["key1", "value1"], ["key2", null]]},
           {"id": 2, "scores": null, "metadata": [["key3", null]]},
           {"id": 3, "scores": [["charlie", null], ["diana", 98]], "metadata": []},
           {"id": 4, "scores": [], "metadata": [["key4", null], ["key5", "value5"]]}
         ])",
    },
    {
        .name = "DeeplyNestedWithNulls",
        .iceberg_schema = Schema({
            SchemaField::MakeRequired(
                1, "root",
                std::make_shared<StructType>(std::vector<SchemaField>{
                    SchemaField::MakeRequired(2, "id", int32()),
                    SchemaField::MakeOptional(
                        3, "nested",
                        std::make_shared<StructType>(std::vector<SchemaField>{
                            SchemaField::MakeOptional(4, "name", string()),
                            SchemaField::MakeOptional(
                                5, "values",
                                std::make_shared<ListType>(
                                    SchemaField::MakeOptional(6, "element", int32()))),
                        })),
                    SchemaField::MakeOptional(
                        7, "tags",
                        std::make_shared<ListType>(
                            SchemaField::MakeOptional(8, "element", string()))),
                })),
        }),
        .arrow_json = R"([
           {"root": {"id": 1, "nested": {"name": "test", "values": [1, null, 3]}, "tags": ["a", "b"]}},
           {"root": {"id": 2, "nested": null, "tags": null}},
           {"root": {"id": 3, "nested": {"name": null, "values": null}, "tags": [null, "c"]}},
           {"root": {"id": 4, "nested": {"name": "empty", "values": []}, "tags": []}}
         ])",
    },
    {
        .name = "AllNullsVariations",
        .iceberg_schema = Schema({
            SchemaField::MakeOptional(1, "always_null", string()),
            SchemaField::MakeOptional(2, "sometimes_null", int32()),
            SchemaField::MakeOptional(
                3, "nested_struct",
                std::make_shared<StructType>(std::vector<SchemaField>{
                    SchemaField::MakeOptional(4, "inner_null", string()),
                    SchemaField::MakeRequired(5, "inner_required", boolean()),
                })),
            SchemaField::MakeRequired(6, "id", int32()),
        }),
        .arrow_json = R"([
           {"always_null": null, "sometimes_null": 42, "nested_struct": {"inner_null": "value", "inner_required": true}, "id": 1},
           {"always_null": null, "sometimes_null": null, "nested_struct": null, "id": 2},
           {"always_null": null, "sometimes_null": 123, "nested_struct": {"inner_null": null, "inner_required": false}, "id": 3},
           {"always_null": null, "sometimes_null": null, "nested_struct": {"inner_null": null, "inner_required": true}, "id": 4}
         ])",
    },
};

INSTANTIATE_TEST_SUITE_P(AllTypes, AvroRoundTripConversionTest,
                         ::testing::ValuesIn(kRoundTripTestCases),
                         [](const ::testing::TestParamInfo<RoundTripParam>& info) {
                           return info.param.name;
                         });

}  // namespace iceberg::avro
