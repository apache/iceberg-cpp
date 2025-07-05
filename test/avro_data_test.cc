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

}  // namespace iceberg::avro
