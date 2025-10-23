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

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/parquet/parquet_data_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/schema_util.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg::parquet {

struct ProjectRecordBatchParam {
  std::string name;
  std::shared_ptr<Type> projected_type;
  std::shared_ptr<Type> source_type;
  std::string input_json;
  std::string expected_json;
};

std::shared_ptr<::arrow::RecordBatch> RecordBatchFromJson(
    const std::shared_ptr<::arrow::Schema>& schema, const std::string& json_data) {
  auto struct_type = ::arrow::struct_(schema->fields());
  auto array = ::arrow::json::ArrayFromJSONString(struct_type, json_data).ValueOrDie();
  auto struct_array = std::static_pointer_cast<::arrow::StructArray>(array);
  return ::arrow::RecordBatch::Make(schema, struct_array->length(),
                                    struct_array->fields());
}

void VerifyProjectRecordBatch(const Schema& projected_schema, const Schema& source_schema,
                              const std::string& input_json,
                              const std::string& expected_json) {
  auto schema_projection_result =
      Project(projected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(schema_projection_result, IsOk());
  auto schema_projection = std::move(schema_projection_result.value());

  ArrowSchema source_arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(source_schema, &source_arrow_c_schema), IsOk());
  auto source_arrow_schema = ::arrow::ImportSchema(&source_arrow_c_schema).ValueOrDie();
  auto input_record_batch = RecordBatchFromJson(source_arrow_schema, input_json);

  ArrowSchema projected_arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(projected_schema, &projected_arrow_c_schema), IsOk());
  auto projected_arrow_schema =
      ::arrow::ImportSchema(&projected_arrow_c_schema).ValueOrDie();

  auto project_result =
      ProjectRecordBatch(input_record_batch, projected_arrow_schema, projected_schema,
                         schema_projection, ::arrow::default_memory_pool());
  ASSERT_THAT(project_result, IsOk());
  auto projected_record_batch = std::move(project_result.value());

  auto expected_record_batch = RecordBatchFromJson(projected_arrow_schema, expected_json);
  ASSERT_TRUE(projected_record_batch->Equals(*expected_record_batch))
      << "projected_record_batch: " << projected_record_batch->ToString()
      << "\nexpected_record_batch: " << expected_record_batch->ToString();
}

class ProjectRecordBatchTest : public ::testing::TestWithParam<ProjectRecordBatchParam> {
};

TEST_P(ProjectRecordBatchTest, PrimitiveType) {
  const auto& test_case = GetParam();

  Schema projected_schema({SchemaField::MakeRequired(
      /*field_id=*/1, /*name=*/"a", test_case.projected_type)});
  Schema source_schema({SchemaField::MakeRequired(
      /*field_id=*/1, /*name=*/"a", test_case.source_type)});

  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(
      projected_schema, source_schema, test_case.input_json, test_case.expected_json));
}

const std::vector<ProjectRecordBatchParam> kPrimitiveTestCases = {
    {
        .name = "Boolean",
        .projected_type = boolean(),
        .source_type = boolean(),
        .input_json = R"([{"a": true}, {"a": false}, {"a": true}])",
        .expected_json = R"([{"a": true}, {"a": false}, {"a": true}])",
    },
    {
        .name = "Int",
        .projected_type = int32(),
        .source_type = int32(),
        .input_json = R"([{"a": 0}, {"a": 100}, {"a": 200}])",
        .expected_json = R"([{"a": 0}, {"a": 100}, {"a": 200}])",
    },
    {
        .name = "Long",
        .projected_type = int64(),
        .source_type = int64(),
        .input_json = R"([{"a": 0}, {"a": 1000000}, {"a": 2000000}])",
        .expected_json = R"([{"a": 0}, {"a": 1000000}, {"a": 2000000}])",
    },
    {
        .name = "Float",
        .projected_type = float32(),
        .source_type = float32(),
        .input_json = R"([{"a": 0.0}, {"a": 3.14}, {"a": 6.28}])",
        .expected_json = R"([{"a": 0.0}, {"a": 3.14}, {"a": 6.28}])",
    },
    {
        .name = "Double",
        .projected_type = float64(),
        .source_type = float64(),
        .input_json = R"([{"a": 0.0}, {"a": 1.234567890}, {"a": 2.469135780}])",
        .expected_json = R"([{"a": 0.0}, {"a": 1.234567890}, {"a": 2.469135780}])",
    },
    {
        .name = "String",
        .projected_type = string(),
        .source_type = string(),
        .input_json =
            R"([{"a": "test_string_0"}, {"a": "test_string_1"}, {"a": "test_string_2"}])",
        .expected_json =
            R"([{"a": "test_string_0"}, {"a": "test_string_1"}, {"a": "test_string_2"}])",
    },
    {
        .name = "Binary",
        .projected_type = binary(),
        .source_type = binary(),
        .input_json = R"([{"a": "abc"}, {"a": "bcd"}, {"a": "cde"}])",
        .expected_json = R"([{"a": "abc"}, {"a": "bcd"}, {"a": "cde"}])",
    },
    {
        .name = "Fixed",
        .projected_type = fixed(4),
        .source_type = fixed(4),
        .input_json = R"([{"a": "abcd"}, {"a": "bcde"}, {"a": "cdef"}])",
        .expected_json = R"([{"a": "abcd"}, {"a": "bcde"}, {"a": "cdef"}])",
    },
    {
        .name = "Decimal",
        .projected_type = decimal(10, 2),
        .source_type = decimal(10, 2),
        .input_json = R"([{"a": "0.00"}, {"a": "10.01"}, {"a": "20.02"}])",
        .expected_json = R"([{"a": "0.00"}, {"a": "10.01"}, {"a": "20.02"}])",
    },
    {
        .name = "Date",
        .projected_type = date(),
        .source_type = date(),
        .input_json = R"([{"a": 18000}, {"a": 18001}, {"a": 18002}])",
        .expected_json = R"([{"a": 18000}, {"a": 18001}, {"a": 18002}])",
    },
    {
        .name = "Time",
        .projected_type = time(),
        .source_type = time(),
        .input_json = R"([{"a": 45045123456}, {"a": 45046123456}, {"a": 45047123456}])",
        .expected_json =
            R"([{"a": 45045123456}, {"a": 45046123456}, {"a": 45047123456}])",
    },
    {
        .name = "Timestamp",
        .projected_type = timestamp(),
        .source_type = timestamp(),
        .input_json = R"([{"a": 0}, {"a": 1000000}, {"a": 2000000}])",
        .expected_json = R"([{"a": 0}, {"a": 1000000}, {"a": 2000000}])",
    },
    {
        .name = "TimestampTz",
        .projected_type = timestamp_tz(),
        .source_type = timestamp_tz(),
        .input_json =
            R"([{"a": 1672531200000000}, {"a": 1672531201000000}, {"a": 1672531202000000}])",
        .expected_json =
            R"([{"a": 1672531200000000}, {"a": 1672531201000000}, {"a": 1672531202000000}])",
    },
    {
        .name = "IntToLongPromotion",
        .projected_type = int64(),
        .source_type = int32(),
        .input_json = R"([{"a": 0}, {"a": 100}, {"a": 200}])",
        .expected_json = R"([{"a": 0}, {"a": 100}, {"a": 200}])",
    },
    {
        .name = "FloatToDoublePromotion",
        .projected_type = float64(),
        .source_type = float32(),
        .input_json = R"([{"a": 0.0}, {"a": 1.0}, {"a": 2.0}])",
        .expected_json = R"([{"a": 0.0}, {"a": 1.0}, {"a": 2.0}])",
    },
    {
        .name = "DecimalPrecisionPromotion",
        .projected_type = decimal(10, 2),
        .source_type = decimal(6, 2),
        .input_json = R"([{"a": "0.00"}, {"a": "10.01"}, {"a": "20.02"}])",
        .expected_json = R"([{"a": "0.00"}, {"a": "10.01"}, {"a": "20.02"}])",
    },
};

INSTANTIATE_TEST_SUITE_P(
    AllPrimitiveTypes, ProjectRecordBatchTest, ::testing::ValuesIn(kPrimitiveTestCases),
    [](const ::testing::TestParamInfo<ProjectRecordBatchParam>& info) {
      return info.param.name;
    });

TEST(ProjectRecordBatchTest, StructWithTwoFields) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(2, "name", string()),
  });

  const std::string input_json = R"([{"id": 42, "name": "test"}])";
  const std::string expected_json = R"([{"id": 42, "name": "test"}])";

  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(iceberg_schema, iceberg_schema,
                                                   input_json, expected_json));
}

TEST(ProjectRecordBatchTest, NestedStruct) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(2, "person",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeRequired(3, "name", string()),
                                    SchemaField::MakeRequired(4, "age", int32()),
                                })),
  });

  const std::string input_json = R"([
    {"id": 1, "person": {"name": "Person0", "age": 25}},
    {"id": 2, "person": {"name": "Person1", "age": 26}}
  ])";

  ASSERT_NO_FATAL_FAILURE(
      VerifyProjectRecordBatch(iceberg_schema, iceberg_schema, input_json, input_json));
}

TEST(ProjectRecordBatchTest, ListOfIntegers) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(
          1, "numbers",
          std::make_shared<ListType>(SchemaField::MakeRequired(2, "element", int32()))),
  });

  const std::string input_json = R"([
    {"numbers": [0, 1, 2]},
    {"numbers": [10, 11, 12]}
  ])";

  ASSERT_NO_FATAL_FAILURE(
      VerifyProjectRecordBatch(iceberg_schema, iceberg_schema, input_json, input_json));
}

TEST(ProjectRecordBatchTest, ListOfStructs) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "people",
                                std::make_shared<ListType>(SchemaField::MakeRequired(
                                    2, "element",
                                    std::make_shared<StructType>(std::vector<SchemaField>{
                                        SchemaField::MakeRequired(3, "name", string()),
                                        SchemaField::MakeRequired(4, "age", int32()),
                                    })))),
  });

  const std::string input_json = R"([
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
      VerifyProjectRecordBatch(iceberg_schema, iceberg_schema, input_json, input_json));
}

TEST(ProjectRecordBatchTest, MapStringToInt) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(
          1, "scores",
          std::make_shared<MapType>(SchemaField::MakeRequired(2, "key", string()),
                                    SchemaField::MakeRequired(3, "value", int32()))),
  });

  const std::string input_json = R"([
    {"scores": [["score_0", 100], ["score_1", 105]]},
    {"scores": [["score_2", 110], ["score_3", 115]]}
  ])";

  ASSERT_NO_FATAL_FAILURE(
      VerifyProjectRecordBatch(iceberg_schema, iceberg_schema, input_json, input_json));
}

TEST(ProjectRecordBatchTest, MapStringToStruct) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(
          1, "users",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(2, "key", string()),
              SchemaField::MakeRequired(
                  3, "value",
                  std::make_shared<StructType>(std::vector<SchemaField>{
                      SchemaField::MakeRequired(4, "id", int32()),
                      SchemaField::MakeRequired(5, "email", string()),
                  })))),
  });

  const std::string input_json = R"([
    {"users": [["user_0", {"id": 1000, "email": "user0@example.com"}]]},
    {"users": [["user_1", {"id": 1001, "email": "user1@example.com"}]]}
  ])";

  ASSERT_NO_FATAL_FAILURE(
      VerifyProjectRecordBatch(iceberg_schema, iceberg_schema, input_json, input_json));
}

TEST(ProjectRecordBatchTest, StructWithMissingOptionalField) {
  Schema projected_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(2, "name", string()),
      SchemaField::MakeOptional(3, "age", int32()),     // Missing in source
      SchemaField::MakeOptional(4, "email", string()),  // Missing in source
  });

  Schema source_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(2, "name", string()),
  });

  const std::string input_json = R"([
    {"id": 1, "name": "Person0"},
    {"id": 2, "name": "Person1"}
  ])";
  const std::string expected_json = R"([
    {"id": 1, "name": "Person0", "age": null, "email": null},
    {"id": 2, "name": "Person1", "age": null, "email": null}
  ])";

  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(projected_schema, source_schema,
                                                   input_json, expected_json));
}

TEST(ProjectRecordBatchTest, NestedStructWithMissingOptionalFields) {
  Schema projected_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(
          2, "person",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeRequired(3, "name", string()),
              SchemaField::MakeOptional(4, "age", int32()),     // Missing
              SchemaField::MakeOptional(5, "phone", string()),  // Missing
          })),
      SchemaField::MakeOptional(6, "department", string()),  // Missing
  });

  Schema source_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(2, "person",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeRequired(3, "name", string()),
                                })),
  });

  const std::string input_json = R"([
    {"id": 100, "person": {"name": "Employee0"}},
    {"id": 101, "person": {"name": "Employee1"}}
  ])";
  const std::string expected_json = R"([
    {"id": 100, "person": {"name": "Employee0", "age": null, "phone": null}, "department": null},
    {"id": 101, "person": {"name": "Employee1", "age": null, "phone": null}, "department": null}
  ])";

  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(projected_schema, source_schema,
                                                   input_json, expected_json));
}

TEST(ProjectRecordBatchTest, ListWithMissingOptionalElementFields) {
  Schema projected_schema({
      SchemaField::MakeRequired(
          1, "people",
          std::make_shared<ListType>(SchemaField::MakeRequired(
              2, "element",
              std::make_shared<StructType>(std::vector<SchemaField>{
                  SchemaField::MakeRequired(3, "name", string()),
                  SchemaField::MakeOptional(4, "age", int32()),     // Missing in source
                  SchemaField::MakeOptional(5, "email", string()),  // Missing in source
              })))),
  });

  Schema source_schema({
      SchemaField::MakeRequired(1, "people",
                                std::make_shared<ListType>(SchemaField::MakeRequired(
                                    2, "element",
                                    std::make_shared<StructType>(std::vector<SchemaField>{
                                        SchemaField::MakeRequired(3, "name", string()),
                                    })))),
  });

  const std::string input_json = R"([
    {"people": [
      {"name": "Person0_0"},
      {"name": "Person0_1"}
    ]},
    {"people": [
      {"name": "Person1_0"},
      {"name": "Person1_1"}
    ]}
  ])";
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

  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(projected_schema, source_schema,
                                                   input_json, expected_json));
}

TEST(ProjectRecordBatchTest, FieldReordering) {
  Schema projected_schema({
      SchemaField::MakeRequired(2, "name", string()),
      SchemaField::MakeRequired(1, "id", int32()),
  });

  Schema source_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(2, "name", string()),
  });

  const std::string input_json = R"([
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ])";
  const std::string expected_json = R"([
    {"name": "Alice", "id": 1},
    {"name": "Bob", "id": 2}
  ])";

  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(projected_schema, source_schema,
                                                   input_json, expected_json));
}

TEST(ProjectRecordBatchTest, FieldSubset) {
  Schema projected_schema({
      SchemaField::MakeRequired(2, "name", string()),
  });

  Schema source_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(2, "name", string()),
      SchemaField::MakeRequired(3, "age", int32()),
  });

  const std::string input_json = R"([
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 30}
  ])";
  const std::string expected_json = R"([
    {"name": "Alice"},
    {"name": "Bob"}
  ])";

  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(projected_schema, source_schema,
                                                   input_json, expected_json));
}

TEST(ProjectRecordBatchTest, EmptyRecordBatch) {
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(2, "name", string()),
  });

  const std::string input_json = R"([])";

  ASSERT_NO_FATAL_FAILURE(
      VerifyProjectRecordBatch(iceberg_schema, iceberg_schema, input_json, input_json));
}

}  // namespace iceberg::parquet
