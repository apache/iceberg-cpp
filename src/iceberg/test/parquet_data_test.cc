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
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/expression/literal.h"
#include "iceberg/metadata_columns.h"
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

void VerifyProjectRecordBatch(
    const Schema& projected_schema, const Schema& source_schema,
    const std::string& input_json, const std::string& expected_json,
    const arrow::MetadataColumnContext& metadata_context = {},
    const std::shared_ptr<::arrow::Int64Array>& row_positions = nullptr) {
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

  auto project_result = ProjectRecordBatch(
      input_record_batch, projected_arrow_schema, projected_schema, schema_projection,
      metadata_context, row_positions, ::arrow::default_memory_pool());
  ASSERT_THAT(project_result, IsOk());
  auto projected_record_batch = std::move(project_result.value());

  auto expected_record_batch = RecordBatchFromJson(projected_arrow_schema, expected_json);
  ASSERT_TRUE(projected_record_batch->Equals(*expected_record_batch))
      << "projected_record_batch: " << projected_record_batch->ToString()
      << "\nexpected_record_batch: " << expected_record_batch->ToString();
}

class ProjectRecordBatchTest : public ::testing::TestWithParam<ProjectRecordBatchParam> {
};

TEST(ProjectRecordBatchTest, InheritedLineageUsesFilePositions) {
  Schema source_schema({SchemaField::MakeRequired(1, "id", int32())});
  Schema projected_schema({SchemaField::MakeRequired(1, "id", int32()),
                           MetadataColumns::kRowPosition, MetadataColumns::kRowId,
                           MetadataColumns::kLastUpdatedSequenceNumber});
  auto positions = std::static_pointer_cast<::arrow::Int64Array>(
      ::arrow::json::ArrayFromJSONString(::arrow::int64(), "[12, 3, 30]").ValueOrDie());
  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(
      projected_schema, source_schema, "[[10], [20], [30]]",
      "[[10, 12, 112, 7], [20, 3, 103, 7], [30, 30, 130, 7]]",
      {.first_row_id = 100, .data_sequence_number = 7}, positions));
}

TEST(ProjectRecordBatchTest, PhysicalLineageUsesFilePositionsForNulls) {
  Schema source_schema({SchemaField::MakeRequired(1, "id", int32()),
                        MetadataColumns::kRowId,
                        MetadataColumns::kLastUpdatedSequenceNumber});
  Schema projected_schema({SchemaField::MakeRequired(1, "id", int32()),
                           MetadataColumns::kRowPosition, MetadataColumns::kRowId,
                           MetadataColumns::kLastUpdatedSequenceNumber});
  // A sliced array verifies that lineage uses the array's logical offset.
  auto positions = std::static_pointer_cast<::arrow::Int64Array>(
      ::arrow::json::ArrayFromJSONString(::arrow::int64(), "[999, 12, 3, 30]")
          .ValueOrDie()
          ->Slice(1));
  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(
      projected_schema, source_schema, "[[10, null, null], [20, 777, 8], [30, null, 9]]",
      "[[10, 12, 112, 7], [20, 3, 777, 8], [30, 30, 130, 9]]",
      {.first_row_id = 100, .data_sequence_number = 7}, positions));
}

TEST(ProjectRecordBatchTest, RowPositionsMustMatchBatch) {
  Schema source_schema(std::vector<SchemaField>{});
  auto input = ::arrow::RecordBatch::Make(::arrow::schema({}), 3, ::arrow::ArrayVector{});
  for (const auto& field : {MetadataColumns::kRowPosition, MetadataColumns::kRowId}) {
    SCOPED_TRACE(field.name());
    Schema projected_schema({field});
    ICEBERG_UNWRAP_OR_FAIL(auto projection, Project(projected_schema, source_schema,
                                                    /*prune_source=*/false));
    auto output_schema = ::arrow::schema(
        {::arrow::field(std::string(field.name()), ::arrow::int64(), field.optional())});

    EXPECT_THAT(ProjectRecordBatch(input, output_schema, projected_schema, projection,
                                   {.first_row_id = 100}, nullptr,
                                   ::arrow::default_memory_pool()),
                IsError(ErrorKind::kInvalidArgument));
    for (const auto& json : {"[12, 3]", "[12, null, 30]"}) {
      auto positions = std::static_pointer_cast<::arrow::Int64Array>(
          ::arrow::json::ArrayFromJSONString(::arrow::int64(), json).ValueOrDie());
      EXPECT_THAT(ProjectRecordBatch(input, output_schema, projected_schema, projection,
                                     {.first_row_id = 100}, positions,
                                     ::arrow::default_memory_pool()),
                  IsError(ErrorKind::kInvalidArgument));
    }
  }
}

TEST(ProjectRecordBatchTest, RowPositionsRequiredForPhysicalRowIdInheritance) {
  Schema schema({MetadataColumns::kRowId});
  ICEBERG_UNWRAP_OR_FAIL(auto projection,
                         Project(schema, schema, /*prune_source=*/false));
  auto arrow_schema = ::arrow::schema({::arrow::field("_row_id", ::arrow::int64())});
  auto input = RecordBatchFromJson(arrow_schema, "[[null], [777], [null]]");
  EXPECT_THAT(
      ProjectRecordBatch(input, arrow_schema, schema, projection, {.first_row_id = 100},
                         nullptr, ::arrow::default_memory_pool()),
      IsError(ErrorKind::kInvalidArgument));
}

TEST(ProjectRecordBatchTest, NestedMetadataRequiresRowPositions) {
  auto id_field = SchemaField::MakeRequired(2, "id", int32());
  Schema source_schema({SchemaField::MakeRequired(
      1, "nested", std::make_shared<StructType>(std::vector<SchemaField>{id_field}))});
  auto nested_type = ::arrow::struct_({::arrow::field("id", ::arrow::int32(), false)});
  auto input = RecordBatchFromJson(
      ::arrow::schema({::arrow::field("nested", nested_type, false)}), "[[[10]]]");

  for (const auto& field : {MetadataColumns::kRowPosition, MetadataColumns::kRowId}) {
    SCOPED_TRACE(field.name());
    Schema projected_schema({SchemaField::MakeRequired(
        1, "nested",
        std::make_shared<StructType>(std::vector<SchemaField>{id_field, field}))});
    ICEBERG_UNWRAP_OR_FAIL(auto projection, Project(projected_schema, source_schema,
                                                    /*prune_source=*/false));
    auto output_type = ::arrow::struct_(
        {::arrow::field("id", ::arrow::int32(), false),
         ::arrow::field(std::string(field.name()), ::arrow::int64(), field.optional())});
    auto output_schema = ::arrow::schema({::arrow::field("nested", output_type, false)});
    // Query engines are expected to project these metadata columns at the top level.
    // Construct a nested projection deliberately to exercise the check.
    EXPECT_THAT(ProjectRecordBatch(input, output_schema, projected_schema, projection,
                                   {.first_row_id = 100}, nullptr,
                                   ::arrow::default_memory_pool()),
                IsError(ErrorKind::kInvalidArgument));
  }
}

TEST(ProjectRecordBatchTest, LineageWithoutRowPositions) {
  Schema data_schema({SchemaField::MakeRequired(1, "id", int32())});
  Schema lineage_schema({SchemaField::MakeRequired(1, "id", int32()),
                         MetadataColumns::kRowId,
                         MetadataColumns::kLastUpdatedSequenceNumber});
  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(
      lineage_schema, data_schema, "[[10], [20], [30]]",
      "[[10, null, 7], [20, null, 7], [30, null, 7]]", {.data_sequence_number = 7}));
  const std::string physical_values = "[[10, null, null], [20, 777, 8], [30, null, 9]]";
  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(
      lineage_schema, lineage_schema, physical_values,
      "[[10, null, 7], [20, 777, 8], [30, null, 9]]", {.data_sequence_number = 7}));
  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(lineage_schema, lineage_schema,
                                                   physical_values, physical_values));
}

TEST(ProjectRecordBatchTest, RowPositionsAreIndependentOfPhysicalColumns) {
  Schema source_schema({SchemaField::MakeRequired(1, "_pos", int64())});
  Schema projected_schema({MetadataColumns::kRowPosition, MetadataColumns::kRowId});
  ICEBERG_UNWRAP_OR_FAIL(
      auto projection, Project(projected_schema, source_schema, /*prune_source=*/false));
  auto positions = std::static_pointer_cast<::arrow::Int64Array>(
      ::arrow::json::ArrayFromJSONString(::arrow::int64(), "[12, 3, 30]").ValueOrDie());
  auto physical =
      ::arrow::json::ArrayFromJSONString(::arrow::int64(), "[10, 20, 30]").ValueOrDie();
  auto output_schema = ::arrow::schema({::arrow::field("_pos", ::arrow::int64(), false),
                                        ::arrow::field("_row_id", ::arrow::int64())});
  // A physical column named _pos must not be mistaken for the virtual column.
  auto input_schema = ::arrow::schema({::arrow::field("_pos", ::arrow::int64())});
  auto input = ::arrow::RecordBatch::Make(input_schema, 3, {physical});
  EXPECT_THAT(
      ProjectRecordBatch(input, output_schema, projected_schema, projection,
                         {.first_row_id = 100}, nullptr, ::arrow::default_memory_pool()),
      IsError(ErrorKind::kInvalidArgument));

  ICEBERG_UNWRAP_OR_FAIL(
      auto output, ProjectRecordBatch(input, output_schema, projected_schema, projection,
                                      {.first_row_id = 100}, positions,
                                      ::arrow::default_memory_pool()));
  EXPECT_TRUE(output->column(0)->Equals(*positions));
  EXPECT_EQ(output->column(0)->data()->buffers[1], positions->data()->buffers[1]);
  EXPECT_TRUE(output->Equals(
      *RecordBatchFromJson(output_schema, "[[12, 112], [3, 103], [30, 130]]")));
}

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

TEST(ProjectRecordBatchTest, NestedUnknownFields) {
  Schema projected_schema({
      SchemaField::MakeRequired(1, "profile",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeRequired(2, "name", string()),
                                    SchemaField::MakeOptional(3, "mystery", unknown()),
                                })),
      SchemaField::MakeRequired(
          4, "mysteries",
          std::make_shared<ListType>(SchemaField::MakeOptional(5, "element", unknown()))),
      SchemaField::MakeRequired(
          6, "properties",
          std::make_shared<MapType>(SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeOptional(8, "value", unknown()))),
  });

  Schema source_schema({
      SchemaField::MakeRequired(1, "profile",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeRequired(2, "name", string()),
                                    SchemaField::MakeOptional(3, "mystery", int32()),
                                })),
      SchemaField::MakeRequired(
          4, "mysteries",
          std::make_shared<ListType>(SchemaField::MakeOptional(5, "element", int32()))),
      SchemaField::MakeRequired(
          6, "properties",
          std::make_shared<MapType>(SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeOptional(8, "value", int32()))),
  });

  const std::string input_json = R"([
    {"profile": {"name": "Person0", "mystery": 10}, "mysteries": [1, 2], "properties": [["a", 100], ["b", 200]]},
    {"profile": {"name": "Person1", "mystery": null}, "mysteries": [], "properties": []}
  ])";
  const std::string expected_json = R"([
    {"profile": {"name": "Person0", "mystery": null}, "mysteries": [null, null], "properties": [["a", null], ["b", null]]},
    {"profile": {"name": "Person1", "mystery": null}, "mysteries": [], "properties": []}
  ])";

  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(projected_schema, source_schema,
                                                   input_json, expected_json));
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

TEST(ProjectRecordBatchTest, StructWithMissingDefaultFields) {
  Schema projected_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      // Missing required field with an initial-default: filled with the default.
      SchemaField(2, "score", int64(), /*optional=*/false, /*doc=*/{},
                  std::make_shared<const Literal>(Literal::Long(100))),
      // Missing optional field with an initial-default: also filled, not null.
      SchemaField(3, "grade", string(), /*optional=*/true, /*doc=*/{},
                  std::make_shared<const Literal>(Literal::String("A"))),
  });

  Schema source_schema({
      SchemaField::MakeRequired(1, "id", int32()),
  });

  const std::string input_json = R"([
    {"id": 1},
    {"id": 2}
  ])";
  const std::string expected_json = R"([
    {"id": 1, "score": 100, "grade": "A"},
    {"id": 2, "score": 100, "grade": "A"}
  ])";

  ASSERT_NO_FATAL_FAILURE(VerifyProjectRecordBatch(projected_schema, source_schema,
                                                   input_json, expected_json));
}

TEST(ProjectRecordBatchTest, NestedStructWithMissingDefaultField) {
  Schema projected_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeRequired(
          2, "person",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeRequired(3, "name", string()),
              // Missing in source; filled with its initial-default.
              SchemaField(4, "age", int32(), /*optional=*/false, /*doc=*/{},
                          std::make_shared<const Literal>(Literal::Int(18))),
          })),
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
    {"id": 100, "person": {"name": "Employee0", "age": 18}},
    {"id": 101, "person": {"name": "Employee1", "age": 18}}
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

TEST(ProjectRecordBatchTest, LargeListOfIntegers) {
  // Create a LargeListArray manually (JSON parsing creates regular ListArray)
  auto value_builder = std::make_shared<::arrow::Int32Builder>();
  ::arrow::LargeListBuilder list_builder(::arrow::default_memory_pool(), value_builder,
                                         ::arrow::large_list(::arrow::int32()));

  // Build: [[1, 2, 3], [4, 5]]
  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(value_builder->Append(1).ok());
  ASSERT_TRUE(value_builder->Append(2).ok());
  ASSERT_TRUE(value_builder->Append(3).ok());

  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(value_builder->Append(4).ok());
  ASSERT_TRUE(value_builder->Append(5).ok());

  auto large_list_array_result = list_builder.Finish();
  ASSERT_TRUE(large_list_array_result.ok());
  auto large_list_array = large_list_array_result.ValueOrDie();

  // Create input record batch with the LargeListArray
  auto input_arrow_schema =
      ::arrow::schema({::arrow::field("numbers", ::arrow::large_list(::arrow::int32()))});
  auto input_record_batch =
      ::arrow::RecordBatch::Make(input_arrow_schema, 2, {large_list_array});

  // Create Iceberg schema (uses ListType which maps to both LIST and LARGE_LIST)
  Schema iceberg_schema({
      SchemaField::MakeRequired(
          1, "numbers",
          std::make_shared<ListType>(SchemaField::MakeRequired(2, "element", int32()))),
  });

  // Create schema projection
  auto schema_projection_result =
      Project(iceberg_schema, iceberg_schema, /*prune_source=*/false);
  ASSERT_THAT(schema_projection_result, IsOk());
  auto schema_projection = std::move(schema_projection_result.value());

  // Create output Arrow schema with LargeListType
  auto output_arrow_schema =
      ::arrow::schema({::arrow::field("numbers", ::arrow::large_list(::arrow::int32()))});

  // Project the record batch
  auto project_result = ProjectRecordBatch(
      input_record_batch, output_arrow_schema, iceberg_schema, schema_projection,
      /*metadata_context=*/{}, /*row_positions=*/nullptr, ::arrow::default_memory_pool());
  ASSERT_THAT(project_result, IsOk());
  auto projected_record_batch = std::move(project_result.value());

  // Verify the result is a LargeListArray
  ASSERT_EQ(projected_record_batch->num_columns(), 1);
  ASSERT_EQ(projected_record_batch->column(0)->type()->id(), ::arrow::Type::LARGE_LIST);

  // Verify the values
  auto projected_array = std::static_pointer_cast<::arrow::LargeListArray>(
      projected_record_batch->column(0));
  ASSERT_EQ(projected_array->length(), 2);

  // First list: [1, 2, 3]
  auto first_list = projected_array->value_slice(0);
  ASSERT_EQ(first_list->length(), 3);
  auto first_values = std::static_pointer_cast<::arrow::Int32Array>(first_list);
  EXPECT_EQ(first_values->Value(0), 1);
  EXPECT_EQ(first_values->Value(1), 2);
  EXPECT_EQ(first_values->Value(2), 3);

  // Second list: [4, 5]
  auto second_list = projected_array->value_slice(1);
  ASSERT_EQ(second_list->length(), 2);
  auto second_values = std::static_pointer_cast<::arrow::Int32Array>(second_list);
  EXPECT_EQ(second_values->Value(0), 4);
  EXPECT_EQ(second_values->Value(1), 5);
}

TEST(ProjectRecordBatchTest, LargeListOfStructs) {
  // Create a LargeListArray with struct elements
  auto name_builder = std::make_shared<::arrow::StringBuilder>();
  auto age_builder = std::make_shared<::arrow::Int32Builder>();
  std::vector<std::shared_ptr<::arrow::ArrayBuilder>> field_builders = {name_builder,
                                                                        age_builder};
  auto struct_type = ::arrow::struct_(
      {::arrow::field("name", ::arrow::utf8()), ::arrow::field("age", ::arrow::int32())});
  auto struct_builder = std::make_shared<::arrow::StructBuilder>(
      struct_type, ::arrow::default_memory_pool(), field_builders);

  ::arrow::LargeListBuilder list_builder(::arrow::default_memory_pool(), struct_builder,
                                         ::arrow::large_list(struct_type));

  // Build: [[{name: "Alice", age: 30}], [{name: "Bob", age: 25}, {name: "Carol", age:
  // 35}]]
  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(struct_builder->Append().ok());
  ASSERT_TRUE(name_builder->Append("Alice").ok());
  ASSERT_TRUE(age_builder->Append(30).ok());

  ASSERT_TRUE(list_builder.Append().ok());
  ASSERT_TRUE(struct_builder->Append().ok());
  ASSERT_TRUE(name_builder->Append("Bob").ok());
  ASSERT_TRUE(age_builder->Append(25).ok());
  ASSERT_TRUE(struct_builder->Append().ok());
  ASSERT_TRUE(name_builder->Append("Carol").ok());
  ASSERT_TRUE(age_builder->Append(35).ok());

  auto large_list_array_result = list_builder.Finish();
  ASSERT_TRUE(large_list_array_result.ok());
  auto large_list_array = large_list_array_result.ValueOrDie();

  // Create input record batch
  auto input_arrow_schema =
      ::arrow::schema({::arrow::field("people", ::arrow::large_list(struct_type))});
  auto input_record_batch =
      ::arrow::RecordBatch::Make(input_arrow_schema, 2, {large_list_array});

  // Create Iceberg schema
  Schema iceberg_schema({
      SchemaField::MakeRequired(1, "people",
                                std::make_shared<ListType>(SchemaField::MakeRequired(
                                    2, "element",
                                    std::make_shared<StructType>(std::vector<SchemaField>{
                                        SchemaField::MakeRequired(3, "name", string()),
                                        SchemaField::MakeRequired(4, "age", int32()),
                                    })))),
  });

  // Create schema projection
  auto schema_projection_result =
      Project(iceberg_schema, iceberg_schema, /*prune_source=*/false);
  ASSERT_THAT(schema_projection_result, IsOk());
  auto schema_projection = std::move(schema_projection_result.value());

  // Create output Arrow schema with LargeListType
  auto output_arrow_schema =
      ::arrow::schema({::arrow::field("people", ::arrow::large_list(struct_type))});

  // Project the record batch
  auto project_result = ProjectRecordBatch(
      input_record_batch, output_arrow_schema, iceberg_schema, schema_projection,
      /*metadata_context=*/{}, /*row_positions=*/nullptr, ::arrow::default_memory_pool());
  ASSERT_THAT(project_result, IsOk());
  auto projected_record_batch = std::move(project_result.value());

  // Verify the result is a LargeListArray
  ASSERT_EQ(projected_record_batch->num_columns(), 1);
  ASSERT_EQ(projected_record_batch->column(0)->type()->id(), ::arrow::Type::LARGE_LIST);

  auto projected_array = std::static_pointer_cast<::arrow::LargeListArray>(
      projected_record_batch->column(0));
  ASSERT_EQ(projected_array->length(), 2);

  // Verify first list has 1 element
  EXPECT_EQ(projected_array->value_length(0), 1);
  // Verify second list has 2 elements
  EXPECT_EQ(projected_array->value_length(1), 2);
}

}  // namespace iceberg::parquet
