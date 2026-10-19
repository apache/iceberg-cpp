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

/// \file arrow_c_data_util_test.cc
/// Verifies ProjectBatch behavior across registered implementations.

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/extension_type.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_register.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg::internal {

namespace {

std::shared_ptr<::arrow::RecordBatch> MakeBatch(const Schema& schema,
                                                std::string_view json) {
  ArrowSchema c_schema;
  EXPECT_THAT(ToArrowSchema(schema, &c_schema), IsOk());
  // ImportSchema takes ownership of c_schema and calls release.
  auto arrow_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();
  auto struct_type = ::arrow::struct_(arrow_schema->fields());
  return ::arrow::RecordBatch::FromStructArray(
             ::arrow::json::ArrayFromJSONString(struct_type, std::string(json))
                 .ValueOrDie())
      .ValueOrDie();
}

ProjectionContext::ProjectBatchFunction ArrowComputeFunction() {
  arrow::RegisterAll();
  auto function = ProjectionContext::ResolveProjectBatchFunction();
  EXPECT_NE(function, nullptr);
  return function;
}

std::shared_ptr<::arrow::RecordBatch> RunProjectBatch(
    const ::arrow::RecordBatch& batch, const std::vector<int32_t>& alive_indices,
    const Schema& required_schema, const Schema& projected_schema,
    ProjectionContext::ProjectBatchFunction project_batch_function) {
  ArrowSchema c_schema;
  ArrowArray c_array;
  EXPECT_TRUE(::arrow::ExportRecordBatch(batch, &c_array, &c_schema).ok());
  ArrowSchemaGuard schema_guard(&c_schema);
  ArrowArrayGuard array_guard(&c_array);

  auto projection =
      ProjectionContext::Make(required_schema, projected_schema, project_batch_function);
  EXPECT_THAT(projection, IsOk());

  auto result = ProjectBatch(&c_array, alive_indices, projection.value());
  EXPECT_THAT(result, IsOk());

  ArrowSchema out_c_schema;
  EXPECT_THAT(ToArrowSchema(projected_schema, &out_c_schema), IsOk());
  auto arrow_out_schema = ::arrow::ImportSchema(&out_c_schema).ValueOrDie();

  ArrowArray out_array = std::exchange(result.value(), ArrowArray{});
  return ::arrow::ImportRecordBatch(&out_array, arrow_out_schema).ValueOrDie();
}

void ExpectProjectBatch(const ::arrow::RecordBatch& batch,
                        const std::vector<int32_t>& alive_indices,
                        const Schema& required_schema, const Schema& projected_schema,
                        std::string_view expected_json) {
  auto expected = MakeBatch(projected_schema, expected_json);
  auto nanoarrow =
      RunProjectBatch(batch, alive_indices, required_schema, projected_schema, nullptr);
  auto arrow_compute = RunProjectBatch(batch, alive_indices, required_schema,
                                       projected_schema, ArrowComputeFunction());

  EXPECT_TRUE(nanoarrow->Equals(*expected)) << "nanoarrow:\n"
                                            << nanoarrow->ToString() << "expected:\n"
                                            << expected->ToString();
  EXPECT_TRUE(arrow_compute->Equals(*expected))
      << "arrow_compute:\n"
      << arrow_compute->ToString() << "expected:\n"
      << expected->ToString();
  EXPECT_TRUE(nanoarrow->Equals(*arrow_compute))
      << "nanoarrow:\n"
      << nanoarrow->ToString() << "arrow_compute:\n"
      << arrow_compute->ToString();
}

std::shared_ptr<Schema> MakeFullSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "name", string()),
                               SchemaField::MakeOptional(3, "score", float64())});
}

Result<std::shared_ptr<::arrow::RecordBatch>> AlignForWrite(
    const Schema& input_schema, const Schema& write_schema,
    const ::arrow::RecordBatch& input) {
  ICEBERG_ASSIGN_OR_RAISE(auto projection,
                          WriteProjection::Make(input_schema, write_schema));
  ArrowArray c_array;
  ICEBERG_ARROW_RETURN_NOT_OK(::arrow::ExportRecordBatch(input, &c_array));
  ICEBERG_ASSIGN_OR_RAISE(auto aligned, projection->AlignBatch(&c_array));
  ArrowArrayGuard aligned_guard(&aligned);

  ArrowSchema c_schema;
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(write_schema, &c_schema));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto arrow_schema, ::arrow::ImportSchema(&c_schema));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto result,
                                 ::arrow::ImportRecordBatch(&aligned, arrow_schema));
  ICEBERG_ARROW_RETURN_NOT_OK(result->ValidateFull());
  return result;
}

SchemaField DefaultedField(int32_t id, std::string name, std::shared_ptr<Type> type,
                           bool optional, Literal initial_default,
                           Literal write_default) {
  return {id,
          std::move(name),
          std::move(type),
          optional,
          {},
          std::make_shared<const Literal>(std::move(initial_default)),
          std::make_shared<const Literal>(std::move(write_default))};
}

}  // namespace

TEST(ProjectBatchTest, ProjectSelectedRowsWithoutColumnProjection) {
  auto schema = MakeFullSchema();
  auto batch = MakeBatch(*schema, R"([[1,"a",1.0],[2,"b",2.0],[3,"c",3.0],[4,"d",4.0]])");
  std::vector<int32_t> alive = {0, 2};

  ExpectProjectBatch(*batch, alive, *schema, *schema, R"([[1,"a",1.0],[3,"c",3.0]])");
}

TEST(ProjectBatchTest, ProjectColumnsWithoutRowFiltering) {
  auto full_schema = MakeFullSchema();
  auto projected = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "name", string())});
  auto batch = MakeBatch(*full_schema, R"([[1,"a",1.0],[2,"b",2.0],[3,"c",3.0]])");
  std::vector<int32_t> alive = {0, 1, 2};

  ExpectProjectBatch(*batch, alive, *full_schema, *projected,
                     R"([[1,"a"],[2,"b"],[3,"c"]])");
}

TEST(ProjectBatchTest, ProjectSelectedRowsAndReorderColumns) {
  auto full_schema = MakeFullSchema();
  // Reorder: score(3) before name(2), drop id(1).
  auto projected = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(3, "score", float64()),
                               SchemaField::MakeOptional(2, "name", string())});
  auto batch = MakeBatch(*full_schema, R"([[1,"a",1.0],[2,"b",2.0],[3,"c",3.0]])");
  std::vector<int32_t> alive = {1, 2};

  ExpectProjectBatch(*batch, alive, *full_schema, *projected, R"([[2.0,"b"],[3.0,"c"]])");
}

TEST(ProjectBatchTest, NullValues) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(2, "name", string())});
  auto batch = MakeBatch(*schema, R"([[1,null],[2,"b"],[3,null]])");
  std::vector<int32_t> alive = {0, 2};

  ExpectProjectBatch(*batch, alive, *schema, *schema, R"([[1,null],[3,null]])");
}

TEST(ProjectBatchTest, EmptyRowSelection) {
  auto schema = MakeFullSchema();
  auto batch = MakeBatch(*schema, R"([[1,"a",1.0],[2,"b",2.0]])");
  std::vector<int32_t> alive = {};

  ExpectProjectBatch(*batch, alive, *schema, *schema, R"([])");
}

TEST(ProjectBatchTest, ProjectionRejectsNestedPruning) {
  auto input_schema = Schema(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "person",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeOptional(2, "name", string()),
                                    SchemaField::MakeOptional(3, "age", int32()),
                                })),
  });
  auto output_schema = Schema(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "person",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeOptional(2, "name", string()),
                                })),
  });

  auto projection = ProjectionContext::Make(input_schema, output_schema, nullptr);

  EXPECT_THAT(projection, IsError(ErrorKind::kInvalidArgument));
}

TEST(ProjectBatchTest, RejectsNonMonotonicStringOffsets) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "value", string())});

  // Arrow's builders always emit monotonic offsets, so a batch with a
  // corrupt offsets buffer has to be hand-assembled via the nanoarrow C ABI.
  // offsets = [0, 5, 2] with a 2-byte data buffer: DEFAULT validation only
  // checks the first (0) and last (2) offsets, both of which are consistent
  // with the 2-byte buffer, so it accepts this array. FULL validation also
  // walks the offsets buffer and rejects it because 2 < 5.
  ArrowArray root;
  ASSERT_EQ(ArrowArrayInitFromType(&root, NANOARROW_TYPE_STRUCT), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAllocateChildren(&root, 1), NANOARROW_OK);

  ArrowArray* value_array = root.children[0];
  ASSERT_EQ(ArrowArrayInitFromType(value_array, NANOARROW_TYPE_STRING), NANOARROW_OK);
  const std::array<int32_t, 3> raw_offsets = {0, 5, 2};
  ASSERT_EQ(ArrowBufferAppend(ArrowArrayBuffer(value_array, 1), raw_offsets.data(),
                              raw_offsets.size() * sizeof(int32_t)),
            NANOARROW_OK);
  ASSERT_EQ(ArrowBufferAppend(ArrowArrayBuffer(value_array, 2), "ab", 2), NANOARROW_OK);
  value_array->length = 2;
  value_array->null_count = 0;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(value_array, nullptr), NANOARROW_OK);

  root.length = 2;
  root.null_count = 0;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&root, nullptr), NANOARROW_OK);

  auto projection = ProjectionContext::Make(*schema, *schema, nullptr);
  ASSERT_THAT(projection, IsOk());

  // No rows are selected, so the malformed offsets must be caught by batch
  // validation itself rather than by appending the corrupt row.
  auto result = ProjectBatch(&root, /*row_indices=*/{}, projection.value());
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArrowData));
}

TEST(WriteProjectionTest, IdenticalSchemasTransferBatchWithoutConversion) {
  auto input_schema = MakeFullSchema();
  auto write_schema = MakeFullSchema();
  auto input =
      MakeBatch(*input_schema, R"([[1,"alice",1.0],[2,null,2.0],[3,"carol",3.0]])")
          ->Slice(1, 2);
  ICEBERG_UNWRAP_OR_FAIL(auto projection,
                         WriteProjection::Make(*input_schema, *write_schema));
  ArrowArray c_array;
  ASSERT_TRUE(::arrow::ExportRecordBatch(*input, &c_array).ok());
  auto original = c_array;

  ICEBERG_UNWRAP_OR_FAIL(auto aligned, projection->AlignBatch(&c_array));
  ArrowArrayGuard aligned_guard(&aligned);
  EXPECT_EQ(c_array.release, nullptr);
  EXPECT_EQ(aligned.private_data, original.private_data);
  EXPECT_EQ(aligned.release, original.release);
  EXPECT_EQ(aligned.buffers, original.buffers);
  EXPECT_EQ(aligned.children, original.children);

  auto actual = ::arrow::ImportRecordBatch(&aligned, input->schema()).ValueOrDie();
  ASSERT_TRUE(actual->ValidateFull().ok());
  EXPECT_TRUE(actual->Equals(*input));
}

TEST(WriteProjectionTest, MaterializesPrimitiveDefaults) {
  ICEBERG_UNWRAP_OR_FAIL(auto uuid,
                         Uuid::FromString("30313233-3435-3637-3839-616263646566"));
  struct TestColumn {
    std::string name;
    Literal value;
    std::string_view expected_json;
  };
  const std::vector<TestColumn> columns = {
      {.name = "decimal",
       .value = Literal::Decimal(-12345, 9, 2),
       .expected_json = R"(["-123.45","-123.45"])"},
      {.name = "uuid",
       .value = Literal::UUID(uuid),
       .expected_json = R"(["0123456789abcdef","0123456789abcdef"])"},
      {.name = "binary",
       .value = Literal::Binary({'a', 0, 'b'}),
       .expected_json = R"(["a\u0000b","a\u0000b"])"},
      {.name = "fixed",
       .value = Literal::Fixed({0, 'x', 0}),
       .expected_json = R"(["\u0000x\u0000","\u0000x\u0000"])"},
      {.name = "float", .value = Literal::Float(1.25F), .expected_json = "[1.25,1.25]"},
      {.name = "double", .value = Literal::Double(-2.5), .expected_json = "[-2.5,-2.5]"},
      {.name = "date", .value = Literal::Date(-1), .expected_json = "[-1,-1]"},
      {.name = "timestamp",
       .value = Literal::Timestamp(-1000001),
       .expected_json = "[-1000001,-1000001]"},
  };
  Schema input_schema({SchemaField::MakeRequired(1, "id", int32())});
  std::vector<SchemaField> write_fields(input_schema.fields().begin(),
                                        input_schema.fields().end());
  for (const auto& column : columns) {
    write_fields.push_back(
        SchemaField::MakeRequired(static_cast<int32_t>(write_fields.size() + 1),
                                  column.name, column.value.type())
            .WithWriteDefault(std::make_shared<const Literal>(column.value)));
  }
  Schema write_schema(std::move(write_fields));
  auto input = MakeBatch(input_schema, "[[1],[2]]");
  ICEBERG_UNWRAP_OR_FAIL(auto actual, AlignForWrite(input_schema, write_schema, *input));

  for (size_t index = 0; index < columns.size(); ++index) {
    SCOPED_TRACE(columns[index].name);
    auto actual_column = actual->column(static_cast<int>(index + 1));
    if (actual_column->type_id() == ::arrow::Type::EXTENSION) {
      actual_column =
          std::static_pointer_cast<::arrow::ExtensionArray>(actual_column)->storage();
    }
    auto expected = ::arrow::json::ArrayFromJSONString(
                        actual_column->type(), std::string(columns[index].expected_json))
                        .ValueOrDie();
    EXPECT_TRUE(actual_column->Equals(expected)) << actual_column->ToString();
  }
}

TEST(WriteProjectionTest, DetachedChildRetainsInputAndReleasesItOnce) {
  Schema input_schema({SchemaField::MakeRequired(1, "id", int32())});
  Schema write_schema({SchemaField::MakeRequired(1, "id", int32()),
                       SchemaField::MakeOptional(2, "name", string())});
  ICEBERG_UNWRAP_OR_FAIL(auto projection,
                         WriteProjection::Make(input_schema, write_schema));
  auto input = MakeBatch(input_schema, "[[1],[2]]");
  struct ReleaseState {
    ArrowArray original{};
    int count = 0;
  } state;
  ArrowArray c_array;
  ASSERT_TRUE(::arrow::ExportRecordBatch(*input, &c_array).ok());
  input.reset();
  state.original = c_array;
  c_array.private_data = &state;
  c_array.release = [](ArrowArray* array) {
    auto* state = static_cast<ReleaseState*>(array->private_data);
    ++state->count;
    *array = state->original;
    array->release(array);
  };
  ArrowArray retained_child{};
  ArrowArrayGuard child_guard(&retained_child);
  {
    ICEBERG_UNWRAP_OR_FAIL(auto aligned, projection->AlignBatch(&c_array));
    ArrowArrayGuard aligned_guard(&aligned);
    EXPECT_EQ(c_array.release, nullptr);
    retained_child = std::exchange(*aligned.children[0], ArrowArray{});
  }
  auto actual = ::arrow::ImportArray(&retained_child, ::arrow::int32()).ValueOrDie();
  auto expected =
      ::arrow::json::ArrayFromJSONString(::arrow::int32(), "[1,2]").ValueOrDie();
  EXPECT_TRUE(actual->Equals(expected));
  actual.reset();
  EXPECT_EQ(state.count, 1);
}

struct WriteProjectionParam {
  std::string name;
  std::vector<SchemaField> input_fields;
  std::vector<SchemaField> write_fields;
  std::string_view input_json;
  std::string_view expected_json;
  int64_t slice_offset = 0;
};

class WriteProjectionParamTest : public ::testing::TestWithParam<WriteProjectionParam> {};

TEST_P(WriteProjectionParamTest, AlignBatch) {
  const auto& param = GetParam();
  Schema input_schema(param.input_fields);
  Schema write_schema(param.write_fields);
  auto input = MakeBatch(input_schema, param.input_json)->Slice(param.slice_offset);

  ICEBERG_UNWRAP_OR_FAIL(auto actual, AlignForWrite(input_schema, write_schema, *input));
  auto expected = MakeBatch(write_schema, param.expected_json);
  EXPECT_TRUE(actual->Equals(*expected)) << actual->ToString();
}

std::vector<WriteProjectionParam> WriteAlignmentCases() {
  std::vector<WriteProjectionParam> cases;

  {
    auto input_item = std::make_shared<StructType>(std::vector<SchemaField>{
        SchemaField::MakeRequired(4, "name", string()),
        SchemaField::MakeRequired(5, "score", int32()),
    });
    auto write_item = std::make_shared<StructType>(std::vector<SchemaField>{
        SchemaField::MakeRequired(5, "rating", int32()),
        SchemaField::MakeRequired(4, "renamed", string()),
        DefaultedField(11, "age", int32(), /*optional=*/false, Literal::Int(10),
                       Literal::Int(18)),
    });
    auto input_value = std::make_shared<StructType>(std::vector<SchemaField>{
        SchemaField::MakeRequired(9, "label", string()),
        SchemaField::MakeRequired(10, "code", int32()),
    });
    auto write_value = std::make_shared<StructType>(std::vector<SchemaField>{
        SchemaField::MakeRequired(10, "code_new", int32()),
        SchemaField::MakeRequired(9, "title", string()),
        DefaultedField(12, "flag", boolean(), /*optional=*/false, Literal::Boolean(false),
                       Literal::Boolean(true)),
    });
    auto nested_list = [](std::shared_ptr<Type> element) {
      return list(SchemaField::MakeOptional(
          2, "element",
          list(SchemaField::MakeOptional(3, "element", std::move(element)))));
    };
    auto nested_map = [](std::shared_ptr<Type> value) {
      return map(SchemaField::MakeRequired(7, "key", string()),
                 SchemaField::MakeOptional(8, "value", std::move(value)));
    };
    cases.push_back({
        .name = "StructsInsideSlicedNestedCollections",
        .input_fields = {SchemaField::MakeOptional(1, "items", nested_list(input_item)),
                         SchemaField::MakeOptional(6, "mapping",
                                                   nested_map(input_value))},
        .write_fields = {SchemaField::MakeOptional(1, "items", nested_list(write_item)),
                         SchemaField::MakeOptional(6, "mapping",
                                                   nested_map(write_value))},
        .input_json = R"([
    [[[]],[]],
    [[[{"name":"alice","score":1},null],[]],
     [["a",{"label":"bob","code":2}],["b",null]]],
    [null,null],
    [[],[]]
  ])",
        .expected_json = R"([
    [[[{"rating":1,"renamed":"alice","age":18},null],[]],
     [["a",{"code_new":2,"title":"bob","flag":true}],["b",null]]],
    [null,null],
    [[],[]]
  ])",
        .slice_offset = 1,
    });
  }

  {
    auto input_info = std::make_shared<StructType>(
        std::vector<SchemaField>{SchemaField::MakeRequired(4, "age", int32())});
    auto write_info = std::make_shared<StructType>(std::vector<SchemaField>{
        SchemaField::MakeRequired(4, "age", int32()),
        DefaultedField(5, "score", int32(), /*optional=*/false, Literal::Int(0),
                       Literal::Int(99)),
    });
    auto input_person = std::make_shared<StructType>(std::vector<SchemaField>{
        SchemaField::MakeRequired(2, "name", string()),
        SchemaField::MakeOptional(3, "info", input_info),
    });
    auto write_person = std::make_shared<StructType>(std::vector<SchemaField>{
        SchemaField::MakeOptional(3, "info", write_info),
        SchemaField::MakeRequired(2, "username", string()),
        SchemaField::MakeOptional(6, "comment", string()),
    });
    cases.push_back({
        .name = "SlicedStructOffsetsAndParentNulls",
        .input_fields = {SchemaField::MakeOptional(1, "person", input_person)},
        .write_fields = {SchemaField::MakeOptional(1, "person", write_person)},
        .input_json = R"([
    [{"name":"first","info":{"age":10}}],
    [null],
    [{"name":"bob","info":null}],
    [{"name":"carol","info":{"age":30}}]
  ])",
        .expected_json = R"([
    [null],
    [{"info":null,"username":"bob","comment":null}],
    [{"info":{"age":30,"score":99},"username":"carol","comment":null}]
  ])",
        .slice_offset = 1,
    });
  }

  return cases;
}

INSTANTIATE_TEST_SUITE_P(WriteAlignment, WriteProjectionParamTest,
                         ::testing::ValuesIn(WriteAlignmentCases()),
                         [](const ::testing::TestParamInfo<WriteProjectionParam>& info) {
                           return info.param.name;
                         });

}  // namespace iceberg::internal
