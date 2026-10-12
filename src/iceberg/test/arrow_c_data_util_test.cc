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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/memory_pool.h>
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
  ArrowArray c_array;
  ICEBERG_ARROW_RETURN_NOT_OK(::arrow::ExportRecordBatch(input, &c_array));
  ICEBERG_ASSIGN_OR_RAISE(auto aligned,
                          arrow::AlignBatchForWrite(&c_array, input_schema, write_schema,
                                                    ::arrow::default_memory_pool()));

  ArrowSchema c_schema;
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(write_schema, &c_schema));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto arrow_schema, ::arrow::ImportSchema(&c_schema));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto result,
                                 ::arrow::ImportRecordBatch(&aligned, arrow_schema));
  ICEBERG_ARROW_RETURN_NOT_OK(result->ValidateFull());
  return result;
}

Result<std::shared_ptr<::arrow::RecordBatch>> AlignForWrite(const Schema& input_schema,
                                                            const Schema& write_schema,
                                                            std::string_view input_json) {
  return AlignForWrite(input_schema, write_schema, *MakeBatch(input_schema, input_json));
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

TEST(AlignBatchForWriteTest, IdenticalSchemasTransferBatchWithoutConversion) {
  auto input_schema = MakeFullSchema();
  auto write_schema = MakeFullSchema();
  auto input =
      MakeBatch(*input_schema, R"([[1,"alice",1.0],[2,null,2.0],[3,"carol",3.0]])")
          ->Slice(1, 2);
  ArrowArray c_array;
  ASSERT_TRUE(::arrow::ExportRecordBatch(*input, &c_array).ok());
  auto original = c_array;

  ICEBERG_UNWRAP_OR_FAIL(auto aligned,
                         arrow::AlignBatchForWrite(&c_array, *input_schema, *write_schema,
                                                   ::arrow::default_memory_pool()));
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

TEST(AlignBatchForWriteTest, ReordersAndMaterializesWriteDefaultsAndNulls) {
  Schema input_schema({SchemaField::MakeOptional(2, "name", string()),
                       SchemaField::MakeRequired(1, "id", int32())});
  Schema write_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "name", string()),
      DefaultedField(3, "score", int32(), /*optional=*/false, Literal::Int(42),
                     Literal::Int(7)),
      SchemaField::MakeOptional(4, "comment", string()),
  });

  ICEBERG_UNWRAP_OR_FAIL(auto actual, AlignForWrite(input_schema, write_schema,
                                                    R"([["alice",1],[null,2]])"));
  auto expected = MakeBatch(write_schema, R"([[1,"alice",7,null],[2,null,7,null]])");
  EXPECT_TRUE(actual->Equals(*expected)) << "actual:\n" << actual->ToString();
}

TEST(AlignBatchForWriteTest, RejectsMissingRequiredFieldWithoutWriteDefault) {
  Schema input_schema({SchemaField::MakeRequired(1, "id", int32())});
  Schema write_schema({SchemaField::MakeRequired(1, "id", int32()),
                       SchemaField::MakeRequired(2, "required", string())});

  auto result = AlignForWrite(input_schema, write_schema, R"([[1]])");

  EXPECT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(result, HasErrorMessage("without a write-default"));
}

TEST(AlignBatchForWriteTest, RecursivelyAlignsNestedStructs) {
  auto input_person = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeRequired(2, "name", string())});
  auto write_person = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeRequired(2, "name", string()),
      DefaultedField(3, "age", int32(), /*optional=*/false, Literal::Int(10),
                     Literal::Int(18)),
  });
  Schema input_schema({SchemaField::MakeOptional(1, "person", std::move(input_person))});
  Schema write_schema({SchemaField::MakeOptional(1, "person", std::move(write_person))});

  ICEBERG_UNWRAP_OR_FAIL(auto actual, AlignForWrite(input_schema, write_schema,
                                                    R"([[{"name":"alice"}],[null]])"));
  auto expected = MakeBatch(write_schema, R"([[{"name":"alice","age":18}],[null]])");
  EXPECT_TRUE(actual->Equals(*expected)) << "actual:\n" << actual->ToString();
}

TEST(AlignBatchForWriteTest, RejectsIncompatibleTypes) {
  Schema input_schema({SchemaField::MakeRequired(1, "id", int32())});
  Schema write_schema({SchemaField::MakeRequired(1, "id", int64())});

  auto result = AlignForWrite(input_schema, write_schema, R"([[1]])");

  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
  EXPECT_THAT(result, HasErrorMessage("type promotion is not allowed"));
}

TEST(AlignBatchForWriteTest, RejectsInputFieldsOutsideWriteSchema) {
  Schema input_schema({SchemaField::MakeRequired(1, "id", int32()),
                       SchemaField::MakeOptional(2, "extra", string())});
  Schema write_schema({SchemaField::MakeRequired(1, "id", int32())});

  auto result = AlignForWrite(input_schema, write_schema, R"([[1,"extra"]])");

  EXPECT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(result, HasErrorMessage("Input field id 2"));
}

TEST(AlignBatchForWriteTest, PreservesCollectionsWhenAddingAndReorderingFields) {
  auto list_field = SchemaField::MakeOptional(
      1, "items", list(SchemaField::MakeOptional(2, "element", int32())));
  auto map_field =
      SchemaField::MakeOptional(3, "properties",
                                map(SchemaField::MakeRequired(4, "key", string()),
                                    SchemaField::MakeOptional(5, "value", int32())));
  Schema input_schema({list_field, map_field});
  Schema write_schema({
      map_field,
      list_field,
      DefaultedField(6, "name", string(), /*optional=*/false, Literal::String("initial"),
                     Literal::String("written")),
  });

  ICEBERG_UNWRAP_OR_FAIL(auto actual,
                         AlignForWrite(input_schema, write_schema,
                                       R"([[[1,2],[["a",3]]],[null,[]],[[],null]])"));
  auto expected = MakeBatch(
      write_schema,
      R"([[[["a",3]],[1,2],"written"],[[],null,"written"],[null,[],"written"]])");
  EXPECT_TRUE(actual->Equals(*expected)) << actual->ToString();
}

TEST(AlignBatchForWriteTest, AlignsStructsInsideSlicedNestedCollections) {
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
        2, "element", list(SchemaField::MakeOptional(3, "element", std::move(element)))));
  };
  auto nested_map = [](std::shared_ptr<Type> value) {
    return map(SchemaField::MakeRequired(7, "key", string()),
               SchemaField::MakeOptional(8, "value", std::move(value)));
  };
  Schema input_schema({SchemaField::MakeOptional(1, "items", nested_list(input_item)),
                       SchemaField::MakeOptional(6, "mapping", nested_map(input_value))});
  Schema write_schema({SchemaField::MakeOptional(1, "items", nested_list(write_item)),
                       SchemaField::MakeOptional(6, "mapping", nested_map(write_value))});
  auto input = MakeBatch(input_schema, R"([
    [[[]],[]],
    [[[{"name":"alice","score":1},null],[]],
     [["a",{"label":"bob","code":2}],["b",null]]],
    [null,null],
    [[],[]]
  ])");

  ICEBERG_UNWRAP_OR_FAIL(auto actual,
                         AlignForWrite(input_schema, write_schema, *input->Slice(1, 3)));
  auto expected = MakeBatch(write_schema, R"([
    [[[{"rating":1,"renamed":"alice","age":18},null],[]],
     [["a",{"code_new":2,"title":"bob","flag":true}],["b",null]]],
    [null,null],
    [[],[]]
  ])");
  EXPECT_TRUE(actual->Equals(*expected)) << actual->ToString();
}

TEST(AlignBatchForWriteTest, PreservesSlicedStructOffsetsAndParentNulls) {
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
  Schema input_schema({SchemaField::MakeOptional(1, "person", input_person)});
  Schema write_schema({SchemaField::MakeOptional(1, "person", write_person)});
  auto input = MakeBatch(input_schema, R"([
    [{"name":"first","info":{"age":10}}],
    [null],
    [{"name":"bob","info":null}],
    [{"name":"carol","info":{"age":30}}]
  ])");

  ICEBERG_UNWRAP_OR_FAIL(auto actual,
                         AlignForWrite(input_schema, write_schema, *input->Slice(1, 3)));
  auto expected = MakeBatch(write_schema, R"([
    [null],
    [{"info":null,"username":"bob","comment":null}],
    [{"info":{"age":30,"score":99},"username":"carol","comment":null}]
  ])");
  EXPECT_TRUE(actual->Equals(*expected)) << actual->ToString();
}

TEST(AlignBatchForWriteTest, RejectsUnknownFieldsInsideCollections) {
  auto input_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeRequired(4, "value", int32()),
      SchemaField::MakeOptional(5, "extra", string()),
  });
  auto write_struct = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeRequired(4, "value", int32())});
  for (bool use_map : {false, true}) {
    auto wrap = [use_map](std::shared_ptr<Type> child) -> std::shared_ptr<Type> {
      if (use_map) {
        return map(SchemaField::MakeRequired(2, "key", string()),
                   SchemaField::MakeOptional(3, "value", std::move(child)));
      }
      return list(SchemaField::MakeOptional(
          2, "element", list(SchemaField::MakeOptional(3, "element", std::move(child)))));
    };
    Schema input_schema({SchemaField::MakeOptional(1, "items", wrap(input_struct))});
    Schema write_schema({SchemaField::MakeOptional(1, "items", wrap(write_struct))});

    auto result = AlignForWrite(input_schema, write_schema, R"([[[]]])");

    EXPECT_THAT(result, IsError(ErrorKind::kInvalidSchema));
    EXPECT_THAT(result, HasErrorMessage("Input field id 5"));
  }
}

TEST(AlignBatchForWriteTest, MaterializesUnknownInputAsTypedNulls) {
  Schema input_schema({SchemaField::MakeOptional(1, "value", unknown())});
  Schema write_schema({SchemaField::MakeOptional(1, "value", int32())});

  ICEBERG_UNWRAP_OR_FAIL(auto actual,
                         AlignForWrite(input_schema, write_schema, R"([[null],[null]])"));
  auto expected = MakeBatch(write_schema, R"([[null],[null]])");
  EXPECT_TRUE(actual->Equals(*expected)) << actual->ToString();
}

}  // namespace iceberg::internal
