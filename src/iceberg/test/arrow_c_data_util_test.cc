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
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_register.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

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

}  // namespace iceberg::internal
