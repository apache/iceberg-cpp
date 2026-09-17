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
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/write_schema_util_internal.h"
#include "iceberg/arrow_c_data.h"
#include "iceberg/expression/literal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg::arrow {
namespace {

std::shared_ptr<::arrow::RecordBatch> MakeBatch(const Schema& schema,
                                                std::string_view json) {
  ArrowSchema c_schema;
  EXPECT_THAT(ToArrowSchema(schema, &c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();
  auto array = ::arrow::json::ArrayFromJSONString(
                   ::arrow::struct_(arrow_schema->fields()), std::string(json))
                   .ValueOrDie();
  return ::arrow::RecordBatch::FromStructArray(array).ValueOrDie();
}

Result<std::shared_ptr<::arrow::RecordBatch>> Align(const Schema& input_schema,
                                                    const Schema& write_schema,
                                                    std::string_view input_json) {
  auto input = MakeBatch(input_schema, input_json);
  ArrowArray c_array;
  ICEBERG_ARROW_RETURN_NOT_OK(::arrow::ExportRecordBatch(*input, &c_array));
  ICEBERG_ASSIGN_OR_RAISE(auto aligned,
                          AlignBatchForWrite(&c_array, input_schema, write_schema,
                                             ::arrow::default_memory_pool()));

  ArrowSchema c_schema;
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(write_schema, &c_schema));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto arrow_schema, ::arrow::ImportSchema(&c_schema));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto result,
                                 ::arrow::ImportRecordBatch(&aligned, arrow_schema));
  return result;
}

SchemaField DefaultedField(int32_t id, std::string name, std::shared_ptr<Type> type,
                           bool optional, Literal initial_default,
                           Literal write_default) {
  return SchemaField(id, std::move(name), std::move(type), optional, {},
                     std::make_shared<const Literal>(std::move(initial_default)),
                     std::make_shared<const Literal>(std::move(write_default)));
}

}  // namespace

TEST(WriteSchemaUtilTest, ReordersAndMaterializesWriteDefaultsAndNulls) {
  Schema input_schema({SchemaField::MakeOptional(2, "name", string()),
                       SchemaField::MakeRequired(1, "id", int32())});
  Schema write_schema({
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "name", string()),
      DefaultedField(3, "score", int32(), /*optional=*/false, Literal::Int(42),
                     Literal::Int(7)),
      SchemaField::MakeOptional(4, "comment", string()),
  });

  ICEBERG_UNWRAP_OR_FAIL(auto actual,
                         Align(input_schema, write_schema, R"([["alice",1],[null,2]])"));
  auto expected = MakeBatch(write_schema, R"([[1,"alice",7,null],[2,null,7,null]])");
  EXPECT_TRUE(actual->Equals(*expected)) << "actual:\n" << actual->ToString();
}

TEST(WriteSchemaUtilTest, RejectsMissingRequiredFieldWithoutWriteDefault) {
  Schema input_schema({SchemaField::MakeRequired(1, "id", int32())});
  Schema write_schema({SchemaField::MakeRequired(1, "id", int32()),
                       SchemaField::MakeRequired(2, "required", string())});

  auto result = Align(input_schema, write_schema, R"([[1]])");

  EXPECT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(result, HasErrorMessage("without a write-default"));
}

TEST(WriteSchemaUtilTest, RecursivelyAlignsNestedStructs) {
  auto input_person = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeRequired(2, "name", string())});
  auto write_person = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeRequired(2, "name", string()),
      DefaultedField(3, "age", int32(), /*optional=*/false, Literal::Int(10),
                     Literal::Int(18)),
  });
  Schema input_schema({SchemaField::MakeOptional(1, "person", std::move(input_person))});
  Schema write_schema({SchemaField::MakeOptional(1, "person", std::move(write_person))});

  ICEBERG_UNWRAP_OR_FAIL(
      auto actual, Align(input_schema, write_schema, R"([[{"name":"alice"}],[null]])"));
  auto expected = MakeBatch(write_schema, R"([[{"name":"alice","age":18}],[null]])");
  EXPECT_TRUE(actual->Equals(*expected)) << "actual:\n" << actual->ToString();
}

TEST(WriteSchemaUtilTest, RejectsIncompatibleTypes) {
  Schema input_schema({SchemaField::MakeRequired(1, "id", int32())});
  Schema write_schema({SchemaField::MakeRequired(1, "id", int64())});

  auto result = Align(input_schema, write_schema, R"([[1]])");

  EXPECT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(result, HasErrorMessage("Cannot align field id 1"));
}

TEST(WriteSchemaUtilTest, RejectsInputFieldsOutsideWriteSchema) {
  Schema input_schema({SchemaField::MakeRequired(1, "id", int32()),
                       SchemaField::MakeOptional(2, "extra", string())});
  Schema write_schema({SchemaField::MakeRequired(1, "id", int32())});

  auto result = Align(input_schema, write_schema, R"([[1,"extra"]])");

  EXPECT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(result, HasErrorMessage("Input field id 2"));
}

}  // namespace iceberg::arrow
