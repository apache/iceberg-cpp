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

/// \file row_builder_test.cc
/// Unit tests for ArrowRowBuilder and its typed append helpers.

#include <memory>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include "iceberg/inspect/row_builder_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

/// \brief A schema exercising every append helper: int32, string, int64,
/// boolean, and map<string, string>.
std::shared_ptr<Schema> MakeTestSchema() {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "name", string()),
      SchemaField::MakeOptional(3, "count", int64()),
      SchemaField::MakeOptional(4, "active", boolean()),
      SchemaField::MakeOptional(
          5, "props",
          std::make_shared<MapType>(SchemaField::MakeRequired(6, "key", string()),
                                    SchemaField::MakeRequired(7, "value", string())))});
}

/// \brief Finish a builder and import the result into an Arrow RecordBatch.
std::shared_ptr<::arrow::RecordBatch> FinishAndImport(ArrowRowBuilder builder,
                                                      const Schema& schema) {
  auto array_result = std::move(builder).Finish();
  EXPECT_THAT(array_result, IsOk());

  ArrowSchema c_schema;
  EXPECT_THAT(ToArrowSchema(schema, &c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();

  // ImportRecordBatch takes ownership of the array and releases it.
  return ::arrow::ImportRecordBatch(&array_result.value(), arrow_schema).ValueOrDie();
}

}  // namespace

TEST(ArrowRowBuilderTest, BuildsRowsWithTypedValues) {
  auto schema = MakeTestSchema();
  auto builder_result = ArrowRowBuilder::Make(*schema);
  ASSERT_THAT(builder_result, IsOk());
  auto builder = std::move(*builder_result);

  ASSERT_EQ(builder.num_columns(), 5);

  // Row 0
  ASSERT_THAT(AppendInt(builder.column(0), 1), IsOk());
  ASSERT_THAT(AppendString(builder.column(1), "alice"), IsOk());
  ASSERT_THAT(AppendInt(builder.column(2), 100), IsOk());
  ASSERT_THAT(AppendBoolean(builder.column(3), true), IsOk());
  ASSERT_THAT(AppendStringMap(builder.column(4), {{"k", "v"}}), IsOk());
  ASSERT_THAT(builder.FinishRow(), IsOk());

  // Row 1
  ASSERT_THAT(AppendInt(builder.column(0), 2), IsOk());
  ASSERT_THAT(AppendString(builder.column(1), "bob"), IsOk());
  ASSERT_THAT(AppendInt(builder.column(2), 200), IsOk());
  ASSERT_THAT(AppendBoolean(builder.column(3), false), IsOk());
  ASSERT_THAT(AppendStringMap(builder.column(4), {}), IsOk());
  ASSERT_THAT(builder.FinishRow(), IsOk());

  auto batch = FinishAndImport(std::move(builder), *schema);
  ASSERT_EQ(batch->num_rows(), 2);
  ASSERT_EQ(batch->num_columns(), 5);

  auto id = std::static_pointer_cast<::arrow::Int32Array>(batch->column(0));
  EXPECT_EQ(id->Value(0), 1);
  EXPECT_EQ(id->Value(1), 2);

  auto name = std::static_pointer_cast<::arrow::StringArray>(batch->column(1));
  EXPECT_EQ(name->GetString(0), "alice");
  EXPECT_EQ(name->GetString(1), "bob");

  auto count = std::static_pointer_cast<::arrow::Int64Array>(batch->column(2));
  EXPECT_EQ(count->Value(0), 100);
  EXPECT_EQ(count->Value(1), 200);

  auto active = std::static_pointer_cast<::arrow::BooleanArray>(batch->column(3));
  EXPECT_TRUE(active->Value(0));
  EXPECT_FALSE(active->Value(1));

  // props: one entry in row 0, empty (but non-null) map in row 1.
  auto props = std::static_pointer_cast<::arrow::MapArray>(batch->column(4));
  EXPECT_FALSE(props->IsNull(0));
  EXPECT_FALSE(props->IsNull(1));
  EXPECT_EQ(props->value_length(0), 1);
  EXPECT_EQ(props->value_length(1), 0);
}

TEST(ArrowRowBuilderTest, AppendsNullForOptionalColumns) {
  auto schema = MakeTestSchema();
  auto builder_result = ArrowRowBuilder::Make(*schema);
  ASSERT_THAT(builder_result, IsOk());
  auto builder = std::move(*builder_result);

  ASSERT_THAT(AppendInt(builder.column(0), 42), IsOk());
  ASSERT_THAT(AppendNull(builder.column(1)), IsOk());
  ASSERT_THAT(AppendNull(builder.column(2)), IsOk());
  ASSERT_THAT(AppendNull(builder.column(3)), IsOk());
  ASSERT_THAT(AppendStringMap(builder.column(4), {}), IsOk());
  ASSERT_THAT(builder.FinishRow(), IsOk());

  auto batch = FinishAndImport(std::move(builder), *schema);
  ASSERT_EQ(batch->num_rows(), 1);

  auto id = std::static_pointer_cast<::arrow::Int32Array>(batch->column(0));
  EXPECT_FALSE(id->IsNull(0));
  EXPECT_EQ(id->Value(0), 42);

  EXPECT_TRUE(batch->column(1)->IsNull(0));
  EXPECT_TRUE(batch->column(2)->IsNull(0));
  EXPECT_TRUE(batch->column(3)->IsNull(0));
}

TEST(ArrowRowBuilderTest, AppendsMultiEntryStringMap) {
  auto schema = MakeTestSchema();
  auto builder_result = ArrowRowBuilder::Make(*schema);
  ASSERT_THAT(builder_result, IsOk());
  auto builder = std::move(*builder_result);

  ASSERT_THAT(AppendInt(builder.column(0), 1), IsOk());
  ASSERT_THAT(AppendNull(builder.column(1)), IsOk());
  ASSERT_THAT(AppendNull(builder.column(2)), IsOk());
  ASSERT_THAT(AppendNull(builder.column(3)), IsOk());
  ASSERT_THAT(AppendStringMap(builder.column(4), {{"a", "1"}, {"b", "2"}, {"c", "3"}}),
              IsOk());
  ASSERT_THAT(builder.FinishRow(), IsOk());

  auto batch = FinishAndImport(std::move(builder), *schema);
  auto props = std::static_pointer_cast<::arrow::MapArray>(batch->column(4));
  EXPECT_EQ(props->value_length(0), 3);
}

TEST(ArrowRowBuilderTest, EmptyBuilderProducesZeroRowBatch) {
  auto schema = MakeTestSchema();
  auto builder_result = ArrowRowBuilder::Make(*schema);
  ASSERT_THAT(builder_result, IsOk());

  auto batch = FinishAndImport(std::move(*builder_result), *schema);
  EXPECT_EQ(batch->num_rows(), 0);
  EXPECT_EQ(batch->num_columns(), 5);
}

TEST(ArrowRowBuilderTest, ColumnIndexOutOfRangeReturnsNull) {
  auto schema = MakeTestSchema();
  auto builder_result = ArrowRowBuilder::Make(*schema);
  ASSERT_THAT(builder_result, IsOk());
  auto builder = std::move(*builder_result);

  EXPECT_EQ(builder.num_columns(), 5);
  EXPECT_NE(builder.column(0), nullptr);
  EXPECT_NE(builder.column(4), nullptr);
  EXPECT_EQ(builder.column(-1), nullptr);
  EXPECT_EQ(builder.column(5), nullptr);
}

}  // namespace iceberg
