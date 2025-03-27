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

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/result.h>
#include <gtest/gtest.h>

#include "iceberg/arrow_c_data_internal.h"

namespace iceberg {

TEST(ArrowCDataTest, CheckArrowSchemaAndArrayByNanoarrow) {
  auto [schema, array] = internal::CreateExampleArrowSchemaAndArrayByNanoarrow();

  auto arrow_schema = ::arrow::ImportSchema(&schema).ValueOrDie();
  EXPECT_EQ(arrow_schema->num_fields(), 2);

  auto id_field = arrow_schema->field(0);
  EXPECT_EQ(id_field->name(), "id");
  EXPECT_EQ(id_field->type()->id(), ::arrow::Type::INT64);
  EXPECT_FALSE(id_field->nullable());

  auto name_field = arrow_schema->field(1);
  EXPECT_EQ(name_field->name(), "name");
  EXPECT_EQ(name_field->type()->id(), ::arrow::Type::STRING);
  EXPECT_TRUE(name_field->nullable());

  auto arrow_record_batch = ::arrow::ImportRecordBatch(&array, arrow_schema).ValueOrDie();
  EXPECT_EQ(arrow_record_batch->num_rows(), 3);
  EXPECT_EQ(arrow_record_batch->num_columns(), 2);

  auto id_column = arrow_record_batch->column(0);
  EXPECT_EQ(id_column->type()->id(), ::arrow::Type::INT64);
  EXPECT_EQ(id_column->GetScalar(0).ValueOrDie()->ToString(), "1");
  EXPECT_EQ(id_column->GetScalar(1).ValueOrDie()->ToString(), "2");
  EXPECT_EQ(id_column->GetScalar(2).ValueOrDie()->ToString(), "3");

  auto name_column = arrow_record_batch->column(1);
  EXPECT_EQ(name_column->type()->id(), ::arrow::Type::STRING);
  EXPECT_EQ(name_column->GetScalar(0).ValueOrDie()->ToString(), "a");
  EXPECT_EQ(name_column->GetScalar(1).ValueOrDie()->ToString(), "b");
  EXPECT_EQ(name_column->GetScalar(2).ValueOrDie()->ToString(), "c");
}

}  // namespace iceberg
