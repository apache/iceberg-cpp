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
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/inspect/metadata_partition_util_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/inspect/metadata_table_test_base.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg::internal {
namespace {
using MetadataPartitionTest = MetadataTableTestBase;

TEST_F(MetadataPartitionTest, ProjectsAcrossPartitionSpecEvolution) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto old_spec,
      PartitionSpec::Make(1, {PartitionField(1, 1000, "old_id", Transform::Identity())}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto new_spec,
      PartitionSpec::Make(2, {PartitionField(1, 1000, "new_id", Transform::Identity()),
                              PartitionField(2, 1001, "data", Transform::Identity())}));
  metadata_->partition_specs.clear();
  metadata_->partition_specs.push_back(std::move(old_spec));
  metadata_->partition_specs.push_back(std::move(new_spec));
  ICEBERG_UNWRAP_OR_FAIL(auto type, UnifiedPartitionType(*metadata_));
  ASSERT_EQ(type->fields().size(), 2);
  EXPECT_EQ(type->fields()[0].name(), "new_id");
  ICEBERG_UNWRAP_OR_FAIL(auto values,
                         ProjectPartitionValues(*type, *metadata_->partition_specs[0],
                                                PartitionValues(Literal::Int(7))));
  ASSERT_EQ(values.num_fields(), 2);
  EXPECT_EQ(values.values()[0], Literal::Long(7));
  EXPECT_TRUE(values.values()[1].IsNull());
}

TEST_F(MetadataPartitionTest, RejectsConflictingPartitionFields) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto first,
      PartitionSpec::Make(1, {PartitionField(1, 1000, "id", Transform::Identity())}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second,
      PartitionSpec::Make(2, {PartitionField(2, 1000, "name", Transform::Identity())}));
  metadata_->partition_specs = {std::move(first), std::move(second)};
  EXPECT_FALSE(UnifiedPartitionType(*metadata_).has_value());
}

TEST_F(MetadataPartitionTest, AppendsProjectedValuesAndRejectsWrongCounts) {
  auto type = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1000, "id", int64()),
                               SchemaField::MakeOptional(1001, "name", string())});
  Schema schema({SchemaField::MakeOptional(1, "partition", type)});
  ICEBERG_UNWRAP_OR_FAIL(auto builder, ArrowRowBuilder::Make(schema));
  EXPECT_THAT(AppendPartitionValues(nullptr, *type, PartitionValues{}),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(
      AppendPartitionValues(builder.column(0), *type, PartitionValues(Literal::Long(7))),
      IsError(ErrorKind::kInvalidArgument));
  ASSERT_THAT(AppendPartitionValues(builder.column(0), *type,
                                    PartitionValues(std::vector<Literal>{
                                        Literal::Long(7), Literal::Null(string())})),
              IsOk());
  ASSERT_THAT(builder.FinishRow(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto array, std::move(builder).Finish());
  auto arrow_schema = ::arrow::schema({::arrow::field(
      "partition", ::arrow::struct_({::arrow::field("id", ::arrow::int64()),
                                     ::arrow::field("name", ::arrow::utf8())}))});
  auto batch_result = ::arrow::ImportRecordBatch(&array, arrow_schema);
  ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
  auto batch = batch_result.ValueOrDie();
  ASSERT_EQ(batch->column(0)->type_id(), ::arrow::Type::STRUCT);
  auto partition = std::static_pointer_cast<::arrow::StructArray>(batch->column(0));
  ASSERT_EQ(partition->field(0)->type_id(), ::arrow::Type::INT64);
  auto ids = std::static_pointer_cast<::arrow::Int64Array>(partition->field(0));
  EXPECT_EQ(ids->Value(0), 7);
  EXPECT_TRUE(partition->field(1)->IsNull(0));
}

}  // namespace
}  // namespace iceberg::internal
