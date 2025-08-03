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

#include <gtest/gtest.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "iceberg/parquet/parquet_schema_util_internal.h"

namespace iceberg::parquet {

namespace {

::parquet::schema::NodePtr MakeInt32Node(const std::string& name, int field_id = -1) {
  return ::parquet::schema::PrimitiveNode::Make(
      name, ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::None(),
      ::parquet::Type::INT32, /*primitive_length=*/-1, field_id);
}

::parquet::schema::NodePtr MakeGroupNode(const std::string& name,
                                         const ::parquet::schema::NodeVector& fields,
                                         int field_id = -1) {
  return ::parquet::schema::GroupNode::Make(name, ::parquet::Repetition::REQUIRED, fields,
                                            /*logical_type=*/nullptr, field_id);
}

}  // namespace

TEST(HasFieldIds, PrimitiveNode) {
  EXPECT_FALSE(HasFieldIds(MakeInt32Node("test_field")));
  EXPECT_TRUE(HasFieldIds(MakeInt32Node("test_field", /*field_id=*/1)));
}

TEST(HasFieldIds, GroupNode) {
  auto group_node_without_field_id =
      MakeGroupNode("test_group", {MakeInt32Node("c1"), MakeInt32Node("c2")});
  EXPECT_FALSE(HasFieldIds(group_node_without_field_id));

  auto group_node_with_full_field_id = MakeGroupNode(
      "test_group",
      {MakeInt32Node("c1", /*field_id=*/2), MakeInt32Node("c2", /*field_id=*/3)},
      /*field_id=*/1);
  EXPECT_TRUE(HasFieldIds(group_node_with_full_field_id));

  auto group_node_with_partial_field_id = MakeGroupNode(
      "test_group", {MakeInt32Node("c1", /*field_id=*/1), MakeInt32Node("c2")});
  EXPECT_TRUE(HasFieldIds(group_node_with_partial_field_id));
}

}  // namespace iceberg::parquet
