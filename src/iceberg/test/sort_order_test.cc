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

#include "iceberg/sort_order.h"

#include <format>
#include <memory>

#include <gtest/gtest.h>

#include "iceberg/schema.h"
#include "iceberg/sort_field.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

class SortOrderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    field1_ = std::make_unique<SchemaField>(1, "x", int32(), true);
    field2_ = std::make_unique<SchemaField>(2, "y", string(), true);
    field3_ = std::make_unique<SchemaField>(5, "ts", iceberg::timestamp(), true);
    field4_ = std::make_unique<SchemaField>(7, "bar", iceberg::string(), true);

    schema_ = std::make_unique<Schema>(
        std::vector<SchemaField>{*field1_, *field2_, *field3_, *field4_}, 1);

    sort_field1_ = std::make_unique<SortField>(
        1, Transform::Identity(), SortDirection::kAscending, NullOrder::kFirst);
    sort_field2_ = std::make_unique<SortField>(
        2, Transform::Bucket(10), SortDirection::kDescending, NullOrder::kLast);
    sort_field3_ = std::make_unique<SortField>(
        5, Transform::Day(), SortDirection::kAscending, NullOrder::kFirst);
  }

  std::unique_ptr<Schema> schema_;
  std::unique_ptr<SchemaField> field1_;
  std::unique_ptr<SchemaField> field2_;
  std::unique_ptr<SchemaField> field3_;
  std::unique_ptr<SchemaField> field4_;

  std::unique_ptr<SortField> sort_field1_;
  std::unique_ptr<SortField> sort_field2_;
  std::unique_ptr<SortField> sort_field3_;
};

TEST_F(SortOrderTest, Basics) {
  auto identity_transform = Transform::Identity();
  SortField st_field1(5, identity_transform, SortDirection::kAscending,
                      NullOrder::kFirst);
  SortField st_field2(7, identity_transform, SortDirection::kDescending,
                      NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order,
                         SortOrder::Make(*schema_, 100, {st_field1, st_field2}));
  ASSERT_EQ(*sort_order, *sort_order);
  std::span<const SortField> fields = sort_order->fields();
  ASSERT_EQ(2, fields.size());
  ASSERT_EQ(st_field1, fields[0]);
  ASSERT_EQ(st_field2, fields[1]);
  auto sort_order_str =
      "[\n"
      "  identity(5) asc nulls-first\n"
      "  identity(7) desc nulls-first\n"
      "]";
  EXPECT_EQ(sort_order->ToString(), sort_order_str);
  EXPECT_EQ(std::format("{}", *sort_order), sort_order_str);
}

TEST_F(SortOrderTest, Equality) {
  auto bucket_transform = Transform::Bucket(8);
  auto identity_transform = Transform::Identity();
  SortField st_field1(5, identity_transform, SortDirection::kAscending,
                      NullOrder::kFirst);
  SortField st_field2(7, identity_transform, SortDirection::kDescending,
                      NullOrder::kFirst);
  SortField st_field3(7, bucket_transform, SortDirection::kAscending, NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order1,
                         SortOrder::Make(*schema_, 100, {st_field1, st_field2}));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order2,
                         SortOrder::Make(*schema_, 100, {st_field2, st_field3}));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order3,
                         SortOrder::Make(*schema_, 100, {st_field1, st_field3}));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order4,
                         SortOrder::Make(*schema_, 101, {st_field1, st_field2}));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order5,
                         SortOrder::Make(*schema_, 100, {st_field2, st_field1}));

  ASSERT_EQ(*sort_order1, *sort_order1);
  ASSERT_NE(*sort_order1, *sort_order2);
  ASSERT_NE(*sort_order2, *sort_order1);
  ASSERT_NE(*sort_order1, *sort_order3);
  ASSERT_NE(*sort_order3, *sort_order1);
  ASSERT_NE(*sort_order1, *sort_order4);
  ASSERT_NE(*sort_order4, *sort_order1);
  ASSERT_NE(*sort_order1, *sort_order5);
  ASSERT_NE(*sort_order5, *sort_order1);
}

TEST_F(SortOrderTest, IsUnsorted) {
  auto unsorted = SortOrder::Unsorted();
  EXPECT_TRUE(unsorted->is_unsorted());
  EXPECT_FALSE(unsorted->is_sorted());
}

TEST_F(SortOrderTest, IsSorted) {
  auto identity_transform = Transform::Identity();
  SortField st_field1(5, identity_transform, SortDirection::kAscending,
                      NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto sorted_order, SortOrder::Make(*schema_, 100, {st_field1}));

  EXPECT_TRUE(sorted_order->is_sorted());
  EXPECT_FALSE(sorted_order->is_unsorted());
}

TEST_F(SortOrderTest, Satisfies) {
  auto identity_transform = Transform::Identity();
  auto bucket_transform = Transform::Bucket(8);

  SortField st_field1(5, identity_transform, SortDirection::kAscending,
                      NullOrder::kFirst);
  SortField st_field2(7, identity_transform, SortDirection::kDescending,
                      NullOrder::kFirst);
  SortField st_field3(7, bucket_transform, SortDirection::kAscending, NullOrder::kFirst);

  ICEBERG_UNWRAP_OR_FAIL(auto sort_order1,
                         SortOrder::Make(*schema_, 100, {st_field1, st_field2}));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order2, SortOrder::Make(*schema_, 101, {st_field1}));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order3,
                         SortOrder::Make(*schema_, 102, {st_field1, st_field3}));
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order4, SortOrder::Make(*schema_, 104, {st_field2}));
  auto unsorted = SortOrder::Unsorted();

  // Any order satisfies an unsorted order, including unsorted itself
  EXPECT_TRUE(unsorted->Satisfies(*unsorted));
  EXPECT_TRUE(sort_order1->Satisfies(*unsorted));
  EXPECT_TRUE(sort_order2->Satisfies(*unsorted));
  EXPECT_TRUE(sort_order3->Satisfies(*unsorted));

  // Unsorted does not satisfy any sorted order
  EXPECT_FALSE(unsorted->Satisfies(*sort_order1));
  EXPECT_FALSE(unsorted->Satisfies(*sort_order2));
  EXPECT_FALSE(unsorted->Satisfies(*sort_order3));

  // A sort order satisfies itself
  EXPECT_TRUE(sort_order1->Satisfies(*sort_order1));
  EXPECT_TRUE(sort_order2->Satisfies(*sort_order2));
  EXPECT_TRUE(sort_order3->Satisfies(*sort_order3));

  // A sort order with more fields satisfy one with fewer fields
  EXPECT_TRUE(sort_order1->Satisfies(*sort_order2));
  EXPECT_TRUE(sort_order3->Satisfies(*sort_order2));

  // A sort order does not satisfy one with more fields
  EXPECT_FALSE(sort_order2->Satisfies(*sort_order1));
  EXPECT_FALSE(sort_order2->Satisfies(*sort_order3));

  // A sort order does not satisfy one with different fields
  EXPECT_FALSE(sort_order4->Satisfies(*sort_order2));
  EXPECT_FALSE(sort_order2->Satisfies(*sort_order4));
}

TEST_F(SortOrderTest, MakeValidSortOrder) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto sort_order,
      SortOrder::Make(*schema_, 1, std::vector<SortField>{*sort_field1_, *sort_field2_}));
  ASSERT_NE(sort_order, nullptr);

  EXPECT_TRUE(sort_order->is_sorted());
  ASSERT_EQ(sort_order->fields().size(), 2);
  EXPECT_EQ(sort_order->fields()[0], *sort_field1_);
  EXPECT_EQ(sort_order->fields()[1], *sort_field2_);
}

TEST_F(SortOrderTest, MakeInvalidSortOrderEmptyFields) {
  auto sort_order = SortOrder::Make(*schema_, 1, std::vector<SortField>{});
  EXPECT_THAT(sort_order, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(sort_order,
              HasErrorMessage("Sort order must have at least one sort field"));
}

TEST_F(SortOrderTest, MakeInvalidSortOrderUnsortedId) {
  auto sort_order = SortOrder::Make(*schema_, SortOrder::kUnsortedOrderId,
                                    std::vector<SortField>{*sort_field1_});
  EXPECT_THAT(sort_order, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(sort_order,
              HasErrorMessage(std::format("{} is reserved for unsorted sort order",
                                          SortOrder::kUnsortedOrderId)));
}

TEST_F(SortOrderTest, MakeValidUnsortedSortOrder) {
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, SortOrder::Make(SortOrder::kUnsortedOrderId,
                                                          std::vector<SortField>{}));
  ASSERT_NE(sort_order, nullptr);

  EXPECT_TRUE(sort_order->is_unsorted());
  EXPECT_EQ(sort_order->fields().size(), 0);
}

TEST_F(SortOrderTest, MakeInvalidSortOrderTransformCannotApply) {
  // Day transform cannot be applied to string type
  SortField sort_field_invalid(2, Transform::Day(), SortDirection::kAscending,
                               NullOrder::kFirst);

  auto sort_order = SortOrder::Make(
      *schema_, 1, std::vector<SortField>{*sort_field1_, sort_field_invalid});
  EXPECT_THAT(sort_order, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(sort_order, HasErrorMessage("Invalid source type"));
}

TEST_F(SortOrderTest, MakeInvalidSortOrderNonPrimitiveField) {
  auto struct_field = std::make_unique<SchemaField>(
      4, "struct_field",
      std::make_shared<StructType>(std::vector<SchemaField>{
          SchemaField::MakeRequired(41, "inner_field", iceberg::int32()),
      }),
      true);

  Schema schema_with_struct(
      std::vector<SchemaField>{*field1_, *field2_, *field3_, *struct_field}, 1);

  SortField sort_field_invalid(4, Transform::Identity(), SortDirection::kAscending,
                               NullOrder::kFirst);

  auto sort_order = SortOrder::Make(
      schema_with_struct, 1, std::vector<SortField>{*sort_field1_, sort_field_invalid});
  EXPECT_THAT(sort_order, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(sort_order, HasErrorMessage("Invalid source type"));
}

TEST_F(SortOrderTest, MakeInvalidSortOrderFieldNotInSchema) {
  SortField sort_field_invalid(999, Transform::Identity(), SortDirection::kAscending,
                               NullOrder::kFirst);

  auto sort_order = SortOrder::Make(
      *schema_, 1, std::vector<SortField>{*sort_field1_, sort_field_invalid});
  EXPECT_THAT(sort_order, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(sort_order, HasErrorMessage("Cannot find source column for sort field"));
}

TEST_F(SortOrderTest, MakeUnboundSortOrder) {
  SortField sort_field_invalid(999, Transform::Identity(), SortDirection::kAscending,
                               NullOrder::kFirst);

  ICEBERG_UNWRAP_OR_FAIL(
      auto sort_order,
      SortOrder::Make(1, std::vector<SortField>{*sort_field1_, sort_field_invalid}));
  auto validate_status = sort_order->Validate(*schema_);
  EXPECT_THAT(validate_status, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(validate_status,
              HasErrorMessage("Cannot find source column for sort field"));
}

}  // namespace iceberg
