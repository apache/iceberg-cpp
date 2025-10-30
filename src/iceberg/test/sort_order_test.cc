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
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

TEST(SortOrderTest, Basics) {
  {
    SchemaField field1(5, "ts", iceberg::timestamp(), true);
    SchemaField field2(7, "bar", iceberg::string(), true);

    auto identity_transform = Transform::Identity();
    SortField st_field1(5, identity_transform, SortDirection::kAscending,
                        NullOrder::kFirst);
    SortField st_field2(7, identity_transform, SortDirection::kDescending,
                        NullOrder::kFirst);
    SortOrder sort_order(100, {st_field1, st_field2});
    ASSERT_EQ(sort_order, sort_order);
    std::span<const SortField> fields = sort_order.fields();
    ASSERT_EQ(2, fields.size());
    ASSERT_EQ(st_field1, fields[0]);
    ASSERT_EQ(st_field2, fields[1]);
    auto sort_order_str =
        "[\n"
        "  identity(5) asc nulls-first\n"
        "  identity(7) desc nulls-first\n"
        "]";
    EXPECT_EQ(sort_order.ToString(), sort_order_str);
    EXPECT_EQ(std::format("{}", sort_order), sort_order_str);
  }
}

TEST(SortOrderTest, Equality) {
  SchemaField field1(5, "ts", iceberg::timestamp(), true);
  SchemaField field2(7, "bar", iceberg::string(), true);
  auto bucket_transform = Transform::Bucket(8);
  auto identity_transform = Transform::Identity();
  SortField st_field1(5, identity_transform, SortDirection::kAscending,
                      NullOrder::kFirst);
  SortField st_field2(7, identity_transform, SortDirection::kDescending,
                      NullOrder::kFirst);
  SortField st_field3(7, bucket_transform, SortDirection::kAscending, NullOrder::kFirst);
  SortOrder sort_order1(100, {st_field1, st_field2});
  SortOrder sort_order2(100, {st_field2, st_field3});
  SortOrder sort_order3(100, {st_field1, st_field3});
  SortOrder sort_order4(101, {st_field1, st_field2});
  SortOrder sort_order5(100, {st_field2, st_field1});

  ASSERT_EQ(sort_order1, sort_order1);
  ASSERT_NE(sort_order1, sort_order2);
  ASSERT_NE(sort_order2, sort_order1);
  ASSERT_NE(sort_order1, sort_order3);
  ASSERT_NE(sort_order3, sort_order1);
  ASSERT_NE(sort_order1, sort_order4);
  ASSERT_NE(sort_order4, sort_order1);
  ASSERT_NE(sort_order1, sort_order5);
  ASSERT_NE(sort_order5, sort_order1);
}

TEST(SortOrderTest, IsUnsorted) {
  auto unsorted = SortOrder::Unsorted();
  EXPECT_TRUE(unsorted->IsUnsorted());
  EXPECT_FALSE(unsorted->IsSorted());
}

TEST(SortOrderTest, IsSorted) {
  SchemaField field1(5, "ts", iceberg::timestamp(), true);
  auto identity_transform = Transform::Identity();
  SortField st_field1(5, identity_transform, SortDirection::kAscending,
                      NullOrder::kFirst);
  SortOrder sorted_order(100, {st_field1});

  EXPECT_TRUE(sorted_order.IsSorted());
  EXPECT_FALSE(sorted_order.IsUnsorted());
}

TEST(SortOrderTest, Satisfies) {
  SchemaField field1(5, "ts", iceberg::timestamp(), true);
  SchemaField field2(7, "bar", iceberg::string(), true);
  auto identity_transform = Transform::Identity();
  auto bucket_transform = Transform::Bucket(8);

  SortField st_field1(5, identity_transform, SortDirection::kAscending,
                      NullOrder::kFirst);
  SortField st_field2(7, identity_transform, SortDirection::kDescending,
                      NullOrder::kFirst);
  SortField st_field3(7, bucket_transform, SortDirection::kAscending, NullOrder::kFirst);

  SortOrder sort_order1(100, {st_field1, st_field2});
  SortOrder sort_order2(101, {st_field1});
  SortOrder sort_order3(102, {st_field1, st_field3});
  SortOrder sort_order4(104, {st_field2});
  auto unsorted = SortOrder::Unsorted();

  // Any order satisfies an unsorted order, including unsorted itself
  EXPECT_TRUE(unsorted->Satisfies(*unsorted));
  EXPECT_TRUE(sort_order1.Satisfies(*unsorted));
  EXPECT_TRUE(sort_order2.Satisfies(*unsorted));
  EXPECT_TRUE(sort_order3.Satisfies(*unsorted));

  // Unsorted does not satisfy any sorted order
  EXPECT_FALSE(unsorted->Satisfies(sort_order1));
  EXPECT_FALSE(unsorted->Satisfies(sort_order2));
  EXPECT_FALSE(unsorted->Satisfies(sort_order3));

  // A sort order satisfies itself
  EXPECT_TRUE(sort_order1.Satisfies(sort_order1));
  EXPECT_TRUE(sort_order2.Satisfies(sort_order2));
  EXPECT_TRUE(sort_order3.Satisfies(sort_order3));

  // A sort order with more fields satisfy one with fewer fields
  EXPECT_TRUE(sort_order1.Satisfies(sort_order2));
  EXPECT_TRUE(sort_order3.Satisfies(sort_order2));

  // A sort order does not satisfy one with more fields
  EXPECT_FALSE(sort_order2.Satisfies(sort_order1));
  EXPECT_FALSE(sort_order2.Satisfies(sort_order3));

  // A sort order does not satify one with different fields
  EXPECT_FALSE(sort_order4.Satisfies(sort_order2));
  EXPECT_FALSE(sort_order2.Satisfies(sort_order4));
}

}  // namespace iceberg
