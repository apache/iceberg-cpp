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

#include "iceberg/sort_field.h"

#include <format>

#include <gtest/gtest.h>

#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

TEST(SortFieldTest, Basics) {
  {
    const auto transform = Transform::Identity();
    SortField field(1, transform, SortDirection::kAscending, NullOrder::kFirst);
    EXPECT_EQ(1, field.source_id());
    EXPECT_EQ(*transform, *field.transform());
    EXPECT_EQ(SortDirection::kAscending, field.direction());
    EXPECT_EQ(NullOrder::kFirst, field.null_order());
    EXPECT_EQ(field.ToString(), "identity(1) asc nulls-first");
    EXPECT_EQ(std::format("{}", field), "identity(1) asc nulls-first");
  }
}

TEST(SortFieldTest, Equality) {
  const auto bucket_transform = Transform::Bucket(8);
  const auto identity_transform = Transform::Identity();

  SortField field1(1, bucket_transform, SortDirection::kAscending, NullOrder::kFirst);
  SortField field2(2, bucket_transform, SortDirection::kAscending, NullOrder::kFirst);
  SortField field3(1, identity_transform, SortDirection::kAscending, NullOrder::kFirst);
  SortField field4(1, bucket_transform, SortDirection::kDescending, NullOrder::kFirst);
  SortField field5(1, bucket_transform, SortDirection::kAscending, NullOrder::kLast);

  ASSERT_EQ(field1, field1);
  ASSERT_NE(field1, field2);
  ASSERT_NE(field2, field1);
  ASSERT_NE(field1, field3);
  ASSERT_NE(field3, field1);
  ASSERT_NE(field1, field4);
  ASSERT_NE(field4, field1);
  ASSERT_NE(field1, field5);
  ASSERT_NE(field5, field1);
}

TEST(SortFieldTest, Satisfies) {
  const auto bucket_transform = Transform::Bucket(8);
  const auto identity_transform = Transform::Identity();

  SortField field1(1, bucket_transform, SortDirection::kAscending, NullOrder::kFirst);
  SortField field2(1, bucket_transform, SortDirection::kAscending, NullOrder::kFirst);
  SortField field3(1, identity_transform, SortDirection::kAscending, NullOrder::kFirst);
  SortField field4(1, bucket_transform, SortDirection::kDescending, NullOrder::kFirst);
  SortField field5(1, bucket_transform, SortDirection::kAscending, NullOrder::kLast);
  SortField field6(2, bucket_transform, SortDirection::kAscending, NullOrder::kFirst);

  EXPECT_TRUE(field1.Satisfies(field2));   // Same fields
  EXPECT_FALSE(field1.Satisfies(field3));  // Different transform
  EXPECT_FALSE(field1.Satisfies(field4));  // Different direction
  EXPECT_FALSE(field1.Satisfies(field5));  // Different null order
  EXPECT_FALSE(field1.Satisfies(field6));  // Different source_id
}

}  // namespace iceberg
