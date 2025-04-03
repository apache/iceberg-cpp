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
#include "iceberg/util/formatter.h"

namespace iceberg {

namespace {
class TestTransformFunction : public TransformFunction {
 public:
  TestTransformFunction() : TransformFunction(TransformType::kUnknown) {}
  expected<ArrowArray, Error> Transform(const ArrowArray& input) override {
    return unexpected(
        Error{.kind = ErrorKind::kNotSupported, .message = "test transform function"});
  }
};

}  // namespace

TEST(SortOrderTest, Basics) {
  {
    SchemaField field1(5, "ts", std::make_shared<TimestampType>(), true);
    SchemaField field2(7, "bar", std::make_shared<StringType>(), true);

    auto identity_transform = std::make_shared<IdentityTransformFunction>();
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
        "sort_order[order_id<100>,\n"
        "  sort_field(source_id=5, transform=identity, direction=asc, "
        "null_order=nulls-first)\n"
        "  sort_field(source_id=7, transform=identity, direction=desc, "
        "null_order=nulls-first)\n]";
    EXPECT_EQ(sort_order_str, sort_order.ToString());
    EXPECT_EQ(sort_order_str, std::format("{}", sort_order));
  }
}

TEST(SortOrderTest, Equality) {
  SchemaField field1(5, "ts", std::make_shared<TimestampType>(), true);
  SchemaField field2(7, "bar", std::make_shared<StringType>(), true);
  auto test_transform = std::make_shared<TestTransformFunction>();
  auto identity_transform = std::make_shared<IdentityTransformFunction>();
  SortField st_field1(5, identity_transform, SortDirection::kAscending,
                      NullOrder::kFirst);
  SortField st_field2(7, identity_transform, SortDirection::kDescending,
                      NullOrder::kFirst);
  SortField st_field3(7, test_transform, SortDirection::kAscending, NullOrder::kFirst);
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
}  // namespace iceberg
