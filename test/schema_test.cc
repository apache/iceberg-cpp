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

#include "iceberg/schema.h"

#include <format>
#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/schema_field.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

TEST(SchemaTest, Basics) {
  {
    iceberg::SchemaField field1(5, "foo", std::make_shared<iceberg::IntType>(), true);
    iceberg::SchemaField field2(7, "bar", std::make_shared<iceberg::StringType>(), true);
    iceberg::Schema schema(100, {field1, field2});
    ASSERT_EQ(schema, schema);
    ASSERT_EQ(100, schema.schema_id());
    std::span<const iceberg::SchemaField> fields = schema.fields();
    ASSERT_EQ(2, fields.size());
    ASSERT_EQ(field1, fields[0]);
    ASSERT_EQ(field2, fields[1]);
    ASSERT_THAT(schema.GetFieldById(5), ::testing::Optional(field1));
    ASSERT_THAT(schema.GetFieldById(7), ::testing::Optional(field2));
    ASSERT_THAT(schema.GetFieldByIndex(0), ::testing::Optional(field1));
    ASSERT_THAT(schema.GetFieldByIndex(1), ::testing::Optional(field2));
    ASSERT_THAT(schema.GetFieldByName("foo"), ::testing::Optional(field1));
    ASSERT_THAT(schema.GetFieldByName("bar"), ::testing::Optional(field2));

    ASSERT_EQ(std::nullopt, schema.GetFieldById(0));
    ASSERT_EQ(std::nullopt, schema.GetFieldByIndex(2));
    ASSERT_EQ(std::nullopt, schema.GetFieldByIndex(-1));
    ASSERT_EQ(std::nullopt, schema.GetFieldByName("element"));
  }
  ASSERT_THAT(
      []() {
        iceberg::SchemaField field1(5, "foo", std::make_shared<iceberg::IntType>(), true);
        iceberg::SchemaField field2(5, "bar", std::make_shared<iceberg::StringType>(),
                                    true);
        iceberg::Schema schema(100, {field1, field2});
      },
      ::testing::ThrowsMessage<std::runtime_error>(
          ::testing::HasSubstr("duplicate field ID 5")));
}

TEST(SchemaTest, Equality) {
  iceberg::SchemaField field1(5, "foo", std::make_shared<iceberg::IntType>(), true);
  iceberg::SchemaField field2(7, "bar", std::make_shared<iceberg::StringType>(), true);
  iceberg::SchemaField field3(5, "foobar", std::make_shared<iceberg::IntType>(), true);
  iceberg::Schema schema1(100, {field1, field2});
  iceberg::Schema schema2(101, {field1, field2});
  iceberg::Schema schema3(101, {field1});
  iceberg::Schema schema4(101, {field3, field2});
  iceberg::Schema schema5(100, {field1, field2});

  ASSERT_EQ(schema1, schema1);
  ASSERT_NE(schema1, schema2);
  ASSERT_NE(schema2, schema1);
  ASSERT_NE(schema1, schema3);
  ASSERT_NE(schema3, schema1);
  ASSERT_NE(schema1, schema4);
  ASSERT_NE(schema4, schema1);
  ASSERT_EQ(schema1, schema5);
  ASSERT_EQ(schema5, schema1);
}
