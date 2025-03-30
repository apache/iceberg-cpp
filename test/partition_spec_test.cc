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

#include "iceberg/partition_spec.h"

#include <format>
#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/partition_field.h"
#include "iceberg/schema.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"

namespace iceberg {

TEST(PartitionSpecTest, Basics) {
  {
    SchemaField field1(5, "ts", std::make_shared<TimestampType>(), true);
    SchemaField field2(7, "bar", std::make_shared<StringType>(), true);
    auto const schema = std::make_shared<Schema>(100, std::vector{field1, field2});

    auto identity_transform = std::make_shared<IdentityTransformFunction>();
    PartitionField pt_field1(5, 1000, "day", identity_transform);
    PartitionField pt_field2(5, 1001, "hour", identity_transform);
    PartitionSpec spec(schema, 100, {pt_field1, pt_field2});
    ASSERT_EQ(spec, spec);
    ASSERT_EQ(100, spec.spec_id());
    std::span<const PartitionField> fields = spec.fields();
    ASSERT_EQ(2, fields.size());
    ASSERT_EQ(pt_field1, fields[0]);
    ASSERT_EQ(pt_field2, fields[1]);
    ASSERT_EQ(*schema, *spec.schema());
    auto spec_str =
        "partition_spec[spec_id<100>,\n  day (1000 identity(5))\n  hour (1001 "
        "identity(5))\n]";
    EXPECT_EQ(spec_str, spec.ToString());
    EXPECT_EQ(spec_str, std::format("{}", spec));
  }
}

TEST(PartitionSpecTest, Equality) {
  SchemaField field1(5, "ts", std::make_shared<TimestampType>(), true);
  SchemaField field2(7, "bar", std::make_shared<StringType>(), true);
  auto const schema = std::make_shared<Schema>(100, std::vector{field1, field2});
  auto identity_transform = std::make_shared<IdentityTransformFunction>();
  PartitionField pt_field1(5, 1000, "day", identity_transform);
  PartitionField pt_field2(7, 1001, "hour", identity_transform);
  PartitionField pt_field3(7, 1001, "hour", identity_transform);
  PartitionSpec schema1(schema, 100, {pt_field1, pt_field2});
  PartitionSpec schema2(schema, 101, {pt_field1, pt_field2});
  PartitionSpec schema3(schema, 101, {pt_field1});
  PartitionSpec schema4(schema, 101, {pt_field3, pt_field1});
  PartitionSpec schema5(schema, 100, {pt_field1, pt_field2});
  PartitionSpec schema6(schema, 100, {pt_field2, pt_field1});

  ASSERT_EQ(schema1, schema1);
  ASSERT_NE(schema1, schema2);
  ASSERT_NE(schema2, schema1);
  ASSERT_NE(schema1, schema3);
  ASSERT_NE(schema3, schema1);
  ASSERT_NE(schema1, schema4);
  ASSERT_NE(schema4, schema1);
  ASSERT_EQ(schema1, schema5);
  ASSERT_EQ(schema5, schema1);
  ASSERT_NE(schema1, schema6);
  ASSERT_NE(schema6, schema1);
}
}  // namespace iceberg
