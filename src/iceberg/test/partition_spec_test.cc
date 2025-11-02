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
#include <nlohmann/json.hpp>

#include "iceberg/json_internal.h"
#include "iceberg/partition_field.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

TEST(PartitionSpecTest, Basics) {
  {
    SchemaField field1(5, "ts", iceberg::timestamp(), true);
    SchemaField field2(7, "bar", iceberg::string(), true);
    auto const schema =
        std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 100);

    auto identity_transform = Transform::Identity();
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
  SchemaField field1(5, "ts", iceberg::timestamp(), true);
  SchemaField field2(7, "bar", iceberg::string(), true);
  auto const schema =
      std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 100);
  auto identity_transform = Transform::Identity();
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

TEST(PartitionSpecTest, PartitionSchemaTest) {
  SchemaField field1(5, "ts", iceberg::timestamp(), true);
  SchemaField field2(7, "bar", iceberg::string(), true);
  auto const schema =
      std::make_shared<Schema>(std::vector<SchemaField>{field1, field2}, 100);
  auto identity_transform = Transform::Identity();
  PartitionField pt_field1(5, 1000, "day", identity_transform);
  PartitionField pt_field2(7, 1001, "hour", identity_transform);
  PartitionSpec spec(schema, 100, {pt_field1, pt_field2});

  auto partition_schema = spec.PartitionType();
  ASSERT_TRUE(partition_schema.has_value());
  ASSERT_EQ(2, partition_schema.value()->fields().size());
  EXPECT_EQ(pt_field1.name(), partition_schema.value()->fields()[0].name());
  EXPECT_EQ(pt_field1.field_id(), partition_schema.value()->fields()[0].field_id());
  EXPECT_EQ(pt_field2.name(), partition_schema.value()->fields()[1].name());
  EXPECT_EQ(pt_field2.field_id(), partition_schema.value()->fields()[1].field_id());
}

TEST(PartitionSpecTest, PartitionTypeTest) {
  nlohmann::json json = R"(
  {
  "spec-id": 1,
  "fields": [ {
      "source-id": 4,
      "field-id": 1000,
      "name": "ts_day",
      "transform": "day"
      }, {
      "source-id": 1,
      "field-id": 1001,
      "name": "id_bucket",
      "transform": "bucket[16]"
      }, {
      "source-id": 2,
      "field-id": 1002,
      "name": "id_truncate",
      "transform": "truncate[4]"
      } ]
  })"_json;

  SchemaField field1(1, "id", int32(), false);
  SchemaField field2(2, "name", string(), false);
  SchemaField field3(3, "ts", timestamp(), false);
  SchemaField field4(4, "ts_day", timestamp(), false);
  SchemaField field5(5, "id_bucket", int32(), false);
  SchemaField field6(6, "id_truncate", int32(), false);
  auto const schema = std::make_shared<Schema>(
      std::vector<SchemaField>{field1, field2, field3, field4, field5, field6},
      Schema::kInitialSchemaId);

  auto parsed_spec_result = PartitionSpecFromJson(schema, json);
  ASSERT_TRUE(parsed_spec_result.has_value()) << parsed_spec_result.error().message;

  auto partition_schema = parsed_spec_result.value()->PartitionType();

  SchemaField pt_field1(1000, "ts_day", date(), true);
  SchemaField pt_field2(1001, "id_bucket", int32(), true);
  SchemaField pt_field3(1002, "id_truncate", string(), true);

  ASSERT_TRUE(partition_schema.has_value());
  ASSERT_EQ(3, partition_schema.value()->fields().size());

  EXPECT_EQ(pt_field1, partition_schema.value()->fields()[0]);
  EXPECT_EQ(pt_field2, partition_schema.value()->fields()[1]);
  EXPECT_EQ(pt_field3, partition_schema.value()->fields()[2]);
}

}  // namespace iceberg
