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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/json_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_identifier.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"
#include "iceberg/util/macros.h"
#include "matchers.h"

namespace iceberg::rest {

TEST(RestJsonInternalTest, ListNamespaceResponse) {
  ListNamespaceResponse response;
  response.namespaces = {{"db1"}, {"db2", "schema1"}};

  auto json = ToJson(response);
  nlohmann::json expected_json = R"({
    "namespaces": [["db1"], ["db2", "schema1"]]
  })"_json;

  EXPECT_EQ(json, expected_json);

  auto parsed_result = ListNamespaceResponseFromJson(expected_json);
  ASSERT_TRUE(parsed_result.has_value());
  EXPECT_EQ(response.namespaces, parsed_result->namespaces);
}

TEST(RestJsonInternalTest, UpdateNamespacePropsRequest) {
  UpdateNamespacePropsRequest request;
  request.removals = std::vector<std::string>{"key1", "key2"};
  request.updates = std::unordered_map<std::string, std::string>{{"key3", "value3"}};

  auto json = ToJson(request);
  nlohmann::json expected_json = R"({
    "removals": ["key1", "key2"],
    "updates": {"key3": "value3"}
  })"_json;

  EXPECT_EQ(json, expected_json);

  auto parsed_result = UpdateNamespacePropsRequestFromJson(expected_json);
  ASSERT_TRUE(parsed_result.has_value());
  EXPECT_EQ(*request.removals, *parsed_result->removals);
  EXPECT_EQ(*request.updates, *parsed_result->updates);
}

TEST(RestJsonInternalTest, UpdateNamespacePropsResponse) {
  UpdateNamespacePropsResponse response;
  response.updated = {"key1", "key2"};
  response.removed = {"key3"};
  response.missing = std::vector<std::string>{"key4"};

  auto json = ToJson(response);
  nlohmann::json expected_json = R"({
    "updated": ["key1", "key2"],
    "removed": ["key3"],
    "missing": ["key4"]
  })"_json;

  EXPECT_EQ(json, expected_json);

  auto parsed_result = UpdateNamespacePropsResponseFromJson(expected_json);
  ASSERT_TRUE(parsed_result.has_value());
  EXPECT_EQ(response.updated, parsed_result->updated);
  EXPECT_EQ(response.removed, parsed_result->removed);
  EXPECT_EQ(*response.missing, *parsed_result->missing);
}

TEST(RestJsonInternalTest, ListTableResponse) {
  ListTableResponse response;
  response.identifiers = {
      TableIdentifier{.ns = Namespace{{"db1"}}, .name = "table1"},
      TableIdentifier{.ns = Namespace{{"db2", "schema1"}}, .name = "table2"}};

  auto json = ToJson(response);
  nlohmann::json expected_json = R"({
    "identifiers": [
      {"namespace": ["db1"], "name": "table1"},
      {"namespace": ["db2", "schema1"], "name": "table2"}
    ]
  })"_json;

  EXPECT_EQ(json, expected_json);

  auto parsed_result = ListTableResponseFromJson(expected_json);
  ASSERT_TRUE(parsed_result.has_value());
  EXPECT_EQ(response.identifiers.size(), parsed_result->identifiers.size());
  EXPECT_EQ(response.identifiers[0].name, parsed_result->identifiers[0].name);
  EXPECT_EQ(response.identifiers[1].name, parsed_result->identifiers[1].name);
}

TEST(RestJsonInternalTest, RenameTableRequest) {
  RenameTableRequest request;
  request.source = TableIdentifier{.ns = Namespace{{"db1"}}, .name = "old_table"};
  request.destination = TableIdentifier{.ns = Namespace{{"db2"}}, .name = "new_table"};

  auto json = ToJson(request);
  nlohmann::json expected_json = R"({
    "source": {"namespace": ["db1"], "name": "old_table"},
    "destination": {"namespace": ["db2"], "name": "new_table"}
  })"_json;

  EXPECT_EQ(json, expected_json);

  auto parsed_result = RenameTableRequestFromJson(expected_json);
  ASSERT_TRUE(parsed_result.has_value());
  EXPECT_EQ(request.source.name, parsed_result->source.name);
  EXPECT_EQ(request.destination.name, parsed_result->destination.name);
}

TEST(RestJsonInternalTest, CreateTableRequestBasic) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", iceberg::int64(), false),
                               SchemaField(2, "data", iceberg::string(), true)},
      0);

  CreateTableRequest request;
  request.name = "test_table";
  request.location = "/tmp/test_location";
  request.schema = schema;
  request.partition_spec = nullptr;
  request.write_order = nullptr;
  request.stage_create = false;
  request.properties = std::unordered_map<std::string, std::string>{{"key1", "value1"}};

  auto json = ToJson(request);

  nlohmann::json expected_json = R"({
    "name": "test_table",
    "location": "/tmp/test_location",
    "schema": {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "data", "required": false, "type": "string"}
      ]
    },
    "stage-create": false,
    "properties": {"key1": "value1"}
  })"_json;

  EXPECT_EQ(json, expected_json);

  auto parsed_result = CreateTableRequestFromJson(expected_json);
  ASSERT_TRUE(parsed_result.has_value()) << parsed_result.error().message;

  EXPECT_EQ(request.name, parsed_result->name);
  EXPECT_EQ(*request.location, *parsed_result->location);
  EXPECT_EQ(*request.schema, *parsed_result->schema);
  EXPECT_EQ(request.partition_spec == nullptr, parsed_result->partition_spec == nullptr);
  EXPECT_EQ(request.write_order == nullptr, parsed_result->write_order == nullptr);
  EXPECT_EQ(*request.stage_create, *parsed_result->stage_create);
  EXPECT_EQ(*request.properties, *parsed_result->properties);
}

TEST(RestJsonInternalTest, CreateTableRequestWithPartitionSpec) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", iceberg::int64(), false),
                               SchemaField(2, "ts", iceberg::timestamp(), false),
                               SchemaField(3, "data", iceberg::string(), true)},
      0);

  auto identity_transform = Transform::Identity();
  auto partition_spec =
      std::make_shared<PartitionSpec>(schema, 1,
                                      std::vector<PartitionField>{PartitionField(
                                          2, 1000, "ts_partition", identity_transform)});

  CreateTableRequest request;
  request.name = "partitioned_table";
  request.schema = schema;
  request.partition_spec = partition_spec;
  request.write_order = nullptr;

  auto json = ToJson(request);

  nlohmann::json expected_json = R"({
    "name": "partitioned_table",
    "schema": {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "ts", "required": true, "type": "timestamp"},
        {"id": 3, "name": "data", "required": false, "type": "string"}
      ]
    },
    "partition-spec": {
      "spec-id": 1,
      "fields": [
        {"source-id": 2, "field-id": 1000, "transform": "identity", "name": "ts_partition"}
      ]
    }
  })"_json;

  EXPECT_EQ(json, expected_json);

  auto parsed_result = CreateTableRequestFromJson(expected_json);
  ASSERT_TRUE(parsed_result.has_value()) << parsed_result.error().message;

  EXPECT_EQ(request.name, parsed_result->name);
  EXPECT_EQ(*request.schema, *parsed_result->schema);
  ASSERT_NE(parsed_result->partition_spec, nullptr);
  EXPECT_EQ(*request.partition_spec, *parsed_result->partition_spec);
  EXPECT_EQ(request.write_order == nullptr, parsed_result->write_order == nullptr);
}

TEST(RestJsonInternalTest, CreateTableRequestWithSortOrder) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", iceberg::int64(), false),
                               SchemaField(2, "ts", iceberg::timestamp(), false)},
      0);

  auto identity_transform = Transform::Identity();
  SortField sort_field(1, identity_transform, SortDirection::kAscending,
                       NullOrder::kFirst);
  auto write_order = std::make_shared<SortOrder>(1, std::vector<SortField>{sort_field});

  CreateTableRequest request;
  request.name = "sorted_table";
  request.schema = schema;
  request.partition_spec = nullptr;
  request.write_order = write_order;

  auto json = ToJson(request);

  nlohmann::json expected_json = R"({
    "name": "sorted_table",
    "schema": {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "ts", "required": true, "type": "timestamp"}
      ]
    },
    "write-order": {
      "order-id": 1,
      "fields": [
        {"transform": "identity", "source-id": 1, "direction": "asc", "null-order": "nulls-first"}
      ]
    }
  })"_json;

  EXPECT_EQ(json, expected_json);

  auto parsed_result = CreateTableRequestFromJson(expected_json);
  ASSERT_TRUE(parsed_result.has_value()) << parsed_result.error().message;

  EXPECT_EQ(request.name, parsed_result->name);
  EXPECT_EQ(*request.schema, *parsed_result->schema);
  EXPECT_EQ(request.partition_spec == nullptr, parsed_result->partition_spec == nullptr);
  ASSERT_NE(parsed_result->write_order, nullptr);
  EXPECT_EQ(*request.write_order, *parsed_result->write_order);
}

TEST(RestJsonInternalTest, CreateTableRequestComplete) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", iceberg::int64(), false),
                               SchemaField(2, "ts", iceberg::timestamp(), false),
                               SchemaField(3, "data", iceberg::string(), true)},
      0);

  auto identity_transform = Transform::Identity();
  auto partition_spec =
      std::make_shared<PartitionSpec>(schema, 1,
                                      std::vector<PartitionField>{PartitionField(
                                          2, 1000, "ts_partition", identity_transform)});

  SortField sort_field(1, identity_transform, SortDirection::kAscending,
                       NullOrder::kFirst);
  auto write_order = std::make_shared<SortOrder>(1, std::vector<SortField>{sort_field});

  CreateTableRequest request;
  request.name = "complete_table";
  request.location = "/tmp/complete";
  request.schema = schema;
  request.partition_spec = partition_spec;
  request.write_order = write_order;
  request.stage_create = true;
  request.properties = std::unordered_map<std::string, std::string>{{"key1", "value1"},
                                                                    {"key2", "value2"}};

  auto json = ToJson(request);

  auto parsed_result = CreateTableRequestFromJson(json);
  ASSERT_TRUE(parsed_result.has_value()) << parsed_result.error().message;

  EXPECT_EQ(request.name, parsed_result->name);
  EXPECT_EQ(*request.location, *parsed_result->location);
  EXPECT_EQ(*request.schema, *parsed_result->schema);
  ASSERT_NE(parsed_result->partition_spec, nullptr);
  EXPECT_EQ(*request.partition_spec, *parsed_result->partition_spec);
  ASSERT_NE(parsed_result->write_order, nullptr);
  EXPECT_EQ(*request.write_order, *parsed_result->write_order);
  EXPECT_EQ(*request.stage_create, *parsed_result->stage_create);
  EXPECT_EQ(*request.properties, *parsed_result->properties);
}

TEST(RestJsonInternalTest, CreateTableRequestMissingRequiredFields) {
  nlohmann::json invalid_json = R"({
    "location": "/tmp/test"
  })"_json;

  auto result = CreateTableRequestFromJson(invalid_json);
  EXPECT_FALSE(result.has_value());
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("Missing 'name'"));
}

TEST(RestJsonInternalTest, RegisterTableRequest) {
  RegisterTableRequest request;
  request.name = "registered_table";
  request.metadata_location = "/tmp/metadata.json";
  request.overwrite = true;

  auto json = ToJson(request);
  nlohmann::json expected_json = R"({
    "name": "registered_table",
    "metadata-location": "/tmp/metadata.json",
    "overwrite": true
  })"_json;

  EXPECT_EQ(json, expected_json);

  auto parsed_result = RegisterTableRequestFromJson(expected_json);
  ASSERT_TRUE(parsed_result.has_value());
  EXPECT_EQ(request.name, parsed_result->name);
  EXPECT_EQ(request.metadata_location, parsed_result->metadata_location);
  EXPECT_EQ(*request.overwrite, *parsed_result->overwrite);
}

}  // namespace iceberg::rest
