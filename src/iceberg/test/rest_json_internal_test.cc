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
#include <string>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"

namespace iceberg::rest {

namespace {

// Helper templates for type-specific deserialization
template <typename T>
Result<T> FromJsonHelper(const nlohmann::json& json);

template <>
Result<CreateNamespaceRequest> FromJsonHelper<CreateNamespaceRequest>(
    const nlohmann::json& j) {
  return CreateNamespaceRequestFromJson(j);
}

template <>
Result<UpdateNamespacePropertiesRequest> FromJsonHelper<UpdateNamespacePropertiesRequest>(
    const nlohmann::json& j) {
  return UpdateNamespacePropertiesRequestFromJson(j);
}

template <>
Result<CreateTableRequest> FromJsonHelper<CreateTableRequest>(const nlohmann::json& j) {
  return CreateTableRequestFromJson(j);
}

template <>
Result<RegisterTableRequest> FromJsonHelper<RegisterTableRequest>(
    const nlohmann::json& j) {
  return RegisterTableRequestFromJson(j);
}

template <>
Result<RenameTableRequest> FromJsonHelper<RenameTableRequest>(const nlohmann::json& j) {
  return RenameTableRequestFromJson(j);
}

template <>
Result<LoadTableResult> FromJsonHelper<LoadTableResult>(const nlohmann::json& j) {
  return LoadTableResultFromJson(j);
}

template <>
Result<ListNamespacesResponse> FromJsonHelper<ListNamespacesResponse>(
    const nlohmann::json& j) {
  return ListNamespacesResponseFromJson(j);
}

template <>
Result<CreateNamespaceResponse> FromJsonHelper<CreateNamespaceResponse>(
    const nlohmann::json& j) {
  return CreateNamespaceResponseFromJson(j);
}

template <>
Result<GetNamespaceResponse> FromJsonHelper<GetNamespaceResponse>(
    const nlohmann::json& j) {
  return GetNamespaceResponseFromJson(j);
}

template <>
Result<UpdateNamespacePropertiesResponse>
FromJsonHelper<UpdateNamespacePropertiesResponse>(const nlohmann::json& j) {
  return UpdateNamespacePropertiesResponseFromJson(j);
}

template <>
Result<ListTablesResponse> FromJsonHelper<ListTablesResponse>(const nlohmann::json& j) {
  return ListTablesResponseFromJson(j);
}

// Test helper functions
template <typename T>
void TestJsonRoundTrip(const T& obj) {
  auto j = ToJson(obj);
  auto parsed = FromJsonHelper<T>(j);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().message;
  auto j2 = ToJson(parsed.value());
  EXPECT_EQ(j, j2) << "Round-trip JSON mismatch.";
}

template <typename T>
void TestJsonConversion(const T& obj, const nlohmann::json& expected_json) {
  auto got = ToJson(obj);
  EXPECT_EQ(expected_json, got) << "JSON conversion mismatch.";

  auto parsed = FromJsonHelper<T>(expected_json);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().message;
  auto back = ToJson(parsed.value());
  EXPECT_EQ(expected_json, back) << "JSON conversion mismatch.";
}

template <typename T>
void TestMissingRequiredField(const nlohmann::json& invalid_json,
                              const std::string& expected_field_name) {
  auto result = FromJsonHelper<T>(invalid_json);
  EXPECT_FALSE(result.has_value());
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("Missing '" + expected_field_name + "'"));
}

template <typename T>
void TestUnknownFieldIgnored(const nlohmann::json& minimal_valid) {
  auto j = minimal_valid;
  j["__unknown__"] = {{"nested", 1}};
  auto parsed = FromJsonHelper<T>(j);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().message;
  auto out = ToJson(parsed.value());
  EXPECT_FALSE(out.contains("__unknown__"));
}

std::shared_ptr<TableMetadata> CreateTableMetadata() {
  std::vector<SchemaField> schema_fields{
      SchemaField(1, "id", iceberg::int64(), false),
      SchemaField(2, "name", iceberg::string(), true),
      SchemaField(3, "ts", iceberg::timestamp(), false),
  };
  auto schema = std::make_shared<Schema>(std::move(schema_fields), /*schema_id=*/1);

  auto spec = std::make_shared<PartitionSpec>(
      schema, 1,
      std::vector<PartitionField>{PartitionField(3, 1000, "ts_day", Transform::Day())});

  auto order = std::make_shared<SortOrder>(
      1, std::vector<SortField>{SortField(1, Transform::Identity(),
                                          SortDirection::kAscending, NullOrder::kFirst)});

  return std::make_shared<TableMetadata>(TableMetadata{
      .format_version = 2,
      .table_uuid = "test-uuid-12345",
      .location = "s3://bucket/warehouse/table",
      .last_sequence_number = 42,
      .last_updated_ms = TimePointMsFromUnixMs(1700000000000).value(),
      .last_column_id = 3,
      .schemas = {schema},
      .current_schema_id = 1,
      .partition_specs = {spec},
      .default_spec_id = 1,
      .last_partition_id = 1000,
      .properties = {{"engine.version", "1.2.3"}, {"write.format.default", "parquet"}},
      .current_snapshot_id = 1234567890,
      .snapshots = {std::make_shared<Snapshot>(
          Snapshot{.snapshot_id = 1234567890,
                   .sequence_number = 1,
                   .timestamp_ms = TimePointMsFromUnixMs(1699999999999).value(),
                   .manifest_list = "s3://bucket/metadata/snap-1234567890.avro",
                   .summary = {{"operation", "append"}, {"added-files", "10"}}})},
      .snapshot_log = {},
      .metadata_log = {},
      .sort_orders = {order},
      .default_sort_order_id = 1,
      .refs = {},
      .statistics = {},
      .partition_statistics = {},
      .next_row_id = 100});
}

TableIdentifier CreateTableIdentifier(std::vector<std::string> ns, std::string name) {
  return TableIdentifier{.ns = Namespace{std::move(ns)}, .name = std::move(name)};
}

}  // namespace

TEST(JsonRestNewTest, CreateNamespaceRequestFull) {
  CreateNamespaceRequest req{
      .namespace_ = Namespace{.levels = {"db1", "schema1"}},
      .properties = std::unordered_map<std::string, std::string>{
          {"owner", "user1"}, {"location", "/warehouse/db1/schema1"}}};

  nlohmann::json golden = R"({
    "namespace": ["db1","schema1"],
    "properties": {"owner":"user1","location":"/warehouse/db1/schema1"}
  })"_json;

  TestJsonConversion(req, golden);
  TestJsonRoundTrip(req);
}

TEST(JsonRestNewTest, CreateNamespaceRequestMinimal) {
  CreateNamespaceRequest req{.namespace_ = Namespace{.levels = {"db_only"}},
                             .properties = {}};

  nlohmann::json golden = R"({
    "namespace": ["db_only"]
  })"_json;

  TestJsonConversion(req, golden);
}

TEST(JsonRestNewTest, CreateNamespaceRequestMissingNamespace) {
  nlohmann::json invalid = R"({
    "properties": {"key": "value"}
  })"_json;

  TestMissingRequiredField<CreateNamespaceRequest>(invalid, "namespace");
}

TEST(JsonRestNewTest, CreateNamespaceRequestIgnoreUnknown) {
  nlohmann::json minimal = R"({"namespace":["db"]})"_json;
  TestUnknownFieldIgnored<CreateNamespaceRequest>(minimal);
}

TEST(JsonRestNewTest, UpdateNamespacePropertiesRequestBoth) {
  UpdateNamespacePropertiesRequest req{
      .removals = std::vector<std::string>{"k1", "k2"},
      .updates = std::unordered_map<std::string, std::string>{{"a", "b"}}};

  nlohmann::json golden = R"({
    "removals": ["k1","k2"],
    "updates": {"a":"b"}
  })"_json;

  TestJsonConversion(req, golden);
  TestJsonRoundTrip(req);
}

TEST(JsonRestNewTest, UpdateNamespacePropertiesRequestRemovalsOnly) {
  UpdateNamespacePropertiesRequest req{.removals = std::vector<std::string>{"k"},
                                       .updates = {}};

  nlohmann::json golden = R"({
    "removals": ["k"]
  })"_json;

  TestJsonConversion(req, golden);
}

TEST(JsonRestNewTest, UpdateNamespacePropertiesRequestUpdatesOnly) {
  UpdateNamespacePropertiesRequest req{
      .removals = {},
      .updates = std::unordered_map<std::string, std::string>{{"x", "y"}}};

  nlohmann::json golden = R"({
    "updates": {"x":"y"}
  })"_json;

  TestJsonConversion(req, golden);
}

TEST(JsonRestNewTest, UpdateNamespacePropertiesRequestEmpty) {
  UpdateNamespacePropertiesRequest req{.removals = {}, .updates = {}};

  nlohmann::json golden = R"({})"_json;

  TestJsonConversion(req, golden);
}

TEST(JsonRestNewTest, CreateTableRequestFull) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", iceberg::int64(), false),
                               SchemaField(2, "ts", iceberg::timestamp(), false),
                               SchemaField(3, "data", iceberg::string(), true)},
      0);

  auto partition_spec =
      std::make_shared<PartitionSpec>(schema, 1,
                                      std::vector<PartitionField>{PartitionField(
                                          2, 1000, "ts_month", Transform::Month())});

  SortField sort_field(1, Transform::Identity(), SortDirection::kAscending,
                       NullOrder::kFirst);
  auto write_order = std::make_shared<SortOrder>(1, std::vector<SortField>{sort_field});

  CreateTableRequest request{
      .name = "test_table",
      .location = "s3://bucket/warehouse/test_table",
      .schema = schema,
      .partition_spec = partition_spec,
      .write_order = write_order,
      .stage_create = true,
      .properties = std::unordered_map<std::string, std::string>{
          {"write.format.default", "parquet"}, {"commit.retry.num-retries", "10"}}};

  TestJsonRoundTrip(request);
}

TEST(JsonRestNewTest, CreateTableRequestMinimal) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", iceberg::int64(), false)}, 0);

  CreateTableRequest request{.name = "minimal_table",
                             .location = "",
                             .schema = schema,
                             .partition_spec = nullptr,
                             .write_order = nullptr,
                             .stage_create = std::nullopt,
                             .properties = {}};

  TestJsonRoundTrip(request);

  auto json = ToJson(request);
  EXPECT_EQ(json["name"], "minimal_table");
  EXPECT_FALSE(json.contains("location"));
  EXPECT_TRUE(json.contains("schema"));
  EXPECT_FALSE(json.contains("partition-spec"));
  EXPECT_FALSE(json.contains("write-order"));
  EXPECT_FALSE(json.contains("stage-create"));
  EXPECT_FALSE(json.contains("properties"));
}

TEST(JsonRestNewTest, CreateTableRequestMissingRequiredFields) {
  nlohmann::json invalid_json = R"({
    "location": "/tmp/test"
  })"_json;

  TestMissingRequiredField<CreateTableRequest>(invalid_json, "name");

  invalid_json = R"({
    "name": "test_table"
  })"_json;

  TestMissingRequiredField<CreateTableRequest>(invalid_json, "schema");
}

TEST(JsonRestNewTest, RegisterTableRequestWithOverwriteTrue) {
  RegisterTableRequest r1{
      .name = "t", .metadata_location = "s3://m/v1.json", .overwrite = true};

  nlohmann::json g1 = R"({
    "name":"t",
    "metadata-location":"s3://m/v1.json",
    "overwrite": true
  })"_json;

  TestJsonConversion(r1, g1);
  TestJsonRoundTrip(r1);
}

TEST(JsonRestNewTest, RegisterTableRequestWithOverwriteFalse) {
  RegisterTableRequest r2{
      .name = "t2", .metadata_location = "/tmp/m.json", .overwrite = false};

  nlohmann::json g2 = R"({
    "name":"t2",
    "metadata-location":"/tmp/m.json"
  })"_json;

  TestJsonConversion(r2, g2);
}

TEST(JsonRestNewTest, RegisterTableRequestMissingFields) {
  TestMissingRequiredField<RegisterTableRequest>(
      R"({"metadata-location":"s3://m/v1.json"})"_json, "name");
  TestMissingRequiredField<RegisterTableRequest>(R"({"name":"t"})"_json,
                                                 "metadata-location");
}

TEST(JsonRestNewTest, RenameTableRequestBasic) {
  RenameTableRequest req{.source = CreateTableIdentifier({"old"}, "t0"),
                         .destination = CreateTableIdentifier({"new", "s"}, "t1")};

  nlohmann::json golden = R"({
    "source": {"namespace":["old"], "name":"t0"},
    "destination": {"namespace":["new","s"], "name":"t1"}
  })"_json;

  TestJsonConversion(req, golden);
  TestJsonRoundTrip(req);
}

TEST(JsonRestNewTest, RenameTableRequestDecodeWithUnknownField) {
  nlohmann::json golden = R"({
    "source": {"namespace":["a"], "name":"b"},
    "destination": {"namespace":["x","y"], "name":"z"},
    "__extra__": true
  })"_json;

  auto parsed = RenameTableRequestFromJson(golden);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->source.ns.levels, (std::vector<std::string>{"a"}));
  EXPECT_EQ(parsed->destination.ns.levels, (std::vector<std::string>{"x", "y"}));
}

TEST(JsonRestNewTest, RenameTableRequestInvalidNested) {
  nlohmann::json invalid = R"({
    "source": {
      "namespace": "should be array",
      "name": "table"
    },
    "destination": {
      "namespace": ["db"],
      "name": "new_table"
    }
  })"_json;

  auto result = RenameTableRequestFromJson(invalid);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
}

TEST(JsonRestNewTest, LoadTableResultFull) {
  LoadTableResult r{.metadata_location = "s3://bucket/metadata/v1.json",
                    .metadata = CreateTableMetadata(),
                    .config = std::unordered_map<std::string, std::string>{
                        {"catalog-impl", "org.apache.iceberg.rest.RESTCatalog"},
                        {"warehouse", "s3://bucket/warehouse"}}};

  TestJsonRoundTrip(r);

  auto j = ToJson(r);
  EXPECT_TRUE(j.contains("metadata-location"));
  EXPECT_TRUE(j.contains("metadata"));
  EXPECT_TRUE(j.contains("config"));
  EXPECT_EQ(j["metadata"]["table-uuid"], "test-uuid-12345");
}

TEST(JsonRestNewTest, LoadTableResultMinimal) {
  LoadTableResult r{
      .metadata_location = "", .metadata = CreateTableMetadata(), .config = {}};

  TestJsonRoundTrip(r);

  auto j = ToJson(r);
  EXPECT_FALSE(j.contains("metadata-location"));
  EXPECT_TRUE(j.contains("metadata"));
  EXPECT_FALSE(j.contains("config"));
}

TEST(JsonRestNewTest, LoadTableResultWithEmptyMetadataLocation) {
  LoadTableResult r{
      .metadata_location = "", .metadata = CreateTableMetadata(), .config = {}};

  auto j = ToJson(r);
  EXPECT_FALSE(j.contains("metadata-location"));
}

TEST(JsonRestNewTest, ListNamespacesResponseWithPageToken) {
  ListNamespacesResponse r{
      .next_page_token = "token123",
      .namespaces = {Namespace{.levels = {"db1"}}, Namespace{.levels = {"db2", "s1"}},
                     Namespace{.levels = {"db3", "s2", "sub"}}}};

  nlohmann::json golden = R"({
    "next-page-token": "token123",
    "namespaces": [
      ["db1"], ["db2","s1"], ["db3","s2","sub"]
    ]
  })"_json;

  TestJsonConversion(r, golden);
  TestJsonRoundTrip(r);
}

TEST(JsonRestNewTest, ListNamespacesResponseWithoutPageToken) {
  ListNamespacesResponse r{
      .next_page_token = "",
      .namespaces = {Namespace{.levels = {"db1"}}, Namespace{.levels = {"db2"}}}};

  nlohmann::json golden = R"({
    "namespaces": [["db1"], ["db2"]]
  })"_json;

  TestJsonConversion(r, golden);
}

TEST(JsonRestNewTest, CreateNamespaceResponseFull) {
  CreateNamespaceResponse resp{
      .namespace_ = Namespace{.levels = {"db1", "s1"}},
      .properties = std::unordered_map<std::string, std::string>{{"created", "true"}}};

  nlohmann::json golden = R"({
    "namespace": ["db1","s1"],
    "properties": {"created":"true"}
  })"_json;

  TestJsonConversion(resp, golden);
  TestJsonRoundTrip(resp);
}

TEST(JsonRestNewTest, CreateNamespaceResponseMinimal) {
  CreateNamespaceResponse resp{.namespace_ = Namespace{.levels = {"db1", "s1"}},
                               .properties = {}};

  nlohmann::json golden = R"({"namespace":["db1","s1"]})"_json;

  TestJsonConversion(resp, golden);
}

TEST(JsonRestNewTest, CreateNamespaceResponseIgnoreUnknown) {
  nlohmann::json minimal = R"({"namespace":["db","s"]})"_json;
  TestUnknownFieldIgnored<CreateNamespaceResponse>(minimal);
}

TEST(JsonRestNewTest, GetNamespaceResponseFull) {
  GetNamespaceResponse r{.namespace_ = Namespace{.levels = {"prod", "analytics"}},
                         .properties = std::unordered_map<std::string, std::string>{
                             {"owner", "team-analytics"}, {"retention", "90days"}}};

  nlohmann::json golden = R"({
    "namespace": ["prod","analytics"],
    "properties": {"owner":"team-analytics","retention":"90days"}
  })"_json;

  TestJsonConversion(r, golden);
  TestJsonRoundTrip(r);
}

TEST(JsonRestNewTest, GetNamespaceResponseMinimal) {
  GetNamespaceResponse r{.namespace_ = Namespace{.levels = {"db"}}, .properties = {}};

  nlohmann::json golden = R"({"namespace":["db"]})"_json;

  TestJsonConversion(r, golden);
}

TEST(JsonRestNewTest, UpdateNamespacePropertiesResponseFull) {
  UpdateNamespacePropertiesResponse full{
      .updated = {"u1", "u2"}, .removed = {"r1"}, .missing = {"m1"}};

  nlohmann::json g_full = R"({
    "updated": ["u1","u2"],
    "removed": ["r1"],
    "missing": ["m1"]
  })"_json;

  TestJsonConversion(full, g_full);
  TestJsonRoundTrip(full);
}

TEST(JsonRestNewTest, UpdateNamespacePropertiesResponseMinimal) {
  UpdateNamespacePropertiesResponse minimal{
      .updated = {"u"}, .removed = {}, .missing = {}};

  nlohmann::json g_min = R"({
    "updated": ["u"],
    "removed": []
  })"_json;

  TestJsonConversion(minimal, g_min);
}

TEST(JsonRestNewTest, UpdateNamespacePropertiesResponseNoMissing) {
  UpdateNamespacePropertiesResponse resp{
      .updated = {"u1"}, .removed = {"r1"}, .missing = {}};

  nlohmann::json golden = R"({
    "updated": ["u1"],
    "removed": ["r1"]
  })"_json;

  TestJsonConversion(resp, golden);
}

TEST(JsonRestNewTest, ListTablesResponseWithPageToken) {
  ListTablesResponse r{.next_page_token = "token456",
                       .identifiers = {CreateTableIdentifier({"db1"}, "t1"),
                                       CreateTableIdentifier({"db2", "s"}, "t2")}};

  nlohmann::json golden = R"({
    "next-page-token": "token456",
    "identifiers": [
      {"namespace":["db1"], "name":"t1"},
      {"namespace":["db2","s"], "name":"t2"}
    ]
  })"_json;

  TestJsonConversion(r, golden);
  TestJsonRoundTrip(r);
}

TEST(JsonRestNewTest, ListTablesResponseWithoutPageToken) {
  ListTablesResponse r{.next_page_token = "",
                       .identifiers = {CreateTableIdentifier({"db1"}, "t1")}};

  nlohmann::json golden = R"({
    "identifiers": [
      {"namespace":["db1"], "name":"t1"}
    ]
  })"_json;

  TestJsonConversion(r, golden);
}

TEST(JsonRestNewTest, ListTablesResponseEmpty) {
  ListTablesResponse r{.next_page_token = "", .identifiers = {}};

  nlohmann::json golden = R"({
    "identifiers": []
  })"_json;

  TestJsonConversion(r, golden);
}

TEST(JsonRestNewTest, ListTablesResponseIgnoreUnknown) {
  nlohmann::json minimal = R"({"identifiers":[{"namespace":["db"],"name":"t"}]})"_json;
  TestUnknownFieldIgnored<ListTablesResponse>(minimal);
}

TEST(JsonRestNewBoundaryTest, EmptyCollections) {
  CreateNamespaceRequest request{
      .namespace_ = Namespace{.levels = {"ns"}},
      .properties = std::unordered_map<std::string, std::string>{}};

  auto json = ToJson(request);
  EXPECT_FALSE(json.contains("properties"));

  auto parsed = CreateNamespaceRequestFromJson(json);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_TRUE(parsed->properties.empty());
}

TEST(JsonRestNewBoundaryTest, InvalidJsonStructure) {
  nlohmann::json invalid = nlohmann::json::array();

  auto result = ListNamespacesResponseFromJson(invalid);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));

  invalid = R"({
    "namespaces": "should be array"
  })"_json;

  result = ListNamespacesResponseFromJson(invalid);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
}

TEST(JsonRestNewBoundaryTest, DeepNestedNamespace) {
  std::vector<std::string> deep_namespace;
  for (int i = 0; i < 100; ++i) {
    deep_namespace.push_back("level" + std::to_string(i));
  }

  CreateNamespaceRequest req{.namespace_ = Namespace{.levels = deep_namespace},
                             .properties = {}};

  TestJsonRoundTrip(req);
}

}  // namespace iceberg::rest
