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

#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

// Helper function to create a simple schema for testing
static std::shared_ptr<Schema> MakeSimpleSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", int32(), false),  // required
                               SchemaField(2, "data", string(), true)});
}

// Helper function to create a simple TableMetadata for testing
static std::shared_ptr<TableMetadata> MakeSimpleTableMetadata() {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", int32(), false)}, 1);
  return std::make_shared<TableMetadata>(TableMetadata{
      .format_version = 2,
      .table_uuid = "test-uuid-1234",
      .location = "s3://bucket/test",
      .last_sequence_number = 0,
      .last_updated_ms = TimePointMs{},
      .last_column_id = 1,
      .schemas = {schema},
      .current_schema_id = 1,
      .partition_specs = {PartitionSpec::Unpartitioned()},
      .default_spec_id = 0,
      .last_partition_id = 0,
      .properties = {},
      .current_snapshot_id = -1,
      .snapshots = {},
      .snapshot_log = {},
      .metadata_log = {},
      .sort_orders = {SortOrder::Unsorted()},
      .default_sort_order_id = 0,
      .refs = {},
      .statistics = {},
      .partition_statistics = {},
      .next_row_id = 0,
  });
}

// Test parameter structure for roundtrip tests
template <typename Model>
struct JsonRoundTripParam {
  std::string test_name;
  std::string expected_json_str;
  Model model;
};

// Generic test class for roundtrip tests
template <typename Model>
class JsonRoundTripTest : public ::testing::TestWithParam<JsonRoundTripParam<Model>> {
  using Base = ::testing::TestWithParam<JsonRoundTripParam<Model>>;

 protected:
  void TestRoundTrip() {
    const auto& param = Base::GetParam();

    // ToJson
    auto json = ToJson(param.model);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    ASSERT_EQ(json, expected_json) << "ToJson mismatch";

    // FromJson
    auto result = FromJson<Model>(expected_json);
    ASSERT_THAT(result, IsOk()) << result.error().message;
    auto parsed = std::move(result.value());
    ASSERT_EQ(parsed, param.model);
  }
};

#define DECLARE_ROUNDTRIP_TEST(Model)             \
  using Model##Test = JsonRoundTripTest<Model>;   \
  using Model##Param = JsonRoundTripParam<Model>; \
  TEST_P(Model##Test, RoundTrip) { TestRoundTrip(); }

// Invalid JSON test parameter structure
template <typename Model>
struct JsonInvalidParam {
  std::string test_name;
  std::string invalid_json_str;
  std::string expected_error_message;
};

// Generic test class for invalid JSON deserialization
template <typename Model>
class JsonInvalidTest : public ::testing::TestWithParam<JsonInvalidParam<Model>> {
  using Base = ::testing::TestWithParam<JsonInvalidParam<Model>>;

 protected:
  void TestInvalidJson() {
    const auto& param = Base::GetParam();

    auto result = FromJson<Model>(nlohmann::json::parse(param.invalid_json_str));
    ASSERT_THAT(result, IsError(ErrorKind::kJsonParseError));
    ASSERT_THAT(result, HasErrorMessage(param.expected_error_message))
        << result.error().message;
  }
};

#define DECLARE_INVALID_TEST(Model)                    \
  using Model##InvalidTest = JsonInvalidTest<Model>;   \
  using Model##InvalidParam = JsonInvalidParam<Model>; \
  TEST_P(Model##InvalidTest, InvalidJson) { TestInvalidJson(); }

// Deserialization test parameter structure
template <typename Model>
struct JsonDeserParam {
  std::string test_name;
  std::string json_str;
  Model expected_model;
};

// Generic test class for deserialization tests (FromJson only)
template <typename Model>
class JsonDeserTest : public ::testing::TestWithParam<JsonDeserParam<Model>> {
  using Base = ::testing::TestWithParam<JsonDeserParam<Model>>;

 protected:
  void TestDeserialize() {
    const auto& param = Base::GetParam();

    auto result = FromJson<Model>(nlohmann::json::parse(param.json_str));
    ASSERT_THAT(result, IsOk()) << result.error().message;
    auto parsed = std::move(result.value());
    ASSERT_EQ(parsed, param.expected_model);
  }
};

#define DECLARE_DESERIALIZE_TEST(Model)                  \
  using Model##DeserializeTest = JsonDeserTest<Model>;   \
  using Model##DeserializeParam = JsonDeserParam<Model>; \
  TEST_P(Model##DeserializeTest, Deserialize) { TestDeserialize(); }

DECLARE_ROUNDTRIP_TEST(CreateNamespaceRequest)

INSTANTIATE_TEST_SUITE_P(
    CreateNamespaceRequestCases, CreateNamespaceRequestTest,
    ::testing::Values(
        // Full request with properties
        CreateNamespaceRequestParam{
            .test_name = "FullRequest",
            .expected_json_str =
                R"({"namespace":["accounting","tax"],"properties":{"owner":"Hank"}})",
            .model = {.namespace_ = Namespace{{"accounting", "tax"}},
                      .properties = {{"owner", "Hank"}}}},
        // Request with empty properties (omit properties field when empty)
        CreateNamespaceRequestParam{
            .test_name = "EmptyProperties",
            .expected_json_str = R"({"namespace":["accounting","tax"]})",
            .model = {.namespace_ = Namespace{{"accounting", "tax"}}},
        },
        // Request with empty namespace
        CreateNamespaceRequestParam{
            .test_name = "EmptyNamespace",
            .expected_json_str = R"({"namespace":[]})",
            .model = {.namespace_ = Namespace{}, .properties = {}},
        }),
    [](const ::testing::TestParamInfo<CreateNamespaceRequestParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(CreateNamespaceRequest)

INSTANTIATE_TEST_SUITE_P(
    CreateNamespaceRequestInvalidCases, CreateNamespaceRequestInvalidTest,
    ::testing::Values(
        // Incorrect type for namespace field
        CreateNamespaceRequestInvalidParam{
            .test_name = "WrongNamespaceType",
            .invalid_json_str = R"({"namespace":"accounting%1Ftax","properties":null})",
            .expected_error_message = "type must be array, but is string"},
        // Incorrect type for properties field
        CreateNamespaceRequestInvalidParam{
            .test_name = "WrongPropertiesType",
            .invalid_json_str = R"({"namespace":["accounting","tax"],"properties":[]})",
            .expected_error_message = "type must be object, but is array"},
        // Misspelled required field
        CreateNamespaceRequestInvalidParam{
            .test_name = "MisspelledKeys",
            .invalid_json_str =
                R"({"namepsace":["accounting","tax"],"propertiezzzz":{"owner":"Hank"}})",
            .expected_error_message = "Missing 'namespace'"},
        // Empty JSON object
        CreateNamespaceRequestInvalidParam{
            .test_name = "EmptyJson",
            .invalid_json_str = R"({})",
            .expected_error_message = "Missing 'namespace'"}),
    [](const ::testing::TestParamInfo<CreateNamespaceRequestInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(CreateNamespaceRequest)

INSTANTIATE_TEST_SUITE_P(
    CreateNamespaceRequestDeserializeCases, CreateNamespaceRequestDeserializeTest,
    ::testing::Values(
        // Properties field is null (should deserialize to empty map)
        CreateNamespaceRequestDeserializeParam{
            .test_name = "NullProperties",
            .json_str = R"({"namespace":["accounting","tax"],"properties":null})",
            .expected_model = {.namespace_ = Namespace{{"accounting", "tax"}}}},
        // Properties field is missing (should deserialize to empty map)
        CreateNamespaceRequestDeserializeParam{
            .test_name = "MissingProperties",
            .json_str = R"({"namespace":["accounting","tax"]})",
            .expected_model = {.namespace_ = Namespace{{"accounting", "tax"}}}}),
    [](const ::testing::TestParamInfo<CreateNamespaceRequestDeserializeParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(CreateNamespaceResponse)

INSTANTIATE_TEST_SUITE_P(
    CreateNamespaceResponseCases, CreateNamespaceResponseTest,
    ::testing::Values(
        // Full response with namespace and properties
        CreateNamespaceResponseParam{
            .test_name = "FullResponse",
            .expected_json_str =
                R"({"namespace":["accounting","tax"],"properties":{"owner":"Hank"}})",
            .model = {.namespace_ = Namespace{{"accounting", "tax"}},
                      .properties = {{"owner", "Hank"}}}},
        // Response with empty properties (omit properties field when empty)
        CreateNamespaceResponseParam{
            .test_name = "EmptyProperties",
            .expected_json_str = R"({"namespace":["accounting","tax"]})",
            .model = {.namespace_ = Namespace{{"accounting", "tax"}}}},
        // Response with empty namespace
        CreateNamespaceResponseParam{.test_name = "EmptyNamespace",
                                     .expected_json_str = R"({"namespace":[]})",
                                     .model = {.namespace_ = Namespace{}}}),
    [](const ::testing::TestParamInfo<CreateNamespaceResponseParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(CreateNamespaceResponse)

INSTANTIATE_TEST_SUITE_P(
    CreateNamespaceResponseDeserializeCases, CreateNamespaceResponseDeserializeTest,
    ::testing::Values(
        // Properties field is missing (should deserialize to empty map)
        CreateNamespaceResponseDeserializeParam{
            .test_name = "MissingProperties",
            .json_str = R"({"namespace":["accounting","tax"]})",
            .expected_model = {.namespace_ = Namespace{{"accounting", "tax"}}}},
        // Properties field is null (should deserialize to empty map)
        CreateNamespaceResponseDeserializeParam{
            .test_name = "NullProperties",
            .json_str = R"({"namespace":["accounting","tax"],"properties":null})",
            .expected_model = {.namespace_ = Namespace{{"accounting", "tax"}}}}),
    [](const ::testing::TestParamInfo<CreateNamespaceResponseDeserializeParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(CreateNamespaceResponse)

INSTANTIATE_TEST_SUITE_P(
    CreateNamespaceResponseInvalidCases, CreateNamespaceResponseInvalidTest,
    ::testing::Values(
        // Incorrect type for namespace field
        CreateNamespaceResponseInvalidParam{
            .test_name = "WrongNamespaceType",
            .invalid_json_str = R"({"namespace":"accounting%1Ftax","properties":null})",
            .expected_error_message = "type must be array, but is string"},
        // Incorrect type for properties field
        CreateNamespaceResponseInvalidParam{
            .test_name = "WrongPropertiesType",
            .invalid_json_str = R"({"namespace":["accounting","tax"],"properties":[]})",
            .expected_error_message = "type must be object, but is array"},
        // Empty JSON object
        CreateNamespaceResponseInvalidParam{
            .test_name = "EmptyJson",
            .invalid_json_str = R"({})",
            .expected_error_message = "Missing 'namespace'"}),
    [](const ::testing::TestParamInfo<CreateNamespaceResponseInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(GetNamespaceResponse)

INSTANTIATE_TEST_SUITE_P(
    GetNamespaceResponseCases, GetNamespaceResponseTest,
    ::testing::Values(
        // Full response with namespace and properties
        GetNamespaceResponseParam{
            .test_name = "FullResponse",
            .expected_json_str =
                R"({"namespace":["accounting","tax"],"properties":{"owner":"Hank"}})",
            .model = {.namespace_ = Namespace{{"accounting", "tax"}},
                      .properties = {{"owner", "Hank"}}}},
        // Response with empty properties (omit properties field when empty)
        GetNamespaceResponseParam{
            .test_name = "EmptyProperties",
            .expected_json_str = R"({"namespace":["accounting","tax"]})",
            .model = {.namespace_ = Namespace{{"accounting", "tax"}}}}),
    [](const ::testing::TestParamInfo<GetNamespaceResponseParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(GetNamespaceResponse)

INSTANTIATE_TEST_SUITE_P(
    GetNamespaceResponseDeserializeCases, GetNamespaceResponseDeserializeTest,
    ::testing::Values(
        // Properties field is null (should deserialize to empty map)
        GetNamespaceResponseDeserializeParam{
            .test_name = "NullProperties",
            .json_str = R"({"namespace":["accounting","tax"],"properties":null})",
            .expected_model = {.namespace_ = Namespace{{"accounting", "tax"}}}}),
    [](const ::testing::TestParamInfo<GetNamespaceResponseDeserializeParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(GetNamespaceResponse)

INSTANTIATE_TEST_SUITE_P(
    GetNamespaceResponseInvalidCases, GetNamespaceResponseInvalidTest,
    ::testing::Values(
        // Incorrect type for namespace field
        GetNamespaceResponseInvalidParam{
            .test_name = "WrongNamespaceType",
            .invalid_json_str = R"({"namespace":"accounting%1Ftax","properties":null})",
            .expected_error_message = "type must be array, but is string"},
        // Incorrect type for properties field
        GetNamespaceResponseInvalidParam{
            .test_name = "WrongPropertiesType",
            .invalid_json_str = R"({"namespace":["accounting","tax"],"properties":[]})",
            .expected_error_message = "type must be object, but is array"},
        // Empty JSON object
        GetNamespaceResponseInvalidParam{
            .test_name = "EmptyJson",
            .invalid_json_str = R"({})",
            .expected_error_message = "Missing 'namespace'"}),
    [](const ::testing::TestParamInfo<GetNamespaceResponseInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(ListNamespacesResponse)

INSTANTIATE_TEST_SUITE_P(
    ListNamespacesResponseCases, ListNamespacesResponseTest,
    ::testing::Values(
        // Full response with multiple namespaces
        ListNamespacesResponseParam{
            .test_name = "FullResponse",
            .expected_json_str = R"({"namespaces":[["accounting"],["tax"]]})",
            .model = {.next_page_token = "",
                      .namespaces = {Namespace{{"accounting"}}, Namespace{{"tax"}}}}},
        // Response with empty namespaces
        ListNamespacesResponseParam{.test_name = "EmptyNamespaces",
                                    .expected_json_str = R"({"namespaces":[]})",
                                    .model = {.next_page_token = ""}},
        // Response with page token
        ListNamespacesResponseParam{
            .test_name = "WithPageToken",
            .expected_json_str =
                R"({"namespaces":[["accounting"],["tax"]],"next-page-token":"token"})",
            .model = {.next_page_token = "token",
                      .namespaces = {Namespace{{"accounting"}}, Namespace{{"tax"}}}}}),
    [](const ::testing::TestParamInfo<ListNamespacesResponseParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(ListNamespacesResponse)

INSTANTIATE_TEST_SUITE_P(
    ListNamespacesResponseInvalidCases, ListNamespacesResponseInvalidTest,
    ::testing::Values(
        // Incorrect type for namespaces field
        ListNamespacesResponseInvalidParam{
            .test_name = "WrongNamespacesType",
            .invalid_json_str = R"({"namespaces":"accounting"})",
            .expected_error_message = "Cannot parse namespace from non-array"},
        // Empty JSON object
        ListNamespacesResponseInvalidParam{
            .test_name = "EmptyJson",
            .invalid_json_str = R"({})",
            .expected_error_message = "Missing 'namespaces'"}),
    [](const ::testing::TestParamInfo<ListNamespacesResponseInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(UpdateNamespacePropertiesRequest)

INSTANTIATE_TEST_SUITE_P(
    UpdateNamespacePropertiesRequestCases, UpdateNamespacePropertiesRequestTest,
    ::testing::Values(
        // Full request with both removals and updates
        UpdateNamespacePropertiesRequestParam{
            .test_name = "FullRequest",
            .expected_json_str =
                R"({"removals":["foo","bar"],"updates":{"owner":"Hank"}})",
            .model = {.removals = {"foo", "bar"}, .updates = {{"owner", "Hank"}}}},
        // Request with only updates
        UpdateNamespacePropertiesRequestParam{
            .test_name = "OnlyUpdates",
            .expected_json_str = R"({"updates":{"owner":"Hank"}})",
            .model = {.updates = {{"owner", "Hank"}}}},
        // Request with only removals
        UpdateNamespacePropertiesRequestParam{
            .test_name = "OnlyRemovals",
            .expected_json_str = R"({"removals":["foo","bar"]})",
            .model = {.removals = {"foo", "bar"}}},
        // Request with all empty fields
        UpdateNamespacePropertiesRequestParam{
            .test_name = "AllEmpty", .expected_json_str = R"({})", .model = {}}),
    [](const ::testing::TestParamInfo<UpdateNamespacePropertiesRequestParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(UpdateNamespacePropertiesRequest)

INSTANTIATE_TEST_SUITE_P(
    UpdateNamespacePropertiesRequestDeserializeCases,
    UpdateNamespacePropertiesRequestDeserializeTest,
    ::testing::Values(
        // Removals is null (should deserialize to empty vector)
        UpdateNamespacePropertiesRequestDeserializeParam{
            .test_name = "NullRemovals",
            .json_str = R"({"removals":null,"updates":{"owner":"Hank"}})",
            .expected_model = {.updates = {{"owner", "Hank"}}}},
        // Removals is missing (should deserialize to empty vector)
        UpdateNamespacePropertiesRequestDeserializeParam{
            .test_name = "MissingRemovals",
            .json_str = R"({"updates":{"owner":"Hank"}})",
            .expected_model = {.updates = {{"owner", "Hank"}}}},
        // Updates is null (should deserialize to empty map)
        UpdateNamespacePropertiesRequestDeserializeParam{
            .test_name = "NullUpdates",
            .json_str = R"({"removals":["foo","bar"],"updates":null})",
            .expected_model = {.removals = {"foo", "bar"}}},
        // All fields missing (should deserialize to empty)
        UpdateNamespacePropertiesRequestDeserializeParam{
            .test_name = "AllMissing", .json_str = R"({})", .expected_model = {}}),
    [](const ::testing::TestParamInfo<UpdateNamespacePropertiesRequestDeserializeParam>&
           info) { return info.param.test_name; });

DECLARE_INVALID_TEST(UpdateNamespacePropertiesRequest)

INSTANTIATE_TEST_SUITE_P(
    UpdateNamespacePropertiesRequestInvalidCases,
    UpdateNamespacePropertiesRequestInvalidTest,
    ::testing::Values(
        // Incorrect type for removals field
        UpdateNamespacePropertiesRequestInvalidParam{
            .test_name = "WrongRemovalsType",
            .invalid_json_str =
                R"({"removals":{"foo":"bar"},"updates":{"owner":"Hank"}})",
            .expected_error_message = "type must be array, but is object"},
        // Incorrect type for updates field
        UpdateNamespacePropertiesRequestInvalidParam{
            .test_name = "WrongUpdatesType",
            .invalid_json_str = R"({"removals":["foo","bar"],"updates":["owner"]})",
            .expected_error_message = "type must be object, but is array"}),
    [](const ::testing::TestParamInfo<UpdateNamespacePropertiesRequestInvalidParam>&
           info) { return info.param.test_name; });

DECLARE_ROUNDTRIP_TEST(UpdateNamespacePropertiesResponse)

INSTANTIATE_TEST_SUITE_P(
    UpdateNamespacePropertiesResponseCases, UpdateNamespacePropertiesResponseTest,
    ::testing::Values(
        // Full response with updated, removed, and missing fields
        UpdateNamespacePropertiesResponseParam{
            .test_name = "FullResponse",
            .expected_json_str =
                R"({"removed":["foo"],"updated":["owner"],"missing":["bar"]})",
            .model = {.updated = {"owner"}, .removed = {"foo"}, .missing = {"bar"}}},
        // Response with only updated field
        UpdateNamespacePropertiesResponseParam{
            .test_name = "OnlyUpdated",
            .expected_json_str = R"({"removed":[],"updated":["owner"]})",
            .model = {.updated = {"owner"}}},
        // Response with only removed field
        UpdateNamespacePropertiesResponseParam{
            .test_name = "OnlyRemoved",
            .expected_json_str = R"({"removed":["foo"],"updated":[]})",
            .model = {.removed = {"foo"}}},
        // Response with only missing field
        UpdateNamespacePropertiesResponseParam{
            .test_name = "OnlyMissing",
            .expected_json_str = R"({"removed":[],"updated":[],"missing":["bar"]})",
            .model = {.missing = {"bar"}}},
        // Response with all empty fields
        UpdateNamespacePropertiesResponseParam{
            .test_name = "AllEmpty",
            .expected_json_str = R"({"removed":[],"updated":[]})",
            .model = {}}),
    [](const ::testing::TestParamInfo<UpdateNamespacePropertiesResponseParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(UpdateNamespacePropertiesResponse)

INSTANTIATE_TEST_SUITE_P(
    UpdateNamespacePropertiesResponseDeserializeCases,
    UpdateNamespacePropertiesResponseDeserializeTest,
    ::testing::Values(
        // Only updated and removed present, missing is optional
        UpdateNamespacePropertiesResponseDeserializeParam{
            .test_name = "MissingOptional",
            .json_str = R"({"updated":["owner"],"removed":[]})",
            .expected_model = {.updated = {"owner"}}},
        // All fields are missing
        UpdateNamespacePropertiesResponseDeserializeParam{
            .test_name = "AllMissing", .json_str = R"({})", .expected_model = {}}),
    [](const ::testing::TestParamInfo<UpdateNamespacePropertiesResponseDeserializeParam>&
           info) { return info.param.test_name; });

DECLARE_INVALID_TEST(UpdateNamespacePropertiesResponse)

INSTANTIATE_TEST_SUITE_P(
    UpdateNamespacePropertiesResponseInvalidCases,
    UpdateNamespacePropertiesResponseInvalidTest,
    ::testing::Values(
        // Incorrect type for removed field
        UpdateNamespacePropertiesResponseInvalidParam{
            .test_name = "WrongRemovedType",
            .invalid_json_str =
                R"({"removed":{"foo":true},"updated":["owner"],"missing":["bar"]})",
            .expected_error_message = "type must be array, but is object"},
        // Incorrect type for updated field
        UpdateNamespacePropertiesResponseInvalidParam{
            .test_name = "WrongUpdatedType",
            .invalid_json_str = R"({"updated":"owner","missing":["bar"]})",
            .expected_error_message = "type must be array, but is string"},
        // Valid top-level (array) types, but at least one entry in the list is not the
        // expected type
        UpdateNamespacePropertiesResponseInvalidParam{
            .test_name = "InvalidArrayEntryType",
            .invalid_json_str =
                R"({"removed":["foo", "bar", 123456],"updated":["owner"],"missing":["bar"]})",
            .expected_error_message = " type must be string, but is number"}),
    [](const ::testing::TestParamInfo<UpdateNamespacePropertiesResponseInvalidParam>&
           info) { return info.param.test_name; });

DECLARE_ROUNDTRIP_TEST(ListTablesResponse)

INSTANTIATE_TEST_SUITE_P(
    ListTablesResponseCases, ListTablesResponseTest,
    ::testing::Values(
        // Full response with table identifiers
        ListTablesResponseParam{
            .test_name = "FullResponse",
            .expected_json_str =
                R"({"identifiers":[{"namespace":["accounting","tax"],"name":"paid"}]})",
            .model = {.next_page_token = "",
                      .identifiers = {TableIdentifier{Namespace{{"accounting", "tax"}},
                                                      "paid"}}}},
        // Response with empty identifiers
        ListTablesResponseParam{.test_name = "EmptyIdentifiers",
                                .expected_json_str = R"({"identifiers":[]})",
                                .model = {.next_page_token = ""}},
        // Response with page token
        ListTablesResponseParam{
            .test_name = "WithPageToken",
            .expected_json_str =
                R"({"identifiers":[{"namespace":["accounting","tax"],"name":"paid"}],"next-page-token":"token"})",
            .model = {.next_page_token = "token",
                      .identifiers = {TableIdentifier{Namespace{{"accounting", "tax"}},
                                                      "paid"}}}}),
    [](const ::testing::TestParamInfo<ListTablesResponseParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(ListTablesResponse)

INSTANTIATE_TEST_SUITE_P(
    ListTablesResponseInvalidCases, ListTablesResponseInvalidTest,
    ::testing::Values(
        // Incorrect type for identifiers field (string instead of array)
        ListTablesResponseInvalidParam{
            .test_name = "WrongIdentifiersType",
            .invalid_json_str = R"({"identifiers":"accounting%1Ftax"})",
            .expected_error_message = "Missing 'name'"},
        // Empty JSON object
        ListTablesResponseInvalidParam{.test_name = "EmptyJson",
                                       .invalid_json_str = R"({})",
                                       .expected_error_message = "Missing 'identifiers'"},
        // Invalid identifier with wrong namespace type
        ListTablesResponseInvalidParam{
            .test_name = "InvalidIdentifierNamespaceType",
            .invalid_json_str =
                R"({"identifiers":[{"namespace":"accounting.tax","name":"paid"}]})",
            .expected_error_message = "type must be array, but is string"}),
    [](const ::testing::TestParamInfo<ListTablesResponseInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(RenameTableRequest)

INSTANTIATE_TEST_SUITE_P(
    RenameTableRequestCases, RenameTableRequestTest,
    ::testing::Values(
        // Full request with source and destination table identifiers
        RenameTableRequestParam{
            .test_name = "FullRequest",
            .expected_json_str =
                R"({"source":{"namespace":["accounting","tax"],"name":"paid"},"destination":{"namespace":["accounting","tax"],"name":"paid_2022"}})",
            .model = {.source = TableIdentifier{Namespace{{"accounting", "tax"}}, "paid"},
                      .destination = TableIdentifier{Namespace{{"accounting", "tax"}},
                                                     "paid_2022"}}}),
    [](const ::testing::TestParamInfo<RenameTableRequestParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(RenameTableRequest)

INSTANTIATE_TEST_SUITE_P(
    RenameTableRequestInvalidCases, RenameTableRequestInvalidTest,
    ::testing::Values(
        // Source table name is null
        RenameTableRequestInvalidParam{
            .test_name = "SourceNameNull",
            .invalid_json_str =
                R"({"source":{"namespace":["accounting","tax"],"name":null},"destination":{"namespace":["accounting","tax"],"name":"paid_2022"}})",
            .expected_error_message = "Missing 'name'"},
        // Destination table name is null
        RenameTableRequestInvalidParam{
            .test_name = "DestinationNameNull",
            .invalid_json_str =
                R"({"source":{"namespace":["accounting","tax"],"name":"paid"},"destination":{"namespace":["accounting","tax"],"name":null}})",
            .expected_error_message = "Missing 'name'"},
        // Empty JSON object
        RenameTableRequestInvalidParam{.test_name = "EmptyJson",
                                       .invalid_json_str = R"({})",
                                       .expected_error_message = "Missing 'source'"}),
    [](const ::testing::TestParamInfo<RenameTableRequestInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(RegisterTableRequest)

INSTANTIATE_TEST_SUITE_P(
    RegisterTableRequestCases, RegisterTableRequestTest,
    ::testing::Values(
        // Request with overwrite set to true
        RegisterTableRequestParam{
            .test_name = "WithOverwriteTrue",
            .expected_json_str =
                R"({"name":"table1","metadata-location":"s3://bucket/metadata.json","overwrite":true})",
            .model = {.name = "table1",
                      .metadata_location = "s3://bucket/metadata.json",
                      .overwrite = true}},
        // Request without overwrite field (defaults to false, omitted in serialization)
        RegisterTableRequestParam{
            .test_name = "WithoutOverwrite",
            .expected_json_str =
                R"({"name":"table1","metadata-location":"s3://bucket/metadata.json"})",
            .model = {.name = "table1",
                      .metadata_location = "s3://bucket/metadata.json"}}),
    [](const ::testing::TestParamInfo<RegisterTableRequestParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(RegisterTableRequest)

INSTANTIATE_TEST_SUITE_P(
    RegisterTableRequestDeserializeCases, RegisterTableRequestDeserializeTest,
    ::testing::Values(
        // Overwrite missing (should default to false)
        RegisterTableRequestDeserializeParam{
            .test_name = "MissingOverwrite",
            .json_str =
                R"({"name":"table1","metadata-location":"s3://bucket/metadata.json"})",
            .expected_model = {.name = "table1",
                               .metadata_location = "s3://bucket/metadata.json",
                               .overwrite = false}}),
    [](const ::testing::TestParamInfo<RegisterTableRequestDeserializeParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(RegisterTableRequest)

INSTANTIATE_TEST_SUITE_P(
    RegisterTableRequestInvalidCases, RegisterTableRequestInvalidTest,
    ::testing::Values(
        // Missing required name field
        RegisterTableRequestInvalidParam{
            .test_name = "MissingName",
            .invalid_json_str = R"({"metadata-location":"s3://bucket/metadata.json"})",
            .expected_error_message = "Missing 'name' in"},
        // Missing required metadata-location field
        RegisterTableRequestInvalidParam{
            .test_name = "MissingMetadataLocation",
            .invalid_json_str = R"({"name":"table1"})",
            .expected_error_message = "Missing 'metadata-location'"},
        // Empty JSON object
        RegisterTableRequestInvalidParam{.test_name = "EmptyJson",
                                         .invalid_json_str = R"({})",
                                         .expected_error_message = "Missing 'name'"}),
    [](const ::testing::TestParamInfo<RegisterTableRequestInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(CatalogConfig)

INSTANTIATE_TEST_SUITE_P(
    CatalogConfigCases, CatalogConfigTest,
    ::testing::Values(
        // Full config with both defaults and overrides
        CatalogConfigParam{
            .test_name = "FullConfig",
            .expected_json_str =
                R"({"defaults":{"warehouse":"s3://bucket/warehouse"},"overrides":{"clients":"5"}})",
            .model = {.defaults = {{"warehouse", "s3://bucket/warehouse"}},
                      .overrides = {{"clients", "5"}}}},
        // Only defaults
        CatalogConfigParam{
            .test_name = "OnlyDefaults",
            .expected_json_str =
                R"({"defaults":{"warehouse":"s3://bucket/warehouse"},"overrides":{}})",
            .model = {.defaults = {{"warehouse", "s3://bucket/warehouse"}}}},
        // Only overrides
        CatalogConfigParam{
            .test_name = "OnlyOverrides",
            .expected_json_str = R"({"defaults":{},"overrides":{"clients":"5"}})",
            .model = {.overrides = {{"clients", "5"}}}},
        // Both empty
        CatalogConfigParam{.test_name = "BothEmpty",
                           .expected_json_str = R"({"defaults":{},"overrides":{}})",
                           .model = {}},
        // With valid endpoints
        CatalogConfigParam{
            .test_name = "WithEndpoints",
            .expected_json_str =
                R"({"defaults":{"warehouse":"s3://bucket/warehouse"},"overrides":{"clients":"5"},"endpoints":["GET /v1/config","POST /v1/tables"]})",
            .model = {.defaults = {{"warehouse", "s3://bucket/warehouse"}},
                      .overrides = {{"clients", "5"}},

                      .endpoints = {*Endpoint::Make(HttpMethod::kGet, "/v1/config"),
                                    *Endpoint::Make(HttpMethod::kPost, "/v1/tables")}}},
        // Only endpoints
        CatalogConfigParam{
            .test_name = "OnlyEndpoints",
            .expected_json_str =
                R"({"defaults":{},"overrides":{},"endpoints":["GET /v1/config"]})",
            .model = {.endpoints = {*Endpoint::Make(HttpMethod::kGet, "/v1/config")}}}),
    [](const ::testing::TestParamInfo<CatalogConfigParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(CatalogConfig)

INSTANTIATE_TEST_SUITE_P(
    CatalogConfigDeserializeCases, CatalogConfigDeserializeTest,
    ::testing::Values(
        // Missing overrides field
        CatalogConfigDeserializeParam{
            .test_name = "MissingOverrides",
            .json_str = R"({"defaults":{"warehouse":"s3://bucket/warehouse"}})",
            .expected_model = {.defaults = {{"warehouse", "s3://bucket/warehouse"}}}},
        // Null overrides field
        CatalogConfigDeserializeParam{
            .test_name = "NullOverrides",
            .json_str =
                R"({"defaults":{"warehouse":"s3://bucket/warehouse"},"overrides":null})",
            .expected_model = {.defaults = {{"warehouse", "s3://bucket/warehouse"}}}},
        // Missing defaults field
        CatalogConfigDeserializeParam{
            .test_name = "MissingDefaults",
            .json_str = R"({"overrides":{"clients":"5"}})",
            .expected_model = {.overrides = {{"clients", "5"}}}},
        // Null defaults field
        CatalogConfigDeserializeParam{
            .test_name = "NullDefaults",
            .json_str = R"({"defaults":null,"overrides":{"clients":"5"}})",
            .expected_model = {.overrides = {{"clients", "5"}}}},
        // Empty JSON object
        CatalogConfigDeserializeParam{
            .test_name = "EmptyJson", .json_str = R"({})", .expected_model = {}},
        // Both fields null
        CatalogConfigDeserializeParam{.test_name = "BothNull",
                                      .json_str = R"({"defaults":null,"overrides":null})",
                                      .expected_model = {}},
        // Missing endpoints field, client will uses default endpoints
        CatalogConfigDeserializeParam{
            .test_name = "MissingEndpoints",
            .json_str = R"({"defaults":{},"overrides":{}})",
            .expected_model = {.defaults = {}, .overrides = {}, .endpoints = {}}}),
    [](const ::testing::TestParamInfo<CatalogConfigDeserializeParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(CatalogConfig)

INSTANTIATE_TEST_SUITE_P(
    CatalogConfigInvalidCases, CatalogConfigInvalidTest,
    ::testing::Values(
        // Defaults has wrong type (array instead of object)
        CatalogConfigInvalidParam{
            .test_name = "WrongDefaultsType",
            .invalid_json_str =
                R"({"defaults":["warehouse","s3://bucket/warehouse"],"overrides":{"clients":"5"}})",
            .expected_error_message = "type must be object, but is array"},
        // Overrides has wrong type (string instead of object)
        CatalogConfigInvalidParam{
            .test_name = "WrongOverridesType",
            .invalid_json_str =
                R"({"defaults":{"warehouse":"s3://bucket/warehouse"},"overrides":"clients"})",
            .expected_error_message = "type must be object, but is string"},
        // Invalid endpoint format - missing space separator
        CatalogConfigInvalidParam{
            .test_name = "InvalidEndpointMissingSpace",
            .invalid_json_str = R"({"endpoints":["GET_v1/namespaces/{namespace}"]})",
            .expected_error_message =
                "Invalid endpoint format (must consist of two elements separated by a "
                "single space)"},
        // Invalid endpoint format - extra element after path
        CatalogConfigInvalidParam{
            .test_name = "InvalidEndpointExtraElement",
            .invalid_json_str =
                R"({"endpoints":["GET v1/namespaces/{namespace} INVALID"]})",
            .expected_error_message =
                "Invalid endpoint format (must consist of two elements separated by a "
                "single space)"}),
    [](const ::testing::TestParamInfo<CatalogConfigInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(ErrorResponse)

INSTANTIATE_TEST_SUITE_P(
    ErrorResponseCases, ErrorResponseTest,
    ::testing::Values(
        // Error without stack trace
        ErrorResponseParam{
            .test_name = "WithoutStack",
            .expected_json_str =
                R"({"error":{"message":"The given namespace does not exist","type":"NoSuchNamespaceException","code":404}})",
            .model = {.code = 404,
                      .type = "NoSuchNamespaceException",
                      .message = "The given namespace does not exist"}},
        // Error with stack trace
        ErrorResponseParam{
            .test_name = "WithStack",
            .expected_json_str =
                R"({"error":{"message":"The given namespace does not exist","type":"NoSuchNamespaceException","code":404,"stack":["a","b"]}})",
            .model = {.code = 404,
                      .type = "NoSuchNamespaceException",
                      .message = "The given namespace does not exist",
                      .stack = {"a", "b"}}},
        // Different error type
        ErrorResponseParam{
            .test_name = "DifferentError",
            .expected_json_str =
                R"({"error":{"message":"Internal server error","type":"InternalServerError","code":500,"stack":["line1","line2","line3"]}})",
            .model = {.code = 500,
                      .type = "InternalServerError",
                      .message = "Internal server error",
                      .stack = {"line1", "line2", "line3"}}}),
    [](const ::testing::TestParamInfo<ErrorResponseParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(ErrorResponse)

INSTANTIATE_TEST_SUITE_P(
    ErrorResponseDeserializeCases, ErrorResponseDeserializeTest,
    ::testing::Values(
        // Stack field is null (should deserialize to empty vector)
        ErrorResponseDeserializeParam{
            .test_name = "NullStack",
            .json_str =
                R"({"error":{"message":"The given namespace does not exist","type":"NoSuchNamespaceException","code":404,"stack":null}})",
            .expected_model = {.code = 404,
                               .type = "NoSuchNamespaceException",
                               .message = "The given namespace does not exist"}},
        // Stack field is missing (should deserialize to empty vector)
        ErrorResponseDeserializeParam{
            .test_name = "MissingStack",
            .json_str =
                R"({"error":{"message":"The given namespace does not exist","type":"NoSuchNamespaceException","code":404}})",
            .expected_model = {.code = 404,
                               .type = "NoSuchNamespaceException",
                               .message = "The given namespace does not exist"}}),
    [](const ::testing::TestParamInfo<ErrorResponseDeserializeParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(ErrorResponse)

INSTANTIATE_TEST_SUITE_P(
    ErrorResponseInvalidCases, ErrorResponseInvalidTest,
    ::testing::Values(
        // Missing error field
        ErrorResponseInvalidParam{.test_name = "MissingError",
                                  .invalid_json_str = R"({})",
                                  .expected_error_message = "Missing 'error'"},
        // Null error field
        ErrorResponseInvalidParam{.test_name = "NullError",
                                  .invalid_json_str = R"({"error":null})",
                                  .expected_error_message = "Missing 'error'"},
        // Missing required type field
        ErrorResponseInvalidParam{
            .test_name = "MissingType",
            .invalid_json_str =
                R"({"error":{"message":"The given namespace does not exist","code":404}})",
            .expected_error_message = "Missing 'type'"},
        // Missing required code field
        ErrorResponseInvalidParam{
            .test_name = "MissingCode",
            .invalid_json_str =
                R"({"error":{"message":"The given namespace does not exist","type":"NoSuchNamespaceException"}})",
            .expected_error_message = "Missing 'code'"},
        // Wrong type for message field
        ErrorResponseInvalidParam{
            .test_name = "WrongMessageType",
            .invalid_json_str =
                R"({"error":{"message":123,"type":"NoSuchNamespaceException","code":404}})",
            .expected_error_message = "type must be string, but is number"}),
    [](const ::testing::TestParamInfo<ErrorResponseInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(CreateTableRequest)

INSTANTIATE_TEST_SUITE_P(
    CreateTableRequestCases, CreateTableRequestTest,
    ::testing::Values(
        // Minimal request with only required fields (name and schema)
        CreateTableRequestParam{
            .test_name = "MinimalRequest",
            .expected_json_str =
                R"({"name":"my_table","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"int","required":true},{"id":2,"name":"data","type":"string","required":false}]}})",
            .model = {.name = "my_table", .schema = MakeSimpleSchema()}},
        // Request with location
        CreateTableRequestParam{
            .test_name = "WithLocation",
            .expected_json_str =
                R"({"name":"my_table","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"int","required":true},{"id":2,"name":"data","type":"string","required":false}]},"location":"s3://bucket/warehouse/my_table"})",
            .model = {.name = "my_table",
                      .location = "s3://bucket/warehouse/my_table",
                      .schema = MakeSimpleSchema()}},
        // Request with properties
        CreateTableRequestParam{
            .test_name = "WithProperties",
            .expected_json_str =
                R"({"name":"my_table","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"int","required":true},{"id":2,"name":"data","type":"string","required":false}]},"properties":{"owner":"alice","version":"1.0"}})",
            .model = {.name = "my_table",
                      .schema = MakeSimpleSchema(),
                      .properties = {{"owner", "alice"}, {"version", "1.0"}}}},
        // Request with stage_create = true
        CreateTableRequestParam{
            .test_name = "WithStageCreate",
            .expected_json_str =
                R"({"name":"my_table","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"int","required":true},{"id":2,"name":"data","type":"string","required":false}]},"stage-create":true})",
            .model = {.name = "my_table",
                      .schema = MakeSimpleSchema(),
                      .stage_create = true}},
        // Request with partition_spec (unpartitioned)
        CreateTableRequestParam{
            .test_name = "WithUnpartitionedSpec",
            .expected_json_str =
                R"({"name":"my_table","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"int","required":true},{"id":2,"name":"data","type":"string","required":false}]},"partition-spec":{"spec-id":0,"fields":[]}})",
            .model = {.name = "my_table",
                      .schema = MakeSimpleSchema(),
                      .partition_spec = PartitionSpec::Unpartitioned()}},
        // Request with write_order (unsorted)
        CreateTableRequestParam{
            .test_name = "WithUnsortedOrder",
            .expected_json_str =
                R"({"name":"my_table","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"int","required":true},{"id":2,"name":"data","type":"string","required":false}]},"write-order":{"order-id":0,"fields":[]}})",
            .model = {.name = "my_table",
                      .schema = MakeSimpleSchema(),
                      .write_order = SortOrder::Unsorted()}}),
    [](const ::testing::TestParamInfo<CreateTableRequestParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(CreateTableRequest)

INSTANTIATE_TEST_SUITE_P(
    CreateTableRequestDeserializeCases, CreateTableRequestDeserializeTest,
    ::testing::Values(
        // Location field is missing (should deserialize to empty string)
        CreateTableRequestDeserializeParam{
            .test_name = "MissingLocation",
            .json_str =
                R"({"name":"my_table","schema":{"type":"struct","fields":[{"id":1,"name":"id","type":"int","required":true}]}})",
            .expected_model = {.name = "my_table",
                               .schema =
                                   std::make_shared<Schema>(std::vector<SchemaField>{
                                       SchemaField(1, "id", int32(), false)})}},
        // stage-create field is missing (should default to false)
        CreateTableRequestDeserializeParam{
            .test_name = "MissingStageCreate",
            .json_str =
                R"({"name":"my_table","schema":{"type":"struct","fields":[{"id":1,"name":"id","type":"int","required":true}]}})",
            .expected_model = {.name = "my_table",
                               .schema =
                                   std::make_shared<Schema>(std::vector<SchemaField>{
                                       SchemaField(1, "id", int32(), false)}),
                               .stage_create = false}},
        // Properties field is missing (should deserialize to empty map)
        CreateTableRequestDeserializeParam{
            .test_name = "MissingProperties",
            .json_str =
                R"({"name":"my_table","schema":{"type":"struct","fields":[{"id":1,"name":"id","type":"int","required":true}]}})",
            .expected_model = {.name = "my_table",
                               .schema =
                                   std::make_shared<Schema>(std::vector<SchemaField>{
                                       SchemaField(1, "id", int32(), false)})}}),
    [](const ::testing::TestParamInfo<CreateTableRequestDeserializeParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(CreateTableRequest)

INSTANTIATE_TEST_SUITE_P(
    CreateTableRequestInvalidCases, CreateTableRequestInvalidTest,
    ::testing::Values(
        // Missing required name field
        CreateTableRequestInvalidParam{
            .test_name = "MissingName",
            .invalid_json_str =
                R"({"schema":{"type":"struct","fields":[{"id":1,"name":"id","type":"int","required":true}]}})",
            .expected_error_message = "Missing 'name'"},
        // Missing required schema field
        CreateTableRequestInvalidParam{.test_name = "MissingSchema",
                                       .invalid_json_str = R"({"name":"my_table"})",
                                       .expected_error_message = "Missing 'schema'"},
        // Empty JSON object
        CreateTableRequestInvalidParam{.test_name = "EmptyJson",
                                       .invalid_json_str = R"({})",
                                       .expected_error_message = "Missing 'name'"},
        // Wrong type for name field
        CreateTableRequestInvalidParam{
            .test_name = "WrongNameType",
            .invalid_json_str =
                R"({"name":123,"schema":{"type":"struct","fields":[{"id":1,"name":"id","type":"int","required":true}]}})",
            .expected_error_message = "type must be string, but is number"},
        // Wrong type for schema field
        CreateTableRequestInvalidParam{
            .test_name = "WrongSchemaType",
            .invalid_json_str = R"({"name":"my_table","schema":"invalid"})",
            .expected_error_message = "Unknown primitive type: invalid"}),
    [](const ::testing::TestParamInfo<CreateTableRequestInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(LoadTableResult)

INSTANTIATE_TEST_SUITE_P(
    LoadTableResultCases, LoadTableResultTest,
    ::testing::Values(
        // Minimal case - only required metadata field
        LoadTableResultParam{
            .test_name = "MinimalMetadata",
            .expected_json_str =
                R"({"metadata":{"current-schema-id":1,"current-snapshot-id":null,"default-sort-order-id":0,"default-spec-id":0,"format-version":2,"last-column-id":1,"last-partition-id":0,"last-sequence-number":0,"last-updated-ms":0,"location":"s3://bucket/test","metadata-log":[],"partition-specs":[{"fields":[],"spec-id":0}],"partition-statistics":[],"properties":{},"refs":{},"schemas":[{"fields":[{"id":1,"name":"id","required":true,"type":"int"}],"schema-id":1,"type":"struct"}],"snapshot-log":[],"snapshots":[],"sort-orders":[{"fields":[],"order-id":0}],"statistics":[],"table-uuid":"test-uuid-1234"}})",
            .model = {.metadata = MakeSimpleTableMetadata()}},
        // With metadata location
        LoadTableResultParam{
            .test_name = "WithMetadataLocation",
            .expected_json_str =
                R"({"metadata":{"current-schema-id":1,"current-snapshot-id":null,"default-sort-order-id":0,"default-spec-id":0,"format-version":2,"last-column-id":1,"last-partition-id":0,"last-sequence-number":0,"last-updated-ms":0,"location":"s3://bucket/test","metadata-log":[],"partition-specs":[{"fields":[],"spec-id":0}],"partition-statistics":[],"properties":{},"refs":{},"schemas":[{"fields":[{"id":1,"name":"id","required":true,"type":"int"}],"schema-id":1,"type":"struct"}],"snapshot-log":[],"snapshots":[],"sort-orders":[{"fields":[],"order-id":0}],"statistics":[],"table-uuid":"test-uuid-1234"},"metadata-location":"s3://bucket/metadata/v1.json"})",
            .model = {.metadata_location = "s3://bucket/metadata/v1.json",
                      .metadata = MakeSimpleTableMetadata()}},
        // With config
        LoadTableResultParam{
            .test_name = "WithConfig",
            .expected_json_str =
                R"({"config":{"warehouse":"s3://bucket/warehouse"},"metadata":{"current-schema-id":1,"current-snapshot-id":null,"default-sort-order-id":0,"default-spec-id":0,"format-version":2,"last-column-id":1,"last-partition-id":0,"last-sequence-number":0,"last-updated-ms":0,"location":"s3://bucket/test","metadata-log":[],"partition-specs":[{"fields":[],"spec-id":0}],"partition-statistics":[],"properties":{},"refs":{},"schemas":[{"fields":[{"id":1,"name":"id","required":true,"type":"int"}],"schema-id":1,"type":"struct"}],"snapshot-log":[],"snapshots":[],"sort-orders":[{"fields":[],"order-id":0}],"statistics":[],"table-uuid":"test-uuid-1234"}})",
            .model = {.metadata = MakeSimpleTableMetadata(),
                      .config = {{"warehouse", "s3://bucket/warehouse"}}}},
        // With both metadata location and config
        LoadTableResultParam{
            .test_name = "WithMetadataLocationAndConfig",
            .expected_json_str =
                R"({"config":{"foo":"bar","warehouse":"s3://bucket/warehouse"},"metadata":{"current-schema-id":1,"current-snapshot-id":null,"default-sort-order-id":0,"default-spec-id":0,"format-version":2,"last-column-id":1,"last-partition-id":0,"last-sequence-number":0,"last-updated-ms":0,"location":"s3://bucket/test","metadata-log":[],"partition-specs":[{"fields":[],"spec-id":0}],"partition-statistics":[],"properties":{},"refs":{},"schemas":[{"fields":[{"id":1,"name":"id","required":true,"type":"int"}],"schema-id":1,"type":"struct"}],"snapshot-log":[],"snapshots":[],"sort-orders":[{"fields":[],"order-id":0}],"statistics":[],"table-uuid":"test-uuid-1234"},"metadata-location":"s3://bucket/metadata/v1.json"})",
            .model = {.metadata_location = "s3://bucket/metadata/v1.json",
                      .metadata = MakeSimpleTableMetadata(),
                      .config = {{"warehouse", "s3://bucket/warehouse"},
                                 {"foo", "bar"}}}}),
    [](const ::testing::TestParamInfo<LoadTableResultParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(LoadTableResult)

INSTANTIATE_TEST_SUITE_P(
    LoadTableResultDeserializeCases, LoadTableResultDeserializeTest,
    ::testing::Values(
        // Minimal metadata - tests basic deserialization
        LoadTableResultDeserializeParam{
            .test_name = "MinimalMetadata",
            .json_str =
                R"({"metadata":{"format-version":2,"table-uuid":"test-uuid-1234","location":"s3://bucket/test","last-sequence-number":0,"last-updated-ms":0,"last-column-id":1,"schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"id","type":"int","required":true}]}],"current-schema-id":1,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0,"properties":{}}})",
            .expected_model = {.metadata = MakeSimpleTableMetadata()}},
        // With metadata location
        LoadTableResultDeserializeParam{
            .test_name = "WithMetadataLocation",
            .json_str =
                R"({"metadata-location":"s3://bucket/metadata/v1.json","metadata":{"format-version":2,"table-uuid":"test-uuid-1234","location":"s3://bucket/test","last-sequence-number":0,"last-updated-ms":0,"last-column-id":1,"schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"id","type":"int","required":true}]}],"current-schema-id":1,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0,"properties":{}}})",
            .expected_model = {.metadata_location = "s3://bucket/metadata/v1.json",
                               .metadata = MakeSimpleTableMetadata()}},
        // With config
        LoadTableResultDeserializeParam{
            .test_name = "WithConfig",
            .json_str =
                R"({"metadata":{"format-version":2,"table-uuid":"test-uuid-1234","location":"s3://bucket/test","last-sequence-number":0,"last-updated-ms":0,"last-column-id":1,"schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"id","type":"int","required":true}]}],"current-schema-id":1,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0,"properties":{}},"config":{"warehouse":"s3://bucket/warehouse"}})",
            .expected_model = {.metadata = MakeSimpleTableMetadata(),
                               .config = {{"warehouse", "s3://bucket/warehouse"}}}}),
    [](const ::testing::TestParamInfo<LoadTableResultDeserializeParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(LoadTableResult)

INSTANTIATE_TEST_SUITE_P(
    LoadTableResultInvalidCases, LoadTableResultInvalidTest,
    ::testing::Values(
        // Missing required metadata field
        LoadTableResultInvalidParam{.test_name = "MissingMetadata",
                                    .invalid_json_str = R"({})",
                                    .expected_error_message = "Missing 'metadata'"},
        // Null metadata field
        LoadTableResultInvalidParam{.test_name = "NullMetadata",
                                    .invalid_json_str = R"({"metadata":null})",
                                    .expected_error_message = "Missing 'metadata'"},
        // Wrong type for metadata field
        LoadTableResultInvalidParam{
            .test_name = "WrongMetadataType",
            .invalid_json_str = R"({"metadata":"invalid"})",
            .expected_error_message = "Cannot parse metadata from a non-object"},
        // Wrong type for metadata-location field
        LoadTableResultInvalidParam{
            .test_name = "WrongMetadataLocationType",
            .invalid_json_str =
                R"({"metadata-location":123,"metadata":{"format-version":2,"table-uuid":"test","location":"s3://test","last-sequence-number":0,"schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"id","type":"int","required":true}]}],"current-schema-id":1,"default-spec-id":0,"last-partition-id":0,"default-sort-order-id":0}})",
            .expected_error_message = "type must be string, but is number"},
        // Wrong type for config field
        LoadTableResultInvalidParam{
            .test_name = "WrongConfigType",
            .invalid_json_str =
                R"({"config":"invalid","metadata":{"format-version":2,"table-uuid":"test","location":"s3://test","last-sequence-number":0,"last-column-id":1,"last-updated-ms":0,"schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"id","type":"int","required":true}]}],"current-schema-id":1,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0}})",
            .expected_error_message = "type must be object, but is string"},
        // Invalid metadata content
        LoadTableResultInvalidParam{
            .test_name = "InvalidMetadataContent",
            .invalid_json_str = R"({"metadata":{"format-version":"invalid"}})",
            .expected_error_message = "type must be number, but is string"}),
    [](const ::testing::TestParamInfo<LoadTableResultInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(CommitTableRequest)

INSTANTIATE_TEST_SUITE_P(
    CommitTableRequestCases, CommitTableRequestTest,
    ::testing::Values(
        // Full request with identifier, requirements, and updates
        CommitTableRequestParam{
            .test_name = "FullRequest",
            .expected_json_str =
                R"({"identifier":{"namespace":["ns1"],"name":"table1"},"requirements":[{"type":"assert-table-uuid","uuid":"2cc52516-5e73-41f2-b139-545d41a4e151"},{"type":"assert-create"}],"updates":[{"action":"assign-uuid","uuid":"2cc52516-5e73-41f2-b139-545d41a4e151"},{"action":"set-current-schema","schema-id":23}]})",
            .model = {.identifier = TableIdentifier{Namespace{{"ns1"}}, "table1"},
                      .requirements = {std::make_shared<table::AssertUUID>(
                                           "2cc52516-5e73-41f2-b139-545d41a4e151"),
                                       std::make_shared<table::AssertDoesNotExist>()},
                      .updates = {std::make_shared<table::AssignUUID>(
                                      "2cc52516-5e73-41f2-b139-545d41a4e151"),
                                  std::make_shared<table::SetCurrentSchema>(23)}}},
        // Request without identifier (identifier optional)
        CommitTableRequestParam{
            .test_name = "WithoutIdentifier",
            .expected_json_str =
                R"({"requirements":[{"type":"assert-table-uuid","uuid":"2cc52516-5e73-41f2-b139-545d41a4e151"},{"type":"assert-create"}],"updates":[{"action":"assign-uuid","uuid":"2cc52516-5e73-41f2-b139-545d41a4e151"},{"action":"set-current-schema","schema-id":23}]})",
            .model = {.requirements = {std::make_shared<table::AssertUUID>(
                                           "2cc52516-5e73-41f2-b139-545d41a4e151"),
                                       std::make_shared<table::AssertDoesNotExist>()},
                      .updates = {std::make_shared<table::AssignUUID>(
                                      "2cc52516-5e73-41f2-b139-545d41a4e151"),
                                  std::make_shared<table::SetCurrentSchema>(23)}}},
        // Request with empty requirements and updates
        CommitTableRequestParam{
            .test_name = "EmptyRequirementsAndUpdates",
            .expected_json_str =
                R"({"identifier":{"namespace":["ns1"],"name":"table1"},"requirements":[],"updates":[]})",
            .model = {.identifier = TableIdentifier{Namespace{{"ns1"}}, "table1"}}}),
    [](const ::testing::TestParamInfo<CommitTableRequestParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(CommitTableRequest)

INSTANTIATE_TEST_SUITE_P(
    CommitTableRequestDeserializeCases, CommitTableRequestDeserializeTest,
    ::testing::Values(
        // Identifier field is missing (should deserialize to empty identifier)
        CommitTableRequestDeserializeParam{
            .test_name = "MissingIdentifier",
            .json_str = R"({"requirements":[],"updates":[]})",
            .expected_model = {}}),
    [](const ::testing::TestParamInfo<CommitTableRequestDeserializeParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(CommitTableRequest)

INSTANTIATE_TEST_SUITE_P(
    CommitTableRequestInvalidCases, CommitTableRequestInvalidTest,
    ::testing::Values(
        // Invalid table identifier - missing name field
        CommitTableRequestInvalidParam{
            .test_name = "InvalidTableIdentifier",
            .invalid_json_str = R"({"identifier":{},"requirements":[],"updates":[]})",
            .expected_error_message = "Missing 'name'"},
        // Invalid table identifier - wrong type for name
        CommitTableRequestInvalidParam{
            .test_name = "InvalidIdentifierNameType",
            .invalid_json_str =
                R"({"identifier":{"namespace":["ns1"],"name":23},"requirements":[],"updates":[]})",
            .expected_error_message = "type must be string, but is number"},
        // Invalid requirements - non-object value in requirements array
        CommitTableRequestInvalidParam{
            .test_name = "InvalidRequirementsNonObject",
            .invalid_json_str =
                R"({"identifier":{"namespace":["ns1"],"name":"table1"},"requirements":[23],"updates":[]})",
            .expected_error_message = "Missing 'type' in"},
        // Invalid requirements - missing type field
        CommitTableRequestInvalidParam{
            .test_name = "InvalidRequirementsMissingType",
            .invalid_json_str =
                R"({"identifier":{"namespace":["ns1"],"name":"table1"},"requirements":[{}],"updates":[]})",
            .expected_error_message = "Missing 'type'"},
        // Invalid requirements - assert-table-uuid missing uuid field
        CommitTableRequestInvalidParam{
            .test_name = "InvalidRequirementsMissingUUID",
            .invalid_json_str =
                R"({"identifier":{"namespace":["ns1"],"name":"table1"},"requirements":[{"type":"assert-table-uuid"}],"updates":[]})",
            .expected_error_message = "Missing 'uuid'"},
        // Invalid updates - non-object value in updates array
        CommitTableRequestInvalidParam{
            .test_name = "InvalidUpdatesNonObject",
            .invalid_json_str =
                R"({"identifier":{"namespace":["ns1"],"name":"table1"},"requirements":[],"updates":[23]})",
            .expected_error_message = "Missing 'action' in"},
        // Invalid updates - missing action field
        CommitTableRequestInvalidParam{
            .test_name = "InvalidUpdatesMissingAction",
            .invalid_json_str =
                R"({"identifier":{"namespace":["ns1"],"name":"table1"},"requirements":[],"updates":[{}]})",
            .expected_error_message = "Missing 'action'"},
        // Invalid updates - assign-uuid missing uuid field
        CommitTableRequestInvalidParam{
            .test_name = "InvalidUpdatesMissingUUID",
            .invalid_json_str =
                R"({"identifier":{"namespace":["ns1"],"name":"table1"},"requirements":[],"updates":[{"action":"assign-uuid"}]})",
            .expected_error_message = "Missing 'uuid'"},
        // Missing required requirements field
        CommitTableRequestInvalidParam{
            .test_name = "MissingRequirements",
            .invalid_json_str =
                R"({"identifier":{"namespace":["ns1"],"name":"table1"},"updates":[]})",
            .expected_error_message = "Missing 'requirements'"},
        // Missing required updates field
        CommitTableRequestInvalidParam{
            .test_name = "MissingUpdates",
            .invalid_json_str =
                R"({"identifier":{"namespace":["ns1"],"name":"table1"},"requirements":[]})",
            .expected_error_message = "Missing 'updates'"},
        // Empty JSON object
        CommitTableRequestInvalidParam{
            .test_name = "EmptyJson",
            .invalid_json_str = R"({})",
            .expected_error_message = "Missing 'requirements'"}),
    [](const ::testing::TestParamInfo<CommitTableRequestInvalidParam>& info) {
      return info.param.test_name;
    });

DECLARE_ROUNDTRIP_TEST(CommitTableResponse)

INSTANTIATE_TEST_SUITE_P(
    CommitTableResponseCases, CommitTableResponseTest,
    ::testing::Values(
        // Full response with metadata location and metadata
        CommitTableResponseParam{
            .test_name = "FullResponse",
            .expected_json_str =
                R"({"metadata-location":"s3://bucket/metadata/v2.json","metadata":{"current-schema-id":1,"current-snapshot-id":null,"default-sort-order-id":0,"default-spec-id":0,"format-version":2,"last-column-id":1,"last-partition-id":0,"last-sequence-number":0,"last-updated-ms":0,"location":"s3://bucket/test","metadata-log":[],"partition-specs":[{"fields":[],"spec-id":0}],"partition-statistics":[],"properties":{},"refs":{},"schemas":[{"fields":[{"id":1,"name":"id","required":true,"type":"int"}],"schema-id":1,"type":"struct"}],"snapshot-log":[],"snapshots":[],"sort-orders":[{"fields":[],"order-id":0}],"statistics":[],"table-uuid":"test-uuid-1234"}})",
            .model = {.metadata_location = "s3://bucket/metadata/v2.json",
                      .metadata = MakeSimpleTableMetadata()}}),
    [](const ::testing::TestParamInfo<CommitTableResponseParam>& info) {
      return info.param.test_name;
    });

DECLARE_DESERIALIZE_TEST(CommitTableResponse)

INSTANTIATE_TEST_SUITE_P(
    CommitTableResponseDeserializeCases, CommitTableResponseDeserializeTest,
    ::testing::Values(
        // Standard response with all fields
        CommitTableResponseDeserializeParam{
            .test_name = "StandardResponse",
            .json_str =
                R"({"metadata-location":"s3://bucket/metadata/v2.json","metadata":{"format-version":2,"table-uuid":"test-uuid-1234","location":"s3://bucket/test","last-sequence-number":0,"last-updated-ms":0,"last-column-id":1,"schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"id","type":"int","required":true}]}],"current-schema-id":1,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0,"properties":{}}})",
            .expected_model = {.metadata_location = "s3://bucket/metadata/v2.json",
                               .metadata = MakeSimpleTableMetadata()}}),
    [](const ::testing::TestParamInfo<CommitTableResponseDeserializeParam>& info) {
      return info.param.test_name;
    });

DECLARE_INVALID_TEST(CommitTableResponse)

INSTANTIATE_TEST_SUITE_P(
    CommitTableResponseInvalidCases, CommitTableResponseInvalidTest,
    ::testing::Values(
        // Missing required metadata-location field
        CommitTableResponseInvalidParam{
            .test_name = "MissingMetadataLocation",
            .invalid_json_str =
                R"({"metadata":{"format-version":2,"table-uuid":"test","location":"s3://test","last-sequence-number":0,"last-column-id":1,"last-updated-ms":0,"schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"id","type":"int","required":true}]}],"current-schema-id":1,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0}})",
            .expected_error_message = "Missing 'metadata-location'"},
        // Missing required metadata field
        CommitTableResponseInvalidParam{
            .test_name = "MissingMetadata",
            .invalid_json_str = R"({"metadata-location":"s3://bucket/metadata/v2.json"})",
            .expected_error_message = "Missing 'metadata'"},
        // Null metadata field
        CommitTableResponseInvalidParam{
            .test_name = "NullMetadata",
            .invalid_json_str =
                R"({"metadata-location":"s3://bucket/metadata/v2.json","metadata":null})",
            .expected_error_message = "Missing 'metadata'"},
        // Wrong type for metadata-location field
        CommitTableResponseInvalidParam{
            .test_name = "WrongMetadataLocationType",
            .invalid_json_str =
                R"({"metadata-location":123,"metadata":{"format-version":2,"table-uuid":"test","location":"s3://test","last-sequence-number":0,"last-column-id":1,"last-updated-ms":0,"schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"id","type":"int","required":true}]}],"current-schema-id":1,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0}})",
            .expected_error_message = "type must be string, but is number"},
        // Wrong type for metadata field
        CommitTableResponseInvalidParam{
            .test_name = "WrongMetadataType",
            .invalid_json_str =
                R"({"metadata-location":"s3://bucket/metadata/v2.json","metadata":"invalid"})",
            .expected_error_message = "Cannot parse metadata from a non-object"},
        // Empty JSON object
        CommitTableResponseInvalidParam{
            .test_name = "EmptyJson",
            .invalid_json_str = R"({})",
            .expected_error_message = "Missing 'metadata-location'"}),
    [](const ::testing::TestParamInfo<CommitTableResponseInvalidParam>& info) {
      return info.param.test_name;
    });

}  // namespace iceberg::rest
