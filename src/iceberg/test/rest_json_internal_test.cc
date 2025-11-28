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
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

// TODO(gangwu): perhaps add these equality operators to the types themselves?
bool operator==(const CreateNamespaceRequest& lhs, const CreateNamespaceRequest& rhs) {
  return lhs.namespace_.levels == rhs.namespace_.levels &&
         lhs.properties == rhs.properties;
}

bool operator==(const UpdateNamespacePropertiesRequest& lhs,
                const UpdateNamespacePropertiesRequest& rhs) {
  return lhs.removals == rhs.removals && lhs.updates == rhs.updates;
}

bool operator==(const RegisterTableRequest& lhs, const RegisterTableRequest& rhs) {
  return lhs.name == rhs.name && lhs.metadata_location == rhs.metadata_location &&
         lhs.overwrite == rhs.overwrite;
}

bool operator==(const CreateNamespaceResponse& lhs, const CreateNamespaceResponse& rhs) {
  return lhs.namespace_.levels == rhs.namespace_.levels &&
         lhs.properties == rhs.properties;
}

bool operator==(const GetNamespaceResponse& lhs, const GetNamespaceResponse& rhs) {
  return lhs.namespace_.levels == rhs.namespace_.levels &&
         lhs.properties == rhs.properties;
}

bool operator==(const ListNamespacesResponse& lhs, const ListNamespacesResponse& rhs) {
  if (lhs.namespaces.size() != rhs.namespaces.size()) return false;
  for (size_t i = 0; i < lhs.namespaces.size(); ++i) {
    if (lhs.namespaces[i].levels != rhs.namespaces[i].levels) return false;
  }
  return lhs.next_page_token == rhs.next_page_token;
}

bool operator==(const UpdateNamespacePropertiesResponse& lhs,
                const UpdateNamespacePropertiesResponse& rhs) {
  return lhs.updated == rhs.updated && lhs.removed == rhs.removed &&
         lhs.missing == rhs.missing;
}

bool operator==(const ListTablesResponse& lhs, const ListTablesResponse& rhs) {
  if (lhs.identifiers.size() != rhs.identifiers.size()) return false;
  for (size_t i = 0; i < lhs.identifiers.size(); ++i) {
    if (lhs.identifiers[i].ns.levels != rhs.identifiers[i].ns.levels ||
        lhs.identifiers[i].name != rhs.identifiers[i].name) {
      return false;
    }
  }
  return lhs.next_page_token == rhs.next_page_token;
}

bool operator==(const RenameTableRequest& lhs, const RenameTableRequest& rhs) {
  return lhs.source.ns.levels == rhs.source.ns.levels &&
         lhs.source.name == rhs.source.name &&
         lhs.destination.ns.levels == rhs.destination.ns.levels &&
         lhs.destination.name == rhs.destination.name;
}

bool operator==(const CatalogConfig& lhs, const CatalogConfig& rhs) {
  return lhs.overrides == rhs.overrides && lhs.defaults == rhs.defaults &&
         lhs.endpoints == rhs.endpoints;
}

bool operator==(const ErrorResponse& lhs, const ErrorResponse& rhs) {
  return lhs.message == rhs.message && lhs.type == rhs.type && lhs.code == rhs.code &&
         lhs.stack == rhs.stack;
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
        // With endpoints
        CatalogConfigParam{
            .test_name = "WithEndpoints",
            .expected_json_str =
                R"({"defaults":{"warehouse":"s3://bucket/warehouse"},"overrides":{"clients":"5"},"endpoints":["GET /v1/config","POST /v1/tables"]})",
            .model = {.defaults = {{"warehouse", "s3://bucket/warehouse"}},
                      .overrides = {{"clients", "5"}},

                      .endpoints = {"GET /v1/config", "POST /v1/tables"}}},
        // Only endpoints
        CatalogConfigParam{
            .test_name = "OnlyEndpoints",
            .expected_json_str =
                R"({"defaults":{},"overrides":{},"endpoints":["GET /v1/config"]})",
            .model = {.endpoints = {"GET /v1/config"}}}),
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
                                      .expected_model = {}}),
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
            .expected_error_message = "type must be object, but is string"}),
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
            .model = {.message = "The given namespace does not exist",
                      .type = "NoSuchNamespaceException",
                      .code = 404}},
        // Error with stack trace
        ErrorResponseParam{
            .test_name = "WithStack",
            .expected_json_str =
                R"({"error":{"message":"The given namespace does not exist","type":"NoSuchNamespaceException","code":404,"stack":["a","b"]}})",
            .model = {.message = "The given namespace does not exist",
                      .type = "NoSuchNamespaceException",
                      .code = 404,
                      .stack = {"a", "b"}}},
        // Different error type
        ErrorResponseParam{
            .test_name = "DifferentError",
            .expected_json_str =
                R"({"error":{"message":"Internal server error","type":"InternalServerError","code":500,"stack":["line1","line2","line3"]}})",
            .model = {.message = "Internal server error",
                      .type = "InternalServerError",
                      .code = 500,
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
            .expected_model = {.message = "The given namespace does not exist",
                               .type = "NoSuchNamespaceException",
                               .code = 404}},
        // Stack field is missing (should deserialize to empty vector)
        ErrorResponseDeserializeParam{
            .test_name = "MissingStack",
            .json_str =
                R"({"error":{"message":"The given namespace does not exist","type":"NoSuchNamespaceException","code":404}})",
            .expected_model = {.message = "The given namespace does not exist",
                               .type = "NoSuchNamespaceException",
                               .code = 404}}),
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

}  // namespace iceberg::rest
