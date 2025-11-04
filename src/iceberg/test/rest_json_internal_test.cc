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

bool operator==(const ErrorModel& lhs, const ErrorModel& rhs) {
  return lhs.message == rhs.message && lhs.type == rhs.type && lhs.code == rhs.code &&
         lhs.stack == rhs.stack;
}

bool operator==(const ErrorResponse& lhs, const ErrorResponse& rhs) {
  return lhs.error == rhs.error;
}

struct CreateNamespaceRequestParam {
  std::string test_name;
  std::string expected_json_str;
  Namespace namespace_;
  std::unordered_map<std::string, std::string> properties;
};

class CreateNamespaceRequestTest
    : public ::testing::TestWithParam<CreateNamespaceRequestParam> {
 protected:
  void TestRoundTrip() {
    const auto& param = GetParam();

    // Build original object
    CreateNamespaceRequest original;
    original.namespace_ = param.namespace_;
    original.properties = param.properties;

    // ToJson and verify JSON string
    auto json = ToJson(original);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    EXPECT_EQ(json, expected_json) << "ToJson mismatch";

    // FromJson and verify object equality
    auto result = CreateNamespaceRequestFromJson(expected_json);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto& parsed = result.value();

    EXPECT_EQ(parsed, original);
  }
};

TEST_P(CreateNamespaceRequestTest, RoundTrip) { TestRoundTrip(); }

INSTANTIATE_TEST_SUITE_P(
    CreateNamespaceRequestCases, CreateNamespaceRequestTest,
    ::testing::Values(
        // Full request with properties
        CreateNamespaceRequestParam{
            .test_name = "FullRequest",
            .expected_json_str =
                R"({"namespace":["accounting","tax"],"properties":{"owner":"Hank"}})",
            .namespace_ = Namespace{{"accounting", "tax"}},
            .properties = {{"owner", "Hank"}},
        },
        // Request with empty properties (omit properties field when empty)
        CreateNamespaceRequestParam{
            .test_name = "EmptyProperties",
            .expected_json_str = R"({"namespace":["accounting","tax"]})",
            .namespace_ = Namespace{{"accounting", "tax"}},
            .properties = {},
        },
        // Request with empty namespace
        CreateNamespaceRequestParam{
            .test_name = "EmptyNamespace",
            .expected_json_str = R"({"namespace":[]})",
            .namespace_ = Namespace{},
            .properties = {},
        }),
    [](const ::testing::TestParamInfo<CreateNamespaceRequestParam>& info) {
      return info.param.test_name;
    });

TEST(CreateNamespaceRequestTest, DeserializeWithoutDefaults) {
  // Properties is null
  std::string json_null_props = R"({"namespace":["accounting","tax"],"properties":null})";
  auto result1 = CreateNamespaceRequestFromJson(nlohmann::json::parse(json_null_props));
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(result1.value().namespace_.levels,
            std::vector<std::string>({"accounting", "tax"}));
  EXPECT_TRUE(result1.value().properties.empty());

  // Properties is missing
  std::string json_missing_props = R"({"namespace":["accounting","tax"]})";
  auto result2 =
      CreateNamespaceRequestFromJson(nlohmann::json::parse(json_missing_props));
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(result2.value().namespace_.levels,
            std::vector<std::string>({"accounting", "tax"}));
  EXPECT_TRUE(result2.value().properties.empty());
}

TEST(CreateNamespaceRequestTest, InvalidRequests) {
  // Incorrect type for namespace
  std::string json_wrong_ns_type =
      R"({"namespace":"accounting%1Ftax","properties":null})";
  auto result1 =
      CreateNamespaceRequestFromJson(nlohmann::json::parse(json_wrong_ns_type));
  EXPECT_FALSE(result1.has_value());
  EXPECT_THAT(result1, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result1.error().message,
            "Failed to parse 'namespace' from "
            "{\"namespace\":\"accounting%1Ftax\",\"properties\":null}: "
            "[json.exception.type_error.302] type must be array, but is string");

  // Incorrect type for properties
  std::string json_wrong_props_type =
      R"({"namespace":["accounting","tax"],"properties":[]})";
  auto result2 =
      CreateNamespaceRequestFromJson(nlohmann::json::parse(json_wrong_props_type));
  EXPECT_FALSE(result2.has_value());
  EXPECT_THAT(result2, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result2.error().message,
            "Failed to parse 'properties' from "
            "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":[]}: "
            "[json.exception.type_error.302] type must be object, but is array");

  // Misspelled keys
  std::string json_misspelled =
      R"({"namepsace":["accounting","tax"],"propertiezzzz":{"owner":"Hank"}})";
  auto result3 = CreateNamespaceRequestFromJson(nlohmann::json::parse(json_misspelled));
  EXPECT_FALSE(result3.has_value());
  EXPECT_THAT(result3, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(
      result3.error().message,
      "Missing 'namespace' in "
      "{\"namepsace\":[\"accounting\",\"tax\"],\"propertiezzzz\":{\"owner\":\"Hank\"}}");

  // Empty JSON
  std::string json_empty = R"({})";
  auto result4 = CreateNamespaceRequestFromJson(nlohmann::json::parse(json_empty));
  EXPECT_FALSE(result4.has_value());
  EXPECT_THAT(result4, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result4.error().message, "Missing 'namespace' in {}");
}

struct CreateNamespaceResponseParam {
  std::string test_name;
  std::string expected_json_str;
  Namespace namespace_;
  std::unordered_map<std::string, std::string> properties;
};

class CreateNamespaceResponseTest
    : public ::testing::TestWithParam<CreateNamespaceResponseParam> {
 protected:
  void TestRoundTrip() {
    const auto& param = GetParam();

    CreateNamespaceResponse original;
    original.namespace_ = param.namespace_;
    original.properties = param.properties;

    auto json = ToJson(original);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    EXPECT_EQ(json, expected_json);

    auto result = CreateNamespaceResponseFromJson(expected_json);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto& parsed = result.value();

    EXPECT_EQ(parsed, original);
  }
};

TEST_P(CreateNamespaceResponseTest, RoundTrip) { TestRoundTrip(); }

INSTANTIATE_TEST_SUITE_P(
    CreateNamespaceResponseCases, CreateNamespaceResponseTest,
    ::testing::Values(
        CreateNamespaceResponseParam{
            .test_name = "FullResponse",
            .expected_json_str =
                R"({"namespace":["accounting","tax"],"properties":{"owner":"Hank"}})",
            .namespace_ = Namespace{{"accounting", "tax"}},
            .properties = {{"owner", "Hank"}},
        },
        CreateNamespaceResponseParam{
            .test_name = "EmptyProperties",
            .expected_json_str = R"({"namespace":["accounting","tax"]})",
            .namespace_ = Namespace{{"accounting", "tax"}},
            .properties = {},
        },
        CreateNamespaceResponseParam{.test_name = "EmptyNamespace",
                                     .expected_json_str = R"({"namespace":[]})",
                                     .namespace_ = Namespace{},
                                     .properties = {}}),
    [](const ::testing::TestParamInfo<CreateNamespaceResponseParam>& info) {
      return info.param.test_name;
    });

TEST(CreateNamespaceResponseTest, DeserializeWithoutDefaults) {
  std::string json_missing_props = R"({"namespace":["accounting","tax"]})";
  auto result1 =
      CreateNamespaceResponseFromJson(nlohmann::json::parse(json_missing_props));
  ASSERT_TRUE(result1.has_value());
  EXPECT_TRUE(result1.value().properties.empty());

  std::string json_null_props = R"({"namespace":["accounting","tax"],"properties":null})";
  auto result2 = CreateNamespaceResponseFromJson(nlohmann::json::parse(json_null_props));
  ASSERT_TRUE(result2.has_value());
  EXPECT_TRUE(result2.value().properties.empty());
}

TEST(CreateNamespaceResponseTest, InvalidResponses) {
  std::string json_wrong_ns_type =
      R"({"namespace":"accounting%1Ftax","properties":null})";
  auto result1 =
      CreateNamespaceResponseFromJson(nlohmann::json::parse(json_wrong_ns_type));
  EXPECT_FALSE(result1.has_value());
  EXPECT_THAT(result1, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result1.error().message,
            "Failed to parse 'namespace' from "
            "{\"namespace\":\"accounting%1Ftax\",\"properties\":null}: "
            "[json.exception.type_error.302] type must be array, but is string");

  std::string json_wrong_props_type =
      R"({"namespace":["accounting","tax"],"properties":[]})";
  auto result2 =
      CreateNamespaceResponseFromJson(nlohmann::json::parse(json_wrong_props_type));
  EXPECT_FALSE(result2.has_value());
  EXPECT_THAT(result2, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result2.error().message,
            "Failed to parse 'properties' from "
            "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":[]}: "
            "[json.exception.type_error.302] type must be object, but is array");

  std::string json_empty = R"({})";
  auto result3 = CreateNamespaceResponseFromJson(nlohmann::json::parse(json_empty));
  EXPECT_FALSE(result3.has_value());
  EXPECT_THAT(result3, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result3.error().message, "Missing 'namespace' in {}");
}

struct GetNamespaceResponseParam {
  std::string test_name;
  std::string expected_json_str;
  Namespace namespace_;
  std::unordered_map<std::string, std::string> properties;
};

class GetNamespaceResponseTest
    : public ::testing::TestWithParam<GetNamespaceResponseParam> {
 protected:
  void TestRoundTrip() {
    const auto& param = GetParam();

    GetNamespaceResponse original;
    original.namespace_ = param.namespace_;
    original.properties = param.properties;

    auto json = ToJson(original);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    EXPECT_EQ(json, expected_json);

    auto result = GetNamespaceResponseFromJson(expected_json);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto& parsed = result.value();

    EXPECT_EQ(parsed, original);
  }
};

TEST_P(GetNamespaceResponseTest, RoundTrip) { TestRoundTrip(); }

INSTANTIATE_TEST_SUITE_P(
    GetNamespaceResponseCases, GetNamespaceResponseTest,
    ::testing::Values(
        GetNamespaceResponseParam{
            .test_name = "FullResponse",
            .expected_json_str =
                R"({"namespace":["accounting","tax"],"properties":{"owner":"Hank"}})",
            .namespace_ = Namespace{{"accounting", "tax"}},
            .properties = {{"owner", "Hank"}}},
        GetNamespaceResponseParam{
            .test_name = "EmptyProperties",
            .expected_json_str = R"({"namespace":["accounting","tax"]})",
            .namespace_ = Namespace{{"accounting", "tax"}},
            .properties = {}}),
    [](const ::testing::TestParamInfo<GetNamespaceResponseParam>& info) {
      return info.param.test_name;
    });

TEST(GetNamespaceResponseTest, DeserializeWithoutDefaults) {
  std::string json_null_props = R"({"namespace":["accounting","tax"],"properties":null})";
  auto result = GetNamespaceResponseFromJson(nlohmann::json::parse(json_null_props));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result.value().properties.empty());
}

TEST(GetNamespaceResponseTest, InvalidResponses) {
  std::string json_wrong_ns_type =
      R"({"namespace":"accounting%1Ftax","properties":null})";
  auto result1 = GetNamespaceResponseFromJson(nlohmann::json::parse(json_wrong_ns_type));
  EXPECT_FALSE(result1.has_value());
  EXPECT_THAT(result1, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result1.error().message,
            "Failed to parse 'namespace' from "
            "{\"namespace\":\"accounting%1Ftax\",\"properties\":null}: "
            "[json.exception.type_error.302] type must be array, but is string");

  std::string json_wrong_props_type =
      R"({"namespace":["accounting","tax"],"properties":[]})";
  auto result2 =
      GetNamespaceResponseFromJson(nlohmann::json::parse(json_wrong_props_type));
  EXPECT_FALSE(result2.has_value());
  EXPECT_THAT(result2, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result2.error().message,
            "Failed to parse 'properties' from "
            "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":[]}: "
            "[json.exception.type_error.302] type must be object, but is array");

  std::string json_empty = R"({})";
  auto result3 = GetNamespaceResponseFromJson(nlohmann::json::parse(json_empty));
  EXPECT_FALSE(result3.has_value());
  EXPECT_THAT(result3, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result3.error().message, "Missing 'namespace' in {}");
}

struct ListNamespacesResponseParam {
  std::string test_name;
  std::string expected_json_str;
  std::vector<Namespace> namespaces;
  std::string next_page_token;
};

class ListNamespacesResponseTest
    : public ::testing::TestWithParam<ListNamespacesResponseParam> {
 protected:
  void TestRoundTrip() {
    const auto& param = GetParam();

    ListNamespacesResponse original;
    original.namespaces = param.namespaces;
    original.next_page_token = param.next_page_token;

    auto json = ToJson(original);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    EXPECT_EQ(json, expected_json);

    auto result = ListNamespacesResponseFromJson(expected_json);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto& parsed = result.value();

    EXPECT_EQ(parsed, original);
  }
};

TEST_P(ListNamespacesResponseTest, RoundTrip) { TestRoundTrip(); }

INSTANTIATE_TEST_SUITE_P(
    ListNamespacesResponseCases, ListNamespacesResponseTest,
    ::testing::Values(
        ListNamespacesResponseParam{
            .test_name = "FullResponse",
            .expected_json_str = R"({"namespaces":[["accounting"],["tax"]]})",
            .namespaces = {Namespace{{"accounting"}}, Namespace{{"tax"}}},
            .next_page_token = ""},
        ListNamespacesResponseParam{.test_name = "EmptyNamespaces",
                                    .expected_json_str = R"({"namespaces":[]})",
                                    .namespaces = {},
                                    .next_page_token = ""},
        ListNamespacesResponseParam{
            .test_name = "WithPageToken",
            .expected_json_str =
                R"({"namespaces":[["accounting"],["tax"]],"next-page-token":"token"})",
            .namespaces = {Namespace{{"accounting"}}, Namespace{{"tax"}}},
            .next_page_token = "token"}),
    [](const ::testing::TestParamInfo<ListNamespacesResponseParam>& info) {
      return info.param.test_name;
    });

TEST(ListNamespacesResponseTest, InvalidResponses) {
  std::string json_wrong_type = R"({"namespaces":"accounting"})";
  auto result1 = ListNamespacesResponseFromJson(nlohmann::json::parse(json_wrong_type));
  EXPECT_FALSE(result1.has_value());
  EXPECT_THAT(result1, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result1.error().message,
            "Cannot parse namespace from non-array:\"accounting\"");

  std::string json_empty = R"({})";
  auto result2 = ListNamespacesResponseFromJson(nlohmann::json::parse(json_empty));
  EXPECT_FALSE(result2.has_value());
  EXPECT_THAT(result2, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result2.error().message, "Missing 'namespaces' in {}");
}

struct UpdateNamespacePropertiesRequestParam {
  std::string test_name;
  std::string expected_json_str;
  std::vector<std::string> removals;
  std::unordered_map<std::string, std::string> updates;
};

class UpdateNamespacePropertiesRequestTest
    : public ::testing::TestWithParam<UpdateNamespacePropertiesRequestParam> {
 protected:
  void TestRoundTrip() {
    const auto& param = GetParam();

    UpdateNamespacePropertiesRequest original;
    original.removals = param.removals;
    original.updates = param.updates;

    auto json = ToJson(original);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    EXPECT_EQ(json, expected_json);

    auto result = UpdateNamespacePropertiesRequestFromJson(expected_json);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto& parsed = result.value();

    EXPECT_EQ(parsed, original);
  }
};

TEST_P(UpdateNamespacePropertiesRequestTest, RoundTrip) { TestRoundTrip(); }

INSTANTIATE_TEST_SUITE_P(
    UpdateNamespacePropertiesRequestCases, UpdateNamespacePropertiesRequestTest,
    ::testing::Values(
        UpdateNamespacePropertiesRequestParam{
            .test_name = "FullRequest",
            .expected_json_str =
                R"({"removals":["foo","bar"],"updates":{"owner":"Hank"}})",
            .removals = {"foo", "bar"},
            .updates = {{"owner", "Hank"}}},
        UpdateNamespacePropertiesRequestParam{
            .test_name = "OnlyUpdates",
            .expected_json_str = R"({"updates":{"owner":"Hank"}})",
            .removals = {},
            .updates = {{"owner", "Hank"}}},
        UpdateNamespacePropertiesRequestParam{
            .test_name = "OnlyRemovals",
            .expected_json_str = R"({"removals":["foo","bar"]})",
            .removals = {"foo", "bar"},
            .updates = {}},
        UpdateNamespacePropertiesRequestParam{.test_name = "AllEmpty",
                                              .expected_json_str = R"({})",
                                              .removals = {},
                                              .updates = {}}),
    [](const ::testing::TestParamInfo<UpdateNamespacePropertiesRequestParam>& info) {
      return info.param.test_name;
    });

TEST(UpdateNamespacePropertiesRequestTest, DeserializeWithoutDefaults) {
  // Removals is null
  std::string json1 = R"({"removals":null,"updates":{"owner":"Hank"}})";
  auto result1 = UpdateNamespacePropertiesRequestFromJson(nlohmann::json::parse(json1));
  ASSERT_TRUE(result1.has_value());
  EXPECT_TRUE(result1.value().removals.empty());

  // Removals is missing
  std::string json2 = R"({"updates":{"owner":"Hank"}})";
  auto result2 = UpdateNamespacePropertiesRequestFromJson(nlohmann::json::parse(json2));
  ASSERT_TRUE(result2.has_value());
  EXPECT_TRUE(result2.value().removals.empty());

  // Updates is null
  std::string json3 = R"({"removals":["foo","bar"],"updates":null})";
  auto result3 = UpdateNamespacePropertiesRequestFromJson(nlohmann::json::parse(json3));
  ASSERT_TRUE(result3.has_value());
  EXPECT_TRUE(result3.value().updates.empty());

  // All missing
  std::string json4 = R"({})";
  auto result4 = UpdateNamespacePropertiesRequestFromJson(nlohmann::json::parse(json4));
  ASSERT_TRUE(result4.has_value());
  EXPECT_TRUE(result4.value().removals.empty());
  EXPECT_TRUE(result4.value().updates.empty());
}

TEST(UpdateNamespacePropertiesRequestTest, InvalidRequests) {
  std::string json_wrong_removals_type =
      R"({"removals":{"foo":"bar"},"updates":{"owner":"Hank"}})";
  auto result1 = UpdateNamespacePropertiesRequestFromJson(
      nlohmann::json::parse(json_wrong_removals_type));
  EXPECT_FALSE(result1.has_value());
  EXPECT_THAT(result1, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result1.error().message,
            "Failed to parse 'removals' from "
            "{\"removals\":{\"foo\":\"bar\"},\"updates\":{\"owner\":\"Hank\"}}: "
            "[json.exception.type_error.302] type must be array, but is object");

  std::string json_wrong_updates_type =
      R"({"removals":["foo","bar"],"updates":["owner"]})";
  auto result2 = UpdateNamespacePropertiesRequestFromJson(
      nlohmann::json::parse(json_wrong_updates_type));
  EXPECT_FALSE(result2.has_value());
  EXPECT_THAT(result2, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result2.error().message,
            "Failed to parse 'updates' from "
            "{\"removals\":[\"foo\",\"bar\"],\"updates\":[\"owner\"]}: "
            "[json.exception.type_error.302] type must be object, but is array");
}

struct UpdateNamespacePropertiesResponseParam {
  std::string test_name;
  std::string expected_json_str;
  std::vector<std::string> updated;
  std::vector<std::string> removed;
  std::vector<std::string> missing;
};

class UpdateNamespacePropertiesResponseTest
    : public ::testing::TestWithParam<UpdateNamespacePropertiesResponseParam> {
 protected:
  void TestRoundTrip() {
    const auto& param = GetParam();

    UpdateNamespacePropertiesResponse original;
    original.updated = param.updated;
    original.removed = param.removed;
    original.missing = param.missing;

    auto json = ToJson(original);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    EXPECT_EQ(json, expected_json);

    auto result = UpdateNamespacePropertiesResponseFromJson(expected_json);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto& parsed = result.value();

    EXPECT_EQ(parsed, original);
  }
};

TEST_P(UpdateNamespacePropertiesResponseTest, RoundTrip) { TestRoundTrip(); }

INSTANTIATE_TEST_SUITE_P(
    UpdateNamespacePropertiesResponseCases, UpdateNamespacePropertiesResponseTest,
    ::testing::Values(
        UpdateNamespacePropertiesResponseParam{
            .test_name = "FullResponse",
            .expected_json_str =
                R"({"removed":["foo"],"updated":["owner"],"missing":["bar"]})",
            .updated = {"owner"},
            .removed = {"foo"},
            .missing = {"bar"}},
        UpdateNamespacePropertiesResponseParam{
            .test_name = "OnlyUpdated",
            .expected_json_str = R"({"removed":[],"updated":["owner"]})",
            .updated = {"owner"},
            .removed = {},
            .missing = {}},
        UpdateNamespacePropertiesResponseParam{
            .test_name = "OnlyRemoved",
            .expected_json_str = R"({"removed":["foo"],"updated":[]})",
            .updated = {},
            .removed = {"foo"},
            .missing = {}},
        UpdateNamespacePropertiesResponseParam{
            .test_name = "OnlyMissing",
            .expected_json_str = R"({"removed":[],"updated":[],"missing":["bar"]})",
            .updated = {},
            .removed = {},
            .missing = {"bar"}},
        UpdateNamespacePropertiesResponseParam{
            .test_name = "AllEmpty",
            .expected_json_str = R"({"removed":[],"updated":[]})",
            .updated = {},
            .removed = {},
            .missing = {}}),
    [](const ::testing::TestParamInfo<UpdateNamespacePropertiesResponseParam>& info) {
      return info.param.test_name;
    });

TEST(UpdateNamespacePropertiesResponseTest, DeserializeWithoutDefaults) {
  // Only updated, others missing
  std::string json2 = R"({"updated":["owner"],"removed":[]})";
  auto result2 = UpdateNamespacePropertiesResponseFromJson(nlohmann::json::parse(json2));
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(result2.value().updated, std::vector<std::string>({"owner"}));
  EXPECT_TRUE(result2.value().removed.empty());
  EXPECT_TRUE(result2.value().missing.empty());

  // All missing
  std::string json3 = R"({})";
  auto result3 = UpdateNamespacePropertiesResponseFromJson(nlohmann::json::parse(json3));
  EXPECT_FALSE(result3.has_value());  // updated and removed are required
}

TEST(UpdateNamespacePropertiesResponseTest, InvalidResponses) {
  std::string json_wrong_removed_type =
      R"({"removed":{"foo":true},"updated":["owner"],"missing":["bar"]})";
  auto result1 = UpdateNamespacePropertiesResponseFromJson(
      nlohmann::json::parse(json_wrong_removed_type));
  EXPECT_FALSE(result1.has_value());
  EXPECT_THAT(result1, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result1.error().message,
            "Failed to parse 'removed' from "
            "{\"missing\":[\"bar\"],\"removed\":{\"foo\":true},\"updated\":[\"owner\"]}: "
            "[json.exception.type_error.302] type must be array, but is object");

  std::string json_wrong_updated_type = R"({"updated":"owner","missing":["bar"]})";
  auto result2 = UpdateNamespacePropertiesResponseFromJson(
      nlohmann::json::parse(json_wrong_updated_type));
  EXPECT_FALSE(result2.has_value());
  EXPECT_THAT(result2, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(
      result2.error().message,
      "Failed to parse 'updated' from {\"missing\":[\"bar\"],\"updated\":\"owner\"}: "
      "[json.exception.type_error.302] type must be array, but is string");
}

struct ListTablesResponseParam {
  std::string test_name;
  std::string expected_json_str;
  std::vector<TableIdentifier> identifiers;
  std::string next_page_token;
};

class ListTablesResponseTest : public ::testing::TestWithParam<ListTablesResponseParam> {
 protected:
  void TestRoundTrip() {
    const auto& param = GetParam();

    ListTablesResponse original;
    original.identifiers = param.identifiers;
    original.next_page_token = param.next_page_token;

    auto json = ToJson(original);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    EXPECT_EQ(json, expected_json);

    auto result = ListTablesResponseFromJson(expected_json);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto& parsed = result.value();

    EXPECT_EQ(parsed, original);
  }
};

TEST_P(ListTablesResponseTest, RoundTrip) { TestRoundTrip(); }

INSTANTIATE_TEST_SUITE_P(
    ListTablesResponseCases, ListTablesResponseTest,
    ::testing::Values(
        ListTablesResponseParam{
            .test_name = "FullResponse",
            .expected_json_str =
                R"({"identifiers":[{"namespace":["accounting","tax"],"name":"paid"}]})",
            .identifiers = {TableIdentifier{Namespace{{"accounting", "tax"}}, "paid"}},
            .next_page_token = ""},
        ListTablesResponseParam{.test_name = "EmptyIdentifiers",
                                .expected_json_str = R"({"identifiers":[]})",
                                .identifiers = {},
                                .next_page_token = ""},
        ListTablesResponseParam{
            .test_name = "WithPageToken",
            .expected_json_str =
                R"({"identifiers":[{"namespace":["accounting","tax"],"name":"paid"}],"next-page-token":"token"})",
            .identifiers = {TableIdentifier{Namespace{{"accounting", "tax"}}, "paid"}},
            .next_page_token = "token"}),
    [](const ::testing::TestParamInfo<ListTablesResponseParam>& info) {
      return info.param.test_name;
    });

TEST(ListTablesResponseTest, InvalidResponses) {
  std::string json_wrong_type = R"({"identifiers":"accounting%1Ftax"})";
  auto result1 = ListTablesResponseFromJson(nlohmann::json::parse(json_wrong_type));
  EXPECT_FALSE(result1.has_value());
  EXPECT_THAT(result1, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result1.error().message, "Missing 'name' in \"accounting%1Ftax\"");

  std::string json_empty = R"({})";
  auto result2 = ListTablesResponseFromJson(nlohmann::json::parse(json_empty));
  EXPECT_FALSE(result2.has_value());
  EXPECT_THAT(result2, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result2.error().message, "Missing 'identifiers' in {}");

  std::string json_invalid_identifier =
      R"({"identifiers":[{"namespace":"accounting.tax","name":"paid"}]})";
  auto result3 =
      ListTablesResponseFromJson(nlohmann::json::parse(json_invalid_identifier));
  EXPECT_FALSE(result3.has_value());
  EXPECT_THAT(result3, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result3.error().message,
            "Failed to parse 'namespace' from "
            "{\"name\":\"paid\",\"namespace\":\"accounting.tax\"}: "
            "[json.exception.type_error.302] type must be array, but is string");
}

struct RenameTableRequestParam {
  std::string test_name;
  std::string expected_json_str;
  TableIdentifier source;
  TableIdentifier destination;
};

class RenameTableRequestTest : public ::testing::TestWithParam<RenameTableRequestParam> {
 protected:
  void TestRoundTrip() {
    const auto& param = GetParam();

    RenameTableRequest original;
    original.source = param.source;
    original.destination = param.destination;

    auto json = ToJson(original);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    EXPECT_EQ(json, expected_json);

    auto result = RenameTableRequestFromJson(expected_json);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto& parsed = result.value();

    EXPECT_EQ(parsed, original);
  }
};

TEST_P(RenameTableRequestTest, RoundTrip) { TestRoundTrip(); }

INSTANTIATE_TEST_SUITE_P(
    RenameTableRequestCases, RenameTableRequestTest,
    ::testing::Values(RenameTableRequestParam{
        .test_name = "FullRequest",
        .expected_json_str =
            R"({"source":{"namespace":["accounting","tax"],"name":"paid"},"destination":{"namespace":["accounting","tax"],"name":"paid_2022"}})",
        .source = TableIdentifier{Namespace{{"accounting", "tax"}}, "paid"},
        .destination = TableIdentifier{Namespace{{"accounting", "tax"}}, "paid_2022"}}),
    [](const ::testing::TestParamInfo<RenameTableRequestParam>& info) {
      return info.param.test_name;
    });

TEST(RenameTableRequestTest, InvalidRequests) {
  std::string json_source_null_name =
      R"({"source":{"namespace":["accounting","tax"],"name":null},"destination":{"namespace":["accounting","tax"],"name":"paid_2022"}})";
  auto result1 = RenameTableRequestFromJson(nlohmann::json::parse(json_source_null_name));
  EXPECT_FALSE(result1.has_value());
  EXPECT_THAT(result1, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result1.error().message,
            "Missing 'name' in {\"name\":null,\"namespace\":[\"accounting\",\"tax\"]}");

  std::string json_dest_null_name =
      R"({"source":{"namespace":["accounting","tax"],"name":"paid"},"destination":{"namespace":["accounting","tax"],"name":null}})";
  auto result2 = RenameTableRequestFromJson(nlohmann::json::parse(json_dest_null_name));
  EXPECT_FALSE(result2.has_value());
  EXPECT_THAT(result2, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result2.error().message,
            "Missing 'name' in {\"name\":null,\"namespace\":[\"accounting\",\"tax\"]}");

  std::string json_empty = R"({})";
  auto result3 = RenameTableRequestFromJson(nlohmann::json::parse(json_empty));
  EXPECT_FALSE(result3.has_value());
  EXPECT_THAT(result3, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result3.error().message, "Missing 'source' in {}");
}

struct RegisterTableRequestParam {
  std::string test_name;
  std::string expected_json_str;
  std::string name;
  std::string metadata_location;
  bool overwrite;
};

class RegisterTableRequestTest
    : public ::testing::TestWithParam<RegisterTableRequestParam> {
 protected:
  void TestRoundTrip() {
    const auto& param = GetParam();

    RegisterTableRequest original;
    original.name = param.name;
    original.metadata_location = param.metadata_location;
    original.overwrite = param.overwrite;

    auto json = ToJson(original);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    EXPECT_EQ(json, expected_json);

    auto result = RegisterTableRequestFromJson(expected_json);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto& parsed = result.value();

    EXPECT_EQ(parsed, original);
  }
};

TEST_P(RegisterTableRequestTest, RoundTrip) { TestRoundTrip(); }

INSTANTIATE_TEST_SUITE_P(
    RegisterTableRequestCases, RegisterTableRequestTest,
    ::testing::Values(
        RegisterTableRequestParam{
            .test_name = "WithOverwriteTrue",
            .expected_json_str =
                R"({"name":"table1","metadata-location":"s3://bucket/metadata.json","overwrite":true})",
            .name = "table1",
            .metadata_location = "s3://bucket/metadata.json",
            .overwrite = true},
        RegisterTableRequestParam{
            .test_name = "WithoutOverwrite",
            .expected_json_str =
                R"({"name":"table1","metadata-location":"s3://bucket/metadata.json"})",
            .name = "table1",
            .metadata_location = "s3://bucket/metadata.json",
            .overwrite = false}),
    [](const ::testing::TestParamInfo<RegisterTableRequestParam>& info) {
      return info.param.test_name;
    });

TEST(RegisterTableRequestTest, DeserializeWithoutDefaults) {
  // Overwrite missing (defaults to false)
  std::string json1 =
      R"({"name":"table1","metadata-location":"s3://bucket/metadata.json"})";
  auto result1 = RegisterTableRequestFromJson(nlohmann::json::parse(json1));
  ASSERT_TRUE(result1.has_value());
  EXPECT_FALSE(result1.value().overwrite);
}

TEST(RegisterTableRequestTest, InvalidRequests) {
  std::string json_missing_name = R"({"metadata-location":"s3://bucket/metadata.json"})";
  auto result1 = RegisterTableRequestFromJson(nlohmann::json::parse(json_missing_name));
  EXPECT_FALSE(result1.has_value());
  EXPECT_THAT(result1, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result1.error().message,
            "Missing 'name' in {\"metadata-location\":\"s3://bucket/metadata.json\"}");

  std::string json_missing_location = R"({"name":"table1"})";
  auto result2 =
      RegisterTableRequestFromJson(nlohmann::json::parse(json_missing_location));
  EXPECT_FALSE(result2.has_value());
  EXPECT_THAT(result2, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result2.error().message,
            "Missing 'metadata-location' in {\"name\":\"table1\"}");

  std::string json_empty = R"({})";
  auto result3 = RegisterTableRequestFromJson(nlohmann::json::parse(json_empty));
  EXPECT_FALSE(result3.has_value());
  EXPECT_THAT(result3, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result3.error().message, "Missing 'name' in {}");
}

struct CatalogConfigParam {
  std::string test_name;
  std::string expected_json_str;
  std::unordered_map<std::string, std::string> overrides;
  std::unordered_map<std::string, std::string> defaults;
  std::vector<std::string> endpoints;
};

class CatalogConfigTest : public ::testing::TestWithParam<CatalogConfigParam> {
 protected:
  void TestRoundTrip() {
    const auto& param = GetParam();

    CatalogConfig original;
    original.overrides = param.overrides;
    original.defaults = param.defaults;
    original.endpoints = param.endpoints;

    auto json = ToJson(original);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    EXPECT_EQ(json, expected_json);

    auto result = CatalogConfigFromJson(expected_json);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto& parsed = result.value();

    EXPECT_EQ(parsed, original);
  }
};

TEST_P(CatalogConfigTest, RoundTrip) { TestRoundTrip(); }

INSTANTIATE_TEST_SUITE_P(
    CatalogConfigCases, CatalogConfigTest,
    ::testing::Values(
        // Full config with both defaults and overrides
        CatalogConfigParam{
            .test_name = "FullConfig",
            .expected_json_str =
                R"({"defaults":{"warehouse":"s3://bucket/warehouse"},"overrides":{"clients":"5"}})",
            .overrides = {{"clients", "5"}},
            .defaults = {{"warehouse", "s3://bucket/warehouse"}},
            .endpoints = {}},
        // Only defaults
        CatalogConfigParam{
            .test_name = "OnlyDefaults",
            .expected_json_str =
                R"({"defaults":{"warehouse":"s3://bucket/warehouse"},"overrides":{}})",
            .overrides = {},
            .defaults = {{"warehouse", "s3://bucket/warehouse"}},
            .endpoints = {}},
        // Only overrides
        CatalogConfigParam{
            .test_name = "OnlyOverrides",
            .expected_json_str = R"({"defaults":{},"overrides":{"clients":"5"}})",
            .overrides = {{"clients", "5"}},
            .defaults = {},
            .endpoints = {}},
        // Both empty
        CatalogConfigParam{.test_name = "BothEmpty",
                           .expected_json_str = R"({"defaults":{},"overrides":{}})",
                           .overrides = {},
                           .defaults = {},
                           .endpoints = {}},
        // With endpoints
        CatalogConfigParam{
            .test_name = "WithEndpoints",
            .expected_json_str =
                R"({"defaults":{"warehouse":"s3://bucket/warehouse"},"overrides":{"clients":"5"},"endpoints":["GET /v1/config","POST /v1/tables"]})",
            .overrides = {{"clients", "5"}},
            .defaults = {{"warehouse", "s3://bucket/warehouse"}},
            .endpoints = {"GET /v1/config", "POST /v1/tables"}},
        // Only endpoints
        CatalogConfigParam{
            .test_name = "OnlyEndpoints",
            .expected_json_str =
                R"({"defaults":{},"overrides":{},"endpoints":["GET /v1/config"]})",
            .overrides = {},
            .defaults = {},
            .endpoints = {"GET /v1/config"}}),
    [](const ::testing::TestParamInfo<CatalogConfigParam>& info) {
      return info.param.test_name;
    });

TEST(CatalogConfigTest, DeserializeWithoutDefaults) {
  // Missing overrides field
  std::string json_missing_overrides =
      R"({"defaults":{"warehouse":"s3://bucket/warehouse"}})";
  auto result1 = CatalogConfigFromJson(nlohmann::json::parse(json_missing_overrides));
  ASSERT_TRUE(result1.has_value());
  std::unordered_map<std::string, std::string> expected_defaults = {
      {"warehouse", "s3://bucket/warehouse"}};
  EXPECT_EQ(result1.value().defaults, expected_defaults);
  EXPECT_TRUE(result1.value().overrides.empty());

  // Null overrides field
  std::string json_null_overrides =
      R"({"defaults":{"warehouse":"s3://bucket/warehouse"},"overrides":null})";
  auto result2 = CatalogConfigFromJson(nlohmann::json::parse(json_null_overrides));
  ASSERT_TRUE(result2.has_value());
  EXPECT_TRUE(result2.value().overrides.empty());

  // Missing defaults field
  std::string json_missing_defaults = R"({"overrides":{"clients":"5"}})";
  auto result3 = CatalogConfigFromJson(nlohmann::json::parse(json_missing_defaults));
  ASSERT_TRUE(result3.has_value());
  std::unordered_map<std::string, std::string> expected_overrides = {{"clients", "5"}};
  EXPECT_EQ(result3.value().overrides, expected_overrides);
  EXPECT_TRUE(result3.value().defaults.empty());

  // Null defaults field
  std::string json_null_defaults = R"({"defaults":null,"overrides":{"clients":"5"}})";
  auto result4 = CatalogConfigFromJson(nlohmann::json::parse(json_null_defaults));
  ASSERT_TRUE(result4.has_value());
  EXPECT_TRUE(result4.value().defaults.empty());

  // Empty JSON object
  std::string json_empty = R"({})";
  auto result5 = CatalogConfigFromJson(nlohmann::json::parse(json_empty));
  ASSERT_TRUE(result5.has_value());
  EXPECT_TRUE(result5.value().defaults.empty());
  EXPECT_TRUE(result5.value().overrides.empty());

  // Both fields null
  std::string json_both_null = R"({"defaults":null,"overrides":null})";
  auto result6 = CatalogConfigFromJson(nlohmann::json::parse(json_both_null));
  ASSERT_TRUE(result6.has_value());
  EXPECT_TRUE(result6.value().defaults.empty());
  EXPECT_TRUE(result6.value().overrides.empty());
}

TEST(CatalogConfigTest, InvalidConfig) {
  // Defaults has wrong type (array instead of object)
  std::string json_wrong_defaults_type =
      R"({"defaults":["warehouse","s3://bucket/warehouse"],"overrides":{"clients":"5"}})";
  auto result1 = CatalogConfigFromJson(nlohmann::json::parse(json_wrong_defaults_type));
  EXPECT_FALSE(result1.has_value());
  EXPECT_THAT(result1, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result1.error().message,
            "Failed to parse 'defaults' from "
            "{\"defaults\":[\"warehouse\",\"s3://bucket/"
            "warehouse\"],\"overrides\":{\"clients\":\"5\"}}: "
            "[json.exception.type_error.302] type must be object, but is array");

  // Overrides has wrong type (string instead of object)
  std::string json_wrong_overrides_type =
      R"({"defaults":{"warehouse":"s3://bucket/warehouse"},"overrides":"clients"})";
  auto result2 = CatalogConfigFromJson(nlohmann::json::parse(json_wrong_overrides_type));
  EXPECT_FALSE(result2.has_value());
  EXPECT_THAT(result2, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result2.error().message,
            "Failed to parse 'overrides' from "
            "{\"defaults\":{\"warehouse\":\"s3://bucket/"
            "warehouse\"},\"overrides\":\"clients\"}: "
            "[json.exception.type_error.302] type must be object, but is string");
}

struct ErrorResponseParam {
  std::string test_name;
  std::string expected_json_str;
  std::string message;
  std::string type;
  uint16_t code;
  std::vector<std::string> stack;
};

class ErrorResponseTest : public ::testing::TestWithParam<ErrorResponseParam> {
 protected:
  void TestRoundTrip() {
    const auto& param = GetParam();

    ErrorModel error_model;
    error_model.message = param.message;
    error_model.type = param.type;
    error_model.code = param.code;
    error_model.stack = param.stack;

    ErrorResponse original;
    original.error = error_model;

    auto json = ToJson(original);
    auto expected_json = nlohmann::json::parse(param.expected_json_str);
    EXPECT_EQ(json, expected_json);

    auto result = ErrorResponseFromJson(expected_json);
    ASSERT_TRUE(result.has_value()) << result.error().message;
    auto& parsed = result.value();

    EXPECT_EQ(parsed, original);
  }
};

TEST_P(ErrorResponseTest, RoundTrip) { TestRoundTrip(); }

INSTANTIATE_TEST_SUITE_P(
    ErrorResponseCases, ErrorResponseTest,
    ::testing::Values(
        // Error without stack trace
        ErrorResponseParam{
            .test_name = "WithoutStack",
            .expected_json_str =
                R"({"error":{"message":"The given namespace does not exist","type":"NoSuchNamespaceException","code":404}})",
            .message = "The given namespace does not exist",
            .type = "NoSuchNamespaceException",
            .code = 404,
            .stack = {}},
        // Error with stack trace
        ErrorResponseParam{
            .test_name = "WithStack",
            .expected_json_str =
                R"({"error":{"message":"The given namespace does not exist","type":"NoSuchNamespaceException","code":404,"stack":["a","b"]}})",
            .message = "The given namespace does not exist",
            .type = "NoSuchNamespaceException",
            .code = 404,
            .stack = {"a", "b"}},
        // Different error type
        ErrorResponseParam{
            .test_name = "DifferentError",
            .expected_json_str =
                R"({"error":{"message":"Internal server error","type":"InternalServerError","code":500,"stack":["line1","line2","line3"]}})",
            .message = "Internal server error",
            .type = "InternalServerError",
            .code = 500,
            .stack = {"line1", "line2", "line3"}}),
    [](const ::testing::TestParamInfo<ErrorResponseParam>& info) {
      return info.param.test_name;
    });

TEST(ErrorResponseTest, DeserializeWithExplicitNullStack) {
  std::string json_null_stack =
      R"({"error":{"message":"The given namespace does not exist","type":"NoSuchNamespaceException","code":404,"stack":null}})";
  auto result = ErrorResponseFromJson(nlohmann::json::parse(json_null_stack));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().error.message, "The given namespace does not exist");
  EXPECT_EQ(result.value().error.type, "NoSuchNamespaceException");
  EXPECT_EQ(result.value().error.code, 404);
  EXPECT_TRUE(result.value().error.stack.empty());
}

TEST(ErrorResponseTest, DeserializeWithMissingStack) {
  std::string json_missing_stack =
      R"({"error":{"message":"The given namespace does not exist","type":"NoSuchNamespaceException","code":404}})";
  auto result = ErrorResponseFromJson(nlohmann::json::parse(json_missing_stack));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().error.message, "The given namespace does not exist");
  EXPECT_EQ(result.value().error.type, "NoSuchNamespaceException");
  EXPECT_EQ(result.value().error.code, 404);
  EXPECT_TRUE(result.value().error.stack.empty());
}

TEST(ErrorResponseTest, InvalidErrorResponse) {
  // Missing error field
  std::string json_missing_error = R"({})";
  auto result1 = ErrorResponseFromJson(nlohmann::json::parse(json_missing_error));
  EXPECT_FALSE(result1.has_value());
  EXPECT_THAT(result1, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result1.error().message, "Missing 'error' in {}");

  // Null error field
  std::string json_null_error = R"({"error":null})";
  auto result2 = ErrorResponseFromJson(nlohmann::json::parse(json_null_error));
  EXPECT_FALSE(result2.has_value());
  EXPECT_THAT(result2, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result2.error().message, "Missing 'error' in {\"error\":null}");

  // Missing required type field
  std::string json_missing_type =
      R"({"message":"The given namespace does not exist","code":404})";
  auto result3 = ErrorModelFromJson(nlohmann::json::parse(json_missing_type));
  EXPECT_FALSE(result3.has_value());
  EXPECT_THAT(result3, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result3.error().message,
            "Missing 'type' in {\"code\":404,\"message\":\"The given namespace does not "
            "exist\"}");

  // Missing required code field
  std::string json_missing_code =
      R"({"message":"The given namespace does not exist","type":"NoSuchNamespaceException"})";
  auto result4 = ErrorModelFromJson(nlohmann::json::parse(json_missing_code));
  EXPECT_FALSE(result4.has_value());
  EXPECT_THAT(result4, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result4.error().message,
            "Missing 'code' in {\"message\":\"The given namespace does not "
            "exist\",\"type\":\"NoSuchNamespaceException\"}");

  // Wrong type for message field
  std::string json_wrong_message_type =
      R"({"message":123,"type":"NoSuchNamespaceException","code":404})";
  auto result5 = ErrorModelFromJson(nlohmann::json::parse(json_wrong_message_type));
  EXPECT_FALSE(result5.has_value());
  EXPECT_THAT(result5, IsError(ErrorKind::kJsonParseError));
  EXPECT_EQ(result5.error().message,
            "Failed to parse 'message' from "
            "{\"code\":404,\"message\":123,\"type\":\"NoSuchNamespaceException\"}: "
            "[json.exception.type_error.302] type must be string, but is number");
}

}  // namespace iceberg::rest
