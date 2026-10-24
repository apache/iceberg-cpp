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

#include "iceberg/catalog/rest/rest_util.h"

#include <string>

#include <gtest/gtest.h>

#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/resource_paths.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

TEST(RestUtilTest, TrimTrailingSlash) {
  EXPECT_EQ(TrimTrailingSlash("https://foo"), "https://foo");
  EXPECT_EQ(TrimTrailingSlash("https://foo/"), "https://foo");
  EXPECT_EQ(TrimTrailingSlash("https://foo////"), "https://foo");
}

TEST(RestUtilTest, RoundTripUrlEncodeDecodeNamespace) {
  // {"dogs"}
  EXPECT_THAT(EncodeNamespace(Namespace{.levels = {"dogs"}}),
              HasValue(::testing::Eq("dogs")));
  EXPECT_THAT(DecodeNamespace("dogs"),
              HasValue(::testing::Eq(Namespace{.levels = {"dogs"}})));

  // {"dogs.named.hank"}
  EXPECT_THAT(EncodeNamespace(Namespace{.levels = {"dogs.named.hank"}}),
              HasValue(::testing::Eq("dogs.named.hank")));
  EXPECT_THAT(DecodeNamespace("dogs.named.hank"),
              HasValue(::testing::Eq(Namespace{.levels = {"dogs.named.hank"}})));

  // {"dogs/named/hank"}
  EXPECT_THAT(EncodeNamespace(Namespace{.levels = {"dogs/named/hank"}}),
              HasValue(::testing::Eq("dogs%2Fnamed%2Fhank")));
  EXPECT_THAT(DecodeNamespace("dogs%2Fnamed%2Fhank"),
              HasValue(::testing::Eq(Namespace{.levels = {"dogs/named/hank"}})));

  // {"dogs", "named", "hank"}
  EXPECT_THAT(EncodeNamespace(Namespace{.levels = {"dogs", "named", "hank"}}),
              HasValue(::testing::Eq("dogs%1Fnamed%1Fhank")));
  EXPECT_THAT(DecodeNamespace("dogs%1Fnamed%1Fhank"),
              HasValue(::testing::Eq(Namespace{.levels = {"dogs", "named", "hank"}})));

  // {"dogs.and.cats", "named", "hank.or.james-westfall"}
  EXPECT_THAT(EncodeNamespace(Namespace{
                  .levels = {"dogs.and.cats", "named", "hank.or.james-westfall"}}),
              HasValue(::testing::Eq("dogs.and.cats%1Fnamed%1Fhank.or.james-westfall")));
  EXPECT_THAT(DecodeNamespace("dogs.and.cats%1Fnamed%1Fhank.or.james-westfall"),
              HasValue(::testing::Eq(Namespace{
                  .levels = {"dogs.and.cats", "named", "hank.or.james-westfall"}})));

  // empty namespace
  EXPECT_THAT(EncodeNamespace(Namespace{.levels = {}}), HasValue(::testing::Eq("")));
  EXPECT_THAT(DecodeNamespace(""), HasValue(::testing::Eq(Namespace{.levels = {}})));
}

TEST(RestUtilTest, RoundTripNamespaceWithCustomSeparator) {
  Namespace ns{.levels = {"dogs.and.cats", "named", "hank.or.james-westfall"}};

  EXPECT_THAT(EncodeNamespace(ns, "%2E"),
              HasValue(::testing::Eq("dogs.and.cats%2Enamed%2Ehank.or.james-westfall")));
  EXPECT_THAT(DecodeNamespace("dogs.and.cats%2Enamed%2Ehank.or.james-westfall", "%2E"),
              HasValue(::testing::Eq(ns)));
}

TEST(RestUtilTest, NamespaceRejectsEmptySeparator) {
  auto expect_empty_separator_error = [](const auto& result) {
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(result, HasErrorMessage("REST namespace separator cannot be empty"));
  };

  expect_empty_separator_error(EncodeNamespace(Namespace{.levels = {"dogs"}}, ""));
  expect_empty_separator_error(EncodeNamespace(Namespace{.levels = {}}, ""));
  expect_empty_separator_error(DecodeNamespace("dogs", ""));
  expect_empty_separator_error(DecodeNamespace("", ""));
}

TEST(RestUtilTest, ResourcePathsRejectsEmptyNamespaceSeparator) {
  auto result = ResourcePaths::Make("https://catalog.example.com", "", "");

  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("REST namespace separator cannot be empty"));
}

TEST(RestUtilTest, EncodeString) {
  // RFC 3986 unreserved characters should not be encoded
  EXPECT_THAT(EncodeString("abc123XYZ"), HasValue(::testing::Eq("abc123XYZ")));
  EXPECT_THAT(EncodeString("test-file_name.txt~backup"),
              HasValue(::testing::Eq("test-file_name.txt~backup")));

  // Spaces and special characters should be encoded
  EXPECT_THAT(EncodeString("hello world"), HasValue(::testing::Eq("hello%20world")));
  EXPECT_THAT(EncodeString("test@example.com"),
              HasValue(::testing::Eq("test%40example.com")));
  EXPECT_THAT(EncodeString("path/to/file"), HasValue(::testing::Eq("path%2Fto%2Ffile")));
  EXPECT_THAT(EncodeString("key=value&foo=bar"),
              HasValue(::testing::Eq("key%3Dvalue%26foo%3Dbar")));
  EXPECT_THAT(EncodeString("100%"), HasValue(::testing::Eq("100%25")));
  EXPECT_THAT(EncodeString("hello\x1Fworld"), HasValue(::testing::Eq("hello%1Fworld")));
  EXPECT_THAT(EncodeString(""), HasValue(::testing::Eq("")));
}

TEST(RestUtilTest, DecodeString) {
  // Decode percent-encoded strings
  EXPECT_THAT(DecodeString("hello%20world"), HasValue(::testing::Eq("hello world")));
  EXPECT_THAT(DecodeString("test%40example.com"),
              HasValue(::testing::Eq("test@example.com")));
  EXPECT_THAT(DecodeString("path%2Fto%2Ffile"), HasValue(::testing::Eq("path/to/file")));
  EXPECT_THAT(DecodeString("key%3Dvalue%26foo%3Dbar"),
              HasValue(::testing::Eq("key=value&foo=bar")));
  EXPECT_THAT(DecodeString("100%25"), HasValue(::testing::Eq("100%")));

  // ASCII Unit Separator (0x1F)
  EXPECT_THAT(DecodeString("hello%1Fworld"), HasValue(::testing::Eq("hello\x1Fworld")));

  // Unreserved characters remain unchanged
  EXPECT_THAT(DecodeString("test-file_name.txt~backup"),
              HasValue(::testing::Eq("test-file_name.txt~backup")));
  EXPECT_THAT(DecodeString(""), HasValue(::testing::Eq("")));
}

TEST(RestUtilTest, EncodeDecodeStringRoundTrip) {
  std::vector<std::string> test_cases = {"hello world",
                                         "test@example.com",
                                         "path/to/file",
                                         "key=value&foo=bar",
                                         "100%",
                                         "hello\x1Fworld",
                                         "special!@#$%^&*()chars",
                                         "mixed-123_test.file~ok",
                                         ""};

  for (const auto& test : test_cases) {
    ICEBERG_UNWRAP_OR_FAIL(std::string encoded, EncodeString(test));
    ICEBERG_UNWRAP_OR_FAIL(std::string decoded, DecodeString(encoded));
    EXPECT_EQ(decoded, test) << "Round-trip failed for: " << test;
  }
}

TEST(RestUtilTest, MergeConfigs) {
  std::unordered_map<std::string, std::string> server_defaults = {
      {"default1", "value1"}, {"default2", "value2"}, {"common", "default_value"}};

  std::unordered_map<std::string, std::string> client_configs = {
      {"client1", "value1"}, {"common", "client_value"}, {"extra", "client_value"}};

  std::unordered_map<std::string, std::string> server_overrides = {
      {"override1", "value1"}, {"common", "override_value"}};

  auto merged = MergeConfigs(server_defaults, client_configs, server_overrides);

  EXPECT_EQ(merged.size(), 6);

  // Check precedence: server_overrides > client_configs > server_defaults
  EXPECT_EQ(merged["default1"], "value1");
  EXPECT_EQ(merged["default2"], "value2");
  EXPECT_EQ(merged["client1"], "value1");
  EXPECT_EQ(merged["override1"], "value1");
  EXPECT_EQ(merged["common"], "override_value");
  EXPECT_EQ(merged["extra"], "client_value");

  // Test with empty maps
  auto merged_empty = MergeConfigs({}, {{"key", "value"}}, {});
  EXPECT_EQ(merged_empty.size(), 1);
  EXPECT_EQ(merged_empty["key"], "value");
}

struct SnapshotModeValidCase {
  std::string input;
  SnapshotMode expected;
};

class SnapshotLoadingModeValidTest
    : public ::testing::TestWithParam<SnapshotModeValidCase> {};

TEST_P(SnapshotLoadingModeValidTest, ParsesCorrectly) {
  auto config = RestCatalogProperties::default_properties();
  config.Set(RestCatalogProperties::kSnapshotLoadingMode, GetParam().input);
  auto result = config.SnapshotLoadingMode();
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value(), GetParam().expected);
}

INSTANTIATE_TEST_SUITE_P(RestCatalogProperties, SnapshotLoadingModeValidTest,
                         ::testing::Values(
                             // Exact uppercase
                             SnapshotModeValidCase{"ALL", SnapshotMode::kAll},
                             SnapshotModeValidCase{"REFS", SnapshotMode::kRefs},
                             // Lowercase (Java parity: toUpperCase before parsing)
                             SnapshotModeValidCase{"all", SnapshotMode::kAll},
                             SnapshotModeValidCase{"refs", SnapshotMode::kRefs},
                             // Mixed case
                             SnapshotModeValidCase{"All", SnapshotMode::kAll},
                             SnapshotModeValidCase{"Refs", SnapshotMode::kRefs}));

class SnapshotLoadingModeInvalidTest : public ::testing::TestWithParam<std::string> {};

TEST_P(SnapshotLoadingModeInvalidTest, ReturnsError) {
  auto config = RestCatalogProperties::default_properties();
  config.Set(RestCatalogProperties::kSnapshotLoadingMode, GetParam());
  EXPECT_THAT(config.SnapshotLoadingMode(), IsError(ErrorKind::kInvalidArgument));
}

INSTANTIATE_TEST_SUITE_P(RestCatalogProperties, SnapshotLoadingModeInvalidTest,
                         ::testing::Values("INVALID", "none", ""));

TEST(RestCatalogPropertiesTest, SnapshotLoadingModeDefaultIsAll) {
  auto config = RestCatalogProperties::default_properties();
  auto result = config.SnapshotLoadingMode();
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value(), SnapshotMode::kAll);
}

TEST(RestUtilTest, ParseClientOnlyProperties) {
  // Empty string
  auto result = ParseClientOnlyProperties("");
  EXPECT_TRUE(result.empty());

  // Single key
  result = ParseClientOnlyProperties("oauth2-server-uri");
  EXPECT_EQ(result.size(), 1);
  EXPECT_TRUE(result.contains("oauth2-server-uri"));

  // Multiple keys with spaces
  result = ParseClientOnlyProperties(" a , b , c ");
  EXPECT_EQ(result.size(), 3);
  EXPECT_TRUE(result.contains("a"));
  EXPECT_TRUE(result.contains("b"));
  EXPECT_TRUE(result.contains("c"));

  // Keys with leading/trailing spaces and empty entries
  result = ParseClientOnlyProperties(" a , b ,, a ");
  EXPECT_EQ(result.size(), 2);  // "a" deduplicated
  EXPECT_TRUE(result.contains("a"));
  EXPECT_TRUE(result.contains("b"));

  // Only commas
  result = ParseClientOnlyProperties(",,,");
  EXPECT_TRUE(result.empty());

  // Only spaces
  result = ParseClientOnlyProperties("   ");
  EXPECT_TRUE(result.empty());
}

TEST(RestUtilTest, MergeServerConfigWithoutClientOnlyProperties) {
  std::unordered_map<std::string, std::string> server_defaults = {
      {"default1", "value1"},
      {"oauth2-server-uri", "https://server-default.example/token"}};
  std::unordered_map<std::string, std::string> server_overrides = {
      {"override1", "value1"},
      {"oauth2-server-uri", "https://server-override.example/token"}};
  std::unordered_map<std::string, std::string> client_configs = {
      {"client1", "value1"}, {"uri", "https://catalog.example"}};

  auto result = MergeServerConfig(server_defaults, server_overrides, client_configs);
  ASSERT_THAT(result, IsOk());

  auto merged = result.value();
  EXPECT_EQ(merged["default1"], "value1");
  EXPECT_EQ(merged["override1"], "value1");
  EXPECT_EQ(merged["client1"], "value1");
  // Server override wins when rest.client-only-properties is not set
  EXPECT_EQ(merged["oauth2-server-uri"], "https://server-override.example/token");
}

TEST(RestUtilTest, MergeServerConfigWithClientOnlyPropertiesProtectsOAuth2ServerUri) {
  std::unordered_map<std::string, std::string> server_defaults = {{"default1", "value1"}};
  std::unordered_map<std::string, std::string> server_overrides = {
      {"oauth2-server-uri", "https://malicious.example/token"}};
  std::unordered_map<std::string, std::string> client_configs = {
      {"uri", "https://catalog.example"},
      {"oauth2-server-uri", "https://idp.example/token"},
      {RestCatalogProperties::kClientOnlyProperties.key(), "oauth2-server-uri"}};

  auto result = MergeServerConfig(server_defaults, server_overrides, client_configs);
  ASSERT_THAT(result, IsOk());

  auto merged = result.value();
  EXPECT_EQ(merged["default1"], "value1");
  // Client value is preserved
  EXPECT_EQ(merged["oauth2-server-uri"], "https://idp.example/token");
}

TEST(RestUtilTest, MergeServerConfigResolvesRelativeOAuth2ServerUri) {
  std::unordered_map<std::string, std::string> server_defaults = {};
  std::unordered_map<std::string, std::string> server_overrides = {
      {"uri", "https://collector.example"},
      {"oauth2-server-uri", "https://malicious.example/token"}};
  std::unordered_map<std::string, std::string> client_configs = {
      {"uri", "https://catalog.example"},
      // oauth2-server-uri not set, defaults to relative path "v1/oauth/tokens"
      {RestCatalogProperties::kClientOnlyProperties.key(), "oauth2-server-uri"}};

  auto result = MergeServerConfig(server_defaults, server_overrides, client_configs);
  ASSERT_THAT(result, IsOk());

  auto merged = result.value();
  // oauth2-server-uri should be resolved against client uri before merging
  EXPECT_EQ(merged["oauth2-server-uri"], "https://catalog.example/v1/oauth/tokens");
  // uri can still be overridden by server (but token endpoint is already fixed)
  EXPECT_EQ(merged["uri"], "https://collector.example");
}

TEST(RestUtilTest, MergeServerConfigIgnoresServerProvidedClientOnlyProperties) {
  std::unordered_map<std::string, std::string> server_defaults = {
      {RestCatalogProperties::kClientOnlyProperties.key(), "ignored-key"}};
  std::unordered_map<std::string, std::string> server_overrides = {
      {RestCatalogProperties::kClientOnlyProperties.key(), "another-ignored-key"}};
  std::unordered_map<std::string, std::string> client_configs = {
      {"uri", "https://catalog.example"},
      {RestCatalogProperties::kClientOnlyProperties.key(), "oauth2-server-uri"}};

  auto result = MergeServerConfig(server_defaults, server_overrides, client_configs);
  ASSERT_THAT(result, IsOk());

  auto merged = result.value();
  // Client value is preserved, server values ignored
  EXPECT_EQ(merged[RestCatalogProperties::kClientOnlyProperties.key()],
            "oauth2-server-uri");
}

TEST(RestUtilTest, MergeServerConfigWithAbsoluteOAuth2ServerUri) {
  std::unordered_map<std::string, std::string> server_defaults = {};
  std::unordered_map<std::string, std::string> server_overrides = {
      {"uri", "https://collector.example"},
      {"oauth2-server-uri", "https://malicious.example/token"}};
  std::unordered_map<std::string, std::string> client_configs = {
      {"uri", "https://catalog.example"},
      {"oauth2-server-uri", "https://idp.example/token"},
      {RestCatalogProperties::kClientOnlyProperties.key(), "oauth2-server-uri"}};

  auto result = MergeServerConfig(server_defaults, server_overrides, client_configs);
  ASSERT_THAT(result, IsOk());

  auto merged = result.value();
  // Absolute client endpoint is preserved as-is
  EXPECT_EQ(merged["oauth2-server-uri"], "https://idp.example/token");
  // uri can still be overridden
  EXPECT_EQ(merged["uri"], "https://collector.example");
}

TEST(RestUtilTest, MergeServerConfigWithLeadingSlashOAuth2ServerUri) {
  std::unordered_map<std::string, std::string> server_defaults = {};
  std::unordered_map<std::string, std::string> server_overrides = {
      {"uri", "https://collector.example"}};
  std::unordered_map<std::string, std::string> client_configs = {
      {"uri", "https://catalog.example"},
      {"oauth2-server-uri", "/custom/oauth/tokens"},
      {RestCatalogProperties::kClientOnlyProperties.key(), "oauth2-server-uri"}};

  auto result = MergeServerConfig(server_defaults, server_overrides, client_configs);
  ASSERT_THAT(result, IsOk());

  auto merged = result.value();
  // Leading-slash path is resolved against client uri
  EXPECT_EQ(merged["oauth2-server-uri"], "https://catalog.example/custom/oauth/tokens");
  // uri can still be overridden
  EXPECT_EQ(merged["uri"], "https://collector.example");
}

}  // namespace iceberg::rest
