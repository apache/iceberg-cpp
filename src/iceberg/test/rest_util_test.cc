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

#include <gtest/gtest.h>

#include "iceberg/table_identifier.h"

namespace iceberg::rest {

TEST(RestUtilTest, TrimTrailingSlash) {
  EXPECT_EQ(TrimTrailingSlash("https://foo"), "https://foo");
  EXPECT_EQ(TrimTrailingSlash("https://foo/"), "https://foo");
  EXPECT_EQ(TrimTrailingSlash("https://foo////"), "https://foo");
}

// Ported from Java TestRESTUtil.testRoundTripUrlEncodeDecodeNamespace
TEST(RestUtilTest, RoundTripUrlEncodeDecodeNamespace) {
  // {"dogs"}
  EXPECT_EQ(EncodeNamespaceForUrl(Namespace{.levels = {"dogs"}}), "dogs");
  EXPECT_EQ(DecodeNamespaceFromUrl("dogs").levels, std::vector<std::string>{"dogs"});

  // {"dogs.named.hank"}
  EXPECT_EQ(EncodeNamespaceForUrl(Namespace{.levels = {"dogs.named.hank"}}),
            "dogs.named.hank");
  EXPECT_EQ(DecodeNamespaceFromUrl("dogs.named.hank").levels,
            std::vector<std::string>{"dogs.named.hank"});

  // {"dogs/named/hank"}
  EXPECT_EQ(EncodeNamespaceForUrl(Namespace{.levels = {"dogs/named/hank"}}),
            "dogs%2Fnamed%2Fhank");
  EXPECT_EQ(DecodeNamespaceFromUrl("dogs%2Fnamed%2Fhank").levels,
            std::vector<std::string>{"dogs/named/hank"});

  // {"dogs", "named", "hank"}
  EXPECT_EQ(EncodeNamespaceForUrl(Namespace{.levels = {"dogs", "named", "hank"}}),
            "dogs%1Fnamed%1Fhank");
  EXPECT_EQ(DecodeNamespaceFromUrl("dogs%1Fnamed%1Fhank").levels,
            (std::vector<std::string>{"dogs", "named", "hank"}));

  // {"dogs.and.cats", "named", "hank.or.james-westfall"}
  EXPECT_EQ(EncodeNamespaceForUrl(Namespace{
                .levels = {"dogs.and.cats", "named", "hank.or.james-westfall"}}),
            "dogs.and.cats%1Fnamed%1Fhank.or.james-westfall");
  EXPECT_EQ(
      DecodeNamespaceFromUrl("dogs.and.cats%1Fnamed%1Fhank.or.james-westfall").levels,
      (std::vector<std::string>{"dogs.and.cats", "named", "hank.or.james-westfall"}));

  // empty namespace
  EXPECT_EQ(EncodeNamespaceForUrl(Namespace{.levels = {}}), "");
  EXPECT_EQ(DecodeNamespaceFromUrl("").levels, std::vector<std::string>{});
}

TEST(RestUtilTest, EncodeString) {
  // RFC 3986 unreserved characters should not be encoded
  EXPECT_EQ(EncodeString("abc123XYZ"), "abc123XYZ");
  EXPECT_EQ(EncodeString("test-file_name.txt~backup"), "test-file_name.txt~backup");

  // Spaces and special characters should be encoded
  EXPECT_EQ(EncodeString("hello world"), "hello%20world");
  EXPECT_EQ(EncodeString("test@example.com"), "test%40example.com");
  EXPECT_EQ(EncodeString("path/to/file"), "path%2Fto%2Ffile");
  EXPECT_EQ(EncodeString("key=value&foo=bar"), "key%3Dvalue%26foo%3Dbar");
  EXPECT_EQ(EncodeString("100%"), "100%25");

  // ASCII Unit Separator (0x1F) - important for Iceberg namespaces
  EXPECT_EQ(EncodeString("hello\x1Fworld"), "hello%1Fworld");
  EXPECT_EQ(EncodeString(""), "");
}

TEST(RestUtilTest, DecodeString) {
  // Decode percent-encoded strings
  EXPECT_EQ(DecodeString("hello%20world"), "hello world");
  EXPECT_EQ(DecodeString("test%40example.com"), "test@example.com");
  EXPECT_EQ(DecodeString("path%2Fto%2Ffile"), "path/to/file");
  EXPECT_EQ(DecodeString("key%3Dvalue%26foo%3Dbar"), "key=value&foo=bar");
  EXPECT_EQ(DecodeString("100%25"), "100%");

  // ASCII Unit Separator (0x1F)
  EXPECT_EQ(DecodeString("hello%1Fworld"), "hello\x1Fworld");

  // Unreserved characters remain unchanged
  EXPECT_EQ(DecodeString("test-file_name.txt~backup"), "test-file_name.txt~backup");
  EXPECT_EQ(DecodeString(""), "");
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
    std::string encoded = EncodeString(test);
    std::string decoded = DecodeString(encoded);
    EXPECT_EQ(decoded, test) << "Round-trip failed for: " << test;
  }
}

TEST(RestUtilTest, MergeConfigs) {
  std::unordered_map<std::string, std::string> server_defaults = {
      {"default1", "value1"}, {"default2", "value2"}, {"common", "default_value"}};

  std::unordered_map<std::string, std::string> client_configs = {
      {"client1", "value1"}, {"common", "client_value"}};

  std::unordered_map<std::string, std::string> server_overrides = {
      {"override1", "value1"}, {"common", "override_value"}};

  auto merged = MergeConfigs(server_defaults, client_configs, server_overrides);

  EXPECT_EQ(merged.size(), 5);

  // Check precedence: server_overrides > client_configs > server_defaults
  EXPECT_EQ(merged["default1"], "value1");
  EXPECT_EQ(merged["default2"], "value2");
  EXPECT_EQ(merged["client1"], "value1");
  EXPECT_EQ(merged["override1"], "value1");
  EXPECT_EQ(merged["common"], "override_value");

  // Test with empty maps
  auto merged_empty = MergeConfigs({}, {{"key", "value"}}, {});
  EXPECT_EQ(merged_empty.size(), 1);
  EXPECT_EQ(merged_empty["key"], "value");
}

}  // namespace iceberg::rest
